package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// 处理 reftnode 传来的 Ready 数据，对该应用的条目进行应用，该持久化的持久化，该提交的提交
	if d.RaftGroup.HasReady() {
		// 如果有未处理的消息
		ready := d.RaftGroup.Ready()
		// 先持久化状态，并获取快照 （处理 hardstate， softstate， entry，snapshot）
		applySnapshot, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			log.Panic(ErrResp(err))
		}
		// 当快照应用后，region 信息如果改变，则需要修改 peer 中的 regionrange 
		if applySnapshot != nil {
			if !reflect.DeepEqual(applySnapshot.PrevRegion, applySnapshot.Region) {
				d.peerStorage.SetRegion(applySnapshot.Region)
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regions[applySnapshot.Region.Id] = applySnapshot.Region
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapshot.PrevRegion})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapshot.Region})
				d.ctx.storeMeta.Unlock()
			}
		}

		// 发送消息（处理 message）
		d.Send(d.ctx.trans, ready.Messages)
		// 处理需要 apply 的消息 （处理 commitedEntry）
		for _, en := range ready.CommittedEntries {
			d.peerStorage.applyState.AppliedIndex = en.Index
			kvWB := new(engine_util.WriteBatch)
			if en.EntryType == eraftpb.EntryType_EntryConfChange {
				// RegionLocalState，包括 RegionEpoch (d.peer.peerStorage.region.RegionEpoch) 和 Region (d.peer.peerStorage.region.Peers) 中的 Peers
				// addNode 和 deleteNode
				d.processConfChange(&en, kvWB)
			} else {
				// 转入 Normal 请求处理函数
				req := new(raft_cmdpb.RaftCmdRequest)
				err := req.Unmarshal(en.Data)
				if err != nil {
					log.Panic(ErrResp(err))
				}
				d.processNormalRequest(req, &en, kvWB)
			}
			if d.stopped {
				return
			}		
			// 向 kv 数据库中写入
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)	
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		}
		d.RaftGroup.Advance(ready)
	}
}

// 检查节点是否存在
func (d *peerMsgHandler) checkNodeExist(id uint64) (bool, int) {
	for i, pr := range d.peer.peerStorage.region.Peers {
		if pr.Id == id {
			// 节点已存在，添加节点失败
			// log.Infof("Node: %d 存在", id)
			return true, i
		}
	}

	return false, -1
}

// AddNode
func (d *peerMsgHandler) processAddNode() {
	
}

// 处理 peer 变更请求
func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	var cc eraftpb.ConfChange	
	req := new(raft_cmdpb.RaftCmdRequest)
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		log.Panic("ConfChange Unmarshal error")
	}
	err = req.Unmarshal(cc.Context)
	if err != nil {
		log.Panic("Admin req Unmarshal error")
	}
	header := req.Header
	// check regionEpoch
	if header.RegionEpoch != nil {
		req_epoch := header.RegionEpoch
		if req_epoch != nil && util.IsEpochStale(req_epoch, d.Region().RegionEpoch){
			resp := ErrResp(&util.ErrEpochNotMatch{})
			log.Infof("请求配置版本过期，忽略请求")
			d.processProposal(resp, entry, false)
			return
		}
	}
	resp := d.createAdminResp(raft_cmdpb.AdminCmdType_ChangePeer)
	resp.AdminResponse.ChangePeer.Region = d.Region()
	peer := req.AdminRequest.ChangePeer.Peer
	changetype := cc.ChangeType
	peerId := peer.Id
	d_region := d.Region()
	if changetype == eraftpb.ConfChangeType_AddNode {
		d.RaftGroup.ApplyConfChange(cc)
		// AddNode
		f, _ := d.checkNodeExist(peerId)
		if f {
			log.Infof("Node: %d 已经存在", peerId)
		} else {
			// fmt.Printf("Node:%d 处理 changepeer 请求, type 是 AddNode, 处理节点是 %d\n", d.peer.Meta.Id, peer.Id)
			d.ctx.storeMeta.Lock()
			d_region.RegionEpoch.ConfVer ++
			newPeer := &metapb.Peer{
				Id: cc.NodeId,
				StoreId: peer.StoreId,
			}
			d.peer.insertPeerCache(newPeer)	
			d_region.Peers = append(d_region.Peers, newPeer)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.Unlock()
		}
		d.processProposal(resp, entry, false)
		d.notifyHeartbeatScheduler(d.Region(),d.peer)
	} else {
		d.RaftGroup.ApplyConfChange(cc)
		// RemoveNode
		// 在 Region 的 peer 列表中删除 peer
		f, idx := d.checkNodeExist(peerId)
		if !f {
			log.Infof("Node: %d 不存在", peerId)
			d.processProposal(resp, entry, false)
			return 
		}
		// fmt.Printf("Node:%d 处理 changepeer 请求, type 是 DeleteNode, 处理节点是 %d\n", d.peer.Meta.Id, peer.Id)
		// 如果此时正好是要被删除的节点，则销毁节点
		if d.PeerId() == peerId {
			kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
			d.destroyPeer()
			return 
		} else if f {
			// 配置版本递增
			d.ctx.storeMeta.Lock()
			d_region.RegionEpoch.ConfVer ++
			// 移除 peer
			d_region.Peers[idx] = d_region.Peers[len(d_region.Peers) - 1]
			d_region.Peers = d_region.Peers[:len(d_region.Peers) - 1]
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			d.peer.removePeerCache(peerId)
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.Unlock()
		}
		d.processProposal(resp, entry, false)
		d.notifyHeartbeatScheduler(d.Region(),d.peer)
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}


// 处理函数回调
func (d *peerMsgHandler) processProposal(resp *raft_cmdpb.RaftCmdResponse, entry *eraftpb.Entry, isSanpRequest bool) {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index < entry.Index {
			d.proposals = d.proposals[1:]
			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			continue
		} else if p.index > entry.Index {
			// 代表该条目已经被启用
			break
		} else {
			if p.term != entry.Term {
				// term	不匹配，代表被覆盖
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				if isSanpRequest {
					p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				}
				p.cb.Done(resp)
			}
			d.proposals = d.proposals[1:]
			break
		}
	}
}

// 处理 Put 请求
func (d *peerMsgHandler) processPutRequest(req *raft_cmdpb.Request, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	// 将请求写入 kvwb 中，后续一次性写入数据库中 MustWriteToDB
	kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	// println("put exec" + string(req.Put.Cf) + "_" + string(req.Put.Key) + "_" + string(req.Put.Value))
	resp := d.createNormalResp(raft_cmdpb.CmdType_Put)
	resp.Header.CurrentTerm = d.Term()
	// 处理 proposal 中的函数回调
	d.processProposal(resp, entry, false)
}

// 处理 Delete 请求
func (d *peerMsgHandler) processDelRequest(req *raft_cmdpb.Request, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	cf, key := req.Delete.Cf, req.Delete.Key
	kvWB.DeleteCF(cf, key)
	resp := d.createNormalResp(raft_cmdpb.CmdType_Delete)
	resp.Header.CurrentTerm = d.Term()
	d.processProposal(resp, entry, false)
}

// 处理 Get 请求
func (d *peerMsgHandler) processGetRequest(req *raft_cmdpb.Request, entry *eraftpb.Entry) {
	val, _ := engine_util.GetCF(d.ctx.engine.Kv, req.Get.Cf, req.Get.Key)
	resp := d.createNormalResp(raft_cmdpb.CmdType_Get)
	resp.Header.CurrentTerm = d.Term()
	resp.Responses[0].Get.Value = val
	d.processProposal(resp, entry, false)
}

// 处理 Snap 请求
func (d *peerMsgHandler) processSnapRequest(req *raft_cmdpb.Request, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	resp := d.createNormalResp(raft_cmdpb.CmdType_Snap)
	resp.Header.CurrentTerm = d.Term()
	resp.Responses[0].Snap.Region = d.Region()
	d.processProposal(resp, entry, true)
}

func (d *peerMsgHandler) processCompactLog(req *raft_cmdpb.AdminRequest, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	// SaveReadyState 中已经应用了 snapshot，因此这里直接用修改好的 truncatedIndex 创建一个压缩日志任务
	if req.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index, d.peerStorage.applyState.TruncatedState.Term = req.CompactLog.CompactIndex, req.CompactLog.CompactTerm
		kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		d.ScheduleCompactLog(req.CompactLog.CompactIndex)
	}
	resp := d.createAdminResp(raft_cmdpb.AdminCmdType_CompactLog)
	d.processProposal(resp, entry, false)
}

// 处理 normal request
func (d *peerMsgHandler) processNormalRequest(req *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	// 处理管理员普通请求（split, compact）
	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.processCompactLog(req.AdminRequest, entry, kvWB)
		case raft_cmdpb.AdminCmdType_Split:
		}
		// 管理员请求与普通请求不会在同一个条目中
		return 
	}
	// 对传入的 msg 分别进行判断
	for _, r := range req.Requests {
		switch r.CmdType {
		case raft_cmdpb.CmdType_Put:
			d.processPutRequest(r, entry, kvWB)
		case raft_cmdpb.CmdType_Delete:
			d.processDelRequest(r, entry, kvWB)
		case raft_cmdpb.CmdType_Get:
			d.processGetRequest(r, entry)
		case raft_cmdpb.CmdType_Snap:
			d.processSnapRequest(r, entry, kvWB)
		}
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		// 对应一个读 or 写 or raft 管理命令的一个信息，这个命令应该被提议到 raft 模块
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

// 构建普通请求响应
func (r *peerMsgHandler) createNormalResp(cmdType raft_cmdpb.CmdType) *raft_cmdpb.RaftCmdResponse {
	response := &raft_cmdpb.Response{}
	switch cmdType {
	case raft_cmdpb.CmdType_Get:
		response.CmdType = raft_cmdpb.CmdType_Get
		response.Get = &raft_cmdpb.GetResponse{}
	case raft_cmdpb.CmdType_Put:
		response.CmdType = raft_cmdpb.CmdType_Put
		response.Put = &raft_cmdpb.PutResponse{}
	case raft_cmdpb.CmdType_Delete:
		response.CmdType = raft_cmdpb.CmdType_Delete
		response.Delete = &raft_cmdpb.DeleteResponse{}
	case raft_cmdpb.CmdType_Snap:
		response.CmdType = raft_cmdpb.CmdType_Snap
		response.Snap = &raft_cmdpb.SnapResponse{}
	}
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: make([]*raft_cmdpb.Response, 0),
	}
	resp.Responses = append(resp.Responses, response)
	return resp
}

// 构造管理员响应
func (r *peerMsgHandler) createAdminResp(cmdType raft_cmdpb.AdminCmdType) *raft_cmdpb.RaftCmdResponse {
	adminResp := &raft_cmdpb.AdminResponse{}
	switch cmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		adminResp.CmdType = raft_cmdpb.AdminCmdType_ChangePeer
		adminResp.ChangePeer = &raft_cmdpb.ChangePeerResponse{}
	case raft_cmdpb.AdminCmdType_CompactLog:
		adminResp.CmdType = raft_cmdpb.AdminCmdType_CompactLog
		adminResp.CompactLog = &raft_cmdpb.CompactLogResponse{}
	case raft_cmdpb.AdminCmdType_Split:
		adminResp.CmdType = raft_cmdpb.AdminCmdType_Split
		adminResp.Split = &raft_cmdpb.SplitResponse{}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		adminResp.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
		adminResp.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: adminResp,
	}

	return cmdResp
}

// 检查提议是否合法
func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}
	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// 提议普通请求
func (d *peerMsgHandler) proposeNormalCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	for _, m := range msg.Requests {
		var key []byte
		switch m.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = m.Get.Key
		case raft_cmdpb.CmdType_Delete:
			key = m.Delete.Key
		case raft_cmdpb.CmdType_Put:
			key = m.Put.Key
		}

		if len(key) != 0 {
			err := util.CheckKeyInRegion(key, d.peerStorage.region)
			if err != nil {
				cb.Done(ErrResp(err))
				continue
			}
		}

		p := &proposal{index: d.peer.nextProposalIndex(), term: d.peer.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		data, err := msg.Marshal()
		if err != nil {
			log.Panic(err)
		}
		d.RaftGroup.Propose(data)
	}
}

// ChangePeer 提议
func (d *peerMsgHandler) proposeChangePeer(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	data, err := msg.Marshal()
	if err != nil {
		log.Panic("changepeer marshal err")
	}
	peerId := msg.AdminRequest.ChangePeer.Peer.Id
	changeType := msg.AdminRequest.ChangePeer.ChangeType
	p := &proposal{index: d.peer.nextProposalIndex(), term: d.peer.Term(), cb: cb}
	d.proposals = append(d.proposals, p)
	cc := eraftpb.ConfChange{
		ChangeType: changeType,
		NodeId:     peerId,
		Context: data,
	}
	d.RaftGroup.ProposeConfChange(cc)
}

// compactlog 提议
func (d *peerMsgHandler) proposeCompactLog(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// CompactLog 作为普通的 entry
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(ErrResp(err))
	}
	p := &proposal{index: d.peer.nextProposalIndex(), term: d.peer.Term(), cb: cb}
	d.proposals = append(d.proposals, p)
	d.RaftGroup.Propose(data)
}

// TransferLeader 提议
func (d *peerMsgHandler) proposeTransferLeader(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// rawnode.transferleader
	from_id := msg.AdminRequest.TransferLeader.Peer.Id

	d.peer.RaftGroup.TransferLeader(from_id)
	// 构造回应
	resp := d.createAdminResp(raft_cmdpb.AdminCmdType_TransferLeader)
	cb.Done(resp)
}

// Split 提议
func (d *peerMsgHandler) proposeSplit(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// split 作为普通的 entry
	// data, err := msg.Marshal()
	// if err != nil {
	// 	log.Panic(ErrResp(err))
	// }

	// p := &proposal{index: d.peer.nextProposalIndex(), term: d.peer.Term(), cb: cb}
	// d.proposals = append(d.proposals, p)
	// d.RaftGroup.Propose(data)
}

// 提议管理员请求
func (d *peerMsgHandler) proposeAdminCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	adm_msg := msg.AdminRequest
	switch adm_msg.CmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.proposeChangePeer(msg, cb)
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.proposeCompactLog(msg, cb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.proposeTransferLeader(msg, cb)
	case raft_cmdpb.AdminCmdType_Split:
		d.proposeSplit(msg, cb)
	}
}

// 将消息提议到对应的 Raft 模块
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// 获取消息请求头
	if msg.AdminRequest != nil {
		// 处理 admin 消息
		d.proposeAdminCommand(msg, cb)
		return
	}

	// 普通请求
	d.proposeNormalCommand(msg, cb)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

// 创建并发送一个日志压缩任务
// 上层的 worker 监控chan，当收到任务后，进行压缩处理
func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		// 调用 PeerGC 来删除一个 peer
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	// 如果是 snap 消息，则key != nil
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		// 调用 GetSnapshotForApplying 获取要应用的快照，若无法获取，则返回错误。
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		// apply 后调用 DeleteSnapshot 删除它。
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// 满足压缩条件时，创建压缩请求
	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

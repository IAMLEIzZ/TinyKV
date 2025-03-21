package raftstore

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

// peerState 包含了需要执行 Raft 命令和应用命令的 peer 状态。
// peerState contains the peer states that needs to run raft command and apply command.
type peerState struct {
	closed uint32
	peer   *peer
}

// router routes a message to a peer.
type router struct {
	peers       sync.Map // regionID -> peerState
	peerSender  chan message.Msg
	storeSender chan<- message.Msg
}

func newRouter(storeSender chan<- message.Msg) *router {
	pm := &router{
		peerSender:  make(chan message.Msg, 40960),
		storeSender: storeSender,
	}
	return pm
}

func (pr *router) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

func (pr *router) register(peer *peer) {
	id := peer.regionId
	newPeer := &peerState{
		peer: peer,
	}
	pr.peers.Store(id, newPeer)
}

func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		atomic.StoreUint32(&ps.closed, 1)
		pr.peers.Delete(regionID)
	}
}

// 消息写入 region 的 peer 接收器
func (pr *router) send(regionID uint64, msg message.Msg) error {
	msg.RegionID = regionID
	// 返回对应 region 对应的 peer 组，这里要注意，peer 是一组节点，不是一个 peer
	p := pr.get(regionID)
	if p == nil || atomic.LoadUint32(&p.closed) == 1 {
		return errPeerNotFound
	}
	// 消息写入 peer 的通道内
	pr.peerSender <- msg
	return nil
}

func (pr *router) sendStore(msg message.Msg) {
	pr.storeSender <- msg
}

var errPeerNotFound = errors.New("peer not found")

type RaftstoreRouter struct {
	router *router
}

func NewRaftstoreRouter(router *router) *RaftstoreRouter {
	return &RaftstoreRouter{router: router}
}

func (r *RaftstoreRouter) Send(regionID uint64, msg message.Msg) error {
	return r.router.send(regionID, msg)
}

func (r *RaftstoreRouter) SendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.RegionId
	if r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftMessage, regionID, msg)) != nil {
		r.router.sendStore(message.NewPeerMsg(message.MsgTypeStoreRaftMessage, regionID, msg))
	}
	return nil

}

func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	// 消息
	cmd := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	regionID := req.Header.RegionId
	return r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftCmd, regionID, cmd))
}

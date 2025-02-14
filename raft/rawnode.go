// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg 是在尝试处理本地 Raft 消息时返回的错误。
// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound 在尝试处理响应消息时返回, 但在 raft.Prs 中未找到该节点的 peer。
// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState 提供易失的状态，不需要持久化到 WAL。
// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready 封装了那些准备好被读取、保存到稳定存储、提交或发送给其他节点的条目和消息。Ready 中的所有字段都是只读的。
// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {

	// 当前 Node 的易变状态。如果没有更新，SoftState 将为 nil。不需要使用或存储 SoftState。
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// 节点的当前状态在消息发送之前保存到稳定存储。如果没有更新，HardState 将等于空状态。
	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// 需要写入硬盘的条目
	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot 指定要保存到稳定存储的快照。
	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// 所有已提交但没有应用的条目
	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// 需要发送的消息
	// 如果它包含 MessageType_MsgSnapshot 消息，应用程序必须通过调用 ReportSnapshot 报告快照已接收或失败。
	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode 是 Raft 的一个包装器。
// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	pre_softstate *SoftState
	pre_hardstate pb.HardState
}

// NewRawNode 返回一个新的 RawNode(给定配置和 raft peers 列表)。
// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft_node := newRaft(config)
	softstate := &SoftState{
		Lead: raft_node.Lead,
		RaftState: raft_node.State,
	}
	hardstate := pb.HardState{
		Term: raft_node.Term,
		Vote: raft_node.Vote,
		Commit: raft_node.RaftLog.committed,
	}
	rn := &RawNode{
		Raft: raft_node,
		pre_softstate: softstate,
		pre_hardstate: hardstate,
	}

	return rn, nil
}

// Tick 将内部逻辑时钟前进一个刻度。
// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign 使这个 RawNode 转换到 candidate 状态。
// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose 提议将数据附加到 raft 日志。
// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange 提议一个配置更改。
// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange 将配置更改应用到本地节点。
// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step 使用给定的消息推进状态机。
// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready 返回此 RawNode 当前时间点的状态。
// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	//	返回 new 的新节点
	return rn.newReady()
}

func (rn *RawNode) newReady() Ready {
	// 构建新的 Ready 节点
	rd := Ready{
		//	返回需要 stable 的条目
		Entries: rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
		Messages: rn.Raft.msgs,
	}

	if rn.Raft.Lead != rn.pre_softstate.Lead || rn.Raft.State != rn.pre_softstate.RaftState {
		softstate := &SoftState{
			Lead: rn.Raft.Lead,
			RaftState: rn.Raft.State,
		}
		rd.SoftState = softstate
		rn.pre_softstate = softstate
	} else {
		rd.SoftState = nil
		rn.pre_softstate = nil
	}

	if rn.Raft.Term != rn.pre_hardstate.Term || rn.Raft.Vote != rn.pre_hardstate.Vote || 
	rn.Raft.RaftLog.committed != rn.pre_hardstate.Commit {
		hardstate := pb.HardState{
			Term: rn.Raft.Term,
			Vote: rn.Raft.Vote,
			Commit: rn.Raft.RaftLog.committed,
		}
		rd.HardState = hardstate
		rn.pre_hardstate = hardstate
	}
	if rn.Raft.RaftLog.pendingSnapshot != nil{
		rd.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}
	return rd
}

// HasReady 在 RawNode 用户需要检查是否有任何 Ready 待处理时调用。
// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	if rn.pre_softstate != nil {
		if rn.Raft.Lead != rn.pre_softstate.Lead || rn.Raft.State != rn.pre_softstate.RaftState {
			return true
		}
	}

	if !isHardStateEqual(rn.pre_hardstate, pb.HardState{}) {
		if rn.Raft.Term != rn.pre_hardstate.Term || rn.Raft.Vote != rn.pre_hardstate.Vote || 
			rn.Raft.RaftLog.committed != rn.pre_hardstate.Commit {
			return true
		}
	}

	if rn.Raft.RaftLog.pendingSnapshot != nil {
		return true
	}

	if len(rn.Raft.RaftLog.unstableEntries()) != 0 || len(rn.Raft.msgs) != 0 || len(rn.Raft.RaftLog.nextEnts()) != 0 {
		return true
	}
	return false
}

// Advance 通知 RawNode 应用程序已在上次 Ready 结果中应用并保存了进度。
// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	// 更新 rn.Raft 的进度
	var msg []pb.Message
	rn.pre_softstate = rd.SoftState
	rn.pre_hardstate = rd.HardState
	if len(rd.Entries) > 0 {
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries) - 1].Index
		// rn.Raft.RaftLog.committed = rd.Entries[len(rd.Entries) - 1].Index
	}

	if len(rd.CommittedEntries) > 0 {
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries) - 1].Index
	}
	rn.Raft.msgs = msg
	
	rn.Raft.RaftLog.pendingSnapshot = nil

}

// GetProgress 返回此节点及其对等节点的进度，如果此节点是领导者。
// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader 尝试将领导权转移给指定的transferee。
// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

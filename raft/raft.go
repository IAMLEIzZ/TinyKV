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
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

func init() {
	rand.Seed(time.Now().UnixNano())
}

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	et int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	vote_map := make(map[uint64]bool)
	for _, id := range c.peers{
		vote_map[id] = false
	}

	raft := &Raft{
		id: c.ID,
		RaftLog: new(RaftLog),
		Prs: make(map[uint64]*Progress, len(c.peers)),
		votes: vote_map,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick + 1),
		State: StateFollower,
		msgs: make([]pb.Message, 0),
		Vote: None,
		et: c.ElectionTick,
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// 对于 fellower 和 candidate 和 leader 要处理的 tick 是不同的
	switch r.State{
	case StateFollower:
		// fellower 要处理 electionTick，当 electionTimeout 来临时，开始选举
		r.electionElapsed ++
		if r.electionElapsed >= r.electionTimeout{
			// 设置随机选举时间为 [et, 2 * et - 1]
			r.electionTimeout = r.et + rand.Intn(r.et + 1)
			r.electionElapsed = 0
			r.startElection()
		}
	case StateCandidate:
		// Candidate 要处理 electionTick，当 electionTimeout 来临后，重新发起投票请求
		// 每一个候选人在开始一次选举的时候会重置一个随机的选举超时时间，然后在超时时间内等待投票的结果；这样减少了在新的选举中另外的选票瓜分的可能性
		r.electionElapsed ++
		if r.electionElapsed >= r.electionTimeout{
			// 当candidate 计时器到的时候，检查自己的投票数是否占大多数，如果不占大多数，则归零计数器，重启投票
			if r.canbeLeader() {
				r.becomeLeader()
			} else {
				// 设置随机选举时间为 [et, 2 * et - 1]
				r.electionElapsed = 0
				r.electionTimeout = r.et + rand.Intn(r.et + 1)
				r.startElection()
			}
			
		}
	case StateLeader:
		r.heartbeatElapsed ++
		if r.heartbeatElapsed >= r.heartbeatTimeout{
			// 当心跳计时器到达时，则向初自己外的节点发送心跳
			r.heartbeatElapsed = 0
			for k, _ := range r.votes{
				if k != r.id{
					r.sendHeartbeat(k)
				}
			}
		}
	}
}

// 检查票数
func (r *Raft) canbeLeader() bool{
	vote_num := 0;
	for _, v := range r.votes {
		if v {
			vote_num ++
		}
	}
	return vote_num > len(r.votes)/2
}

// 开始选举
func (r *Raft) sendElection() {
	// 开始发送选举消息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		From: r.id, 
		To: r.id,
	}
	r.msgs = append(r.msgs, msg)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term ++
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.State = StateLeader
	// 清空投票
	for k, _ := range r.votes{
		if k != r.id{
			r.votes[k] = false
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 当收到消息时，分角色筛选，进入不同的 step
	switch r.State {
	case StateFollower:
		r.stepFellower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFellower(m pb.Message){
	// 分消息类型处理
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		// fellower 进行选举
		r.startElection()
		if r.canbeLeader() {
			r.becomeLeader()
		}
		return 
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// fellower 不会收到该消息
		return 
	case pb.MessageType_MsgRequestVote:
		// fellower 收到请求投票消息时，进行投票
		r.sendVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// fellower 收到投票响应无效
		return 
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	}
}

func (r *Raft) stepCandidate(m pb.Message){
	// 分消息类型处理
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		// 当 candidate 收到选举消息时，同样开始选举
		r.startElection()
		if r.canbeLeader() {
			r.becomeLeader()
		}
		return 
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// candidate 不会收到该消息
		return
	case pb.MessageType_MsgRequestVote:
		r.sendVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// candidate 收到投票响应
		if m.Reject{
			return 
		}
		r.votes[m.From] = true
		if r.canbeLeader() {
			r.becomeLeader()
		}
		return 
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	}
}

func (r *Raft) stepLeader(m pb.Message){
	if m.Term < r.Term {
		return 
	}
	// 分消息类型处理
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		// Leader 收到选举消息无效
		return
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// 接收响应 返回
		return 
	case pb.MessageType_MsgRequestVote:
		r.sendVote(m) 
	case pb.MessageType_MsgRequestVoteResponse:
		return
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	}
}

//sendVote r 节点收到投票请求后进行响应
// r 节点在一个任期内，只能投票给一个人
// raft 投票规则：1.当前节点还没有投票，则可以投
// 2. 当前节点已经投票，则在该任期内，只能投票给原来投过的人或则 term 比当前 term 高的人
func (r *Raft) sendVote(m pb.Message){
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
	}
	// 当收到 term 小的消息时，直接拒绝
	if r.Term >= m.Term && r.State == StateCandidate{
		msg.Reject = true
	}
	// 如果已经投过票，直接拒绝
	if r.Vote != None{
		if r.Vote != m.From && r.Term >= m.Term{
			msg.Reject = true
		}
	}
	if r.State == StateFollower{
		// 发送者的最后任期等于 MessageType_MsgRequestVote 的任期但发送者的最后提交索引大于或等于 follower 的时，follower 才会投票给发送者。
		rli := r.RaftLog.LastIndex()
		if m.Index < rli {
				msg.Reject = true
		}
	}
	if msg.Reject {
		r.msgs = append(r.msgs, msg)
		return
	} else {
		// 更新投票
		r.Vote = m.From
		// 回退，更新 term
		r.becomeFollower(m.Term, m.From)
		r.msgs = append(r.msgs, msg)
		return 
	}
}

// campagin
func (r *Raft) startElection() {
	r.becomeCandidate()
	// 给自己投票
	r.votes[r.id] = true
	// 发送选举消息给每一个节点
	for k, _ := range r.votes{
		if k == r.id{
			continue
		}
		lt, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From: r.id,
			To: k,
			Term: r.Term,
			LogTerm: lt,
			Index: r.RaftLog.LastIndex(),
		}
		r.msgs = append(r.msgs, msg)
	}
	r.electionElapsed = 0
}


// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 拒绝比自己 term 小的请求
	if m.Term < r.Term{
		return
	}
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: m.From,
	}

	r.becomeFollower(m.Term, m.From)

	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
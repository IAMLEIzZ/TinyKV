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
	// "fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
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

// 判断传入的配置文件是否有效
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

// Match 是对应 node 的已经匹配到的日志 idx
// Next 是对应 node 的需要匹配的下一个日志的 idx
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

	et            int
	reject_num    int
	heartbeatResp map[uint64]bool
}

// 创建一个新的 Raft 节点
// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardstate, confstate, _ := c.Storage.InitialState()
	var msg []pb.Message
	if c.peers == nil {
		c.peers = confstate.Nodes
	}

	vote_map := make(map[uint64]bool)
	prs_map := make(map[uint64]*Progress)

	raft := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              prs_map,
		votes:            vote_map,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick + rand.Intn(c.ElectionTick),
		// electionTimeout: 20,
		State:         StateFollower,
		msgs:          msg,
		Vote:          hardstate.Vote,
		Term:          hardstate.Term,
		et:            c.ElectionTick,
		heartbeatResp: make(map[uint64]bool),
	}

	for _, id := range c.peers {
		vote_map[id] = false
		prs_map[id] = &Progress{Match: 0, Next: 1}
	}

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	// 将 PendingConfIndex 更新为最早的未应用的 confchange 索引
	// for _, entry := range raft.RaftLog.allEntries() {
	// 	if entry.EntryType == pb.EntryType_EntryConfChange {
	// 		raft.PendingConfIndex = entry.Index
	// 		break
	// 	}
	// }

	return raft
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 提醒发送心跳
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	// Your Code Here (2A).
	// 对于 follower 和 candidate 和 leader 要处理的 tick 是不同的
	switch r.State {
	case StateFollower:
		// follower 要处理 electionTick，当 electionTimeout 来临时，开始选举
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// 设置随机选举时间为 [et, 2 * et - 1]
			r.electionElapsed = 0
			r.electionTimeout = r.et + rand.Intn(r.et)
			if r.leadTransferee != 0 {
				r.leadTransferee = 0
				r.becomeLeader()
			} else {
				r.startElection()
			}
		}
	case StateCandidate:
		// Candidate 要处理 electionTick，当 electionTimeout 来临后，重新发起投票请求
		// 每一个候选人在开始一次选举的时候会重置一个随机的选举超时时间，然后在超时时间内等待投票的结果；这样减少了在新的选举中另外的选票瓜分的可能性
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// 当candidate 计时器到的时候，检查自己的投票数是否占大多数，如果不占大多数，则归零计数器，重启投票
			// 设置随机选举时间为 [et, 2 * et - 1]
			r.electionElapsed = 0
			r.electionTimeout = r.et + rand.Intn(r.et)
			r.startElection()
		}
	case StateLeader:
		r.heartbeatElapsed++
		hbrNum := len(r.heartbeatResp)
		total := len(r.Prs)
		// 选举超时
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			r.heartbeatResp = make(map[uint64]bool)
			r.heartbeatResp[r.id] = true
			// 心跳回应数不超过一半，重新开始选举
			if hbrNum*2 <= total {
				r.startElection()
			}
		}
		// 心跳超时
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// 当心跳计时器到达时，则向初自己外的节点发送心跳
			if r.leadTransferee != 0 {
				// 转移目标宕机，保持 leader
				r.leadTransferee = 0
			}
			r.heartbeatElapsed = 0
			for k := range r.Prs {
				_, ok := r.Prs[k]
				if k != r.id && ok {
					r.sendHeartbeat(k)
				}
			}
		}
	}
}

// 检查选举票数，判断是否能成为 Leader
func (r *Raft) canbeLeader() bool {
	vote_num := 0
	for _, v := range r.votes {
		if v {
			vote_num++
		}
	}
	return vote_num > len(r.votes)/2
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	if r.leadTransferee == lead {
		r.leadTransferee = 0
	}
	// 一个 follower 在一个任期内只能给同一个人投票
	if r.Term != term {
		r.Vote = None
	}
	r.Term = term
	r.Lead = lead
	//	清除投票数量
	r.reject_num = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
	for id := range r.votes {
		r.votes[id] = false // 清空其他节点的投票记录
	}
	r.votes[r.id] = true // 记录自己投了自己
	if r.canbeLeader() {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.State = StateLeader
	r.leadTransferee = 0
	// 清空投票
	for k := range r.votes {
		_, ok := r.votes[k]
		if k != r.id && ok {
			r.votes[k] = false
		}
	}
	// 成为 leader 后，发送一个空条目
	ent := pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1}
	r.RaftLog.entries = append(r.RaftLog.entries, ent)
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1
	// 更新 commited
	if len(r.Prs) < 2 {
		r.RaftLog.committed = max(r.RaftLog.LastIndex(), r.RaftLog.committed)
	}
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 当收到消息时，分角色筛选，进入不同的 step
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	// 分消息类型处理
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// follower 进行选举
		r.startElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		r.appendEntry(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// follower 不会收到该消息
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.sendVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTimeoutNow:
		// r.sendMsgHup(r.id)
		r.startElection()
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	// 分消息类型处理
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		r.appendEntry(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.sendVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// candidate 收到投票响应
		if m.GetReject() {
			r.reject_num++
			if r.reject_num*2 >= len(r.Prs) {
				r.becomeFollower(r.Term, None)
			}
			return
		}
		r.votes[m.From] = true
		if r.canbeLeader() {
			r.becomeLeader()
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTimeoutNow:
		// r.sendMsgHup(r.id)
		r.startElection()
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	// 分消息类型处理
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// Leader 收到选举消息无效
	case pb.MessageType_MsgBeat:
		// 提醒自己发心跳
		for k := range r.Prs {
			_, ok := r.Prs[k]
			if k != r.id && ok {
				r.sendHeartbeat(k)
			}
		}
	case pb.MessageType_MsgPropose:
		r.appendEntry(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// 接收响应 返回
		// 当领导者接收到 append 响应后，进入 leader 对响应的处理阶段
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.sendVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

// func (r *Raft) sendMsgHup(to uint64) {
// 	msg := pb.Message{
// 		MsgType: pb.MessageType_MsgHup,
// 		To: to,
// 	}

// 	r.msgs = append(r.msgs, msg)
// }

// handleTransferLeader Leader 转让领导权
func (r *Raft) handleTransferLeader(m pb.Message) {
	_, exists := r.Prs[m.From]
	if !exists {
		return
	}
	// 转移目标
	target := m.From
	// 将转移目标设置为terget， client 发送来请求时，发现该字段不为nil，拒绝接收请求
	r.leadTransferee = target
	if r.Prs[target].Next != r.RaftLog.LastIndex()+1 && r.State == StateLeader {
		// 如果此时目标节点的日志不是最新状态，则发送 MsgAppend
		r.sendAppend(target)
		return
	}
	// 如果日志请求通过，则发送TimeoutNow
	r.sendTimeoutNowMsg(m.From)
	r.heartbeatElapsed = 0
}

func (r *Raft) sendTimeoutNowMsg(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendSnapshot(to uint64) {
	if _, ok := r.Prs[to]; !ok {
		return
	}
	var snapshot pb.Snapshot
	var err error = nil
	if IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snapshot, err = r.RaftLog.storage.Snapshot()
	} else {
		snapshot = *r.RaftLog.pendingSnapshot
	}
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) {
	_, ok := r.Prs[to]
	if !ok {
		log.Infof("节点 %d 不存在", to)
		return
	}
	// Your Code Here (2A).
	// 要根据每一个成员的进度获取
	// 从 r 的 storage 中获取日志条目，并且发送
	// 对应成员的目前日志复制进度，next 是成员希望收到的，-1 代表已经匹配到的 idx
	prev_idx := r.Prs[to].Next - 1
	// 获取已经匹配到的日志的 Term
	// prev_term, _ := r.RaftLog.Term(prev_idx)
	prev_term, err := r.RaftLog.Term(prev_idx)
	// 根据索引获取日志，要发送多条日志一次,期待收到的消息索引 - 起始索引 = 位置
	// r.Prs[to].Next-r.RaftLog.entries[0].Index 假设期待收到 4 号日志，4 号日志对应的下标为 3，entires[0].Index = 1,
	firstIndex := r.RaftLog.FirstIndex()
	// 这里如果 firstIndex - 1 > prev_idx 的情况，则代表要发送 snapshot (if firstIndex - 1 > prev_idx && prev_idx != 0)
	if firstIndex-1 > prev_idx || err != nil {
		// println("send snapShot to", to)
		r.sendSnapshot(to)
		return
	}

	var ents []*pb.Entry
	for i := 0; i < len(r.RaftLog.entries); i++ {
		if r.RaftLog.entries[i].Index >= r.Prs[to].Next {
			ents = append(ents, &r.RaftLog.entries[i])
		}
	}

	// 发送消息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prev_idx,
		LogTerm: prev_term,
		Entries: ents,
		Commit:  r.RaftLog.committed, // 告知成员 Leader 的日志提交进度
	}

	r.msgs = append(r.msgs, msg)
}

// appendEntry leader 将消息加入日志列表中
func (r *Raft) appendEntry(m pb.Message) {
	// 此时 leader 处于权利转让阶段
	if r.State == StateLeader && r.leadTransferee != 0 {
		return
	}
	// 如果是 follower, 消息转发
	if r.State == StateFollower {
		m.From = r.id
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
		return
	}
	if r.State == StateCandidate {
		return
	}
	// 往 r 中存放日志,存放日志时要按照 lastIndex 的顺序来
	li := r.RaftLog.LastIndex()

	// 更新从节点日志复制进度
	for _, mes := range m.GetEntries() {
		mes.Index = li + 1
		mes.Term = r.Term
		// 将日志写入内存
		r.RaftLog.entries = append(r.RaftLog.entries, *mes)
		r.Prs[r.id].Next = mes.Index + 1
		r.Prs[r.id].Match = r.Prs[r.id].Next - 1
		li++
	}

	// 节点数不多的时候进行特判断，假设只有两个节点，则直接跟新 r.commited 为 li
	if len(r.Prs) < 2 {
		r.RaftLog.committed = max(r.RaftLog.LastIndex(), r.RaftLog.committed)
	}

	// leader 广播日志
	r.bcastAppend()
}

func (r *Raft) sendAppendMessage() {
	for id := range r.Prs {
		_, ok := r.Prs[id]
		if id != r.id && ok {
			r.sendAppend(id)
		}
	}
}

// bcastAppend leader 向其他成员广播日志复制
func (r *Raft) bcastAppend() {
	r.sendAppendMessage()
}

// handleAppendResponse Leader将根据消息排判断是否 commit 某些日志
// 当大多数节点都复制了某条日志后，则 Leader 可以 commit 这条日志
func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.State == StateFollower || r.State == StateCandidate {
		return
	}
	if _, exist := r.Prs[m.From]; !exist {
		log.Infof("节点 %d 已经不存在", m.From)
		return
	}
	// log.Infof("成功接收到来自 %d 的 Append 回应", m.From)
	// if m.Index > r.Prs[m.From].Match && m.GetReject() {
	// 	r.Prs[m.From].Match = m.Index
	// 	r.Prs[m.From].Next = m.Index + 1
	// 	return
	// }
	// 如果拒绝，则会缩小对应的 prs
	from := m.GetFrom()
	if m.GetReject() {
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
		return
	}

	follower_match_idx := m.GetIndex()
	follower_id := from
	r.Prs[follower_id].Next = follower_match_idx + 1
	r.Prs[follower_id].Match = follower_match_idx
	// 统计 commit 日志消息，遍历 r.prs，找到最小的match，然后更新
	r.updateCommit()

	if r.leadTransferee == m.From && r.Lead != r.leadTransferee && follower_match_idx == r.RaftLog.LastIndex() {
		// 转入处理请求
		r.sendTransferMsg(m.From)
	}
}

// sendTransferMsg 发送 leader 变更请求
func (r *Raft) sendTransferMsg(to_leader uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTransferLeader,
		To:      r.id,
		From:    to_leader,
	}

	r.msgs = append(r.msgs, msg)
}

// sendVote r 节点收到投票请求后进行响应
// r 节点在一个任期内，只能投票给一个人
// raft 投票规则：1.当前节点还没有投票，则可以投
// 2. 当前节点已经投票，则在该任期内，只能投票给原来投过的人或则 term 比当前 term 高的人
func (r *Raft) sendVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}

	m_from := m.GetFrom()
	m_term := m.GetTerm()
	// 如果已经投过票，直接拒绝
	if r.Vote != None {
		if r.Vote != m_from && r.Term >= m_term {
			msg.Reject = true
		}
	}

	if r.Term < m.Term {
		r.becomeFollower(m_term, None)
	}
	// 如果 From 正好是要转移的对象，要继续投票
	if r.Term < m.Term && r.leadTransferee == m.From {
		r.becomeFollower(m.Term, None)
	}

	if r.Term >= m.Term && r.State == StateCandidate {
		msg.Reject = true
	}

	// follower 会拒绝掉日志没有自己新的 candidate
	if r.State == StateFollower {
		// 发送者的最后任期等于 MessageType_MsgRequestVote 的任期但发送者的最后提交索引大于或等于 follower 的时，follower 才会投票给发送者。
		rli := r.RaftLog.LastIndex()
		rlog_term, _ := r.RaftLog.Term(rli)
		m_idx := m.GetIndex()
		m_logterm := m.GetLogTerm()
		if rlog_term != m_logterm && rlog_term > m_logterm {
			// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新 paper 5.4.1
			msg.Reject = true
		} else if rlog_term == m_logterm && rli > m_idx {
			// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新
			msg.Reject = true
		}
	}
	if msg.Reject {
		r.msgs = append(r.msgs, msg)
		return
	} else {
		r.becomeFollower(m_term, None)
		// 更新投票
		r.Vote = m_from
		// 回退，更新 term
		r.electionElapsed = 0
		r.msgs = append(r.msgs, msg)
		return
	}
}

// campagin
func (r *Raft) startElection() {
	// 如果此时节点不在集群中，则状态保持不变
	_, exist := r.Prs[r.id]
	if !exist {
		return
	}
	// 给自己投票
	r.becomeCandidate()
	// 发送请求投票消息给每一个节点
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		li := r.RaftLog.LastIndex()
		lt, _ := r.RaftLog.Term(li)
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      k,
			Term:    r.Term,
			LogTerm: lt,
			Index:   li,
		}
		r.msgs = append(r.msgs, msg)
	}
	// 投票后选举计时器清零
	r.electionElapsed = 0
}

// 当 follower 和 candidate 收到日志复制通知后，在复制完后，会告知 Leader，
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) sendAppendResponse(to uint64, idx uint64, rej bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  rej,
		To:      to,
		Term:    r.Term,
		Index:   idx,
		From:    r.id,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendEntries(m pb.Message) {
	// 拒绝 term 小的请求
	if m.Term < r.Term {
		return
	} else {
		r.electionElapsed = 0
		r.becomeFollower(m.Term, m.From)
	}

	msg_prev_index, msg_prev_term := m.Index, m.LogTerm
	// 当前 msg 的前序日志与 raft 节点的日志有 gap
	if msg_prev_index > r.RaftLog.LastIndex() {
		r.sendAppendResponse(m.From, r.RaftLog.LastIndex()+1, true)
		return
	}

	term, err := r.RaftLog.Term(msg_prev_index)
	if term != msg_prev_term && err == nil {
		// 虽然日志的 index 匹配一致了，但是与term 匹配不一致，则拒绝添加日志
		r.sendAppendResponse(m.From, r.RaftLog.LastIndex()+1, true)
		return
	}
	// prev_index 和 prev_term 都对上了，允许添加日志
	for _, en := range m.Entries {
		en_idx := en.Index
		old_term, _ := r.RaftLog.Term(en_idx)
		// 这里会有两种情况
		// 1. 是相同的条目，直接 Pass
		// 2. 不是相同的，直接覆盖 或 追加
		// 如果 en_idx 和 en.term 都和 r 中的日志没对应上，则代表这两个不是同一条日志，
		if en_idx-r.RaftLog.FirstIndex() > uint64(len(r.RaftLog.entries)) || en_idx > r.RaftLog.LastIndex() {
			// 追加
			r.RaftLog.entries = append(r.RaftLog.entries, *en)
		} else if old_term != en.Term || err != nil {
			// 不是同一条日志，需要覆盖
			if en_idx < r.RaftLog.FirstIndex() {
				r.RaftLog.entries = make([]pb.Entry, 0)
			} else {
				r.RaftLog.entries = r.RaftLog.entries[0 : en_idx-r.RaftLog.FirstIndex()]
			}
			// 追加
			r.RaftLog.stabled = min(r.RaftLog.stabled, en_idx-1)
			r.RaftLog.entries = append(r.RaftLog.entries, *en)
		}
	}

	// 当消息的提交信息大于该节点的提交信息时，更新提交信息
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Index+uint64(len(m.Entries)), m.Commit)
	}

	r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), false)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 处理心跳
	if r.Term > m.Term {
		// 只有严格大于当前任期时，才会降级
		return
	}
	m_from := m.GetFrom()
	// 退化为 follower，并重置选举时间
	r.electionElapsed = 0
	r.becomeFollower(m.Term, m_from)
	// 处理心跳，append 日志
	// MessageType_MsgAppend
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleSnapshot(m pb.Message) {
	// fmt.Printf("%x receive snapshot from %x\n",r.id,m.From)
	// Your Code Here (2C).
	// 前置，更新 term 和 State
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if r.Term > m.Term {
		return
	}

	metaData := m.Snapshot.Metadata
	shotIndex := metaData.Index
	shotTerm := metaData.Term
	shotConf := metaData.ConfState

	if shotIndex < r.RaftLog.committed || shotIndex < r.RaftLog.FirstIndex() {
		r.sendAppendResponse(m.From, r.RaftLog.committed, false)
		return
	}

	if r.Lead != m.From {
		r.Lead = m.From
	}

	// 丢弃之前的所有 entry
	if len(r.RaftLog.entries) > 0 {
		if shotIndex >= r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else {
			r.RaftLog.entries = r.RaftLog.entries[shotIndex-r.RaftLog.FirstIndex()+1:]
		}
	}

	r.RaftLog.committed = shotIndex
	r.RaftLog.applied = shotIndex
	r.RaftLog.stabled = shotIndex

	// 集群节点变更
	if shotConf != nil {
		// fmt.Printf("处理 snapshot, 节点数从 %d --> %d\n", len(r.Prs), len(shotConf.Nodes))
		r.Prs = make(map[uint64]*Progress)
		for _, node := range shotConf.Nodes {
			r.Prs[node] = &Progress{}
			r.Prs[node].Next = r.RaftLog.LastIndex() + 1
			r.Prs[node].Match = 0
		}
	}

	if r.RaftLog.LastIndex() < shotIndex {
		// 加一个空条目，以指明 lastIndex 和 lastTerm 与快照一致
		entry := pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Index:     shotIndex,
			Term:      shotTerm,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
	}
	// fmt.Printf("%x want apply snapshot from %x\n",r.id,m.From)
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), false)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	// 增加节点
	_, exist := r.Prs[id]
	if exist {
		// 节点已经存在
		// println("节点已经存在")
		return
	}
	// if r.PendingConfIndex != 0 && r.RaftLog.applied < r.PendingConfIndex {
	// 	return
	// }
	r.Prs[id] = &Progress{}
	r.votes[id] = false
	// for _, entry := range r.RaftLog.allEntries() {
	// 	if entry.EntryType == pb.EntryType_EntryConfChange && entry.Index > r.PendingConfIndex{
	// 		r.PendingConfIndex = entry.Index
	// 		break
	// 	}
	// }
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	_, exist := r.Prs[id]
	if !exist {
		// 节点不存在
		return
	}
	// if r.PendingConfIndex != 0 && r.RaftLog.applied < r.PendingConfIndex {
	// 	return
	// }
	delete(r.Prs, id)
	delete(r.votes, id)
	// for _, entry := range r.RaftLog.allEntries() {
	// 	if entry.EntryType == pb.EntryType_EntryConfChange && entry.Index > r.PendingConfIndex{
	// 		r.PendingConfIndex = entry.Index
	// 		break
	// 	}
	// }

	// 当删除节点后，Leader 需要重新检查当前的提交，因为当节点被删除后，有的条目已经达到半数，可以选择提交
	if r.State == StateLeader {
		r.updateCommit()
	}
}

func (r *Raft) updateCommit() {
	// 获取所有节点的 matchIndex 并排序（从大到小）
	var match_idx_slice []uint64
	for _, rp := range r.Prs {
		match_idx_slice = append(match_idx_slice, rp.Match)
	}
	//  降序排序
	sort.Slice(match_idx_slice, func(x, y int) bool {
		return match_idx_slice[x] > match_idx_slice[y]
	})
	//  多数派满足数量
	majority := len(r.Prs) / 2
	for i, match_idx := range match_idx_slice {
		// 多数派
		if i >= majority {
			t, _ := r.RaftLog.Term(match_idx)
			if match_idx > r.RaftLog.committed && t == r.Term {
				r.RaftLog.committed = match_idx
				r.sendAppendMessage()
				return
			}
		}
	}
}

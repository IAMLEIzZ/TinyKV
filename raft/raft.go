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

	et int

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
	for _, id := range c.peers {
		vote_map[id] = false
		prs_map[id] = &Progress{Match: 0, Next: 1}
	}
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

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

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
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
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
			r.startElection()
		}
	case StateCandidate:
		// Candidate 要处理 electionTick，当 electionTimeout 来临后，重新发起投票请求
		// 每一个候选人在开始一次选举的时候会重置一个随机的选举超时时间，然后在超时时间内等待投票的结果；这样减少了在新的选举中另外的选票瓜分的可能性
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// 当candidate 计时器到的时候，检查自己的投票数是否占大多数，如果不占大多数，则归零计数器，重启投票
			if r.canbeLeader() {
				r.becomeLeader()
			} else {
				// 设置随机选举时间为 [et, 2 * et - 1]
				r.electionElapsed = 0
				r.electionTimeout = r.et + rand.Intn(r.et)
				r.startElection()
			}

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
			if hbrNum * 2 <= total {
				r.startElection()
			}
		}
		// 心跳超时
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// 当心跳计时器到达时，则向初自己外的节点发送心跳
			r.heartbeatElapsed = 0
			for k := range r.votes {
				if k != r.id {
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

	// 清空投票
	for k := range r.votes {
		if k != r.id {
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
		// 当传递给 follower 时，'MessageType_MsgPropose' 由发送方法存储在 follower 的邮箱（msgs）中。
		// 它存储了发送者的 ID，稍后由 rafthttp 包转发给领导者。
		// r.appendEntry(m)
		r.msgs = append(r.msgs, m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// follower 不会收到该消息
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		// follower 收到请求投票消息时，进行投票
		r.sendVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// follower 收到投票响应无效
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// follower 收到心跳回应，说明是从 Leader 退化的，无需处理
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	// 分消息类型处理
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 当 candidate 收到选举消息时，同样开始选举
		r.startElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		// 当传递给 candidate 时，'MessageType_MsgPropose' 被丢弃。
		// r.appendEntry(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// candidate 不会收到该消息
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
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	// 分消息类型处理
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// Leader 收到选举消息无效
	case pb.MessageType_MsgBeat:
		// 提醒自己发心跳
		for k := range r.votes {
			if k != r.id {
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
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) {
	// Your Code Here (2A).
	// 要根据每一个成员的进度获取
	// 从 r 的 storage 中获取日志条目，并且发送
	// 对应成员的目前日志复制进度，next 是成员希望收到的，-1 代表已经匹配到的 idx
	prev_idx := r.Prs[to].Next - 1
	// 获取已经匹配到的日志的 Term
	prev_term, _ := r.RaftLog.Term(prev_idx)
	// 根据索引获取日志，要发送多条日志一次,期待收到的消息索引 - 起始索引 = 位置
	// r.Prs[to].Next-r.RaftLog.entries[0].Index 假设期待收到 4 号日志，4 号日志对应的下标为 3，entires[0].Index = 1,
	firstIndex := r.RaftLog.FirstIndex()
	// 这里如果 firstIndex - 1 > prev_idx 的情况，则代表要发送 snapshot
	if firstIndex - 1 > prev_idx && prev_idx != 0{
		msg := pb.Message{
			MsgType: pb.MessageType_MsgSnapshot,
			From: r.id,
			To: to,
			Term: r.Term,
		}
		if r.RaftLog.pendingSnapshot != nil {
			msg.Snapshot = r.RaftLog.pendingSnapshot
		} else {
			snap, err := r.RaftLog.storage.Snapshot()
			if err != nil {
				return
			}
			msg.Snapshot = &snap
			r.Prs[to].Next = snap.Metadata.Index + 1
			r.Prs[to].Match = snap.Metadata.Index
		}
		r.msgs = append(r.msgs, msg)
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
	// 如果是 follower
	if r.State == StateFollower {
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
		if id != r.id {
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
	// 如果拒绝，则会缩小对应的 prs
	from := m.GetFrom()
	if m.GetReject() {
		if r.Prs[from].Match > 0 {
			r.Prs[from].Match--
		}
		if r.Prs[from].Next > 1 {
			r.Prs[from].Next--
		}
		return
	}

	follower_match_idx := m.GetIndex()
	follower_id := from
	r.Prs[follower_id].Next = follower_match_idx + 1
	r.Prs[follower_id].Match = r.Prs[follower_id].Next - 1
	// 统计 commit 日志消息，遍历 r.prs，找到最小的match，然后更新
	// 是否存在一下情况？idx = 3 的提交已经占了大多数，但是 id = 2 的提交还没有占大多数？（应该不存在）TODO.可能这里有 bug
	// 每次收到消息都检查一下当前这个消息是不是通过大多数投票，如果通过则 leader 提交？
	if follower_match_idx <= r.RaftLog.committed {
		// 如果当前这个 id < r.Raftlog.committed，则代表该日志早就通过大多数投票被 leader 提交
		return
	}

	vote_num := 0
	for k := range r.Prs {
		if r.Prs[k].Match >= follower_match_idx {
			vote_num++
		}
	}
	// 更新 Leader 的 commited
	// 只有领导者当前任期的日志条目才会通过计算副本数提交，这个日志才会被通过计算副本数的方式提交
	log_term, _ := r.RaftLog.Term(follower_match_idx)
	if vote_num > (len(r.Prs)/2) && log_term == r.Term {
		r.RaftLog.committed = max(follower_match_idx, r.RaftLog.committed)
		// 如果有更新，则更新后再给所有节点发送一个append 请求，用于更新 follower 节点的 commited
		r.sendAppendMessage()
	}
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

	if r.Term < m_term {
		r.becomeFollower(m_term, None)
	}
	// if r.State == StateLeader && r.Term >= m.Term{
	// 	msg.Reject = true
	// }
	if r.Term >= m_term && r.State == StateCandidate {
		msg.Reject = true
	}

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
	// 给自己投票
	r.becomeCandidate()
	// 发送请求投票消息给每一个节点
	for k := range r.votes {
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

// 一致性检查
func (r *Raft) check(m pb.Message) (bool, int) {
	idx := -1
	flag := true
	prev_term := m.GetLogTerm()
	prev_index := m.GetIndex()

	if prev_index == 0 {
		flag = false
		return flag, idx
	}

	for i, ent := range r.RaftLog.entries {
		//  匹配成功，继续执行
		if ent.Index == prev_index && ent.Term == prev_term {
			flag = false
			idx = i
			break
		}
	}

	return flag, idx
}

// 当 follower 和 candidate 收到日志复制通知后，在复制完后，会告知 Leader，
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 拒绝 term 小的请求
	m_term := m.GetTerm()
	if m_term < r.Term {
		return
	}

	m_from := m.GetFrom()
	m_entries := m.GetEntries()
	// 一致性检查
	var msg pb.Message
	if f, match_idx := r.check(m); f {
		//	一致性检查不通过，拒绝条目
		msg = pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m_from,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		r.electionElapsed = 0
		return
	} else {
		// 一致性检查通过
		// 1. msg 的最后一个条目被囊括，则直接返回成功
		// 2. 如果没有囊括，则找到最后一个囊括的，将剩余的补充进去
		// 3. 如果发生不一致冲突，则覆盖不一致部分
		// 从 match_idx，开始检查条目是否匹配
		r.becomeFollower(m_term, m_from)
		k := 0
		i := match_idx + 1
		flag := false
		for ; i < len(r.RaftLog.entries) && k < len(m_entries); i++ {
			r_entry := r.RaftLog.entries[i]
			m_entry := m_entries[k]
			if r_entry.Index == m_entry.Index && r_entry.Term == m_entry.Term {
				// 条目匹配
				k++
				continue
			} else if r_entry.Index == m_entry.Index && r_entry.Term != m_entry.Term {
				// 条目不匹配，则直接进行覆盖
				flag = true
				break
			}
		}
		// log的初始长度
		// len_idx := len(r.RaftLog.entries)
		if !flag && k < len(m_entries) {
			// 这代表所有的 log 都匹配完了，只剩追加部分
			// 持久化之前的条目
			// r.RaftLog.storage.(*MemoryStorage).Append(r.RaftLog.entries[:m.Index - r.RaftLog.entries[0].Index + 1])
			// r.RaftLog.stabled = m.Index
			// 从 k 开始追加
			for ; k < len(m_entries); k++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m_entries[k])
				r.RaftLog.stabled = min(r.RaftLog.stabled, m_entries[k].Index-1)
			}
		}
		if flag {
			// 从 i 开始覆盖
			// 持久化条目
			// r.RaftLog.storage.(*MemoryStorage).Append(r.RaftLog.entries[:m.Index - r.RaftLog.entries[0].Index + 1])
			// r.RaftLog.stabled = m.Index
			r.RaftLog.entries = r.RaftLog.entries[:i]
			for ; k < len(m_entries); k++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m_entries[k])
				r.RaftLog.stabled = min(r.RaftLog.stabled, m_entries[k].Index-1)
			}
		}

		msg = pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m_from,
			Index:   r.RaftLog.LastIndex(),
			Term:    r.Term,
		}
	}
	r.electionElapsed = 0
	// 更新 commited
	// r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
	// 在 Raft 协议中，跟随者处理 MsgAppend 消息时，committed 的更新需要遵循以下规则：
	// 1. 不能超过本地日志的最后索引。
	// 2. 如果没有新条目，committed 只能更新为消息中匹配的索引。
	m_commit := m.GetCommit()
	m_idx := m.GetIndex()
	if m_commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m_idx+uint64(len(m_entries)), m_commit)
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 处理心跳
	m_term := m.GetTerm()
	if r.Term > m_term {
		return
	}
	m_from := m.GetFrom()
	// 退化为 follower，并重置选举时间
	r.electionElapsed = 0
	r.becomeFollower(m_term, m_from)
	// 处理心跳，append 日志
	// MessageType_MsgAppend
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m_from,
	}
	r.msgs = append(r.msgs, msg)
}

// 当 Leader 收到心跳回应时，根据 prs 向发送者发送剩余条目
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	m_from := m.GetFrom()
	next_idx := r.Prs[m_from].Next
	// 发送 next 后的所有条目
	ents := make([]*pb.Entry, 0)
	li := r.RaftLog.LastIndex()
	idx := next_idx - r.RaftLog.entries[0].Index
	for ; idx < uint64(len(r.RaftLog.entries)); idx++ {
		ents = append(ents, &r.RaftLog.entries[idx])
	}
	lt, _ := r.RaftLog.Term(li)
	r.heartbeatResp[m.From] = true
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      m_from,
		Term:    r.Term,
		Index:   next_idx - 1,
		LogTerm: lt,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)
}

// func (r *Raft) handleHeartbeatResponse(m pb.Message) {
// 	// 前置，更新 term 和 State
// 	if r.Term < m.Term {
// 		r.Term = m.Term
// 		if r.State != StateFollower {
// 			r.becomeFollower(r.Term, None)
// 		}
// 	}
// 	r.heartbeatResp[m.From] = true
// 	// 如果节点落后了，append
// 	if m.Commit < r.RaftLog.committed {
// 		r.sendAppend(m.From)
// 	}
// }

// 当 follower 和 candidate 收到 snapshot 时，处理 snapshot
// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	rej := false
	if r.Term > m.Term {
		// 非法请求
		rej = true
	}
	// 这里根据 snap 来修改 raft 的状态
	// 根据 snap 的 node 信息，处理 peer
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		// 代表此时的 snapshot 已过时
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From: r.id,
			To: m.From,
			Reject: rej,
			Index: r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, msg)
		return 
	}
	// 当 foolower 收到 snap 时，他的状态一定远远落后于 Leader，因此要直接按照 snapshot 更新状态
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	// 删除 raftlog 中当前的 entry
	r.RaftLog.entries = nil
	// 将 snapshot 放入待安装的 snapshot 中，等待上层的 advance 处理
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.Term = meta.Term
	r.becomeFollower(meta.Term, m.From)
	if len(meta.ConfState.Nodes) != 0 {
		// 这里要直接将 Prs 清零，因为此时系统中的节点可能有变化 eg. 从 1 2 3 -> 2 3 4
		r.Prs = make(map[uint64]*Progress)
		for _, id := range meta.ConfState.Nodes {
			r.Prs[id] = &Progress{}
			r.votes[id] = false
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: m.From,
		Reject: rej,
		Index: meta.Index,
	}

	r.msgs = append(r.msgs, msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

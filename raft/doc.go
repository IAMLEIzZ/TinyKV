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

/*
Package raft sends and receives messages in the Protocol Buffer format
defined in the eraftpb package.

Raft is a protocol with which a cluster of nodes can maintain a replicated state machine.
The state machine is kept in sync through the use of a replicated log.
For more details on Raft, see "In Search of an Understandable Consensus Algorithm"
(https://ramcloud.stanford.edu/raft.pdf) by Diego Ongaro and John Ousterhout.

# Usage

The primary object in raft is a Node. You either start a Node from scratch
using raft.StartNode or start a Node from some initial state using raft.RestartNode.

To start a node from scratch:

	storage := raft.NewMemoryStorage()
	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

To restart a node from previous state:

	storage := raft.NewMemoryStorage()

	// recover the in-memory storage from persistent
	// snapshot, state and entries.
	storage.ApplySnapshot(snapshot)
	storage.SetHardState(state)
	storage.Append(entries)

	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	  MaxInflightMsgs: 256,
	}

	// restart raft without peer information.
	// peer information is already included in the storage.
	n := raft.RestartNode(c)

Now that you are holding onto a Node you have a few responsibilities:

First, you must read from the Node.Ready() channel and process the updates
it contains. These steps may be performed in parallel, except as noted in step
2.

1. Write HardState, Entries, and Snapshot to persistent storage if they are
not empty. Note that when writing an Entry with Index i, any
previously-persisted entries with Index >= i must be discarded.

2. Send all Messages to the nodes named in the To field. It is important that
no messages be sent until the latest HardState has been persisted to disk,
and all Entries written by any previous Ready batch (Messages may be sent while
entries from the same batch are being persisted).

Note: Marshalling messages is not thread-safe; it is important that you
make sure that no new entries are persisted while marshalling.
The easiest way to achieve this is to serialize the messages directly inside
your main raft loop.

3. Apply Snapshot (if any) and CommittedEntries to the state machine.
If any committed Entry has Type EntryType_EntryConfChange, call Node.ApplyConfChange()
to apply it to the node. The configuration change may be cancelled at this point
by setting the NodeId field to zero before calling ApplyConfChange
(but ApplyConfChange must be called one way or the other, and the decision to cancel
must be based solely on the state machine and not external information such as
the observed health of the node).

4. Call Node.Advance() to signal readiness for the next batch of updates.
This may be done at any time after step 1, although all updates must be processed
in the order they were returned by Ready.

Second, all persisted log entries must be made available via an
implementation of the Storage interface. The provided MemoryStorage
type can be used for this (if you repopulate its state upon a
restart), or you can supply your own disk-backed implementation.

Third, when you receive a message from another node, pass it to Node.Step:

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

Finally, you need to call Node.Tick() at regular intervals (probably
via a time.Ticker). Raft has two important timeouts: heartbeat and the
election timeout. However, internally to the raft package time is
represented by an abstract "tick".

The total state machine handling loop will look something like this:

	for {
	  select {
	  case <-s.Ticker:
	    n.Tick()
	  case rd := <-s.Node.Ready():
	    saveToStorage(rd.State, rd.Entries, rd.Snapshot)
	    send(rd.Messages)
	    if !raft.IsEmptySnap(rd.Snapshot) {
	      processSnapshot(rd.Snapshot)
	    }
	    for _, entry := range rd.CommittedEntries {
	      process(entry)
	      if entry.Type == eraftpb.EntryType_EntryConfChange {
	        var cc eraftpb.ConfChange
	        cc.Unmarshal(entry.Data)
	        s.Node.ApplyConfChange(cc)
	      }
	    }
	    s.Node.Advance()
	  case <-s.done:
	    return
	  }
	}

To propose changes to the state machine from your node take your application
data, serialize it into a byte slice and call:

	n.Propose(data)

If the proposal is committed, data will appear in committed entries with type
eraftpb.EntryType_EntryNormal. There is no guarantee that a proposed command will be
committed; you may have to re-propose after a timeout.

To add or remove a node in a cluster, build ConfChange struct 'cc' and call:

	n.ProposeConfChange(cc)

After config change is committed, some committed entry with type
eraftpb.EntryType_EntryConfChange will be returned. You must apply it to node through:

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

Note: An ID represents a unique node in a cluster for all time. A
given ID MUST be used only once even if the old node has been removed.
This means that for example IP addresses make poor node IDs since they
may be reused. Node IDs must be non-zero.

# Implementation notes

This implementation is up to date with the final Raft thesis
(https://ramcloud.stanford.edu/~ongaro/thesis.pdf), although our
implementation of the membership change protocol differs somewhat from
that described in chapter 4. The key invariant that membership changes
happen one node at a time is preserved, but in our implementation the
membership change takes effect when its entry is applied, not when it
is added to the log (so the entry is committed under the old
membership instead of the new). This is equivalent in terms of safety,
since the old and new configurations are guaranteed to overlap.

To ensure that we do not attempt to commit two membership changes at
once by matching log positions (which would be unsafe since they
should have different quorum requirements), we simply disallow any
proposed membership change while any uncommitted change appears in
the leader's log.

This approach introduces a problem when you try to remove a member
from a two-member cluster: If one of the members dies before the
other one receives the commit of the confchange entry, then the member
cannot be removed any more since the cluster cannot make progress.
For this reason it is highly recommended to use three or more nodes in
every cluster.

# MessageType

Package raft sends and receives message in Protocol Buffer format (defined
in eraftpb package). Each state (follower, candidate, leader) implements its
own 'step' method ('stepFollower', 'stepCandidate', 'stepLeader') when
advancing with the given eraftpb.Message. Each step is determined by its
eraftpb.MessageType. Note that every step is checked by one common method
'Step' that safety-checks the terms of node and incoming message to prevent
stale log entries:

	'MessageType_MsgHup' is used for election. If a node is a follower or candidate, the
	'tick' function in 'raft' struct is set as 'tickElection'. If a follower or
	candidate has not received any heartbeat before the election timeout, it
	passes 'MessageType_MsgHup' to its Step method and becomes (or remains) a candidate to
	start a new election.

	'MessageType_MsgBeat' is an internal type that signals the leader to send a heartbeat of
	the 'MessageType_MsgHeartbeat' type. If a node is a leader, the 'tick' function in
	the 'raft' struct is set as 'tickHeartbeat', and triggers the leader to
	send periodic 'MessageType_MsgHeartbeat' messages to its followers.

	'MessageType_MsgPropose' proposes to append data to its log entries. This is a special
	type to redirect proposals to the leader. Therefore, send method overwrites
	eraftpb.Message's term with its HardState's term to avoid attaching its
	local term to 'MessageType_MsgPropose'. When 'MessageType_MsgPropose' is passed to the leader's 'Step'
	method, the leader first calls the 'appendEntry' method to append entries
	to its log, and then calls 'bcastAppend' method to send those entries to
	its peers. When passed to candidate, 'MessageType_MsgPropose' is dropped. When passed to
	follower, 'MessageType_MsgPropose' is stored in follower's mailbox(msgs) by the send
	method. It is stored with sender's ID and later forwarded to the leader by
	rafthttp package.

	'MessageType_MsgAppend' contains log entries to replicate. A leader calls bcastAppend,
	which calls sendAppend, which sends soon-to-be-replicated logs in 'MessageType_MsgAppend'
	type. When 'MessageType_MsgAppend' is passed to candidate's Step method, candidate reverts
	back to follower, because it indicates that there is a valid leader sending
	'MessageType_MsgAppend' messages. Candidate and follower respond to this message in
	'MessageType_MsgAppendResponse' type.

	'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend'). When
	'MessageType_MsgAppend' is passed to candidate or follower's Step method, it responds by
	calling 'handleAppendEntries' method, which sends 'MessageType_MsgAppendResponse' to raft
	mailbox.

	'MessageType_MsgRequestVote' requests votes for election. When a node is a follower or
	candidate and 'MessageType_MsgHup' is passed to its Step method, then the node calls
	'campaign' method to campaign itself to become a leader. Once 'campaign'
	method is called, the node becomes candidate and sends 'MessageType_MsgRequestVote' to peers
	in cluster to request votes. When passed to the leader or candidate's Step
	method and the message's Term is lower than leader's or candidate's,
	'MessageType_MsgRequestVote' will be rejected ('MessageType_MsgRequestVoteResponse' is returned with Reject true).
	If leader or candidate receives 'MessageType_MsgRequestVote' with higher term, it will revert
	back to follower. When 'MessageType_MsgRequestVote' is passed to follower, it votes for the
	sender only when sender's last term is greater than MessageType_MsgRequestVote's term or
	sender's last term is equal to MessageType_MsgRequestVote's term but sender's last committed
	index is greater than or equal to follower's.

	'MessageType_MsgRequestVoteResponse' contains responses from voting request. When 'MessageType_MsgRequestVoteResponse' is
	passed to candidate, the candidate calculates how many votes it has won. If
	it's more than majority (quorum), it becomes leader and calls 'bcastAppend'.
	If candidate receives majority of votes of denials, it reverts back to
	follower.

	'MessageType_MsgSnapshot' requests to install a snapshot message. When a node has just
	become a leader or the leader receives 'MessageType_MsgPropose' message, it calls
	'bcastAppend' method, which then calls 'sendAppend' method to each
	follower. In 'sendAppend', if a leader fails to get term or entries,
	the leader requests snapshot by sending 'MessageType_MsgSnapshot' type message.

	'MessageType_MsgHeartbeat' sends heartbeat from leader. When 'MessageType_MsgHeartbeat' is passed
	to candidate and message's term is higher than candidate's, the candidate
	reverts back to follower and updates its committed index from the one in
	this heartbeat. And it sends the message to its mailbox. When
	'MessageType_MsgHeartbeat' is passed to follower's Step method and message's term is
	higher than follower's, the follower updates its leaderID with the ID
	from the message.

	'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'. When 'MessageType_MsgHeartbeatResponse'
	is passed to the leader's Step method, the leader knows which follower
	responded.
*/
package raft


// 版权所有 2015 The etcd 作者
//
// 根据 Apache 许可证 2.0 版本（“许可证”）授权;
// 除非符合许可证，否则不得使用此文件。
// 您可以在以下网址获取许可证的副本：
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，否则按“原样”分发的软件
// 没有任何明示或暗示的担保或条件。
// 请参阅许可证了解具体的语言管理权限和限制。

/*
Package raft 以 Protocol Buffer 格式发送和接收消息，该格式在 eraftpb 包中定义。

Raft 是一种协议，通过它，一组节点可以维护一个复制的状态机。
状态机通过使用复制的日志保持同步。
有关 Raft 的更多详细信息，请参阅 Diego Ongaro 和 John Ousterhout 的论文
《寻找一种可理解的共识算法》(https://ramcloud.stanford.edu/raft.pdf)。

# 使用方法

raft 中的主要对象是 Node。您可以从头开始启动一个 Node，使用 raft.StartNode，
或者从某个初始状态启动一个 Node，使用 raft.RestartNode。

要从头开始启动一个节点：

	storage := raft.NewMemoryStorage()
	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

要从之前的状态重启一个节点：

	storage := raft.NewMemoryStorage()

	// 从持久化的快照、状态和条目中恢复内存存储。
	storage.ApplySnapshot(snapshot)
	storage.SetHardState(state)
	storage.Append(entries)

	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	  MaxInflightMsgs: 256,
	}

	// 重启 raft，不包含对等节点信息。
	// 对等节点信息已经包含在存储中。
	n := raft.RestartNode(c)

现在您持有一个 Node，您有几个职责：

首先，您必须从 Node.Ready() 通道读取并处理其中的更新。
这些步骤可以并行执行，除非在步骤 2 中另有说明。

1. 如果 HardState、Entries 和 Snapshot 不为空，则将它们写入持久化存储。
注意，当写入索引为 i 的条目时，必须丢弃任何之前持久化的索引 >= i 的条目。

2. 将所有消息发送到 To 字段中指定的节点。重要的是，在最新的 HardState 持久化到磁盘之前，
以及任何之前的 Ready 批次写入的所有条目之前，不得发送任何消息
（可以在同一批次的条目正在持久化时发送消息）。

注意：消息的编组不是线程安全的；重要的是要确保在编组时没有新的条目被持久化。
实现这一点的最简单方法是在主 raft 循环中直接序列化消息。

3. 将快照（如果有）和 CommittedEntries 应用到状态机。
如果任何已提交的条目的类型为 EntryType_EntryConfChange，则调用 Node.ApplyConfChange()
将其应用到节点。此时可以通过将 NodeId 字段设置为零来取消配置更改
（但无论如何都必须调用 ApplyConfChange，并且取消的决定必须仅基于状态机，
而不是外部信息，例如观察到的节点健康状况）。

4. 调用 Node.Advance() 以表示准备好处理下一批更新。
这可以在步骤 1 之后的任何时间完成，尽管所有更新必须按照 Ready 返回的顺序进行处理。

其次，所有持久化的日志条目必须通过 Storage 接口的实现提供。
提供的 MemoryStorage 类型可以用于此目的（如果您在重启时重新填充其状态），
或者您可以提供自己的磁盘支持的实现。

第三，当您从另一个节点接收到消息时，将其传递给 Node.Step：

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

最后，您需要定期调用 Node.Tick()（可能通过 time.Ticker）。
Raft 有两个重要的超时：心跳和选举超时。然而，在 raft 包内部，
时间由一个抽象的“tick”表示。

状态机处理的总循环将类似于以下内容：

	for {
	  select {
	  case <-s.Ticker:
	    n.Tick()
	  case rd := <-s.Node.Ready():
	    saveToStorage(rd.State, rd.Entries, rd.Snapshot)
	    send(rd.Messages)
	    if !raft.IsEmptySnap(rd.Snapshot) {
	      processSnapshot(rd.Snapshot)
	    }
	    for _, entry := range rd.CommittedEntries {
	      process(entry)
	      if entry.Type == eraftpb.EntryType_EntryConfChange {
	        var cc eraftpb.ConfChange
	        cc.Unmarshal(entry.Data)
	        s.Node.ApplyConfChange(cc)
	      }
	    }
	    s.Node.Advance()
	  case <-s.done:
	    return
	  }
	}

要从您的节点提议对状态机的更改，请将您的应用程序数据序列化为字节切片并调用：

	n.Propose(data)

如果提议被提交，数据将出现在类型为 eraftpb.EntryType_EntryNormal 的已提交条目中。
不能保证提议的命令会被提交；您可能需要在超时后重新提议。

要在集群中添加或删除节点，构建 ConfChange 结构体 'cc' 并调用：

	n.ProposeConfChange(cc)

在配置更改提交后，将返回一些类型为 eraftpb.EntryType_EntryConfChange 的已提交条目。
您必须通过以下方式将其应用到节点：

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

注意：ID 代表集群中一个唯一的节点，且该 ID 在整个时间内都是唯一的。
即使旧节点已被移除，给定的 ID 也只能使用一次。
这意味着例如 IP 地址不适合作为节点 ID，因为它们可能会被重用。节点 ID 必须非零。

# 实现说明

此实现与最终的 Raft 论文 (https://ramcloud.stanford.edu/~ongaro/thesis.pdf) 保持同步，
尽管我们在成员变更协议的实现上与第 4 章中描述的内容有所不同。
关键的不变性是成员变更一次只发生一个节点，但在我们的实现中，
成员变更在其条目被应用时生效，而不是在添加到日志时生效
（因此条目是在旧成员下提交的，而不是新成员下）。
这在安全性方面是等效的，因为旧配置和新配置保证有重叠。

为了确保我们不会通过匹配日志位置来尝试同时提交两个成员变更
（这将是不安全的，因为它们应该有不同的法定人数要求），
我们简单地禁止在领导者的日志中出现任何未提交的变更时提出任何成员变更。

这种方法在尝试从两个成员的集群中移除一个成员时引入了一个问题：
如果其中一个成员在另一个成员接收到 confchange 条目的提交之前死亡，
那么该成员将无法再被移除，因为集群无法取得进展。
因此，强烈建议在每个集群中使用三个或更多节点。

# MessageType

raft 包以 Protocol Buffer 格式（在 eraftpb 包中定义）发送和接收消息。
每个状态（follower、candidate、leader）在推进给定的 eraftpb.Message 时实现自己的 'step' 方法
（'stepFollower'、'stepCandidate'、'stepLeader'）。每个步骤由其 eraftpb.MessageType 决定。
请注意，每个步骤都由一个共同的方法 'Step' 进行检查，该方法安全地检查节点和传入消息的任期，
以防止陈旧的日志条目：

MessageType_MsgHup
用于选举。如果一个节点是跟随者（follower）或候选者（candidate），raft 结构体中的 tick 函数会被设置为 tickElection。如果跟随者或候选者在选举超时前没有收到任何心跳，它会将 MessageType_MsgHup 传递给其 Step 方法，并成为（或保持）候选者以开始新一轮选举。

MessageType_MsgBeat
这是一个内部类型，用于通知领导者发送 MessageType_MsgHeartbeat 类型的心跳。如果一个节点是领导者，raft 结构体中的 tick 函数会被设置为 tickHeartbeat，并触发领导者定期向跟随者发送 MessageType_MsgHeartbeat 消息。

MessageType_MsgPropose
用于提议将数据追加到日志条目中。这是一种特殊类型，用于将提议重定向到领导者。因此，send 方法会用其 HardState 的任期（term）覆盖 eraftpb.Message 的任期，以避免将本地任期附加到 MessageType_MsgPropose 上。当 MessageType_MsgPropose 传递给领导者的 Step 方法时，领导者首先调用 appendEntry 方法将条目追加到其日志中，然后调用 bcastAppend 方法将这些条目发送给其同伴。当传递给候选者时，MessageType_MsgPropose 会被丢弃。当传递给跟随者时，MessageType_MsgPropose 会被 send 方法存储在跟随者的邮箱（msgs）中，并附带发送者的 ID，稍后由 rafthttp 包转发给领导者。

MessageType_MsgAppend
包含需要复制的日志条目。领导者调用 bcastAppend，而 bcastAppend 又调用 sendAppend，后者发送即将被复制的日志，类型为 MessageType_MsgAppend。当 MessageType_MsgAppend 传递给候选者的 Step 方法时，候选者会回退为跟随者，因为这表明有一个有效的领导者正在发送 MessageType_MsgAppend 消息。候选者和跟随者会以 MessageType_MsgAppendResponse 类型的消息来响应此消息。

MessageType_MsgAppendResponse
是对日志复制请求（MessageType_MsgAppend）的响应。当 MessageType_MsgAppend 传递给候选者或跟随者的 Step 方法时，它们会通过调用 handleAppendEntries 方法来响应，该方法会向 raft 邮箱发送 MessageType_MsgAppendResponse。

MessageType_MsgRequestVote
用于请求选举投票。当一个节点是跟随者或候选者，并且 MessageType_MsgHup 传递给其 Step 方法时，节点会调用 campaign 方法以竞选成为领导者。一旦调用 campaign 方法，节点会成为候选者并向集群中的同伴发送 MessageType_MsgRequestVote 以请求投票。当传递给领导者或候选者的 Step 方法时，如果消息的任期低于领导者或候选者的任期，MessageType_MsgRequestVote 会被拒绝（返回 MessageType_MsgRequestVoteResponse，且 Reject 为 true）。如果领导者或候选者收到任期更高的 MessageType_MsgRequestVote，它会回退为跟随者。当 MessageType_MsgRequestVote 传递给跟随者时，只有在发送者的最后任期大于 MessageType_MsgRequestVote 的任期，或者发送者的最后任期等于 MessageType_MsgRequestVote 的任期但发送者的最后提交索引大于或等于跟随者的提交索引时，跟随者才会投票给发送者。

MessageType_MsgRequestVoteResponse
包含对投票请求的响应。当 MessageType_MsgRequestVoteResponse 传递给候选者时，候选者会计算它赢得了多少投票。如果超过多数（quorum），它会成为领导者并调用 bcastAppend。如果候选者收到多数拒绝投票，它会回退为跟随者。

MessageType_MsgSnapshot
用于请求安装快照消息。当一个节点刚成为领导者或领导者收到 MessageType_MsgPropose 消息时，它会调用 bcastAppend 方法，然后对每个跟随者调用 sendAppend 方法。在 sendAppend 中，如果领导者无法获取任期或条目，它会通过发送 MessageType_MsgSnapshot 类型的消息请求快照。

MessageType_MsgHeartbeat
用于领导者发送心跳。当 MessageType_MsgHeartbeat 传递给候选者且消息的任期高于候选者的任期时，候选者会回退为跟随者，并从此心跳中更新其提交索引。同时，它会将消息发送到其邮箱。当 MessageType_MsgHeartbeat 传递给跟随者的 Step 方法且消息的任期高于跟随者的任期时，跟随者会使用消息中的 ID 更新其 leaderID。

MessageType_MsgHeartbeatResponse
是对 MessageType_MsgHeartbeat 的响应。当 MessageType_MsgHeartbeatResponse 传递给领导者的 Step 方法时，领导者会知道哪个跟随者做出了响应。
package raft
*/

/*
我们设计了 Raft 日志机制，以保持不同服务器上日志之间的高度一致性。这不仅简化了系统的行为并使其更具可预测性，而且是确保安全性的重要组成部分。
Raft 维护以下属性，这些属性共同构成了图 3 中的 日志匹配属性：

如果两个日志中的条目具有相同的索引和任期，则它们存储相同的命令。

如果两个日志中的条目具有相同的索引和任期，则这两个日志在所有先前的条目中都是相同的。

第一个属性源于一个事实：领导者在给定任期内最多创建一个具有给定日志索引的条目，并且日志条目在日志中的位置永远不会改变。
第二个属性由 AppendEntries 执行的简单一致性检查保证。在发送 AppendEntries RPC 时，领导者会包含其日志中紧接在新条目之前的条目的索引和任期。
如果追随者在其日志中找不到具有相同索引和任期的条目，则它会拒绝新条目。一致性检查充当归纳步骤：日志的初始空状态满足日志匹配属性，并且每当日志扩展时，
一致性检查都会保持日志匹配属性。

在 Raft 中，领导者通过强制追随者的日志复制自己的日志来处理不一致性。这意味着追随者日志中的冲突条目将被领导者日志中的条目覆盖。
第 5.4 节将表明，在加上一个额外限制的情况下，这是安全的。为了使追随者的日志与自己的日志一致，领导者必须找到两个日志一致的最新日志条目，
删除追随者日志中该点之后的任何条目，并向追随者发送该点之后的所有领导者条目。所有这些操作都是在 AppendEntries RPC 执行的一致性检查的响应中发生的。
领导者为每个追随者维护一个 nextIndex，这是领导者将发送给该追随者的下一个日志条目的索引。当领导者首次掌权时，它会将所有 nextIndex 值初始化为其日志
中最后一个条目之后的索引（图 7 中的 11）。如果追随者的日志与领导者的日志不一致，AppendEntries 一致性检查将在下一次 AppendEntries RPC 中失败。
在拒绝后，领导者递减 nextIndex 并重试 AppendEntries RPC。最终，nextIndex 将达到领导者日志和追随者日志匹配的点。当这种情况发生时，AppendEntries 将成功，
这会删除追随者日志中的任何冲突条目，并追加领导者日志中的条目（如果有）。一旦 AppendEntries 成功，追随者的日志将与领导者的日志一致，并且在该任期内将保持这种状态。

如果需要，可以优化协议以减少被拒绝的 AppendEntries RPC 数量。例如，当拒绝 AppendEntries 请求时，追随者可以包含冲突条目的任期以及该任期的第一个索引。
有了这些信息，领导者可以递减 nextIndex 以跳过该任期中的所有冲突条目；每个具有冲突条目的任期只需要一个 AppendEntries RPC，而不是每个条目一个 RPC。
在实践中，我们怀疑这种优化是否必要，因为故障很少发生，并且不太可能有许多不一致的条目。
*/
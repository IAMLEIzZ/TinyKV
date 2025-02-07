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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
// commited --> appliy --> stabled
type RaftLog struct {
	// 存储所有稳定的日志（即已经持久化的日志）
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// 已经提交的日志索引，保证大多数节点持久化
	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// 已经应用到状态机的日志索引
	// 不变量: applied <= committed
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// 已经持久化到存储的日志索引	
	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// 未压缩的日志条目（存放在内存中）
	// all entries that have not yet compact.
	entries []pb.Entry

	// 还未安装的快照
	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog 返回使用给定存储的日志。它将日志恢复到刚刚提交并应用最新快照的状态。
// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// 从 storage 中拿数据恢复 raftlog
	// 返回所有
	fi, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	ents, _ := storage.Entries(fi, hi + 1)
	raftlog := &RaftLog{
		storage: storage,
		pendingSnapshot: new(pb.Snapshot),
		committed: 0,
		applied: 0,
		stabled: 0,
		entries: ents,
	}
	return raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries 返回所有未压缩的条目。 
// 注意，排除任何虚拟条目。 
// 注意，这是你需要实现的测试存根函数之一。
// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// 获取日志数组
	entries := l.entries

	// 如果日志数组为空，直接返回空切片
	if len(entries) == 0 {
		return []pb.Entry{}
	}

	// 检查第一个条目是否是虚拟条目（dummy entry）
	// 假设虚拟条目的索引为 0
	startIndex := 0
	if entries[0].Index == 0 { // 假设虚拟条目的 Index 为 0
		startIndex = 1 // 从第二个条目开始
	}

	// 返回从 startIndex 开始的所有条目
	return entries[startIndex:]
}

// unstableEntries 返回所有不稳定的条目
// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// 所有未压缩的条目 = 所有 commite 后的条目 = l.entries[l.commit:]
	// 已经提交的最大索引
	commite_idx := l.committed
	offset := l.entries[0].Index
	ets := l.entries[commite_idx - offset + 1:]
	return ets
}

// nextEnts 返回所有已提交但未应用的 entries
// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	commited_idx := l.committed
	applied_idx := l.applied
	// offset := l.entries[0].Index
	ents = l.entries[applied_idx: commited_idx]
	if len(ents) == 0 {
		return nil
	}
	return ents
}

// LastIndex 返回日志条目的最后索引
// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) <= 0 {
		return 0
	}
	offset := int(l.entries[0].Index)
	li :=  l.entries[len(l.entries) - offset].Index
	// 这里返回的时日志的 idx，而不是数组下标
	return li
}

// Term 返回给定索引中条目的 term
// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	if i < offset{
		return 0, nil
	} 
	// 这里传进来的日志的 idx，而非下标
	term := l.entries[i - offset].Term
	return term, nil
}

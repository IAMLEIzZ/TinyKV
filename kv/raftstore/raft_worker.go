package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

// raftWorker 负责运行 Raft 命令并应用 Raft 日志。
// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	// raft worker轮询raftCh以获取消息，包括驱动 Raft 模块的基础 tick 和作为 Raft 条目提议的 Raft 命令
	// 它从Raft模块获取并处理ready，包括发送Raft消息、持久化状态、将提交的条目应用到状态机
	raftCh chan message.Msg
	ctx    *GlobalContext

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

// run 运行 Raft 命令。在每个循环中，Raft 命令通过通道缓冲区进行批处理。
// 处理完命令后，我们收集来自 peers 的应用消息，制作一个 applyBatch，并将其发送到应用通道。
// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0]
		select {
		case <-closeCh:
			// 接收到关闭信号后则直接返回
			return
		case msg := <-rw.raftCh:
			// 消息写入
			msgs = append(msgs, msg)
		}
		pending := len(rw.raftCh)
		// 按照消息长度，将消息加入消息队列中
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		// peer 的状态 map, peerState 中的 *peer 包含了所有的node，实际上是一个 peer 组
		peerStateMap := make(map[uint64]*peerState)
		// 给每一个消息对应的 region 的每一个 peer 分发消息
		for _, msg := range msgs {
			// 获取消息对应的 peer 组
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}
			// 处理消息
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleMsg(msg)
		}
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleRaftReady()
		}
	}
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}

## 第二部分

在这一部分中，你将使用在A部分中实现的Raft模块构建一个容错的键值存储服务。你的键值服务将是一个复制的状态机，由多个。使用Raft进行复制的键值服务器组成只要大多数服务器存活并且可以通信，你的键值服务就应该继续处理客户端请求，即使其他服务器发生故障或网络分区。

在项目1中，你已经实现了一个独立的键值服务器，因此你应该已经熟悉了键值服务器的API和`Storage`接口。

在介绍代码之前，你需要先理解三个术语：`Store`、`Peer`和`Region`，它们定义在`proto/proto/metapb.proto`中。

- **Store** 代表一个tinykv-server的实例。
- **Peer** 代表运行在Store上的一个Raft节点。
- **Region** 是一组Peer的集合，也称为Raft组。

![region](imgs/region.png)

为了简化，项目2中一个Store上只会有一个Peer，一个集群中只会有一个Region。因此，你现在不需要考虑Region的范围。多个Region将在项目3中进一步介绍。

### 代码结构

首先，你应该看一下`kv/storage/raft_storage/raft_server.go`中的`RaftStorage`，它也实现了`Storage`接口。与直接写入或读取底层引擎的`StandaloneStorage`不同，它首先将每个写和读请求发送到Raft，然后在Raft提交请求后，再实际写入或读取底层引擎。通过这种方式，它可以保持多个Store之间的一致性。

`RaftStorage`创建了一个`Raftstore`来驱动Raft。当调用`Reader`或`Write`函数时，它实际上会发送一个定义在`proto/proto/raft_cmdpb.proto`中的`RaftCmdRequest`，该请求包含四种基本命令类型（Get/Put/Delete/Snap），并通过通道（通道是`raftWorker`的`raftCh`）发送到raftstore，然后在Raft提交并应用该命令后返回响应。`Reader`和`Write`函数的`kvrpc.Context`参数现在有用了，它携带了客户端视角的Region信息，并作为`RaftCmdRequest`的头部传递。这些信息可能不正确或过时，因此raftstore需要检查它们并决定是否提议该请求。

然后，这里就是TinyKV的核心——raftstore。结构有点复杂，阅读TiKV参考文档以更好地理解设计：

- <https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore>  (中文版)
- <https://pingcap.com/blog/design-and-implementation-of-multi-raft/#raftstore> (英文版)

raftstore的入口是`Raftstore`，参见`kv/raftstore/raftstore.go`。它启动了一些worker来异步处理特定任务，其中大部分现在没有使用，因此你可以忽略它们。你只需要关注`raftWorker`（`kv/raftstore/raft_worker.go`）。

整个过程分为两部分：raft worker轮询`raftCh`以获取消息，包括驱动Raft模块的基础tick和作为Raft条目提议的Raft命令；它从Raft模块获取并处理ready，包括发送Raft消息、持久化状态、将提交的条目应用到状态机。一旦应用完成，就将响应返回给客户端。

### 实现Peer存储

Peer存储是你通过A部分中的`Storage`接口与之交互的部分，但除了Raft日志之外，Peer存储还管理其他持久化的元数据，这些元数据对于在重启后恢复一致的状态机非常重要。此外，`proto/proto/raft_serverpb.proto`中定义了三个重要的状态：

- **RaftLocalState**：用于存储当前Raft的HardState和最后的日志索引。
- **RaftApplyState**：用于存储Raft应用的最后一个日志索引和一些被截断的日志信息。
- **RegionLocalState**：用于存储Region信息和该Store上对应的Peer状态。Normal表示该Peer正常，Tombstone表示该Peer已从Region中移除，无法加入Raft组。

这些状态存储在两个badger实例中：raftdb和kvdb：

- **raftdb** 存储Raft日志和`RaftLocalState`。
- **kvdb** 存储不同列族中的键值数据、`RegionLocalState`和`RaftApplyState`。你可以将kvdb视为Raft论文中提到的状态机。

格式如下，`kv/raftstore/meta`中提供了一些辅助函数，并使用`writebatch.SetMeta()`将它们设置到badger中。

| 键              | 键格式                        | 值            | 数据库   |
| :--------------- | :------------------------------- | :--------------- | :--- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

> 你可能会好奇为什么TinyKV需要两个badger实例。实际上，它可以只使用一个badger来存储Raft日志和状态机数据。分成两个实例只是为了与TiKV设计保持一致。

这些元数据应该在`PeerStorage`中创建和更新。在创建`PeerStorage`时，参见`kv/raftstore/peer_storage.go`。它初始化该Peer的`RaftLocalState`和`RaftApplyState`，或者在重启的情况下从底层引擎获取先前的值。请注意，`RAFT_INIT_LOG_TERM`和`RAFT_INIT_LOG_INDEX`的值都是5（只要大于1即可），而不是0。之所以不设置为0，是为了与在配置变更后被动创建的Peer区分开来。你现在可能不太理解这一点，但请记住它，详细信息将在项目3b中描述配置变更时进一步解释。

在这一部分中，你需要实现的代码只有一个函数：`PeerStorage.SaveReadyState`，该函数的作用是将`raft.Ready`中的数据保存到badger中，包括追加日志条目和保存Raft的硬状态。

要追加日志条目，只需将`raft.Ready.Entries`中的所有日志条目保存到raftdb中，并删除任何之前追加的、永远不会被提交的日志条目。同时，更新Peer存储的`RaftLocalState`并将其保存到raftdb中。

保存硬状态也非常简单，只需更新Peer存储的`RaftLocalState.HardState`并将其保存到raftdb中。

> 提示：
>
> - 使用`WriteBatch`一次性保存这些状态。
> - 参见`peer_storage.go`中的其他函数，了解如何读写这些状态。
> - 设置环境变量`LOG_LEVEL=debug`，这可能有助于调试，参见所有可用的[日志级别](../log/log.go)。

### 实现Raft ready处理

在项目2的A部分中，你已经构建了一个基于tick的Raft模块。现在你需要编写外部流程来驱动它。大部分代码已经在`kv/raftstore/peer_msg_handler.go`和`kv/raftstore/peer.go`中实现。因此，你需要学习代码并完成`proposeRaftCommand`和`HandleRaftReady`的逻辑。以下是对框架的一些解释。

Raft的`RawNode`已经使用`PeerStorage`创建，并存储在`peer`中。在raft worker中，你可以看到它接收`peer`并使用`peerMsgHandler`包装它。`peerMsgHandler`主要有两个功能：一个是`HandleMsg`，另一个是`HandleRaftReady`。

`HandleMsg`处理从raftCh接收到的所有消息，包括调用`RawNode.Tick()`来驱动Raft的`MsgTypeTick`、包装客户端请求的`MsgTypeRaftCmd`以及在Raft对等节点之间传输的`MsgTypeRaftMessage`。所有消息类型都定义在`kv/raftstore/message/msg.go`中。你可以查看它以了解详细信息，其中一些将在以下部分中使用。

处理消息后，Raft节点应该有一些状态更新。因此，`HandleRaftReady`应该从Raft模块获取ready并执行相应的操作，如持久化日志条目、应用已提交的条目以及通过网络向其他对等节点发送Raft消息。

用伪代码表示，raftstore使用Raft的方式如下：

```go
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
}
```

之后，整个读写过程如下：

- 客户端调用RPC RawGet/RawPut/RawDelete/RawScan。
- RPC处理程序调用`RaftStorage`相关方法。
- `RaftStorage`向raftstore发送Raft命令请求，并等待响应。
- `RaftStore`将Raft命令请求提议为Raft日志。
- Raft模块追加日志，并通过`PeerStorage`持久化。
- Raft模块提交日志。
- Raft worker在处理Raft ready时执行Raft命令，并通过回调返回响应。
- `RaftStorage`从回调接收响应并返回给RPC处理程序。
- RPC处理程序执行一些操作并将RPC响应返回给客户端。

你应该运行`make project2b`来通过所有测试。整个测试运行一个包含多个TinyKV实例的模拟集群，并使用模拟网络。它执行一些读写操作并检查返回值是否符合预期。

需要注意的是，错误处理是通过测试的重要部分。你可能已经注意到，`proto/proto/errorpb.proto`中定义了一些错误，并且错误是gRPC响应的一个字段。此外，`kv/raftstore/util/error.go`中定义了相应的错误，它们实现了`error`接口，因此你可以将它们作为函数的返回值。

这些错误主要与Region有关。因此，它也是`RaftCmdResponse`的`RaftResponseHeader`的成员。在提议请求或应用命令时，可能会出现一些错误。如果发生错误，你应该返回带有错误的Raft命令响应，然后错误将进一步传递给gRPC响应。你可以在返回带有错误的响应时使用`kv/raftstore/cmd_resp.go`中提供的`BindRespError`将这些错误转换为`errorpb.proto`中定义的错误。

在这个阶段，你可能会考虑这些错误，其他错误将在项目3中处理：

- **ErrNotLeader**：Raft命令是在follower上提议的。因此，使用它让客户端尝试其他对等节点。
- **ErrStaleCommand**：可能是由于领导者变更，导致一些日志未被提交并被新领导者的日志覆盖。但客户端并不知道这一点，仍在等待响应。因此，你应该返回此错误，让客户端知道并重试命令。

> 提示：
>
> - `PeerStorage`实现了Raft模块的`Storage`接口，你应该使用提供的`SaveReadyState()`方法来持久化Raft相关的状态。
> - 使用`engine_util`中的`WriteBatch`来原子地执行多个写入操作，例如，你需要确保在一个写批处理中应用已提交的条目并更新应用的索引。
> - 使用`Transport`向其他对等节点发送Raft消息，它位于`GlobalContext`中。
> - 如果服务器不是大多数的一部分并且没有最新的数据，则不应完成get RPC。你可以将get操作放入Raft日志中，或者实现Raft论文第8节中描述的只读操作优化。
> - 不要忘记在应用日志条目时更新和持久化apply状态。
> - 你可以像TiKV那样以异步方式应用已提交的Raft日志条目。虽然不是必须的，但这是提高性能的一大挑战。
> - 在提议时记录命令的回调，并在应用后返回回调。
> - 对于snap命令响应，应显式地将badger Txn设置为回调。
> - 在2A之后，一些测试可能需要多次运行才能发现错误。
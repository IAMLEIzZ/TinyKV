package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	/*
	type GetRequest struct {
	Context  *Context
	Key      []byte
	Version  uint64
	}
	*/
	// 获取单个 key 的 Value
	var keys [][]byte
	keys = append(keys, req.Key)
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.GetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}

	// lock 的 ttl 早于此次事务的 ttl，拒绝此次请求
	if lock != nil && lock.Ts < txn.StartTS {
		lockinfo := &kvrpcpb.LockInfo{
			PrimaryLock: lock.Primary,
			LockVersion: lock.Ts,
			Key: req.Key,
			LockTtl: lock.Ttl,
		}
		resp.Error = &kvrpcpb.KeyError{}
		resp.Error.Locked = lockinfo
		return resp, nil
	}
	// 获取 val
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, nil
	}
	if val == nil {
		// key 不存在
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// 2PC 的第一阶段 ———— 预写入
	var keys [][]byte
	for i := range req.Mutations {
		keys = append(keys, req.Mutations[i].Key)
	}
	// 给所有要预写入的 key 上锁
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.PrewriteResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for i := range req.Mutations {
		mKey := req.Mutations[i].Key
		// 检查在当前事务开始后，是否有其他事务进行提交
		write, commitTs, err := txn.MostRecentWrite(mKey)
		if err != nil {
			return resp, err
		}
		
		// 检查是否存在 write conflict
		if write != nil && commitTs > txn.StartTS {
			// 存在写入冲突
			keyErr := &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs: write.StartTS,
					ConflictTs: commitTs,
					Key: mKey,
					Primary: req.PrimaryLock,
				},
			}
			resp.Errors = append(resp.Errors, keyErr)
			return resp, nil
		}
		// 检查是否存在锁
		lock, err := txn.GetLock(mKey)
		if err != nil {
			return resp, err
		}
		if lock!= nil {
			// 存在 Lock，说明当前 key 被其他事务使用中
			KeyErr := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key: mKey,
					LockTtl: lock.Ttl,
				},
			}
			resp.Errors = append(resp.Errors, KeyErr)
			return resp, nil
		}
	}
	// 检查通过，写入 key 和 lock
	for i := range req.Mutations {
		lock := &mvcc.Lock{}
		switch req.Mutations[i].Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(req.Mutations[i].Key, req.Mutations[i].Value)
			lock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(req.Mutations[i].Key)
			lock.Kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Lock:
			lock.Kind = mvcc.WriteKindRollback
		case kvrpcpb.Op_Rollback:
		}

		lock.Primary = req.PrimaryLock
		lock.Ts = txn.StartTS
		lock.Ttl = req.LockTtl
		txn.PutLock(req.Mutations[i].Key, lock)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	// 2PC 的第一阶段 ———— 写入提交
	commiteKeys := req.Keys
	server.Latches.AcquireLatches(commiteKeys)
	defer server.Latches.ReleaseLatches(commiteKeys)

	resp := &kvrpcpb.CommitResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	// 创建提交事务
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// 检查待提交的 key 的 lock 是否合法
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil {
			// 检查是否有回滚
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return resp, nil
			}
			if write == nil {
				continue
			}
			if write.StartTS == txn.StartTS && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
				return resp, nil
			}
			continue
		}

		// 如果 lock 的 statrTime ！= 事务的 startTime，代表 Lock 过期，被其他事务回滚
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}
	}

	// 事务提交检查成功，写入提交
	for _, k := range req.Keys {
		// 写入 write 并且删除对应 key 的 lock
		lock, _ := txn.GetLock(k)
		if lock == nil {
			continue
		}

		txn.PutWrite(k, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind: lock.Kind,
		})
		txn.DeleteLock(k)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	kvPairs := make([]*kvrpcpb.KvPair,0)
	for i := 0 ; i < int(req.Limit); i++ {
		if !scanner.Iter.Valid() {
			break
		}
		// 获取 key
		key, value, err := scanner.Next()

		if err != nil {
			return resp, err
		}
		if key == nil {
			// 如果 key 为 nil，继续搜索下一个 key
			continue
		}
		// 检查当前 key 是否处于未提交的事务中
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts < txn.StartTS {
			// 如果锁的 ts 小于当前事务的 startTs，则意味着当前 key 处于事务之中
			pair := &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			}
			kvPairs = append(kvPairs, pair)
			continue
		}
		// 检查当前 key 是否有对应的 val
		if value != nil {
			pair := &kvrpcpb.KvPair{
				Key: key,
				Value: value,
			}
			kvPairs = append(kvPairs, pair)
		}
	}
	resp.Pairs = kvPairs
	return resp, nil
}

// 用于 Client failure 后，想继续执行时先检查 Primary Key 的状态，以此决定是回滚还是继续推进 commit。
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// 获得当前主键对应的 write
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	// 检查当前主键是否有 lock
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, nil
	}

	// 如果这个 Key 的 wirte 不处于 rollback 状态，则代表commit
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = commitTs
		return resp, nil
	}

	// 如果此次检查时，lock 为 nil，检查时是否需要手动回滚，事务可能已经回滚，或者事务崩溃导致锁丢失
	if lock == nil {
		// 已经被回滚
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			// 如果 write 记录存在且是 Rollback，说明事务早已回滚，直接返回
			// rolled back: lock_ttl == 0 && commit_version == 0
			resp.CommitVersion = 0
			resp.LockTtl = 0
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		} else {
			// 如果 write 记录不存在，说明事务可能崩溃，此时手动写入 Rollback 记录并返回 LockNotExistRollback
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			err := server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return resp, err
			}
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		}
	}

	// 锁超时，清除
	curTs := mvcc.PhysicalTime(req.CurrentTs)
	lockTs := mvcc.PhysicalTime(lock.Ts)
	if curTs > lockTs && curTs - lockTs >= lock.Ttl {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		// 回滚标记
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		// 如果锁未超时，说明事务可能仍在执行，不能清除锁，直接返回 NoAction，表示不做任何操作
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}
	return resp, nil
}

// 用于批量回滚 key 的操作
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader,req.StartVersion)
	keys := req.Keys

	// 检查
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		// 已提交，拒绝回滚
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return resp, nil
		}
	}
	
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			// key 被其他事务 lock 了，但仍然要给回滚标记，因为网络拥塞可能导致的 prewrite 比 rollback 后到，要避免其执行。
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		// 已经回滚完毕，跳过
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		// 回滚标记
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// 这个方法主要用于解决锁冲突，当客户端已经通过 KvCheckTxnStatus() 检查了 primary key 的状态，这里打算要么全部回滚，要么全部提交，具体取决于 ResolveLockRequest 的 CommitVersion。
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, nil
	}

	txn :=mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)

	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, key)
		}
	}

	if req.CommitVersion == 0 {
		// rollback all
		rbReq :=  &kvrpcpb.BatchRollbackRequest{
			Keys: keys,
			StartVersion: txn.StartTS,
			Context: req.Context,
		}
		rbResp, err := server.KvBatchRollback(nil, rbReq)
		if err != nil {
			return resp, err
		}
		resp.Error = rbResp.Error
		resp.RegionError = rbResp.RegionError
		return resp, nil
	} else if req.CommitVersion > 0 {
		// commit those locks with the given commit timestamp
		cmReq := &kvrpcpb.CommitRequest{
			Keys: keys,
			StartVersion: txn.StartTS,
			CommitVersion: req.CommitVersion,
			Context: req.Context,
		}
		cmResp, err := server.KvCommit(nil, cmReq)
		if err != nil {
			return resp, err
		}
		resp.Error = cmResp.Error
		resp.RegionError = cmResp.RegionError
		return resp, nil
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

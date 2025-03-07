package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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

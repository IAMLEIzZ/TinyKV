package standalone_storage

import (

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// P1 这里实现 Storage 的四个接口
// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
// 独立存储引擎的结构体
type StandAloneStorage struct {
	// Your Data Here (1).
	BadgerDB *badger.DB
	DBpath string
}

// storagerader 对象
type StorageReader struct {
	Txn *badger.Txn
}

// 从 Txn 中直接获取对应 CF
func (sr *StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(sr.Txn, cf, key)
}

// 获取 CF 迭代器
func (sr *StorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.Txn)
}

func (sr *StorageReader) Close() {
	sr.Txn.Discard()
}

// 使用 Badger 创建新的存储引擎
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// 直接返回存储创建的存储引擎
	badgerDB := engine_util.CreateDB(conf.DBPath, conf.Raft)
	return &StandAloneStorage{
		BadgerDB: badgerDB,
		DBpath: conf.DBPath,
	}
}

// 连接数据库实例
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.DBpath
	opts.ValueDir = s.DBpath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.BadgerDB = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.BadgerDB.Close(); err != nil {
		return err
	}
	return nil
}

// 返回一个 badger 快照，用于后续的数据库 KV 内容读写 
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 创建事务 txn
	txn := s.BadgerDB.NewTransaction(false)
	sr := &StorageReader{
		Txn: txn,
	}
	return sr, nil
}

// 进行一组数据库的修改
// Modeify 是一个接口类型
// batch 定义了一组数据库操作
// eg. [[storage.put{cf, key, val}], [storage.delete{key, cf}]]
// 我需要挨个执行对应的修改
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.BadgerDB.NewTransaction(true)
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
		case storage.Delete:
			txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
		}
	}
	err := txn.Commit()
	return err
}

package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
// RawGet 根据 RawGetRequest 的 CF（列族）和 Key 字段返回相应的 Get 响应。
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// req 中有 key 和 cf
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	val, _ := sr.GetCF(req.Cf, req.Key)
	res := new(kvrpcpb.RawGetResponse)
	res.Value = val
	if val == nil {
		res.NotFound = true
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
// RawPut 将目标数据存入存储，并返回相应的响应
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	// 这里将 req 中的请求转化为 Storage.Modify 中的 put 类型
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	res := new(kvrpcpb.RawPutResponse)
	if err != nil {
		res.Error = err.Error()
	}
	return res, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})
	res := new(kvrpcpb.RawDeleteResponse)
	if err != nil {
		res.Error = err.Error() 
	}
	return res, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	it := sr.IterCF(req.Cf)
	res := new(kvrpcpb.RawScanResponse)
	i := req.Limit
	for it.Seek(req.StartKey); it.Valid() && i > 0; it.Next(){
		// 如果 it.valid 合法，那么 val 一定不为空
		item := it.Item()
		key := item.Key()
		val, _ := item.Value()
		if len(val) == 0 {
			continue
		}
		kvp := &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		res.Kvs = append(res.Kvs, kvp)
		i --
	}
	return res, nil
}

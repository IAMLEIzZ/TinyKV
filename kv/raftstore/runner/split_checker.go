package runner

import (
	"encoding/hex"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type SplitCheckTask struct {
	Region *metapb.Region
}

type splitCheckHandler struct {
	engine  *badger.DB
	router  message.RaftRouter
	checker *sizeSplitChecker
}

func NewSplitCheckHandler(engine *badger.DB, router message.RaftRouter, conf *config.Config) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine:  engine,
		router:  router,
		checker: newSizeSplitChecker(conf.RegionMaxSize, conf.RegionSplitSize),
	}
	return runner
}

// 运行检查器，使用分裂检查器对一个 Region 进行检查，以生成分裂键，并生成分裂管理命令。
/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) Handle(t worker.Task) {
	spCheckTask, ok := t.(*SplitCheckTask)
	if !ok {
		// 检查任务是否是合格的分裂检查任务
		log.Errorf("unsupported worker.Task: %+v", t)
		return
	}
	region := spCheckTask.Region
	regionId := region.Id
	log.Debugf("executing split check worker.Task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(region.StartKey), hex.EncodeToString(region.EndKey))
	// 分裂检查
	key := r.splitCheck(regionId, region.StartKey, region.EndKey)
	if key != nil {
		_, userKey, err := codec.DecodeBytes(key)
		if err == nil {
			// It's not a raw key.
			// To make sure the keys of same user key locate in one Region, decode and then encode to truncate the timestamp
			key = codec.EncodeBytes(userKey)
		}
		// 分裂检查通过，则对当前 region 进行 split，发送 split 请求至 RaftNode
		msg := message.Msg{
			Type:     message.MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &message.MsgSplitRegion{
				RegionEpoch: region.GetRegionEpoch(),
				SplitKey:    key,
			},
		}
		err = r.router.Send(regionId, msg)
		if err != nil {
			log.Warnf("failed to send check result: [regionId: %d, err: %v]", regionId, err)
		}
	} else {
		log.Debugf("no need to send, split key not found: [regionId: %v]", regionId)
	}
}

/// SplitCheck gets the split keys by scanning the range.
func (r *splitCheckHandler) splitCheck(regionID uint64, startKey, endKey []byte) []byte {
	txn := r.engine.NewTransaction(false)
	defer txn.Discard()

	r.checker.reset()
	it := engine_util.NewCFIterator(engine_util.CfDefault, txn)
	defer it.Close()
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		// 如果超出 key 范围，则发送一条 MsgTypeRegionApproximateSize 消息
		if engine_util.ExceedEndKey(key, endKey) {
			// update region size
			// 如果 key 超过 endKey，说明已经超出该 Region 范围，遍历应该停止。
			r.router.Send(regionID, message.Msg{
				Type: message.MsgTypeRegionApproximateSize,
				Data: r.checker.currentSize,
			})
			break
		}
		if r.checker.onKv(key, item) {
			break
		}
	}
	return r.checker.getSplitKey()
}

type sizeSplitChecker struct {
	maxSize   uint64
	splitSize uint64

	currentSize uint64
	splitKey    []byte
}

func newSizeSplitChecker(maxSize, splitSize uint64) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:   maxSize,
		splitSize: splitSize,
	}
}

func (checker *sizeSplitChecker) reset() {
	checker.currentSize = 0
	checker.splitKey = nil
}

// 返回当前扫描的 key 是否达到了分裂要求
func (checker *sizeSplitChecker) onKv(key []byte, item engine_util.DBItem) bool {
	valueSize := uint64(item.ValueSize())
	size := uint64(len(key)) + valueSize
	checker.currentSize += size
	if checker.currentSize > checker.splitSize && checker.splitKey == nil {
		checker.splitKey = util.SafeCopy(key)
	}
	return checker.currentSize > checker.maxSize
}

func (checker *sizeSplitChecker) getSplitKey() []byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize < checker.maxSize {
		checker.splitKey = nil
	}
	return checker.splitKey
}

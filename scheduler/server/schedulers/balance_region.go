// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// 实现平衡 store 中的 Region，当 Store 的 Region 负载压力过大是，将上面的部分 Region 转移到合适的 store 上面
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. 找到适合转移并且压力过大的 store（按照 store 上的 Region数量 进行排序）
	suiteStores := make([]*core.StoreInfo, 0)
	allStoreInfo := cluster.GetStores()
	// 找到 suitableStores
	maxDownTime := cluster.GetMaxStoreDownTime()
	for i := range allStoreInfo {
		if allStoreInfo[i].IsUp() && allStoreInfo[i].DownTime() < maxDownTime {
			suiteStores = append(suiteStores, allStoreInfo[i])
		}
	}
	// 降序排序suitableStores
	sort.Slice(suiteStores, func(i, j int) bool{
		return suiteStores[i].GetRegionSize() > suiteStores[j].GetRegionSize()
	})

	var suitRegion *core.RegionInfo
	var suitStore *core.StoreInfo
	var targetStore *core.StoreInfo
	// 负载过大的 store
	for i := range suiteStores {
		var sutieRegions core.RegionsContainer
		// 2. 在该 store 上找到需要移动的 region（1. 待处理的 region，2.folllower region，3.leader region）
		cluster.GetPendingRegionsWithLock(suiteStores[i].GetID(), func(rc core.RegionsContainer) {
			sutieRegions = rc
		})
		if sutieRegions != nil && sutieRegions.RandomRegion(nil, nil) != nil {
			suitRegion = sutieRegions.RandomRegion(nil, nil)
			suitStore = suiteStores[i]
			break
		}
			cluster.GetFollowersWithLock(suiteStores[i].GetID(), func(rc core.RegionsContainer) {
			sutieRegions = rc
		})
		if sutieRegions != nil && sutieRegions.RandomRegion(nil, nil) != nil {
			suitRegion = sutieRegions.RandomRegion(nil, nil)
			suitStore = suiteStores[i]
			break
		}
		cluster.GetLeadersWithLock(suiteStores[i].GetID(), func(rc core.RegionsContainer) {
			sutieRegions = rc
		})
		if sutieRegions != nil && sutieRegions.RandomRegion(nil, nil) != nil {
			suitRegion = sutieRegions.RandomRegion(nil, nil)
			suitStore = suiteStores[i]
			break
		}
	}

	if suitStore == nil && suitRegion == nil{
		return nil
	}
	// 进行转移检查和转移
	if len(suitRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	// 4. 选择一个合适的 store 进行转移
	for i := len(suiteStores) - 1; i >= 0; i --{
		id := suiteStores[i].GetID()
		region_store_ids := suitRegion.GetStoreIds()
		if _, ok := region_store_ids[id]; !ok {
			// 不在其中，break
			targetStore = suiteStores[i]
			break
		}
	}
	
	// 如果不满足转移要求，则放弃转移
	if targetStore == nil || suitStore.GetRegionSize() - targetStore.GetRegionSize() < 2 * suitRegion.GetApproximateSize() {
		return nil
	}
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}
	// 进行转移
	desc := fmt.Sprintf("move-from-%d-to-%d", suitStore.GetID(), targetStore.GetID())
	oper, err := operator.CreateMovePeerOperator(desc, cluster, suitRegion, operator.OpBalance, suitStore.GetID(), targetStore.GetID(), newPeer.Id)
	if err != nil {
		return nil
	}
	// fmt.Printf("debug\n")
	return oper
}

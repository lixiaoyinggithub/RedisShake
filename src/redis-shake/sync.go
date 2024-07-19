// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"sync"

	"github.com/alibaba/RedisShake/pkg/libs/log"

	utils "github.com/alibaba/RedisShake/redis-shake/common"
	conf "github.com/alibaba/RedisShake/redis-shake/configure"
	"github.com/alibaba/RedisShake/redis-shake/dbSync"
)

type CmdSync struct {
	dbSyncers []*dbSync.DbSyncer
}

// GetDetailedInfo return send buffer length, delay channel length, target db offset
func (cmd *CmdSync) GetDetailedInfo() interface{} {
	ret := make([]map[string]interface{}, len(cmd.dbSyncers))
	for i, syncer := range cmd.dbSyncers {
		if syncer == nil {
			continue
		}
		ret[i] = syncer.GetExtraInfo()
	}
	return ret
}

func (cmd *CmdSync) Main() {
	// 同步的基本信息结构，原数据源，目标数据源;slot的边界
	type syncNode struct {
		id                int
		source            string
		sourcePassword    string
		target            []string
		targetPassword    string
		slotLeftBoundary  int
		slotRightBoundary int
	}

	var slotDistribution []utils.SlotOwner
	var err error
	if conf.Options.SourceType == conf.RedisTypeCluster && conf.Options.ResumeFromBreakPoint {
		if slotDistribution, err = utils.GetSlotDistribution(conf.Options.SourceAddressList[0], conf.Options.SourceAuthType,
			conf.Options.SourcePasswordRaw, false, false); err != nil {
			log.Errorf("get source slot distribution failed: %v", err)
			return
		}
	}

	// source redis number
	total := utils.GetTotalLink() //数据源的个数
	//协程交互的渠道
	syncChan := make(chan syncNode, total)
	// 根据数据源的数量创建对应的dbSyncers数组
	cmd.dbSyncers = make([]*dbSync.DbSyncer, total)

	//
	for i, source := range conf.Options.SourceAddressList {
		var target []string
		if conf.Options.TargetType == conf.RedisTypeCluster {
			target = conf.Options.TargetAddressList
		} else {
			// round-robin pick，轮训选择
			pick := utils.PickTargetRoundRobin(len(conf.Options.TargetAddressList))
			target = []string{conf.Options.TargetAddressList[pick]}
		}

		// fetch slot boundary
		leftSlotBoundary, rightSlotBoundary := utils.GetSlotBoundary(slotDistribution, source)

		nd := syncNode{
			id:                i,
			source:            source,
			sourcePassword:    conf.Options.SourcePasswordRaw,
			target:            target,
			targetPassword:    conf.Options.TargetPasswordRaw,
			slotLeftBoundary:  leftSlotBoundary,
			slotRightBoundary: rightSlotBoundary,
		}
		syncChan <- nd
	}

	var wg sync.WaitGroup
	wg.Add(len(conf.Options.SourceAddressList))

	// 数据源拉取的并发度；如果配置为0，则在一个协程里完成所有数据源的同步
	for i := 0; i < int(conf.Options.SourceRdbParallel); i++ {
		// 按数据源的粒度启动一个DB的协程
		go func() {
			for {
				nd, ok := <-syncChan
				if !ok {
					break
				}

				// one sync link corresponding to one DbSyncer
				ds := dbSync.NewDbSyncer(nd.id, nd.source, nd.sourcePassword, nd.target, nd.targetPassword,
					nd.slotLeftBoundary, nd.slotRightBoundary, conf.Options.HttpProfile+i)
				cmd.dbSyncers[nd.id] = ds
				// run in routine？ 本身已经在协程的环境下了，但是数据源的异步与真正的数据源可能不一致
				go ds.Sync()

				// wait full sync done
				<-ds.WaitFull

				wg.Done()
			}
		}()
	}

	wg.Wait()
	close(syncChan)

	// never quit because increment syncing is always running
	// select {} 是一个特殊的 select 语句，它用于创建一个无限循环的选择块。
	// select 语句用于在多个通信操作中选择一个可执行的操作。通常，select 语句会包含多个 case 子句，每个 case 子句表示一个通信操作。当 select 语句执行时，它会选择其中一个可执行的 case 子句，并执行相应的操作
	select {}
}

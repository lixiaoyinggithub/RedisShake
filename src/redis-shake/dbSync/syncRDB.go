package dbSync

import (
	"bufio"
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/alibaba/RedisShake/pkg/libs/log"
	"github.com/alibaba/RedisShake/redis-shake/base"
	utils "github.com/alibaba/RedisShake/redis-shake/common"
	conf "github.com/alibaba/RedisShake/redis-shake/configure"
	"github.com/alibaba/RedisShake/redis-shake/filter"
	"github.com/alibaba/RedisShake/redis-shake/metric"
)

func (ds *DbSyncer) syncRDBFile(reader *bufio.Reader, target []string, authType, passwd string, nsize int64, tlsEnable bool, tlsSkipVerify bool) {
	// 创建RDB加载并返回管道，读取到的命令保存在管道
	pipe := utils.NewRDBLoader(reader, &ds.stat.rBytes, base.RDBPipeSize)

	wait := make(chan struct{})

	currentTime := time.Now().Unix()

	log.Infof("RDB同步开始时间：", currentTime)

	// 协程的方式执行
	go func() {
		// 这个协程执行结束的时候执行关闭，在select的地方也关闭
		defer close(wait)
		// 可以让一组协程执行结束后，再继续，类似java的栅栏
		var wg sync.WaitGroup
		wg.Add(conf.Options.Parallel)
		for i := 0; i < conf.Options.Parallel; i++ {
			// 根据配置的并发度，并发读取多个db
			go func() {

				log.Infof("Parallel start")

				defer wg.Done()
				targetConnect := utils.OpenRedisConn(target, authType, passwd, conf.Options.TargetType == conf.RedisTypeCluster,
					tlsEnable, tlsSkipVerify)
				defer targetConnect.Close()
				// 定时器会按照指定的时间间隔重复触发一个事件。
				var ticker = time.NewTicker(time.Second)
				defer ticker.Stop()
				var lastdb uint32 = 0

				// pipeSize := 10
				// pipes := make([]chan *rdb.BinEntry, pipeSize)
				// for i := 0; i < pipeSize; i++ {
				// 	pipes[i] = make(chan *rdb.BinEntry, base.RDBPipeSize)
				// }

				// 遍历pipe的元素（从RDB读取到的entry），直到管道关闭
				for entry := range pipe {
					// DB层面的过滤
					if filter.FilterDB(int(entry.DB)) {
						// db filter
						ds.stat.fullSyncFilter.Incr()
					} else {
						ds.stat.keys.Incr()

						log.Debugf("DbSyncer[%d] try restore key[%s] with value length[%v]", ds.id, entry.Key, len(entry.Value))

						if conf.Options.TargetDB != -1 {
							if conf.Options.TargetDB != int(lastdb) {
								lastdb = uint32(conf.Options.TargetDB)
								utils.SelectDB(targetConnect, uint32(conf.Options.TargetDB))
							}
						} else if tdb, ok := conf.Options.TargetDBMap[int(entry.DB)]; ok {
							lastdb = uint32(tdb)
							utils.SelectDB(targetConnect, lastdb)
						} else {
							if entry.DB != lastdb {
								lastdb = entry.DB
								utils.SelectDB(targetConnect, lastdb)
							}
						}
						// key的过滤
						if filter.FilterKey(string(entry.Key)) == true {
							// 1. judge if not pass filter key
							ds.stat.fullSyncFilter.Incr()
							select {
							case <-ticker.C:
								if _, err := targetConnect.Do("PING"); err != nil {
									log.Infof("PING ", err)
								}
							default:
							}
							continue
						} else {
							slot := int(utils.KeyToSlot(string(entry.Key)))
							if filter.FilterSlot(slot) == true {
								// 2. judge if not pass filter slot
								ds.stat.fullSyncFilter.Incr()
								select {
								case <-ticker.C:
									if _, err := targetConnect.Do("PING"); err != nil {
										log.Infof("PING ", err)
									}
								default:
								}
								continue
							}
						}

						goroutineId := getGoroutineID()
						log.Debugf("DbSyncer[%d] start restoring key[%s] with value length[%v],gid=[%d]", ds.id, entry.Key, len(entry.Value), goroutineId)

						// pipeIndex := int(crc32.ChecksumIEEE(entry.Key)) % pipeSize
						// pipes[pipeIndex] <- entry
						// 转存entry
						utils.RestoreRdbEntry(targetConnect, entry)

						log.Debugf("DbSyncer[%d] restore key[%s] ok", ds.id, entry.Key)
					}
				} //  一直循环直到pipe结束

			}()
		}

		// var wgChild sync.WaitGroup
		// wgChild.Add(10)
		// for i := 0; i < 10; i++ {
		// 	go func() {
		// 		defer wgChild.Done()
		// 		for i := 0; i < pi; i++ {

		// 		}
		// 	}()
		// }
		// wgChild.Wait()

		//等待所有协程执行完毕
		wg.Wait()
	}()

	var stat *syncerStat

	// print stat

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}

		stat = ds.stat.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "DbSyncer[%d] total = %s - %12s [%3d%%]  entry=%-12d",
			ds.id, utils.GetMetric(nsize), utils.GetMetric(stat.rBytes), 100*stat.rBytes/nsize, stat.keys)
		if stat.fullSyncFilter != 0 {
			fmt.Fprintf(&b, "  filter=%-12d", stat.fullSyncFilter)
		}
		log.Info(b.String())
		metric.GetMetric(ds.id).SetFullSyncProgress(ds.id, uint64(100*stat.rBytes/nsize))
	}
	log.Infof("DbSyncer[%d] sync rdb done,currentTime=[%d],t=[%d]", ds.id, time.Now().Unix(), time.Now().Unix()-currentTime)
}

func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idStr := string(buf[:n])

	var id int64
	fmt.Sscanf(idStr, "goroutine %d", &id)

	return id
}

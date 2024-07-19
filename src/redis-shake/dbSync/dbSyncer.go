package dbSync

import (
	"bufio"
	"io"

	"github.com/alibaba/RedisShake/pkg/libs/log"
	"github.com/alibaba/RedisShake/redis-shake/base"
	utils "github.com/alibaba/RedisShake/redis-shake/common"
	"github.com/alibaba/RedisShake/redis-shake/heartbeat"
	"github.com/alibaba/RedisShake/redis-shake/metric"

	"github.com/alibaba/RedisShake/redis-shake/checkpoint"
	conf "github.com/alibaba/RedisShake/redis-shake/configure"
)

// one sync link corresponding to one DbSyncer
func NewDbSyncer(id int, source, sourcePassword string, target []string, targetPassword string,
	slotLeftBoundary, slotRightBoundary int, httpPort int) *DbSyncer {
	ds := &DbSyncer{
		id:                         id,
		source:                     source,
		sourcePassword:             sourcePassword,
		target:                     target,
		targetPassword:             targetPassword,
		slotLeftBoundary:           slotLeftBoundary,
		slotRightBoundary:          slotRightBoundary,
		httpProfilePort:            httpPort,
		enableResumeFromBreakPoint: conf.Options.ResumeFromBreakPoint,
		checkpointName:             utils.CheckpointKey, // default, may be modified
		WaitFull:                   make(chan struct{}),
	}

	// add metric
	metric.AddMetric(id)

	return ds
}

type DbSyncer struct {
	id int // current id in all syncer

	source            string   // source address
	sourcePassword    string   // source password
	target            []string // target address
	targetPassword    string   // target password
	runId             string   // source runId
	slotLeftBoundary  int      // mark the left slot boundary if enable resuming from break point and is cluster
	slotRightBoundary int      // mark the right slot boundary if enable resuming from break point and is cluster
	httpProfilePort   int      // http profile port

	// stat info
	stat Status

	startDbId                  int    // use in break resume from break-point
	enableResumeFromBreakPoint bool   // enable?
	checkpointName             string // checkpoint name, if is shard, this name has suffix

	/*
	 * this channel is used to calculate delay between redis-shake and target redis.
	 * Once oplog sent, the corresponding delayNode push back into this queue. Next time
	 * receive reply from target redis, the front node popped and then delay calculated.
	 */
	delayChannel chan *delayNode

	fullSyncOffset int64          // full sync offset value
	sendBuf        chan cmdDetail // sending queue
	WaitFull       chan struct{}  // wait full sync done
}

func (ds *DbSyncer) GetExtraInfo() map[string]interface{} {
	return map[string]interface{}{
		"SourceAddress":        ds.source,
		"TargetAddress":        ds.target,
		"SenderBufCount":       len(ds.sendBuf),
		"ProcessingCmdCount":   len(ds.delayChannel),
		"TargetDBOffset":       ds.stat.targetOffset.Get(),
		"SourceMasterDBOffset": ds.stat.sourceMasterOffset.Get(),
		"SourceDBOffset":       ds.stat.sourceOffset.Get(),
	}
}

// main
func (ds *DbSyncer) Sync() {
	log.Infof("DbSyncer[%d] starts syncing data from %v to %v with http[%v], enableResumeFromBreakPoint[%v], "+
		"slot boundary[%v, %v]", ds.id, ds.source, ds.target, ds.httpProfilePort, ds.enableResumeFromBreakPoint,
		ds.slotLeftBoundary, ds.slotRightBoundary)

	var err error
	runId, offset, dbid := "?", int64(-1), 0
	if ds.enableResumeFromBreakPoint {
		// assign the checkpoint name with suffix if is cluster
		if ds.slotLeftBoundary != -1 {
			ds.checkpointName = utils.ChoseSlotInRange(utils.CheckpointKey, ds.slotLeftBoundary, ds.slotRightBoundary)
		}

		// checkpoint reload if has
		log.Infof("DbSyncer[%d] enable resume from break point, try to load checkpoint", ds.id)
		runId, offset, dbid, err = checkpoint.LoadCheckpoint(ds.id, ds.source, ds.target, conf.Options.TargetAuthType,
			ds.targetPassword, ds.checkpointName, conf.Options.TargetType == conf.RedisTypeCluster, conf.Options.SourceTLSEnable,
			conf.Options.SourceTLSSkipVerify)
		if err != nil {
			log.Panicf("DbSyncer[%d] load checkpoint from %v failed[%v]", ds.id, ds.target, err)
			return
		}
		log.Infof("DbSyncer[%d] checkpoint info: runId[%v], offset[%v] dbid[%v]", ds.id, runId, offset, dbid)
	}

	// 修改状态
	base.Status = "waitfull"
	var input io.ReadCloser
	var nsize int64
	var isFullSync bool
	if conf.Options.Psync {
		// psync是Redis 2.8版本引入的命令，用于在主节点和从节点之间进行部分同步。当从节点与主节点断开连接后重新连接时，从节点会向主节点发送psync命令，
		// 主节点会根据从节点的请求情况，选择全量同步或增量同步的方式进行数据同步。如果从节点的复制偏移量（replication offset）在主节点的复制积压缓冲区（replication backlog）之内，主节点会执行增量同步，
		// 只传输从节点缺失的部分数据；如果从节点的复制偏移量超出了主节点的复制积压缓冲区，主节点会执行全量同步，传输整个数据集
		log.Infof("sendSyncCmd psync")
		input, nsize, isFullSync, runId = ds.sendPSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword,
			conf.Options.SourceTLSEnable, conf.Options.SourceTLSSkipVerify, runId, offset)
		ds.runId = runId
	} else {
		log.Infof("sendSyncCmd sync")
		// sync sync是Redis 2.6版本之前使用的命令，用于在主节点和从节点之间进行全量同步
		input, nsize = ds.sendSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword,
			conf.Options.SourceTLSEnable, conf.Options.SourceTLSSkipVerify)
		isFullSync = true // 因为psync默认为false，那么磨人会执行到这个位置，所以默认就是全量同步
	}
	defer input.Close()

	// start heartbeat
	if len(conf.Options.HeartbeatUrl) > 0 {
		heartbeatCtl := heartbeat.HeartbeatController{
			ServerUrl: conf.Options.HeartbeatUrl,
			Interval:  int32(conf.Options.HeartbeatInterval),
		}
		go heartbeatCtl.Start()
	}

	// input可以理解为一个来自网络的输入，具体类型为net.Conn；通过它实例化一个reader
	reader := bufio.NewReaderSize(input, utils.ReaderBufferSize)

	//如果为全量同步，读取RDB文件同步存量的数据
	if isFullSync {
		// sync rdb
		log.Infof("DbSyncer[%d] rdb file size = %d", ds.id, nsize)
		base.Status = "full"
		ds.syncRDBFile(reader, ds.target, conf.Options.TargetAuthType, ds.targetPassword, nsize, conf.Options.TargetTLSEnable, conf.Options.TargetTLSSkipVerify)
		ds.startDbId = 0
	} else {
		log.Infof("DbSyncer[%d] run incr-sync directly with db_id[%v]", ds.id, dbid)
		ds.startDbId = dbid
		// set fullSyncProgress to 100 when skip full sync stage
		metric.GetMetric(ds.id).SetFullSyncProgress(ds.id, 100)
	}

	// sync increment; 转为增量同步
	base.Status = "incr"
	close(ds.WaitFull)
	// 同步命令，传入了reader，可以读取来自数据源网络的命令
	//ds.syncCommand(reader, ds.target, conf.Options.TargetAuthType, ds.targetPassword, conf.Options.TargetTLSEnable, conf.Options.TargetTLSSkipVerify, dbid)
}

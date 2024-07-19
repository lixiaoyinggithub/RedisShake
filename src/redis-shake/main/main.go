// Copyright 2019 Aliyun Cloud.
// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"reflect"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/alibaba/RedisShake/pkg/libs/log"
	run "github.com/alibaba/RedisShake/redis-shake"
	"github.com/alibaba/RedisShake/redis-shake/base"
	utils "github.com/alibaba/RedisShake/redis-shake/common"
	conf "github.com/alibaba/RedisShake/redis-shake/configure"
	"github.com/alibaba/RedisShake/redis-shake/metric"
	"github.com/alibaba/RedisShake/redis-shake/restful"

	nimo "github.com/gugemichael/nimo4go"
)

type Exit struct{ Code int }

const (
	defaultHttpPort    = 9320
	defaultSystemPort  = 9310
	defaultSenderSize  = 65535
	defaultSenderCount = 1024
)

func main() {
	var err error
	// 在main方法结束时调用
	// defer handleExit()
	// defer utils.Goodbye()

	// argument options ； flag是标准库中处理命令行参数的库
	// 接收3个参数：配置路径；使用的目的或者类型：解码，转存，下载，同步，rump是什么意思？版本号
	// 命令行的启动方式
	configuration := flag.String("conf", "", "configuration path")
	tp := flag.String("type", "", "run type: decode, restore, dump, sync, rump")
	version := flag.Bool("version", false, "show version")

	//本地调试
	// var versionVal bool = false
	// var tp *string
	// var configuration *string
	// var version *bool

	// var confVal string = "/Users/aly/gospace/RedisShake/conf/redis-shake.conf"
	// var tpVal string = "sync"
	// configuration = &confVal
	// tp = &tpVal
	// version = &versionVal

	flag.Parse()

	if *version {
		fmt.Println(utils.Version)
		return
	}

	if *configuration == "" || *tp == "" {
		if !*version {
			fmt.Println("Please show me the '-conf' and '-type'")
		}
		fmt.Println(utils.Version)
		flag.PrintDefaults()
		return
	}

	// 在导入包的时候就定义了conf，而conf里面有Options的公共变量
	conf.Options.Version = utils.Version
	conf.Options.Type = *tp // 将操作类型赋值给Options的Type

	var file *os.File
	if file, err = os.Open(*configuration); err != nil {
		crash(fmt.Sprintf("Configure file open failed. %v", err), -1)
	}

	// read fcv and do comparison
	if _, err := utils.CheckFcv(*configuration, utils.FcvConfiguration.FeatureCompatibleVersion); err != nil {
		crash(err.Error(), -5)
	}

	configure := nimo.NewConfigLoader(file)
	configure.SetDateFormat(utils.GolangSecurityTime)
	if err := configure.Load(&conf.Options); err != nil {
		crash(fmt.Sprintf("Configure file %s parse failed. %v", *configuration, err), -2)
	}

	// verify parameters
	if err = SanitizeOptions(*tp); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	initSignal()
	initFreeOS()
	nimo.Profiling(int(conf.Options.SystemProfile))
	utils.Welcome()
	utils.StartTime = fmt.Sprintf("%v", time.Now().Format(utils.GolangSecurityTime))

	if err = utils.WritePidById(conf.Options.Id, conf.Options.PidPath); err != nil {
		crash(fmt.Sprintf("write pid failed. %v", err), -5)
	}

	// create runner；根据类型创建不同的runner，这里使用了多态和封装
	var runner base.Runner
	switch *tp {
	case conf.TypeDecode:
		runner = new(run.CmdDecode)
	case conf.TypeRestore:
		runner = new(run.CmdRestore)
	case conf.TypeDump:
		runner = new(run.CmdDump)
	case conf.TypeSync:
		runner = new(run.CmdSync)
	case conf.TypeRump:
		runner = new(run.CmdRump)
	}

	// create metric
	metric.CreateMetric(runner)
	// 协程的方式启动服务
	go startHttpServer()

	// print configuration
	if opts, err := json.Marshal(conf.GetSafeOptions()); err != nil {
		crash(fmt.Sprintf("marshal configuration failed[%v]", err), -6)
	} else {
		log.Infof("redis-shake configuration: %s", string(opts))
	}

	// 开始运行实现类的入口方法
	runner.Main()

	log.Infof("execute runner[%v] finished!", reflect.TypeOf(runner))
}

func initSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Info("receive signal: ", sig)

		if utils.LogRotater != nil {
			utils.LogRotater.Rotate()
		}

		os.Exit(0)
	}()
}

func initFreeOS() {
	go func() {
		for {
			debug.FreeOSMemory()
			time.Sleep(5 * time.Second)
		}
	}()
}

func startHttpServer() {
	if conf.Options.HttpProfile == -1 {
		return
	}

	utils.InitHttpApi(conf.Options.HttpProfile)
	utils.HttpApi.RegisterAPI("/conf", nimo.HttpGet, func([]byte) interface{} {
		return conf.GetSafeOptions()
	})
	restful.RestAPI()

	if err := utils.HttpApi.Listen(); err != nil {
		crash(fmt.Sprintf("start http listen error[%v]", err), -4)
	}
}

func crash(msg string, errCode int) {
	fmt.Println(msg)
	panic(Exit{errCode})
}

func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(Exit); ok == true {
			os.Exit(exit.Code)
		}
		panic(e)
	}
}

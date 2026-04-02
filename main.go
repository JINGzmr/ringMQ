package main

import (
	"flag"
	"fmt"
	Server "ringMQ/server"
	"ringMQ/zookeeper"
)

func main() {
	// 定义命令行参数接收变量
	var mode string
	var zkAddr string
	var zkTimeout int
	var zkRoot string
	var name string
	var me int
	var zkServerAddr string
	var brokerAddr string
	var raftAddr string

	// 绑定命令行参数
	flag.StringVar(&mode, "mode", "", "start mode: zkserver or broker")
	flag.StringVar(&zkAddr, "zk", "127.0.0.1:2181", "zookeeper host:port")
	flag.IntVar(&zkTimeout, "zk-timeout", 20, "zookeeper timeout in seconds")
	flag.StringVar(&zkRoot, "zk-root", "/ringMQ", "zookeeper root path")
	flag.StringVar(&name, "name", "", "server name")
	flag.IntVar(&me, "me", 0, "broker id")
	flag.StringVar(&zkServerAddr, "zkserver", ":7878", "zkserver listen addr for mode=zkserver, zkserver host:port for mode=broker")
	flag.StringVar(&brokerAddr, "broker", ":7774", "broker listen addr")
	flag.StringVar(&raftAddr, "raft", ":7331", "raft listen addr")

	// 解析命令行参数
	flag.Parse()

	// 初始化ZooKeeper配置信息
	zkInfo := zookeeper.ZkInfo{
		HostPorts: []string{zkAddr},
		Timeout:   zkTimeout,
		Root:      zkRoot,
	}

	// 根据启动模式执行不同逻辑
	switch mode {
	case "zkserver":
		// 未指定名称则使用默认值
		if name == "" {
			name = "ZKServer"
		}
		// 创建并启动ZKServer服务
		Server.NewZKServerAndStart(zkInfo, Server.Options{
			Name:               name,
			Tag:                Server.ZKBROKER,
			Zkserver_Host_Port: zkServerAddr,
		})
	case "broker":
		// 未指定名称则自动生成节点名称
		if name == "" {
			name = fmt.Sprintf("Broker%d", me)
		}
		// 创建并启动Broker服务
		Server.NewBrokerAndStart(zkInfo, Server.Options{
			Me:                 me,
			Name:               name,
			Tag:                Server.BROKER,
			Zkserver_Host_Port: zkServerAddr,
			Broker_Host_Port:   brokerAddr,
			Raft_Host_Port:     raftAddr,
		})
	default:
		// 模式错误，打印使用说明
		fmt.Println("usage:")
		fmt.Println("  go run . -mode=zkserver -zk=127.0.0.1:2181 -zkserver=:7878")
		fmt.Println("  go run . -mode=broker -me=0 -name=Broker0 -zk=127.0.0.1:2181 -zkserver=127.0.0.1:7878 -broker=:7774 -raft=:7331")
		return
	}

	// 阻塞主协程，保持服务持续运行,不写这行，服务启动后会直接结束
	select {}
}

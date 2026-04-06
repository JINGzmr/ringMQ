package server

import (
	"fmt"
	"ringMQ/kitex_gen/api/server_operations"
	"ringMQ/kitex_gen/api/zkserver_operations"
	"ringMQ/logger"
	"ringMQ/zookeeper"

	// "github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/server"
)

// RPCServer 结构体：统一管理两种RPC服务
// 1. 对外服务：处理生产者/消费者的消息请求
// 2. 对内服务：处理Broker集群管理与调度
type RPCServer struct {
	srv_cli  *server.Server   // 对外提供服务的RPC Server（客户端连接）
	srv_bro  *server.Server   // 对内提供服务的RPC Server（Broker/注册中心）
	zkinfo   zookeeper.ZkInfo // Zookeeper配置信息
	server   *Server          // Broker业务服务实例（处理消息存储、转发）
	zkserver *ZkServer        // ZKServer业务服务实例（集群管理、服务发现）
}

// NewRpcServer 构造函数：创建RPCServer实例并初始化日志
// 参数：zkinfo Zookeeper配置
// 返回：初始化好的RPCServer实例
func NewRpcServer(zkinfo zookeeper.ZkInfo) RPCServer {
	// 初始化日志系统
	logger.LOGinit()
	// 返回实例，仅保存ZK配置
	return RPCServer{
		zkinfo: zkinfo,
	}
}

// Start 根据服务类型启动对应的RPC服务
// 参数：
//
//	opts_cli: 客户端服务配置
//	opts_zks: ZKServer服务配置
//	opts_raf: Raft集群服务配置
//	opt: 服务启动参数
func (s *RPCServer) Start(opts_cli, opts_zks, opts_raf []server.Option, opt Options) error {
	// 根据Tag判断启动Broker还是ZKServer
	switch opt.Tag {
	case BROKER:
		// 创建Broker业务实例
		s.server = NewServer(s.zkinfo)
		// 初始化Broker与Raft配置
		s.server.make(opt, opts_raf)

		// 创建对外提供消息服务的RPC Server
		srv_cli_bro := server_operations.NewServer(s, opts_cli...)
		s.srv_cli = &srv_cli_bro

		logger.DEBUG(logger.DLog, "%v the raft %v start rpcserver for clients\n", opt.Name, opt.Me)
		// 协程启动服务，避免阻塞主线程
		go func() {
			err := srv_cli_bro.Run()
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	case ZKBROKER:
		// 创建ZKServer业务实例
		s.zkserver = NewZKServer(s.zkinfo)
		// 初始化ZKServer配置
		s.zkserver.make(opt)

		// 创建对内提供集群管理的RPC Server
		srv_bro_cli := zkserver_operations.NewServer(s, opts_zks...)
		s.srv_bro = &srv_bro_cli

		logger.DEBUG(logger.DLog, "ZkServer start rpcserver for brokers\n")
		// 协程启动服务
		go func() {
			err := srv_bro_cli.Run()
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}

	return nil
}

// ShutDown_server 关闭所有RPC服务
// 停止对外、对内的监听端口，释放资源
func (s *RPCServer) ShutDown_server() {
	if s.srv_bro != nil {
		// 停止对内RPC服务
		(*s.srv_bro).Stop()
	}
	if s.srv_cli != nil {
		// 停止对外RPC服务
		(*s.srv_cli).Stop()
	}
}

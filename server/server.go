package server

/*实现了一个分布式消息队列（ringMQ）的 Broker 服务核心逻辑，基于 Kitex 框架、Zookeeper 服务注册 / 发现、Raft 一致性协议，提供了消息生产、消费、分区管理、集群容错（主从同步）等核心能力。以下是按模块拆解的核心功能：
一、基础初始化与集群注册
Zookeeper 集成
Broker 启动时连接 Zookeeper，创建永久节点（存储 Broker 基本信息：名称、ID、Raft / 业务端口）和临时节点（用于服务状态监控，故障时自动清理）。
向 ZKServer 注册自身信息，完成集群节点发现。
本地资源初始化
创建本地目录（按 Broker 名称），用于存储消息分区文件。
初始化核心数据结构：topics（主题映射）、consumers（消费者连接）、brokers（Raft 集群节点客户端）、parts_rafts（分区级 Raft 管理）等。
启动 Raft 服务，为每个分区维护独立的 Raft 集群，保障消息一致性。
二、Raft 一致性与主从管理
分区级 Raft 集群管理
AddRaftHandle：为指定 Topic-Partition 初始化 Raft 集群，构建节点间客户端连接，加入 Raft 集群。
CloseRaftHandle：销毁指定分区的 Raft 集群。
BecomeLeader：当选 Leader 后向 ZKServer 上报，更新集群主节点状态。
消息同步（ApplyCh 管道）
Raft 提交的日志通过 applych 管道异步处理：Leader 负责写入消息，Follower 同步日志；GetApplych 消费管道消息，完成消息落地或 Leader 状态更新。
三、消息生产（Push）
生产前校验与准备
PrepareAcceptHandle：检查 Topic/Partition 是否存在，不存在则创建；初始化分区文件、文件句柄（FD）、索引等元信息。
PrepareState：校验 Raft 集群状态（如是否已启动），确保生产条件就绪。
消息写入策略（按 ACK 级别）
PushHandle 处理生产者推送的消息，支持 3 种 ACK 策略：
ack=-1：Raft 同步后写入（强一致性）；
ack=1：Leader 直接写入，不等待 Follower 同步（高可用）；
ack=0：异步写入，直接返回（最高性能）。
分区文件管理
CloseAcceptHandle：停止 Leader 节点的消息接收，重命名分区文件（如 NowBlock → 历史块），Follower 继续拉取直至 EOF。
四、消息消费（Pull/Fetch）
1. 主动拉取（Pull）
PrepareSendHandle：校验 Topic / 订阅关系，初始化消费配置，设置超时机制。
PullHandle：处理消费者的 Pull 请求，支持两种消费模式：
PTP（点对点）：消费后更新 Zookeeper 偏移量（Offset），保障消息仅被消费一次；
PSB（发布订阅）：无需负载均衡，每个消费者独立消费分区。
StartGet：为消费者启动独立协程消费指定 Topic-Partition，处理负载均衡（PTP）或直接消费（PSB）。
2. 主从同步（Fetch，Follower 向 Leader 拉取）
AddFetchHandle：Leader 为 Follower 准备 Pull 节点；Follower 连接 Leader，启动异步拉取协程。
FetchMsg：Follower 核心拉取逻辑：
持续向 Leader 拉取消息，落地到本地分区文件；
拉取失败（≥3 次）时向 ZKServer 查询新 Leader，自动切换；
更新 Zookeeper 同步偏移量（Dup），确保主从数据一致；
支持 “历史块” 和 “当前块（NowBlock）” 两种文件的同步。
CloseFetchHandle：停止指定分区的拉取同步，清理本地映射。
五、消费者管理
连接与状态监控
InfoHandle：建立与消费者的 Kitex 客户端连接，缓存到 consumers 映射。
CheckConsumer：监控消费者状态，若消费者下线，自动清理其订阅关系并重新平衡负载。
订阅 / 取消订阅
SubHandle：为消费者添加 Topic-Partition 订阅关系，维护订阅元数据。
UnSubHandle：移除消费者的订阅关系，清理相关资源。
六、容错与高可用
Leader 切换
Follower 拉取失败时自动查询 ZKServer 获取新 Leader，重建连接。
Raft 集群通过选举保障 Leader 高可用，Leader 节点故障时自动切换。
数据一致性
Raft 日志同步保障消息不丢失、不重复；
主从同步（Fetch）确保 Follower 与 Leader 数据一致，Leader 故障后可快速切换。
七、辅助功能
配置加载
IntiBroker（未启用）：从 Zookeeper 加载历史配置，恢复 Topic/Partition 元数据。
日志与监控
集成日志模块（logger），输出不同级别日志（错误、调试、普通），便于问题排查。
文件系统管理
CheckList：检查本地目录是否存在，不存在则创建，保障消息文件存储路径。*/

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"ringMQ/kitex_gen/api"
	"ringMQ/kitex_gen/api/client_operations"
	"ringMQ/kitex_gen/api/raft_operations"
	"ringMQ/kitex_gen/api/server_operations"
	"ringMQ/kitex_gen/api/zkserver_operations"
	"ringMQ/logger"
	"ringMQ/zookeeper"
	"sync"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

const (
	NODE_SIZE = 24
)

type Server struct {
	Name     string
	me       int
	zk       *zookeeper.ZK
	zkclient zkserver_operations.Client
	mu       sync.RWMutex

	aplych    chan info
	topics    map[string]*Topic
	consumers map[string]*Client
	brokers   map[string]*raft_operations.Client

	//raft
	parts_rafts *parts_raft

	//fetch
	parts_fetch   map[string]string                    //topicName + partitionName to broker HostPort
	brokers_fetch map[string]*server_operations.Client //brokerName to Client
}

type Key struct {
	Size        int64 `json:"size"`
	Start_index int64 `json:"start_index"`
	End_index   int64 `json:"end_index"`
}

type Message struct {
	Index      int64  `json:"index"`
	Size       int8   `json:"size"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

// type Msg struct {
// 	producer string
// 	topic    string
// 	key      string
// 	msg      []byte
// }

type info struct {
	// name       string //broker name
	topic_name string
	part_name  string
	file_name  string
	new_name   string
	option     int8
	offset     int64
	size       int8

	ack int8

	producer string
	consumer string
	cmdindex int64
	// startIndex  int64
	// endIndex  	int64
	message []byte

	//raft
	brokers map[string]string
	brok_me map[string]int
	me      int

	//fetch
	LeaderBroker string
	HostPort     string

	//update dup
	zkclient   *zkserver_operations.Client
	BrokerName string
	// BlockName string
}

// type startget struct {
// 	cli_name   string
// 	topic_name string
// 	part_name  string
// 	index      int64
// 	option     int8
// }

func NewServer(zkinfo zookeeper.ZkInfo) *Server {
	return &Server{
		zk: zookeeper.NewZK(zkinfo), //连接上zookeeper
		mu: sync.RWMutex{},
	}
}

func (s *Server) make(opt Options, opt_cli []server.Option) {

	s.consumers = make(map[string]*Client)
	s.topics = make(map[string]*Topic)
	s.brokers = make(map[string]*raft_operations.Client)
	s.parts_fetch = make(map[string]string)
	s.brokers_fetch = make(map[string]*server_operations.Client)
	s.aplych = make(chan info)

	s.CheckList()
	s.Name = opt.Name
	s.me = opt.Me

	//本地创建parts——raft，为raft同步做准备
	s.parts_rafts = NewParts_Raft()
	go s.parts_rafts.Make(opt.Name, opt_cli, s.aplych, s.me)
	s.parts_rafts.StartServer()

	//在zookeeper上创建一个永久节点, 若存在则不需要创建
	err := s.zk.RegisterNode(zookeeper.BrokerNode{
		Name:         s.Name,
		Me:           s.me,
		BrokHostPort: opt.Broker_Host_Port,
		RaftHostPort: opt.Raft_Host_Port,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	//创建临时节点,用于zkserver的watch
	err = s.zk.CreateState(s.Name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//连接zkServer，并将自己的Info发送到zkServer,
	zkclient, err := zkserver_operations.NewClient(opt.Name, client.WithHostPorts(opt.Zkserver_Host_Port))
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	s.zkclient = zkclient

	resp, err := zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BrokerName:     opt.Name,
		BrokerHostPort: opt.Broker_Host_Port,
	})
	if err != nil || !resp.Ret {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//开启获取管道中的内容，写入文件或更新leader
	go s.GetApplych(s.aplych)

	// s.IntiBroker() 根据zookeeper上的历史信息，加载缓存信息
}

// 接收applych管道的内容
// 写入partition文件中
func (s *Server) GetApplych(applych chan info) {

	for msg := range applych {

		if msg.producer == "Leader" {
			s.BecomeLeader(msg) //成为leader
		} else {
			s.mu.RLock()
			topic, ok := s.topics[msg.topic_name]
			s.mu.RUnlock()

			logger.DEBUG(logger.DLog, "S%d the message from applych is %v\n", s.me, msg)
			if !ok {
				logger.DEBUG(logger.DError, "topic(%v) is not in this broker\n", msg.topic_name)
			} else {
				msg.me = s.me
				msg.BrokerName = s.Name
				msg.zkclient = &s.zkclient
				msg.file_name = "NowBlock.txt"
				topic.addMessage(msg) //信息同步
			}
		}
	}
}

func (s *Server) BecomeLeader(in info) {
	resp, err := s.zkclient.BecomeLeader(context.Background(), &api.BecomeLeaderRequest{
		Broker:    s.Name,
		Topic:     in.topic_name,
		Partition: in.part_name,
	})
	if err != nil || !resp.Ret {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
}

// IntiBroker
// not used
// 获取该Broker需要负责的Topic和Partition,并在本地创建对应配置
func (s *Server) IntiBroker() {
	s.mu.Lock()
	info := Property{
		Name:  s.Name,
		Power: 1,
		//获取Broker性能指标
	}
	data, err := json.Marshal(info)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	resp, err := s.zkclient.BroGetConfig(context.Background(), &api.BroGetConfigRequest{
		Propertyinfo: data,
	})

	if err != nil || !resp.Ret {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	BroInfo := BroNodeInfo{
		Topics: make(map[string]TopNodeInfo),
	}
	json.Unmarshal(resp.Brokerinfo, &BroInfo)

	s.HandleTopics(BroInfo.Topics)

	s.mu.Unlock()
}

// HandleTopics not used
func (s *Server) HandleTopics(Topics map[string]TopNodeInfo) {

	for topic_name, topic := range Topics {
		_, ok := s.topics[topic_name]
		if !ok {
			top := NewTopic(s.Name, topic_name)
			top.HandleParttitions(topic.Partitions)

			s.topics[topic_name] = top
		} else {
			logger.DEBUG(logger.DWarn, "This topic(%v) had in s.topics\n", topic_name)
		}
	}
}

func (s *Server) CheckList() {
	str, _ := os.Getwd()
	str += "/" + s.Name
	ret := CheckFileOrList(str)
	// logger.DEBUG(logger.DLog, "Check list %v is %v\n", str, ret)
	if !ret {
		CreateList(str)
	}
}

const (
	ErrHadStart = "this partition had Start"
)

func (s *Server) PrepareState(in info) (ret string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch in.option {
	case -1:
		ok := s.parts_rafts.CheckPartState(in.topic_name, in.part_name)
		if !ok {
			ret = "the raft not exits"
			err = errors.New(ret)
		}
	default:

	}

	return ret, err
}

// PrepareAcceptHandle
// 准备接收信息，
// 检查topic和partition是否存在，不存在则需要创建，
// 设置partition中的file和fd，start_index等信息
func (s *Server) PrepareAcceptHandle(in info) (ret string, err error) {
	//检查或创建完topic
	s.mu.Lock()
	topic, ok := s.topics[in.topic_name]
	if !ok {
		topic = NewTopic(s.Name, in.topic_name)
		s.topics[in.topic_name] = topic
	}

	s.mu.Unlock()
	//检查或创建partition
	return topic.PrepareAcceptHandle(in)
}

// 停止接收文件，并将文件名修改成newfilename
// 指Nowblock中的Leader停止接收信息，副本可继续Pull信息，当EOF后关闭
func (s *Server) CloseAcceptHandle(in info) (start, end int64, ret string, err error) {
	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	if !ok {
		ret = "this topic is not in this broker"
		logger.DEBUG(logger.DError, "this topic(%d) is not in this broker\n", in.topic_name)
		return 0, 0, ret, errors.New(ret)
	}
	s.mu.RUnlock()
	return topic.CloseAcceptPart(in)
}

// PrepareSendHandle
// 准备发送信息，
// 检查topic和subscription是否存在，不存在则需要创建
// 检查该文件的config是否存在，不存在则创建，并开启协程
// 协程设置超时时间，时间到则关闭
func (s *Server) PrepareSendHandle(in info) (ret string, err error) {
	//检查或创建topic
	s.mu.Lock()
	topic, ok := s.topics[in.topic_name]
	if !ok {
		logger.DEBUG(logger.DLog, "%v not have topic(%v), create topic\n", s.Name, in.topic_name)
		topic = NewTopic(s.Name, in.topic_name)
		s.topics[in.topic_name] = topic
	}
	s.mu.Unlock()
	//检查或创建partition
	return topic.PrepareSendHandle(in, &s.zkclient)
}

func (s *Server) AddRaftHandle(in info) (ret string, err error) {
	//检测该Partition的Raft是否已经启动

	logger.DEBUG(logger.DLog, "the raft brokers is %v\n", in.brokers)

	s.mu.Lock()
	// Me := 0
	index := 0
	nodes := make(map[int]string)
	for k, v := range in.brok_me {
		nodes[v] = k
	}

	var peers []*raft_operations.Client
	for index < len(in.brokers) {
		logger.DEBUG(logger.DLog, "%v index (%v)  Me(%v)   k(%v) == Name(%v)\n", s.Name, index, index, nodes[index], s.Name)
		// if Me == 0 && k == s.Name {
		// 	Me = index
		// }
		bro_cli, ok := s.brokers[nodes[index]]
		if !ok {
			cli, err := raft_operations.NewClient(s.Name, client.WithHostPorts(in.brokers[nodes[index]]))
			if err != nil {
				logger.DEBUG(logger.DError, "%v new raft client fail err %v\n", s.Name, err.Error())
				return ret, err
			}
			s.brokers[nodes[index]] = &cli
			bro_cli = &cli
			logger.DEBUG(logger.DLog, "%v New client to broker %v\n", s.Name, nodes[index])
		} else {
			logger.DEBUG(logger.DLog, "%v had client to broker %v\n", s.Name, nodes[index])
		}
		peers = append(peers, bro_cli)
		index++
	}

	logger.DEBUG(logger.DLog, "the Broker %v raft Me %v\n", s.Name, s.me)
	//检查或创建底层part_raft
	s.parts_rafts.AddPart_Raft(peers, s.me, in.topic_name, in.part_name, s.aplych)

	s.mu.Unlock()

	logger.DEBUG(logger.DLog, "the %v add over\n", s.Name)

	return ret, err
}

func (s *Server) CloseRaftHandle(in info) (ret string, err error) {
	s.mu.RLock()
	err = s.parts_rafts.DeletePart_raft(in.topic_name, in.part_name)
	s.mu.RUnlock()
	if err != nil {
		return err.Error(), err
	}
	return ret, err
}

func (s *Server) AddFetchHandle(in info) (ret string, err error) {
	//检测该Partition的fetch机制是否已经启动

	//检查该topic_partition 是否准备昊accept信息
	ret, err = s.PrepareAcceptHandle(in)
	if err != nil {
		logger.DEBUG(logger.DError, "%v err is %v\n", ret, err)
		return ret, err
	}

	if in.LeaderBroker == s.Name {
		//Leader Broker将准备好接收follower的Pull请求
		s.mu.RLock()
		defer s.mu.RUnlock()
		topic, ok := s.topics[in.topic_name]
		if !ok {
			ret = "this topic is not in this broker"
			logger.DEBUG(logger.DError, "%v, info(%v)\n", ret, in)
			return ret, errors.New(ret)
		}
		logger.DEBUG(logger.DLog, "%v prepare send for follower brokers\n", s.Name)
		//给每个follower broker准备node（PSB_PULL），等待pull请求
		for BrokerName := range in.brokers {
			ret, err = topic.PrepareSendHandle(info{
				topic_name: in.topic_name,
				part_name:  in.part_name,
				file_name:  in.file_name,
				consumer:   BrokerName,
				option:     TOPIC_KEY_PSB_PULL, //PSB_PULL
			}, &s.zkclient)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}
		return ret, err
	} else {

		time.Sleep(time.Microsecond * 100)

		str := in.topic_name + in.part_name + in.file_name
		s.mu.Lock()
		broker, ok := s.brokers_fetch[in.LeaderBroker]
		if !ok {
			logger.DEBUG(logger.DLog, "%v connection the leader broker %v the HP(%v)\n", s.Name, in.LeaderBroker, in.HostPort)
			bro_ptr, err := server_operations.NewClient(s.Name, client.WithHostPorts(in.HostPort))
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return err.Error(), err
			}
			s.brokers_fetch[in.LeaderBroker] = &bro_ptr
			broker = &bro_ptr
		}

		_, ok = s.parts_fetch[str]
		if !ok {
			s.parts_fetch[str] = in.LeaderBroker
		}
		topic, ok := s.topics[in.topic_name]
		if !ok {
			ret = "this topic is not in this broker"
			logger.DEBUG(logger.DError, "%v, info(%v)\n", ret, in)
			return ret, errors.New(ret)
		}
		s.mu.Unlock()

		return s.FetchMsg(in, broker, topic)
	}
}

func (s *Server) CloseFetchHandle(in info) (ret string, err error) {
	str := in.topic_name + in.part_name + in.file_name
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.parts_fetch[str]
	if !ok {
		ret := "this topic-partition is not in this brpoker"
		logger.DEBUG(logger.DError, "this topic(%v)-partition(%v) is not in this brpoker\n", in.topic_name, in.part_name)
		return ret, errors.New(ret)
	} else {

		//关闭NowBlock的fetch机制，将NowBlock更改，需要重新为之前的Block开启fetch机制
		//直到EOF退出
		delete(s.parts_fetch, str)
		return ret, err
	}
}

func (s *Server) InfoHandle(ipport string) error {

	logger.DEBUG(logger.DLog, "get consumer's ip_port %v\n", ipport)

	client, err := client_operations.NewClient("client", client.WithHostPorts(ipport))
	if err == nil {
		logger.DEBUG(logger.DLog, "connect consumer server successful\n")
		s.mu.Lock()
		consumer, ok := s.consumers[ipport]
		if !ok {
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
		go s.CheckConsumer(consumer)
		s.mu.Unlock()
		logger.DEBUG(logger.DLog, "return resp to consumer\n")
		return nil
	}

	logger.DEBUG(logger.DError, "Connect client failed")

	return err
}

func (s *Server) StartGet(in info) (err error) {
	/*
		新开启一个consumer关于一个topic和partition的协程来消费该partition的信息；

		查询是否有该订阅的信息；

		PTP：需要负载均衡

		PSB：不需要负载均衡,每个PSB开一个Part来发送信息
	*/
	err = nil
	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()
		//已经由zkserver检查过是否订阅
		sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)
		//添加到Config后会进行负载均衡，生成新的配置，然后执行新的配置
		return s.topics[in.topic_name].HandleStartToGet(sub_name, in, s.consumers[in.consumer].GetCli())

	case TOPIC_KEY_PSB_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()

		sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)

		logger.DEBUG(logger.DLog, "consumer(%v) start to get topic(%v) partition(%v) offset(%v) in sub(%v)\n", in.consumer, in.topic_name, in.part_name, in.offset, sub_name)

		return s.topics[in.topic_name].HandleStartToGet(sub_name, in, s.consumers[in.consumer].GetCli())
	default:
		err = errors.New("the option is not PTP or PSB")
	}

	return err
}

func (s *Server) CheckConsumer(client *Client) {
	shutdown := client.CheckConsumer()
	if shutdown { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList {
			subscription.ShutdownConsumerInGroup(client.name)
		}
		client.mu.Unlock()
	}
}

// SubHandle
// subscribe 订阅
func (s *Server) SubHandle(in info) (err error) {
	s.mu.Lock()
	logger.DEBUG(logger.DLog, "get sub information\n")
	top, ok := s.topics[in.topic_name]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub, err := top.AddSubScription(in)
	if err != nil {
		s.consumers[in.consumer].AddSubScription(sub)
	}
	// resp.parts = GetPartKeyArray(s.topics[req.topic].GetParts())
	// resp.size = len(resp.parts)
	s.mu.Unlock()

	return nil
}

func (s *Server) UnSubHandle(in info) error {

	s.mu.Lock()
	logger.DEBUG(logger.DLog, "get unsub information\n")
	top, ok := s.topics[in.topic_name]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub_name, err := top.ReduceSubScription(in)
	if err != nil {
		s.consumers[in.consumer].ReduceSubScription(sub_name)
	}

	s.mu.Unlock()
	return nil
}

// start到该partition中的raft集群中
// 收到返回后判断该写入还是返回
func (s *Server) PushHandle(in info) (ret string, err error) {

	logger.DEBUG(logger.DLog, "get Message form producer\n")
	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	part_raft := s.parts_rafts
	s.mu.RUnlock()

	if !ok {
		ret = "this topic is not in this broker"
		logger.DEBUG(logger.DError, "Topic %v, is not in this broker\n", in.topic_name)
		return ret, errors.New(ret)
	}

	switch in.ack {
	case -1: //raft同步,并写入
		ret, err = part_raft.Append(in)
	case 1: //leader写入,不等待同步
		err = topic.addMessage(in)
	case 0: //直接返回
		go topic.addMessage(in)
	}

	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error(), err
	}

	return ret, err
}

// PullHandle
// Pull message
func (s *Server) PullHandle(in info) (MSGS, error) {

	/*
		若该请求属于PTP则
		读取index，获得上次的index，写入zookeeper中
	*/
	logger.DEBUG(logger.DLog, "%v get pull request the in.op(%v) TOP_PTP_PULL(%v)\n", s.Name, in.option, TOPIC_NIL_PTP_PULL)
	if in.option == TOPIC_NIL_PTP_PULL {
		s.zkclient.UpdatePTPOffset(context.Background(), &api.UpdatePTPOffsetRequest{
			Topic:  in.topic_name,
			Part:   in.part_name,
			Offset: in.offset,
		})
	}

	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	s.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DError, "this topic is not in this broker\n")
		return MSGS{}, errors.New("this topic is not in this broker")
	}

	return topic.PullMessage(in)
}

// 主动向leader pull信息，当获取信息失败后将询问zkserver，新的leader
func (s *Server) FetchMsg(in info, cli *server_operations.Client, topic *Topic) (ret string, err error) {
	//向zkserver请求向Leader Broker Pull信息

	//向LeaderBroker发起Pull请求
	//获得本地本地当前文件end_index
	File, fd := topic.GetFile(in)
	index := File.GetIndex(fd)
	index += 1
	// _, err := File.FindOffset(fd, index)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error(), err
	}

	logger.DEBUG(logger.DLog2, "the cli is %v\n", cli)

	if in.file_name != "Nowfile.txt" {
		//当文件名不为nowfile时
		//创建一partition, 并向该File中写入内容

		go func() {

			Partition := NewPartition(s.Name, in.topic_name, in.part_name)
			Partition.StartGetMessage(File, fd, in)
			ice := 0

			for {
				resp, err := (*cli).Pull(context.Background(), &api.PullRequest{
					Consumer: s.Name,
					Topic:    in.topic_name,
					Key:      in.part_name,
					Offset:   index,
					Size:     10,
					Option:   TOPIC_KEY_PSB_PULL,
				})
				num := len(in.file_name)
				if err != nil {
					ice++
					logger.DEBUG(logger.DError, "Err %v, err(%v)\n", resp, err.Error())

					if ice >= 3 {
						time.Sleep(time.Second * 3)
						//询问新的Leader
						resp, err := s.zkclient.GetNewLeader(context.Background(), &api.GetNewLeaderRequest{
							TopicName: in.topic_name,
							PartName:  in.part_name,
							BlockName: in.file_name[:num-4],
						})
						if err != nil {
							logger.DEBUG(logger.DError, "%v\n", err.Error())
						}
						s.mu.Lock()
						_, ok := s.brokers_fetch[in.topic_name+in.part_name]
						if !ok {
							logger.DEBUG(logger.DLog, "this broker(%v) is not connected\n", s.Name)
							leader_bro, err := server_operations.NewClient(s.Name, client.WithHostPorts(resp.HostPort))
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
								return
							}
							s.brokers_fetch[resp.LeaderBroker] = &leader_bro
							cli = &leader_bro
						}
						s.mu.Unlock()
					}
					continue
				}
				if resp.Err == "file EOF" {
					logger.DEBUG(logger.DLog, "This Partition(%d) filename(%d) is over\n", in.part_name, in.file_name)
					fd.Close()
					return
				}
				ice = 0

				if resp.StartIndex <= index && resp.EndIndex > index {
					//index 处于返回包的中间位置
					//需要截断该宝包，并写入
					//your code
					logger.DEBUG(logger.DLog, "need your code\n")
				}

				node := Key{
					Start_index: resp.StartIndex,
					End_index:   resp.EndIndex,
					Size:        int64(len(resp.Msgs)),
				}

				File.WriteFile(fd, node, resp.Msgs)
				index = resp.EndIndex + 1
				s.zkclient.UpdateDup(context.Background(), &api.UpdateDupRequest{
					Topic:      in.topic_name,
					Part:       in.part_name,
					BrokerName: s.Name,
					BlockName:  GetBlockName(in.file_name),
					EndIndex:   resp.EndIndex,
				})
			}

		}()

	} else {
		//当文件名为nowfile时
		//zkserver已经让该broker准备接收文件
		//直接调用addMessage

		go func() {

			fd.Close()
			s.mu.RLock()
			topic, ok := s.topics[in.topic_name]
			s.mu.RUnlock()
			if !ok {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
			ice := 0
			for {

				s.mu.RLock()
				_, ok := s.parts_fetch[in.topic_name+in.part_name+in.file_name]
				s.mu.RUnlock()
				if !ok {
					logger.DEBUG(logger.DLog, "this topic(%v)-partition(%v) is not in this broker\n", in.topic_name, in.part_name)
					return
				}

				resp, err := (*cli).Pull(context.Background(), &api.PullRequest{
					Consumer: s.Name,
					Topic:    in.topic_name,
					Key:      in.part_name,
					Offset:   index,
					Size:     10,
					Option:   TOPIC_KEY_PSB_PULL,
				})

				if err != nil {
					ice++
					logger.DEBUG(logger.DError, "Err %v, err(%v)\n", resp.Err, err.Error())

					if ice >= 3 {
						//询问新的Leader
						resp, err := s.zkclient.GetNewLeader(context.Background(), &api.GetNewLeaderRequest{
							TopicName: in.topic_name,
							PartName:  in.part_name,
							BlockName: "NowBlock",
						})
						if err != nil {
							logger.DEBUG(logger.DError, "%v\n", err.Error())
						}
						s.mu.Lock()
						_, ok := s.brokers_fetch[in.topic_name+in.part_name]
						if !ok {
							logger.DEBUG(logger.DLog, "this broker(%v) is not connected\n")
							leader_bro, err := server_operations.NewClient(s.Name, client.WithHostPorts(resp.HostPort))
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
								return
							}
							s.brokers_fetch[resp.LeaderBroker] = &leader_bro
							cli = &leader_bro
						}
						s.mu.Unlock()
					}
					continue
				}
				ice = 0

				if resp.Size == 0 {

					//等待新消息的生产

					time.Sleep(time.Second * 10)
				} else {
					msgs := make([]Message, resp.Size)
					json.Unmarshal(resp.Msgs, &msgs)

					start_index := resp.StartIndex
					for _, msg := range msgs {

						if index == start_index {
							err := topic.addMessage(info{
								topic_name: in.topic_name,
								part_name:  in.part_name,
								size:       msg.Size,
								message:    msg.Msg,
								BrokerName: s.Name,
								zkclient:   &s.zkclient,
								file_name:  "NowBlock.txt",
							})
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
							}
						}
						index++
					}
				}
			}
		}()

	}
	return ret, err
}

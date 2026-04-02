// 包名：server，所有服务（Broker/ZKServer）的核心实现
package server

// 导入依赖：Kitex RPC框架、日志、ZooKeeper、网络、系统、运行时
import (
	"net"
	"os"
	"ringMQ/kitex_gen/api/client_operations"
	"ringMQ/logger"
	"ringMQ/zookeeper"
	"runtime"

	Ser "github.com/cloudwego/kitex/server" // Kitex RPC服务端
)

// PartKey 分区键：存储主题/分区名称
type PartKey struct {
	Name string `json:"name"`
}

// Options 核心配置结构体
// main函数 就是把命令行参数传进这个结构体里
type Options struct {
	Me                 int    // Broker唯一ID
	Name               string // 服务名称
	Tag                string // 服务标记：zkbroker / broker
	Zkserver_Host_Port string // ZKServer地址
	Broker_Host_Port   string // Broker服务地址
	Raft_Host_Port     string // Raft集群地址
}

// Broker 把自己的 CPU、磁盘、性能 发给 ZKServer
// 用于ZKServer做负载均衡
type Property struct {
	Name    string `json:"name"`    // 节点名
	Power   int64  `json:"power"`   // 总性能
	CPURate int64  `json:"cpurate"` // CPU使用率
	DiskIO  int64  `json:"diskio"`  // 磁盘IO
}

// BroNodeInfo Broker启动时的元数据
// 存储该Broker上的所有主题、分区、文件信息
type BroNodeInfo struct {
	Topics map[string]TopNodeInfo `json:"topics"`
}

// TopNodeInfo 主题元数据
type TopNodeInfo struct {
	Topic_name string
	Part_nums  int
	Partitions map[string]ParNodeInfo
}

// ParNodeInfo 分区元数据
type ParNodeInfo struct {
	Part_name  string
	Block_nums int
	Blocks     map[string]BloNodeInfo
}

// BloNodeInfo 消息块文件元数据
// 记录文件路径、起始/结束偏移量
type BloNodeInfo struct {
	Start_index int64
	End_index   int64
	Path        string
	File_name   string
}

// BrokerS 集群节点信息
// 记录所有Broker的地址、ID映射
// key: broker1
// value: 127.0.0.1:7774
type BrokerS struct {
	BroBrokers map[string]string `json:"brobrokers"` //普通消息节点列表(消息端口)
	RafBrokers map[string]string `json:"rafbrokers"` //Raft 集群节点列表（raft端口）
	Me_Brokers map[string]int    `json:"mebrokers"`  //当前节点自身信息
}

// 常量：服务标记
// main里就是用这两个常量区分启动模式
const (
	ZKBROKER = "zkbroker" // 注册中心
	BROKER   = "broker"   // 消息节点
)

//=============================================================================
// 【核心函数】main函数直接调用的两个启动入口
//=============================================================================

// NewBrokerAndStart 创建并启动Broker
// main函数 -mode=broker 时调用
func NewBrokerAndStart(zkinfo zookeeper.ZkInfo, opt Options) *RPCServer {
	// 解析Broker、Raft监听地址
	addr_bro, _ := net.ResolveTCPAddr("tcp", opt.Broker_Host_Port)
	addr_raf, _ := net.ResolveTCPAddr("tcp", opt.Raft_Host_Port)
	var opts_bro, opts_raf []Ser.Option
	opts_bro = append(opts_bro, Ser.WithServiceAddr(addr_bro)) // 往 opts_bro 里放 Broker 的配置
	opts_raf = append(opts_raf, Ser.WithServiceAddr(addr_raf)) // 往 opts_bro 里放 Raft 的配置

	// 创建RPC服务,打开网络端口，等待别人调用
	rpcServer := NewRpcServer(zkinfo)

	// 【协程】后台启动服务，不阻塞main
	go func() {
		err := rpcServer.Start(opts_bro, nil, opts_raf, opt)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
		}
	}()

	return &rpcServer
}

// NewZKServerAndStart 创建并启动ZKServer
// main函数 -mode=zkserver 时调用
func NewZKServerAndStart(zkinfo zookeeper.ZkInfo, opt Options) *RPCServer {
	// 解析ZKServer监听地址
	addr_zks, _ := net.ResolveTCPAddr("tcp", opt.Zkserver_Host_Port)
	var opts_zks []Ser.Option
	opts_zks = append(opts_zks, Ser.WithServiceAddr(addr_zks))

	// 创建RPC服务
	rpcServer := NewRpcServer(zkinfo)

	// 【协程】后台启动
	go func() {
		err := rpcServer.Start(nil, opts_zks, nil, opt)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
		}
	}()

	return &rpcServer
}

//=============================================================================
// 工具函数：供服务内部使用
//=============================================================================

// GetIpport 获取MAC地址（用于唯一标识机器）
func GetIpport() string {
	interfaces, err := net.Interfaces()
	ipport := ""
	if err != nil {
		panic("Poor soul, here is what you got: " + err.Error())
	}
	for _, inter := range interfaces {
		mac := inter.HardwareAddr
		ipport += mac.String()
	}
	return ipport
}

// GetBlockName 截取文件名（去掉后缀）
func GetBlockName(file_name string) (ret string) {
	if len(file_name) < 4 {
		return file_name
	}
	ret = file_name[:len(file_name)-4]
	return ret
}

// CheckFileOrList 判断文件/文件夹是否存在
func CheckFileOrList(path string) (ret bool) {
	_, err := os.Stat(path)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

// MovName 文件重命名
func MovName(OldFilePath, NewFilePath string) error {
	return os.Rename(OldFilePath, NewFilePath)
}

// CreateList 创建文件夹（递归）
func CreateList(path string) error {
	ret := CheckFileOrList(path)
	if !ret {
		err := os.MkdirAll(path, 0775)
		if err != nil {
			_, file, line, _ := runtime.Caller(1)
			logger.DEBUG(logger.DError, "%v:%v mkdir %v error %v\n", file, line, path, err.Error())
			return err
		}
	}
	return nil
}

// CreateFile 创建文件
func CreateFile(path string) (file *os.File, err error) {
	file, err = os.Create(path)
	return file, err
}

// GetPartKeyArray 把map转成数组，用于返回给客户端
func GetPartKeyArray(parts map[string]*Partition) []PartKey {
	var array []PartKey
	for part_name := range parts {
		array = append(array, PartKey{
			Name: part_name,
		})
	}
	return array
}

// CheckChangeCli 对比新旧客户端列表，找出新增/删除的客户端
func CheckChangeCli(old map[string]*client_operations.Client, new []string) (reduce, add []string) {
	for _, new_cli := range new {
		if _, ok := old[new_cli]; !ok {
			add = append(add, new_cli)
		}
	}
	for old_cli := range old {
		had := false
		for _, name := range new {
			if old_cli == name {
				had = true
				break
			}
		}
		if !had {
			reduce = append(reduce, old_cli)
		}
	}
	return reduce, add
}

//=============================================================================
// 测试用结构体 & 函数
//=============================================================================

// Info 测试用消息结构
type Info struct {
	Topic_name   string
	Part_name    string
	File_name    string
	New_name     string
	Option       int8
	Offset       int64
	Size         int8
	Ack          int8
	Producer     string
	Consumer     string
	Cmdindex     int64
	Message      []byte
	Brokers      map[string]string
	Me           int
	LeaderBroker string
	HostPort     string
}

// GetInfo 测试用：转换消息格式
func GetInfo(in Info) info {
	return info{
		topic_name:   in.Topic_name,
		part_name:    in.Part_name,
		file_name:    in.File_name,
		new_name:     in.New_name,
		option:       in.Option,
		offset:       in.Offset,
		size:         in.Size,
		ack:          in.Ack,
		producer:     in.Producer,
		consumer:     in.Consumer,
		cmdindex:     in.Cmdindex,
		message:      in.Message,
		brokers:      in.Brokers,
		me:           in.Me,
		LeaderBroker: in.LeaderBroker,
		HostPort:     in.HostPort,
	}
}

// GetServerInfoAply 创建测试用管道
func GetServerInfoAply() chan info {
	return make(chan info)
}

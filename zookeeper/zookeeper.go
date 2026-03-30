package zookeeper

import (
	"encoding/json"
	"reflect"
	"ringMQ/logger"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

// ZK 封装了项目中会用到的 zookeeper 连接和几个固定根路径。
type ZK struct {
	conn *zk.Conn

	Root       string
	BrokerRoot string
	TopicRoot  string
}

// ZkInfo 是连接 zookeeper 时的基础配置。
type ZkInfo struct {
	HostPorts []string
	Timeout   int
	Root      string
}

// NewZK 创建 zookeeper 连接，并提前准备项目需要的基础目录。
func NewZK(info ZkInfo) *ZK {
	conn, _, err := zk.Connect(info.HostPorts, time.Duration(info.Timeout)*time.Second)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	z := &ZK{
		conn:       conn,
		Root:       info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topics",
	}

	// 先把项目约定的持久化目录补齐，避免注册节点时父目录不存在。
	_ = z.ensurePath(z.BrokerRoot)
	_ = z.ensurePath(z.TopicRoot)

	return z
}

// BrokerNode 描述 broker 在 zookeeper 中保存的基础信息。
type BrokerNode struct {
	Name         string `json:"name"`
	BrokHostPort string `json:"brokhostport"`
	RaftHostPort string `json:"rafthostport"`
	Me           int    `json:"me"`
	Pnum         int    `json:"pNum"`
}

// TopicNode 描述 topic 的基础元数据。
type TopicNode struct {
	Name string `json:"name"`
	Pnum int    `json:"pNum"`
}

// PartitionNode 描述某个 partition 的元数据。
type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
	Index     int64  `json:"index"`
	Option    int8   `json:"option"`
	DupNum    int8   `json:"dupNum"`
	PTPoffset int64  `json:"ptpOffset"`
}

// SubscriptionNode 描述订阅关系在 zookeeper 中的存储结构。
type SubscriptionNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topic"`
	PartitionName string `json:"part"`
	Option        int8   `json:"option"`
	Groups        []byte `json:"groups"`
}

// BlockNode 描述分块文件的元数据。
type BlockNode struct {
	Name          string `json:"name"`
	FileName      string `json:"filename"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`
	LeaderBroker  string `json:"leaderBroker"`
}

// DuplicateNode 描述副本块的信息。
type DuplicateNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	BlockName     string `json:"blockname"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`
	BrokerName    string `json:"brokerName"`
}

// Map 表示消费者状态映射。
type Map struct {
	Consumers map[string]bool `json:"consumer"`
}

// ensurePath 逐级创建路径，保证 zookeeper 中的父节点存在。
func (z *ZK) ensurePath(path string) error {
	if path == "" || path == "/" {
		return nil
	}

	parts := strings.Split(path, "/")
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		current += "/" + part

		ok, _, err := z.conn.Exists(current)
		if err != nil {
			return err
		}
		if ok {
			continue
		}

		_, err = z.conn.Create(current, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}

	return nil
}

// RegisterNode 根据节点类型计算路径并完成序列化准备。
func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode
	var blnode BlockNode
	var dnode DuplicateNode
	var snode SubscriptionNode

	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode)
		path += z.BrokerRoot + "/" + bnode.Name
		data, err = json.Marshal(bnode)
	case "TopicNode":
		tnode = znode.(TopicNode)
		path += z.TopicRoot + "/" + tnode.Name
		data, err = json.Marshal(tnode)
	case "PartitionNode":
		pnode = znode.(PartitionNode)
		path += z.TopicRoot + "/" + pnode.TopicName + "/Partitions/" + pnode.Name
		data, err = json.Marshal(pnode)
	case "SubscriptionNode":
		snode = znode.(SubscriptionNode)
		path += z.TopicRoot + "/" + snode.TopicName + "/Subscriptions/" + snode.Name
		data, err = json.Marshal(snode)
	case "BlockNode":
		blnode = znode.(BlockNode)
		path += z.TopicRoot + "/" + blnode.TopicName + "/Partitions/" + blnode.PartitionName + "/" + blnode.Name
		data, err = json.Marshal(blnode)
	case "DuplicateNode":
		dnode = znode.(DuplicateNode)
		path += z.TopicRoot + "/" + dnode.TopicName + "/Partitions/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.BrokerName
		data, err = json.Marshal(dnode)
	}

	if err != nil {
		logger.DEBUG(logger.DError, "the node %v turn json fail%v\n", path, err.Error())
		return err
	}

	_ = data
	return nil
}

package api

// ReplyHeader 包含集群重定向的基础元信息
type ReplyHeader struct {
	Redirect     bool   // 是否要求客户端执行重定向
	RedirectAddr string // 如果发生了槽位未命中(Miss)，目标节点的地址是谁
}

// GetArgs 定义获取操作的参数
type GetArgs struct {
	Key string
}

// GetReply 定义获取操作的返回值
type GetReply struct {
	ReplyHeader
	Value interface{}
	Found bool
}

// SetArgs 定义设置操作的参数
type SetArgs struct {
	Key   string
	Value interface{}
}

// SetReply 定义设置操作的返回值
type SetReply struct {
	ReplyHeader
	Success bool
}

// DeleteArgs 定义删除操作的参数
type DeleteArgs struct {
	Key string
}

// DeleteReply 定义删除操作的返回值
type DeleteReply struct {
	ReplyHeader
	Success bool
}

// 对应 C 的 clusterMsgDataGossip
type GossipNodeInfo struct {
	NodeId string
	Addr   string
}

type PingType int

const (
	PingType_Ping PingType = iota
	PingType_Pong
	PintType_Meet
)

type PingArgs struct {
	NodeId       string
	Addr         string
	Slots        [2048]byte
	ConfigEpoch  uint64
	CurrentEpoch uint64
	Type         PingType
	KnownNodes   []GossipNodeInfo // 这就是 hdr->data.ping.gossip
}

type SetSlotAction int

const (
	SetSlotAction_Migrate SetSlotAction = iota
	SetSlotAction_Import
	SetSlotAction_Node
)

type SetSlotArgs struct {
	Action       SetSlotAction
	TargetNodeId string
	Slot         uint
}

type MigrateKeysArgs struct {
	NodeId string
	Addr   string
	Key    string
}

type MigrateKeysReply struct {
	Success bool
}

type RestoreArgs struct {
	Key   string
	Value string
}

type RestoreReply struct {
	Success bool
}

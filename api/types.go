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

type PingArgs struct {
	NodeId     string
	Slots      [2048]byte
	KnownNodes []GossipNodeInfo // 这就是 hdr->data.ping.gossip
}

type PingReply struct{}

// MeetArgs 定义节点加入请求的参数
type MeetArgs struct {
	NodeId string
	Addr   string // 新节点的通信地址
}

// MeetReply 定义节点加入请求的返回值
type MeetReply struct {
	NodeId  string
	Addr    string // 接收节点的通信地址
	Success bool
}

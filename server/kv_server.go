package server

import (
	"fmt"
	"net/rpc"

	"godis/api"
	"godis/cluster"
	"godis/store"
)

// KVServer 包装了存储引擎，提供基于 net/rpc 的外部访问
type KVServer struct {
	store   store.Store
	cluster *cluster.ClusterState
	aof     *store.AOF
}

// NewKVServer 初始化 RPC 服务器，引入动态 Gossip 集群状态
func NewKVServer(s store.Store, c *cluster.ClusterState, aof *store.AOF) *KVServer {
	return &KVServer{
		store:   s,
		cluster: c,
		aof:     aof,
	}
}

// checkSlot 是一个执行前校验切面的辅助方法
// 拦截所有未命中该节点槽位段的请求，并把重定向数据填充入 Header
func (kv *KVServer) checkSlot(key string, header *api.ReplyHeader) bool {
	if kv.cluster == nil {
		return true // 单机模式降级运行
	}

	isMine, ownerAddr := kv.cluster.IsMine(key)
	if !isMine {
		header.Redirect = true
		header.RedirectAddr = ownerAddr
		return false
	}

	return true
}

// Get 处理客户端的 Get 请求
func (kv *KVServer) Get(args *api.GetArgs, reply *api.GetReply) error {
	if !kv.checkSlot(args.Key, &reply.ReplyHeader) {
		return nil // 如果不属于自己，提前返回 Redirect 信息给对端
	}

	val, found := kv.store.Get(args.Key)
	reply.Value = val
	reply.Found = found
	return nil
}

// Set 处理客户端的 Set 请求
func (kv *KVServer) Set(args *api.SetArgs, reply *api.SetReply) error {
	if !kv.checkSlot(args.Key, &reply.ReplyHeader) {
		return nil
	}

	kv.store.Set(args.Key, args.Value)
	if kv.aof != nil {
		if strVal, ok := args.Value.(string); ok {
			kv.aof.Append("SET", args.Key, strVal)
		} else {
			kv.aof.Append("SET", args.Key, fmt.Sprintf("%v", args.Value))
		}
	}
	reply.Success = true

	// 2. 如果我有从节点，发起异步传播
	if kv.cluster != nil {
		go kv.cluster.PropagateToSlaves("SET", args.Key, args.Value)
	}

	return nil
}

// Delete 处理客户端的 Delete 请求
func (kv *KVServer) Delete(args *api.DeleteArgs, reply *api.DeleteReply) error {
	if !kv.checkSlot(args.Key, &reply.ReplyHeader) {
		return nil
	}

	kv.store.Delete(args.Key)
	if kv.aof != nil {
		kv.aof.Append("DEL", args.Key)
	}
	reply.Success = true
	if kv.cluster != nil {
		go kv.cluster.PropagateToSlaves("DEL", args.Key, nil)
	}

	return nil
}

// Register 将 KVServer 挂载到默认的 RPC 路由器上
func (kv *KVServer) Register() error {
	return rpc.Register(kv)
}

// server/kv_server.go

// Propagate 是由 Master 调用的 RPC 方法
func (kv *KVServer) Propagate(args *api.PropagateArgs, reply *api.PropagateReply) error {
	// 1. 安全校验：实际应用中应该校验调用者是否真的是自己的 Master
	
	// 2. 执行操作
	switch args.Op {
	case "SET":
		kv.store.Set(args.Key, args.Value)
	case "DEL":
		kv.store.Delete(args.Key)
	}

	// 3. 更新本地的复制偏移量（可选，用于断线重连后的增量同步）
	// kv.cluster.UpdateReplOffset(args.Offset)

	reply.Success = true
	return nil
}



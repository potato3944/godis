package server

import (
	"log"
	"net"
	"net/rpc"

	"predis/api"
	"predis/cluster"
	"predis/store"
)

// KVServer 包装了存储引擎，提供基于 net/rpc 的外部访问
type KVServer struct {
	store   store.Store
	cluster *cluster.StaticCluster
}

// NewKVServer 初始化 RPC 服务器，引入 Cluster 拓扑支持
func NewKVServer(s store.Store, c *cluster.StaticCluster) *KVServer {
	return &KVServer{
		store:   s,
		cluster: c,
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
	reply.Success = true
	return nil
}

// Delete 处理客户端的 Delete 请求
func (kv *KVServer) Delete(args *api.DeleteArgs, reply *api.DeleteReply) error {
	if !kv.checkSlot(args.Key, &reply.ReplyHeader) {
		return nil
	}

	kv.store.Delete(args.Key)
	reply.Success = true
	return nil
}

// Start 启动 RPC 服务端并在指定的地址监听
func (kv *KVServer) Start(address string) error {
	err := rpc.Register(kv)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	log.Printf("KV RPC Server runing at [%s] on distributed mode.\n", address)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Accept error: %v", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	return nil
}

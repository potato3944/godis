package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"

	"predis/cluster"
	"predis/server"
	"predis/store"
)

func main() {
	addr := flag.String("addr", ":1234", "本节点的网络监听地址 (e.g. :1234)")
	nodeId := flag.String("id", "node1", "本节点的唯一身份标识 (e.g. node1)")
	joinAddr := flag.String("join", "", "你想拉手合并的现有 Gossip 节点地址")
	maxEntries := flag.Int("max-entries", 0, "最大元素数量 (0 表示不限制)")
	policy := flag.String("policy", "LRU", "淘汰策略 (LRU, FIFO, NONE)")
	flag.Parse()

	// 1. 初始化底层引擎
	engine := store.NewConcurrentMap(*maxEntries, store.EvictionPolicyType(*policy))

	// 2. 初始化核心动态集群状态机
	clusterState := cluster.NewClusterState(*nodeId, *addr)

	// 3. 作为创世节点的话，要宣告所有槽位主权
	if *joinAddr == "" {
		clusterState.ClaimAllSlots()
		log.Printf("Starting as Seed Node [%s], claiming all slots.", *nodeId)
	}

	// 4. 初始化两大对外服务： Gossip 节点协同服务 和 KV 查询服务
	gossipServer := server.NewGossipServer(clusterState)
	kvServer := server.NewKVServer(engine, clusterState)

	// 把两个服务注册到原生 rpc 的基础单例复用路由器上
	gossipServer.Register()
	kvServer.Register()

	// 5. 启动网络监听，同时接待 Redis 操作和群内部聊天
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	log.Printf("Predis Network listening on [%s] - Node ID: %s", *addr, *nodeId)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	// 6. 连结外部网络后再启动心跳，防止先发后至
	clusterState.StartHeart()

	// 7. 如果配置了目标，发起拉手请求获取其它节点的拓扑！
	if *joinAddr != "" {
		err := gossipServer.JoinCluster(*joinAddr)
		if err != nil {
			log.Fatalf("Failed to Join cluster at %s: %v", *joinAddr, err)
		}
	}

	// 阻塞主线程
	select {}
}

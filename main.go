package main

import (
	"flag"
	"log"
	"strings"

	"predis/cluster"
	"predis/server"
	"predis/store"
)

func main() {
	// 通过命令行参数支持集群节点的注入与分配
	// 例如测试时可用:  go run main.go -addr=":1234" -nodes=":1234,:1235,:1236"
	addr := flag.String("addr", ":1234", "Current node's address (e.g. :1234)")
	nodesStr := flag.String("nodes", ":1234", "Comma separated list of all cluster nodes")
	flag.Parse()

	allNodes := strings.Split(*nodesStr, ",")

	// 1. 初始化底层线程安全的物理存储
	engine := store.NewConcurrentMap()

	// 2. 初始化分布式集群拓扑管理器，并分配 16384 槽的哈希环位
	staticCluster := cluster.NewStaticCluster(*addr, allNodes)
	log.Printf("Cluster mapping initialized with %d nodes", len(allNodes))

	// 3. 将存储引擎与集群信息注入到 RPC server 环境中
	rpcServer := server.NewKVServer(engine, staticCluster)

	// 4. 在自身绑定的地址上启动监听
	err := rpcServer.Start(*addr)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// 阻塞主线程以保持服务实例在线
	select {}
}

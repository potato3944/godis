package server

import (
	"log"
	"net/rpc"
	"sync"

	"predis/api"
	"predis/cluster"
)

type GossipServer struct {
	mu      sync.RWMutex
	cluster *cluster.ClusterState
}

func NewGossipServer(cs *cluster.ClusterState) *GossipServer {
	return &GossipServer{
		cluster: cs,
	}
}

func (gs *GossipServer) Ping(args *api.PingArgs, reply *api.PingReply) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	reply.NodeId = gs.cluster.Myself.NodeId
	reply.Epoch = gs.cluster.Myself.Epoch
	reply.Slots = [2048]byte(gs.cluster.Myself.Slots)

	// 1. 检查对方的 Epoch 是否比自己新
	if args.Epoch > gs.cluster.Myself.Epoch {
		// 对方的认知更新，立即同步
		gs.cluster.Myself.Epoch = args.Epoch
		gs.cluster.Myself.Slots = cluster.BitMap(args.Slots)
	}

	log.Printf("[Gossip] 收到来自 %s 的 PING 请求，正在回复 PONG...", args.NodeId)
	return nil
}

// Meet 处理外部节点发起的 "加入集群" 请求 (CLUSTER MEET)
func (gs *GossipServer) Meet(args *api.MeetArgs, reply *api.MeetReply) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	// 1. 如果节点不在本地认知拓扑中，将其加入
	if _, exists := gs.cluster.Nodes[args.NodeId]; !exists {
		gs.cluster.Nodes[args.NodeId] = cluster.ClusterNode{
			NodeId: args.NodeId,
			Addr:   args.Addr,
			Flag:   1, // 譬如定义 1 为在线活跃状态
			Epoch:  0, // 新加入，纪元为 0
		}
		log.Printf("[Gossip] 接收到 MEET：新节点 %s (%s) 成功加群", args.NodeId, args.Addr)
	} else {
		log.Printf("[Gossip] 接收到 MEET：节点 %s 已经在群内了", args.NodeId)
	}

	// 2. 作为响应，本节点将自身数据交给对方，供对方的路由表注册
	reply.NodeId = gs.cluster.Myself.NodeId
	reply.Addr = gs.cluster.Myself.Addr
	reply.Success = true

	return nil
}

// Register 仅仅注册自身到默认 RPC 复用器中
func (gs *GossipServer) Register() error {
	return rpc.Register(gs)
}

// JoinCluster 主动连接目标节点执行 MEET 握手
func (gs *GossipServer) JoinCluster(targetAddr string) error {
	client, err := rpc.Dial("tcp", targetAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	gs.mu.RLock()
	args := &api.MeetArgs{
		NodeId: gs.cluster.Myself.NodeId,
		Addr:   gs.cluster.Myself.Addr, // 必须是外部能访问自己 Gossip 的地址
	}
	gs.mu.RUnlock()

	var reply api.MeetReply
	err = client.Call("GossipServer.Meet", args, &reply)
	if err != nil {
		return err
	}

	if reply.Success {
		gs.mu.Lock()
		// 如果本地未记录目标节点，则将其添加
		if _, exists := gs.cluster.Nodes[reply.NodeId]; !exists {
			gs.cluster.Nodes[reply.NodeId] = cluster.ClusterNode{
				NodeId: reply.NodeId,
				Addr:   reply.Addr,
				Flag:   1,
			}
			log.Printf("[Gossip] 主动 MEET 成功，正式和节点 %s (%s) 建联", reply.NodeId, targetAddr)
		}
		gs.mu.Unlock()
	}
	return nil
}

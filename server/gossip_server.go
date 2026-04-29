package server

import (
	"net/rpc"

	"godis/api"
	"godis/cluster"
)

type GossipServer struct {
	cluster *cluster.ClusterState
}

func NewGossipServer(cs *cluster.ClusterState) *GossipServer {
	return &GossipServer{
		cluster: cs,
	}
}

func (gs *GossipServer) PingPong(args *api.PingArgs, reply *api.PingArgs) error {
	done:=make(chan struct{})
	gs.cluster.EventQueue<-&cluster.PingPong{Args: args,Reply: reply,Done: done}
	<-done
	return nil
}

func (gs *GossipServer) RequestVote(args *api.RequestVoteArgs, reply *api.RequestVoteReply) error {
	gs.cluster.HandleRequestVote(args, reply)
	return nil
}

// Register 仅仅注册自身到默认 RPC 复用器中
func (gs *GossipServer) Register() error {
	return rpc.Register(gs)
}

func (gs *GossipServer) JoinCluster(addr string) error {
	node := &cluster.ClusterNode{
		NodeId: addr, // placeholder
		Addr:   addr,
	}
	gs.cluster.ClusterSendPing(node, api.PintType_Meet)
	return nil
}


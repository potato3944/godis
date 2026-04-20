package server

import (
	"net/rpc"

	"predis/api"
	"predis/cluster"
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
	gs.cluster.ProcessPing(args, reply)
	return nil
}

// Register 仅仅注册自身到默认 RPC 复用器中
func (gs *GossipServer) Register() error {
	return rpc.Register(gs)
}

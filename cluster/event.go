package cluster

import (
	"log"
	"net/rpc"
	"godis/api"
	"time"
)

type ClusterSendPing struct {
	receiver *ClusterNode
	typ      api.PingType
}

type ClusterProcessPacket struct{
	Args   *api.PingArgs
	Sender *ClusterNode
}

func (cs *ClusterState) ClusterSendPing(receiver *ClusterNode, typ api.PingType) {
	args := cs.PreparePingArgs(receiver, typ)
	client := receiver.GetOrCreateClient()
	if client == nil {
		return
	}
	// 发起异步 RPC 调用
	reply := &api.PingArgs{}
	call := client.Go("GossipServer.Ping", args, reply, nil)
	go func() {
		select {
		case <-call.Done:
			if call.Error != nil {
				// The user had: cs.EventQueue<-&ClusterProcessPacket{}
				// We don't have enough context, likely just log it or pass empty for now since call failed
				log.Printf("Ping call failed: %v", call.Error)
			}

		case <-time.After(5 * time.Second):

		}
	}()

}

// 准备 ping pong 的参数
func (cs *ClusterState) PreparePingArgs(receiver *ClusterNode, typ api.PingType) (args *api.PingArgs) {

	freshnodes := len(cs.Nodes) - 2
	wanted := min(max(len(cs.Nodes)/10, 3), freshnodes)

	maxIteration := wanted * 3
	gossipCount := 0
	selected := make(map[string]bool)
	knownNodes := make([]api.GossipNodeInfo, 0, wanted)
	for freshnodes > 0 && gossipCount < wanted && maxIteration > 0 {
		maxIteration--
		this := cs.GetRandomNode()

		if this.NodeId == cs.Myself.NodeId || this.NodeId == receiver.NodeId {
			continue
		}
		if selected[this.NodeId] {
			continue
		}
		gossipEntry := api.GossipNodeInfo{
			NodeId: this.NodeId,
			Addr:   this.Addr,
			Flags:  this.Flags,
		}
		knownNodes = append(knownNodes, gossipEntry) // 塞进包里
		selected[this.NodeId] = true
		freshnodes--
		gossipCount++
	}
	args = &api.PingArgs{
		NodeId:     cs.Myself.NodeId,
		Slots:      [2048]byte(cs.Myself.Slots),
		Type:       typ,
		KnownNodes: knownNodes,
	}
	receiver.PingSent = time.Now()
	return args
}

func (node *ClusterNode) GetOrCreateClient() *rpc.Client {
	client := node.RpcClient
	if client == nil {
		newClient, err := rpc.Dial("tcp", node.Addr)
		if err != nil {
			log.Printf("Dial target %s failed: %v\n", node.Addr, err)
			return nil
		}
		node.RpcClient = newClient
		client = newClient
	}
	return client
}

func (cs *ClusterState) ClusterProcessPacket(args *api.PingArgs, sender *ClusterNode) {

	sender.PingSent = time.Time{}
	if sender == nil && args.Type == api.PintType_Meet {
		cs.Nodes[args.NodeId] = &ClusterNode{
			NodeId:       args.NodeId,
			Addr:         args.Addr,
			PongReceived: time.Now(),
		}
		cs.NodesIds = append(cs.NodesIds, args.NodeId)
		go cs.ProcessGossipNodeInfo(nil, args.KnownNodes)
		return
	}
	if sender != nil && args.Type == api.PingType_Pong {
		sender.PongReceived = time.Now()
	}

	if sender != nil {
		go cs.ClusterProcessConfigInfo(args, sender)
		go cs.ProcessGossipNodeInfo(sender, args.KnownNodes)
	}
}

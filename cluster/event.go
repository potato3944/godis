package cluster

import (
	"godis/api"
	"log"
	"net/rpc"
	"time"
)

type ClusterSendPing struct {
	receiver *ClusterNode
	typ      api.PingType
}

type ClusterProcessPacket struct {
	Args   *api.PingArgs
	Sender *ClusterNode
}

type PingPong struct {
	Args  *api.PingArgs
	Reply *api.PingArgs
	Done  chan struct{}
}

const (
	ModifySlotAction_Add = iota
	ModifySlotAction_Del
	ModifySlotAction_Set
)

type SetSlot struct {
	Args *api.SetSlotArgs
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

				cs.EventQueue <- &ClusterProcessPacket{Args: reply, Sender: receiver}
				log.Printf("Ping call failed: %v", call.Error)
			}

		case <-time.After(1 * time.Second):

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
		cs.ProcessGossipNodeInfo(nil, args.KnownNodes)
		return
	}
	if sender != nil && args.Type == api.PingType_Pong {
		sender.PongReceived = time.Now()
	}

	if sender != nil {
		cs.ClusterProcessConfigInfo(args, sender)
		cs.ProcessGossipNodeInfo(sender, args.KnownNodes)
	}
}

func (cs *ClusterState) ClusterProcessConfigInfo(args *api.PingArgs, sender *ClusterNode) {

	dirtySlots := args.Slots != sender.Slots
	if dirtySlots {
		cs.ClusterUpdateSlotsConfigWith(args.Slots, sender, args.ConfigEpoch)
	}
}

// 处理 gossip 携带的其他节点的信息
func (cs *ClusterState) ProcessGossipNodeInfo(sender *ClusterNode, nodes []api.GossipNodeInfo) {

	for _, nodeInfo := range nodes {
		node, ok := cs.Nodes[nodeInfo.NodeId]
		if !ok {
			cs.Nodes[nodeInfo.NodeId] = &ClusterNode{
				NodeId: nodeInfo.NodeId,
				Addr:   nodeInfo.Addr,
			}
			cs.NodesIds = append(cs.NodesIds, nodeInfo.NodeId)
		} else if sender != nil && nodeInfo.NodeId != cs.Myself.NodeId {
			// 如果发送者认为该节点 PFAIL 或 FAIL
			if (nodeInfo.Flags & (CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL)) != 0 {
				node.AddFailureReport(sender)
			} else {
				// 节点状态正常，清除对应的故障报告
				node.DelFailureReport(sender)
			}
		}
	}

}

func (cs *ClusterState) HandlePingPong(args *api.PingArgs, reply *api.PingArgs) {
	sender := cs.Nodes[args.NodeId]
	if args.Type == api.PingType_Ping || args.Type == api.PintType_Meet {
		reply = cs.PreparePingArgs(sender, api.PingType_Pong)
	}
	cs.EventQueue <- &ClusterProcessPacket{Args: args, Sender: sender}
}

func (cs *ClusterState) ClusterSetSlot(args *api.SetSlotArgs) {
	switch args.Action {
	case api.SetSlotAction_Migrate:
		if cs.Slots[args.Slot] != cs.Myself {
			return
		}
		node := cs.Nodes[args.TargetNodeId]
		if node == nil {
			return
		}
		cs.MigratingSlotTo[args.Slot] = node
	case api.SetSlotAction_Import:
		if cs.Slots[args.Slot] == cs.Myself {
			return
		}
		node := cs.Nodes[args.TargetNodeId]
		if node == nil {
			return
		}
		cs.ImportingSlotFrom[args.Slot] = node
	case api.SetSlotAction_Node:
		node := cs.Nodes[args.TargetNodeId]
		if node == nil {
			return
		}
		if node != cs.Myself && cs.Slots[args.Slot] == cs.Myself {
			if cs.CountKeysInSlot(args.Slot) != 0 {
				return
			}
		}
		if cs.CountKeysInSlot(args.Slot) == 0 && cs.MigratingSlotTo[args.Slot] != nil {
			cs.MigratingSlotTo[args.Slot] = nil
		}

		cs.ClusterDelSlot(args.Slot)
		cs.ClusterAddSlot(node, args.Slot)
		if node == cs.Myself && cs.ImportingSlotFrom[args.Slot] != nil {
			cs.ImportingSlotFrom[args.Slot] = nil
			cs.ClusterBumpConfigEpochWithoutConsensus()
			cs.ClusterTodo(CLUSTER_TODO_BROADCAST_PONG)
		}
	}
}



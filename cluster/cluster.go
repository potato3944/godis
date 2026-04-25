package cluster

import (
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"

	"predis/api"
	"predis/store"
	"predis/utils"
)

const NumSlots = 16384

type BitMap [2048]byte

type ClusterNode struct {
	mu          sync.RWMutex
	NodeId      string
	Addr        string
	Slots       BitMap
	RpcClient   *rpc.Client
	ConfigEpoch uint64
}

type ClusterState struct {
	mu                sync.RWMutex
	Myself            *ClusterNode
	Nodes             map[string]*ClusterNode //NodeId -> *ClusterNode
	NodesIds          []string
	MigratingSlotTo   [NumSlots]*ClusterNode
	ImportingSlotFrom [NumSlots]*ClusterNode
	Slots             [NumSlots]*ClusterNode //Slots[i]->	处理第 i 个槽的节点

	CurrentEpoch  uint64
	ConcurrentMap store.Store
	TodoFlags     uint64
}

const (
	CLUSTER_TODO_NONE           uint64 = 0
	CLUSTER_TODO_SAVE_CONFIG    uint64 = 1 << 0
	CLUSTER_TODO_BROADCAST_PONG uint64 = 1 << 1
)

// NewClusterState 创建动态 Gossip 集群的本地视界
func NewClusterState(nodeId, addr string) *ClusterState {
	cs := &ClusterState{
		Myself: &ClusterNode{
			NodeId: nodeId,
			Addr:   addr,
		},
		Nodes:    make(map[string]*ClusterNode),
		NodesIds: make([]string, 0),
	}
	cs.Nodes[nodeId] = cs.Myself
	cs.NodesIds = append(cs.NodesIds, nodeId)
	return cs
}

// IsMine 计算目标 Key 属于谁。如果无人管理默认返回自己管（降级）。
func (cs *ClusterState) IsMine(key string) (bool, string) {
	slot := utils.KeyHashSlot(key)

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for _, node := range cs.Nodes {
		if node.Slots.Get(slot) {
			return node.NodeId == cs.Myself.NodeId, node.Addr
		}
	}
	return true, ""
}

// 发起 rpc
func (cs *ClusterState) ClusterSendPing(target *ClusterNode, typ api.PingType) {

	args := cs.PreparePingArgs(target, typ)
	target.mu.Lock()
	client := target.RpcClient
	if client == nil {
		newClient, err := rpc.Dial("tcp", target.Addr)
		if err != nil {
			log.Printf("Dial target %s failed: %v\n", target.Addr, err)
			return
		}
		target.RpcClient = newClient
		client = newClient
	}
	target.mu.Unlock()
	// 发起异步 RPC 调用
	reply := &api.PingArgs{}
	err := client.Call("GossipServer.Ping", args, reply)
	if err != nil {
		log.Println(err)
	} else {
		go cs.ClusterProcessConfigInfo(args, cs.Nodes[reply.NodeId])
		go cs.ProcessGossipNodeInfo(args.KnownNodes)
	}
	//Todo 处理 reply

}

func (cs *ClusterState) PreparePingArgs(target *ClusterNode, typ api.PingType) (args *api.PingArgs) {
	cs.mu.RLock()

	freshnodes := len(cs.Nodes) - 2
	wanted := min(max(len(cs.Nodes)/10, 3), freshnodes)

	maxIteration := wanted * 3
	gossipCount := 0
	selected := make(map[string]bool)
	knownNodes := make([]api.GossipNodeInfo, 0, wanted)
	for freshnodes > 0 && gossipCount < wanted && maxIteration > 0 {
		maxIteration--
		this := cs.GetRandomNode()

		if this.NodeId == cs.Myself.NodeId || this.NodeId == target.NodeId {
			continue
		}
		if selected[this.NodeId] {
			continue
		}
		gossipEntry := api.GossipNodeInfo{
			NodeId: this.NodeId,
			Addr:   this.Addr,
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
	cs.mu.RUnlock()
	return
}

// 处理 rpc
func (cs *ClusterState) ProcessPing(args *api.PingArgs, reply *api.PingArgs) {
	cs.mu.RLock()
	sender, ok := cs.Nodes[args.NodeId]
	cs.mu.RUnlock()
	if args.Type == api.PingType_Ping || args.Type == api.PintType_Meet {
		reply = cs.PreparePingArgs(sender, api.PingType_Pong)
	}
	if !ok && args.Type == api.PintType_Meet {

		cs.Nodes[args.NodeId] = &ClusterNode{
			NodeId: args.NodeId,
			Addr:   args.Addr,
		}
		cs.NodesIds = append(cs.NodesIds, args.NodeId)
		go cs.ProcessGossipNodeInfo(args.KnownNodes)

	}

	if ok {
		go cs.ClusterProcessConfigInfo(args, sender)
		go cs.ProcessGossipNodeInfo(args.KnownNodes)
	}
}

func (cs *ClusterState) ClusterProcessConfigInfo(args *api.PingArgs, sender *ClusterNode) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	dirtySlots := args.Slots != sender.Slots
	if dirtySlots {
		cs.ClusterUpdateSlotsConfigWith(args.Slots, sender, args.ConfigEpoch)
	}
}

func (cs *ClusterState) ClusterUpdateSlotsConfigWith(slots BitMap, sender *ClusterNode, senderConfigEpoch uint64) {
	for j := range NumSlots {
		slot := uint(j)
		if slots.Get(slot) {
			if cs.ImportingSlotFrom[slot] != nil {
				continue
			}
			if cs.Slots[slot] == nil || cs.Slots[slot].ConfigEpoch < senderConfigEpoch {
				if cs.Slots[slot] == cs.Myself && sender != cs.Myself {
					//Todo
				}

			}
		}
	}

}

// 处理 gossip 携带的其他节点的信息
func (cs *ClusterState) ProcessGossipNodeInfo(nodes []api.GossipNodeInfo) {
	cs.mu.Lock()
	for _, node := range nodes {
		if _, ok := cs.Nodes[node.NodeId]; !ok {
			cs.Nodes[node.NodeId] = &ClusterNode{
				NodeId: node.NodeId,
				Addr:   node.Addr,
			}
			cs.NodesIds = append(cs.NodesIds, node.NodeId)
		}
	}
	cs.mu.Unlock()
}

// 添加 slot
func (cs *ClusterState) ClusterAddSlot(node *ClusterNode, slot uint) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.Slots[slot] != nil {
		return false
	}
	node.Slots.Set(slot)
	cs.Slots[slot] = node
	return true
}

// 删除 slot
func (cs *ClusterState) ClusterDelSlot(slot uint) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	node := cs.Slots[slot]
	if node == nil {
		return false
	}
	node.Slots.Clear(slot)

	return true
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

// GetRandomNode 从所有节点中随机获取一个节点
func (cs *ClusterState) GetRandomNode() *ClusterNode {
	n := len(cs.NodesIds)
	return cs.Nodes[cs.NodesIds[rand.Intn(n)]]
}

func (cs *ClusterState) CountKeysInSlot(slot uint) int {
	return cs.ConcurrentMap.CountKeysInSlot(slot)
}

func (cs *ClusterState) ClusterBumpConfigEpochWithoutConsensus() bool {
	maxEpoch := cs.ClusterGetMaxEpoch()

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.Myself.ConfigEpoch == 0 || cs.Myself.ConfigEpoch != maxEpoch {
		cs.CurrentEpoch++
		cs.Myself.ConfigEpoch = cs.CurrentEpoch
		//Todo broadcast_pong
		cs.ClusterTodo(CLUSTER_TODO_BROADCAST_PONG)
		return true
	} else {
		return false
	}
}

func (cs *ClusterState) ClusterGetMaxEpoch() uint64 {
	cs.mu.RLock()
	var maxEpoch uint64 = 0
	for _, node := range cs.Nodes {
		maxEpoch = max(maxEpoch, node.ConfigEpoch)
	}
	maxEpoch = max(maxEpoch, cs.CurrentEpoch)
	cs.mu.RUnlock()
	return maxEpoch
}

func (cs *ClusterState) ClusterMigrateKeys(args *api.MigrateKeysArgs, reply *api.MigrateKeysReply) {
	val, ok := cs.ConcurrentMap.Get(args.Key)
	if !ok {
		reply.Success = false
		return
	}
	restoreArgs, restoreReply := &api.RestoreArgs{Key: args.Key, Value: val.(string)}, &api.RestoreReply{}
	cs.Nodes[args.NodeId].RpcClient.Call("CommandServer.Restore", restoreArgs, restoreReply)
	if restoreReply.Success {
		cs.ConcurrentMap.Delete(args.Key)
		reply.Success = true
	} else {
		reply.Success = false
	}
}

func (cs *ClusterState) StartHeart() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		if cs.TodoFlags&CLUSTER_TODO_BROADCAST_PONG > 0 {
			//发送 ping
			cs.clusterBroadcastPong()
		}
	}
}

func (cs *ClusterState) ClusterRestore(args *api.RestoreArgs, reply *api.RestoreReply) {
	cs.ConcurrentMap.Set(args.Key, args.Value)
	reply.Success = true
}

func (cs *ClusterState) ClusterTodo(flags uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.TodoFlags |= flags
}

func (cs *ClusterState) clusterBroadcastPong() {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	for _, node := range cs.Nodes {
		if node.NodeId == cs.Myself.NodeId {
			continue
		}
		go cs.ClusterSendPing(node, api.PingType_Pong)
	}
	cs.TodoFlags &= ^CLUSTER_TODO_BROADCAST_PONG
}

// Set 逻辑上开启特定位（标记拥有该 slot）
func (bm *BitMap) Set(index uint) {
	bm[index/8] |= 1 << (index % 8)
}

// Clear 逻辑上清除某个位（标记移除对该 slot 的归属）
func (bm *BitMap) Clear(index uint) {
	bm[index/8] &= ^(1 << (index % 8))
}

// Get 嗅探验证某个位
func (bm *BitMap) Get(index uint) bool {
	return (bm[index/8] & (1 << (index % 8))) != 0
}

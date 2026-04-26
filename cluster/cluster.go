package cluster

import (
	"container/list"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"predis/api"
	"predis/store"
	"predis/utils"
)

const NumSlots = 16384

type BitMap [2048]byte

type ClusterNode struct {
	mu           sync.Mutex
	NodeId       string
	Addr         string
	Slots        BitMap
	RpcClient    *rpc.Client
	ConfigEpoch  uint64
	Flags        int
	NumSlaves    int
	Slaves       []*ClusterNode
	SlaveOf      *ClusterNode
	PingSent     time.Time
	PongReceived time.Time
	FailReports  list.List
}

type ClusterNodeFailReport struct {
	Node *ClusterNode
	Time time.Time
}

func (node *ClusterNode) AddFailureReport(reporter *ClusterNode) {
	node.mu.Lock()
	defer node.mu.Unlock()
	for e := node.FailReports.Front(); e != nil; e = e.Next() {
		report := e.Value.(*ClusterNodeFailReport)
		if report.Node == reporter {
			report.Time = time.Now()
			return
		}
	}
	node.FailReports.PushBack(&ClusterNodeFailReport{
		Node: reporter,
		Time: time.Now(),
	})
}

func (node *ClusterNode) DelFailureReport(reporter *ClusterNode) {
	node.mu.Lock()
	defer node.mu.Unlock()
	for e := node.FailReports.Front(); e != nil; e = e.Next() {
		report := e.Value.(*ClusterNodeFailReport)
		if report.Node == reporter {
			node.FailReports.Remove(e)
			return
		}
	}
}

const (
	CLUSTER_NODE_MASTER = 1 << 0
	CLUSTER_NODE_SLAVE  = 1 << 1
	CLUSTER_NODE_PFAIL  = 1 << 2 // 疑似下线 (Possible Failure)
	CLUSTER_NODE_FAIL   = 1 << 3 // 确认下线 (Confirmed Failure)
	NodeTimeout         = 5000   // 节点超时阈值 (毫秒)
)

type ClusterState struct {
	mu                sync.Mutex
	Myself            *ClusterNode
	Nodes             map[string]*ClusterNode //NodeId -> *ClusterNode
	NodesIds          []string
	MigratingSlotTo   [NumSlots]*ClusterNode
	ImportingSlotFrom [NumSlots]*ClusterNode
	Slots             [NumSlots]*ClusterNode //Slots[i]->	处理第 i 个槽的节点

	CurrentEpoch  uint64
	ConcurrentMap store.Store
	TodoFlags     uint64
	ReplOffset    int64
}

const (
	CLUSTER_TODO_NONE           uint64 = 1
	CLUSTER_TODO_SAVE_CONFIG    uint64 = 1 << 1
	CLUSTER_TODO_BROADCAST_PONG uint64 = 1 << 2
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

// 发起 rpc
func (cs *ClusterState) ClusterSendPing(receiver *ClusterNode, typ api.PingType) {
	args := cs.PreparePingArgs(receiver, typ)
	client := receiver.GetOrCreateClient()
	if client == nil {
		return
	}
	// 发起异步 RPC 调用
	reply := &api.PingArgs{}
	err := client.Call("GossipServer.Ping", args, reply)
	if err != nil {
		log.Println(err)
		return
	}
	cs.mu.Lock()
	sender := cs.Nodes[args.NodeId]
	cs.mu.Unlock()

	cs.ProcessPacket(args, sender)

}

func (cs *ClusterState) ProcessPacket(args *api.PingArgs, sender *ClusterNode) {

	cs.mu.Lock()
	sender.PingSent = time.Time{}
	defer cs.mu.Unlock()
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

func (cs *ClusterState) PreparePingArgs(receiver *ClusterNode, typ api.PingType) (args *api.PingArgs) {
	cs.mu.Lock()

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
	cs.mu.Unlock()
	return
}

// 处理 rpc
func (cs *ClusterState) PingPong(args *api.PingArgs, reply *api.PingArgs) {

	cs.mu.Lock()
	sender := cs.Nodes[args.NodeId]
	cs.mu.Unlock()

	if args.Type == api.PingType_Ping || args.Type == api.PintType_Meet {
		reply = cs.PreparePingArgs(sender, api.PingType_Pong)
	}
	cs.ProcessPacket(args, sender)
}

func (cs *ClusterState) ClusterProcessConfigInfo(args *api.PingArgs, sender *ClusterNode) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

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
func (cs *ClusterState) ProcessGossipNodeInfo(sender *ClusterNode, nodes []api.GossipNodeInfo) {
	cs.mu.Lock()
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
	cs.mu.Lock()
	var maxEpoch uint64 = 0
	for _, node := range cs.Nodes {
		maxEpoch = max(maxEpoch, node.ConfigEpoch)
	}
	maxEpoch = max(maxEpoch, cs.CurrentEpoch)
	cs.mu.Unlock()
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
	cs.mu.Lock()
	defer cs.mu.Unlock()
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

// cluster/cluster.go

func (cs *ClusterState) PropagateToSlaves(op string, key string, value interface{}) {
	cs.mu.Lock()
	slaves := cs.Myself.Slaves
	// 增加偏移量
	atomic.AddInt64(&cs.ReplOffset, 1)
	currentOffset := cs.ReplOffset
	cs.mu.Unlock()

	for _, slave := range slaves {
		go func(node *ClusterNode) {
			args := &api.PropagateArgs{
				Op:     op,
				Key:    key,
				Value:  value,
				Offset: currentOffset,
			}
			reply := &api.PropagateReply{}

			// 复用之前的 RPC 发送逻辑
			client := node.GetOrCreateClient() // 你可以封装一个获取 RPC Client 的辅助函数
			if client != nil {
				client.Call("KVServer.Propagate", args, reply)
			}
		}(slave)
	}
}

func (node *ClusterNode) GetOrCreateClient() *rpc.Client {
	node.mu.Lock()
	client := node.RpcClient
	if client == nil {
		newClient, err := rpc.Dial("tcp", node.Addr)
		if err != nil {
			log.Printf("Dial target %s failed: %v\n", node.Addr, err)
			node.mu.Unlock()
			return nil
		}
		node.RpcClient = newClient
		client = newClient
	}
	node.mu.Unlock()
	return client
}

// IsMine 计算目标 Key 属于谁。如果无人管理默认返回自己管（降级）。
func (cs *ClusterState) IsMine(key string) (bool, string) {
	slot := utils.KeyHashSlot(key)

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, node := range cs.Nodes {
		if node.Slots.Get(slot) {
			return node.NodeId == cs.Myself.NodeId, node.Addr
		}
	}
	return true, ""
}

func (cs *ClusterState) ClusterCron() {
	ticker := time.NewTicker(100 * time.Millisecond)
	var iteration uint64 = 0
	var minPong time.Time
	var minPongNode *ClusterNode
	defer ticker.Stop()
	cs.mu.Lock()
	// --- 1. 遍历所有节点进行超时检查 ---
	for _, node := range cs.Nodes {
		if node == cs.Myself {
			continue
		}
		// 计算距离上次收到 PONG 的时间（毫秒）
		var delay int64
		if !node.PongReceived.IsZero() {
			delay = int64(time.Since(node.PongReceived))
		} else {
			// 如果从未收到过 PONG，且已经发出了 PING
			if !node.PingSent.IsZero() {
				delay = int64(time.Since(node.PongReceived))
			}
			// 如果超时超过 NodeTimeout，标记为 PFAIL
			if delay > NodeTimeout {
				if (node.Flags & CLUSTER_NODE_PFAIL) == 0 {
					log.Printf("Node %s is PFAIL (no PONG for %dms)", node.NodeId, delay)
					node.Flags |= CLUSTER_NODE_PFAIL
				}
			} else {
				// 如果在超时时间内收到了，清除 PFAIL
				node.Flags &= ^CLUSTER_NODE_PFAIL
			}
		}

		for range ticker.C {
			iteration++
			if iteration%10 == 0 {
				for range 5 {
					node := cs.GetRandomNode()

					if node.PingSent.IsZero() || node == cs.Myself {

						continue
					}
					if minPongNode == nil || node.PongReceived.Before(minPong) {
						minPongNode = node
						minPong = node.PongReceived
					}
				}
			}
			if minPongNode != nil {
				go cs.ClusterSendPing(minPongNode, api.PingType_Ping)
			}
		}
		cs.mu.Unlock()
	}
}

//todo 全量同步

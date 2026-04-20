package cluster

import (
	"hash/crc32"
	"math/rand"
	"sync"

	"predis/api"
)

const NumSlots = 16384

type BitMap [2048]byte

type ClusterNode struct {
	NodeId string
	Addr   string
	Slots  BitMap
}

type ClusterState struct {
	mu        sync.RWMutex
	Myself    *ClusterNode
	Nodes     map[string]*ClusterNode //NodeId -> *ClusterNode
	NodesKeys []string
	Slots     [NumSlots]*ClusterNode //Slots[i]->	处理第 i 个槽的节点
}

// NewClusterState 创建动态 Gossip 集群的本地视界
func NewClusterState(nodeId, addr string) *ClusterState {
	cs := &ClusterState{
		Myself: &ClusterNode{
			NodeId: nodeId,
			Addr:   addr,
		},
		Nodes:     make(map[string]*ClusterNode),
		NodesKeys: make([]string, 0),
	}
	cs.Nodes[nodeId] = cs.Myself
	cs.NodesKeys = append(cs.NodesKeys, nodeId)
	return cs
}

// IsMine 计算目标 Key 属于谁。如果无人管理默认返回自己管（降级）。
func (cs *ClusterState) IsMine(key string) (bool, string) {
	slot := int(crc32.ChecksumIEEE([]byte(key))) % NumSlots

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for _, node := range cs.Nodes {
		if node.Slots.Get(slot) {
			return node.NodeId == cs.Myself.NodeId, node.Addr
		}
	}
	return true, ""
}

func (cs *ClusterState) ClusterSendPing(target *ClusterNode) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	freshnodes := len(cs.Nodes) - 2
	wanted := len(cs.Nodes) / 10
	if wanted < 3 {
		wanted = 3
	}
	if wanted > freshnodes {
		wanted = freshnodes
	}

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
	args := &api.PingArgs{
		NodeId:     cs.Myself.NodeId,
		Slots:      [2048]byte(cs.Myself.Slots),
		KnownNodes: knownNodes,
	}
all("GossipServer.Ping", target.Addr, args, &api.PingReply{})
}

// GetRandomNode 从所有节点中随机获取一个节点
func (cs *ClusterState) GetRandomNode() *ClusterNode {
	n := len(cs.NodesKeys)
	return cs.Nodes[cs.NodesKeys[rand.Intn(n)]]
}

// Set 逻辑上开启特定位（标记拥有该 slot）
func (bm *BitMap) Set(index int) {
	bm[index/8] |= 1 << (index % 8)
}

// Clear 逻辑上清除某个位（标记移除对该 slot 的归属）
func (bm *BitMap) Clear(index int) {
	bm[index/8] &= ^(1 << (index % 8))
}

// Get 嗅探验证某个位
func (bm *BitMap) Get(index int) bool {
	return (bm[index/8] & (1 << (index % 8))) != 0
}

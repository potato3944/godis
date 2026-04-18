package cluster

import (
	"hash/crc32"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"

	"predis/api"
)

const NumSlots = 16384

type BitMap [2048]byte

type Node struct {
	NodeId string
	Addr   string
	Slots  BitMap
	Flag   int
	Epoch  int
}

type ClusterState struct {
	mu       sync.RWMutex
	SelfNode *Node
	Nodes    map[string]Node
}

// NewClusterState 创建动态 Gossip 集群的本地视界
func NewClusterState(nodeId, addr string) *ClusterState {
	cs := &ClusterState{
		SelfNode: &Node{
			NodeId: nodeId,
			Addr:   addr,
			Flag:   1, // 在线
			Epoch:  0,
		},
		Nodes: make(map[string]Node),
	}
	cs.Nodes[nodeId] = *cs.SelfNode
	return cs
}

// ClaimAllSlots 把天下所有的槽位纳入麾下并自增纪元 (首个被启动的节点声明主权时使用)
func (cs *ClusterState) ClaimAllSlots() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.SelfNode.Epoch++
	for i := 0; i < NumSlots; i++ {
		cs.SelfNode.Slots.Set(i)
	}
	cs.Nodes[cs.SelfNode.NodeId] = *cs.SelfNode
	log.Printf("[Gossip] 节点 %s 提升纪元至 %d 并认领全部槽位", cs.SelfNode.NodeId, cs.SelfNode.Epoch)
}

// IsMine 计算目标 Key 属于谁。如果无人管理默认返回自己管（降级）。
func (cs *ClusterState) IsMine(key string) (bool, string) {
	slot := int(crc32.ChecksumIEEE([]byte(key))) % NumSlots

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for _, node := range cs.Nodes {
		if node.Slots.Get(slot) {
			return node.NodeId == cs.SelfNode.NodeId, node.Addr
		}
	}
	return true, ""
}

// StartHeart 开启 Gossip 心跳定时循环
func (cs *ClusterState) StartHeart() {
	ticker := time.NewTicker(time.Second * 1) // 每秒心跳
	go func() {
		for range ticker.C {
			cs.heartbeat()
		}
	}()
}

// heartbeat 执行一次 Gossip 数据同步
func (cs *ClusterState) heartbeat() {
	if len(cs.Nodes) <= 1 {
		return
	}

	// 1. 过滤出除自己以外的其他节点
	var peers []string
	for id := range cs.Nodes {
		if id != cs.SelfNode.NodeId {
			peers = append(peers, id)
		}
	}

	if len(peers) == 0 {
		return
	}

	// 2. 随机选中一个 Peer (符合 Gossip 协议精髓：随机散播)
	peerId := peers[rand.Intn(len(peers))]
	peer := cs.Nodes[peerId]

	// 3. 模拟一次 Ping 操作
	// 这里你会把自己的 ClusterState（特别是节点拓扑或者 Epoch 信息）通过 RPC 打包发送给它
	// node 收到后会更新自己的 ClusterState，然后回复一个 PONG 结构包含它的最新认知。
	client, err := rpc.Dial("tcp", peer.Addr)
	if err != nil {
		log.Printf("[Gossip] 无法连接到 Peer %s: %v", peer.NodeId, err)
		return
	}
	defer client.Close()

	args := &api.PingArgs{
		NodeId: cs.SelfNode.NodeId,
		Epoch:  cs.SelfNode.Epoch,
		Slots:  [2048]byte(cs.SelfNode.Slots), // 类型转换: BitMap => [2048]byte
	}

	var reply api.PingReply
	err = client.Call("GossipServer.Ping", args, &reply)
	if err != nil {
		log.Printf("[Gossip] PING %s 失败: %v", peer.NodeId, err)
		return
	}

	// 4. 收到对方回复后，更新本地认知
	if reply.Epoch > cs.SelfNode.Epoch {
		cs.mu.Lock()
		cs.SelfNode.Epoch = reply.Epoch
		cs.SelfNode.Slots = BitMap(reply.Slots) // 类型转换: [2048]byte => BitMap
		cs.Nodes[cs.SelfNode.NodeId] = *cs.SelfNode
		cs.mu.Unlock()
		log.Printf("[Gossip] 收到来自 %s 的更新，纪元提升至 %d", reply.NodeId, reply.Epoch)
	}

	log.Printf("[Gossip] 本机正向随机 Peer (Node: %s 目标地址: %s) 发送 PING 探针...", peer.NodeId, peer.Addr)
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

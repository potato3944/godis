package cluster

import "hash/crc32"

const NumSlots = 16384

// StaticCluster 维护了静态的集群拓扑结构信息
type StaticCluster struct {
	SelfAddr  string                   // 当前节点自己的地址，比如 ":1234"
	SlotNodes [NumSlots]string // Slot 到节点地址的映射表
}

// NewStaticCluster 给定一组节点，预先静态分配好所有的哈希槽位
func NewStaticCluster(selfAddr string, allNodes []string) *StaticCluster {
	sc := &StaticCluster{
		SelfAddr: selfAddr,
	}

	if len(allNodes) == 0 {
		return sc
	}

	// 最简单的静态分配策略：把 16384 个槽位轮流平分给现有的节点
	for i := 0; i < NumSlots; i++ {
		nodeIndex := i % len(allNodes)
		sc.SlotNodes[i] = allNodes[nodeIndex]
	}

	return sc
}

// IsMine 计算目标 Key 应该存放在哪个槽，并返回它是否属于当前节点
// 如果不属于，则同时返回真正持有该 Key 对应槽的节点地址以便重定向
func (sc *StaticCluster) IsMine(key string) (isMine bool, ownerAddr string) {
	slot := HashSlot(key)
	ownerAddr = sc.SlotNodes[slot]
	isMine = (ownerAddr == sc.SelfAddr)
	return
}

// HashSlot 使用 CRC32 算法进行哈希，并对槽位总数取模
func HashSlot(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % NumSlots
}

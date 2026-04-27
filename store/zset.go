package store

import (
	"sort"
	"sync"
)

type ZSetNode struct {
	Member string
	Score  float64
}

// ZSet 简单的有序集合实现 (为了快速验证，底层使用 Map + 有序 Slice。大规模生产环境应替换为跳表 SkipList)
type ZSet struct {
	mu    sync.RWMutex
	dict  map[string]float64
	nodes []ZSetNode 
}

func NewZSet() *ZSet {
	return &ZSet{
		dict:  make(map[string]float64),
		nodes: make([]ZSetNode, 0),
	}
}

// Add 往有序集合添加元素，返回是否是新添加的成员
func (zs *ZSet) Add(score float64, member string) bool {
	zs.mu.Lock()
	defer zs.mu.Unlock()

	oldScore, exists := zs.dict[member]
	if exists {
		if oldScore == score {
			return false
		}
		zs.dict[member] = score
		// 更新有序切片
		for i := range zs.nodes {
			if zs.nodes[i].Member == member {
				zs.nodes[i].Score = score
				break
			}
		}
		zs.sortNodes()
		return false
	}

	zs.dict[member] = score
	zs.nodes = append(zs.nodes, ZSetNode{Member: member, Score: score})
	zs.sortNodes()
	return true
}

// Remove 移除元素
func (zs *ZSet) Remove(member string) bool {
	zs.mu.Lock()
	defer zs.mu.Unlock()

	if _, exists := zs.dict[member]; !exists {
		return false
	}

	delete(zs.dict, member)
	for i, node := range zs.nodes {
		if node.Member == member {
			zs.nodes = append(zs.nodes[:i], zs.nodes[i+1:]...)
			break
		}
	}
	return true
}

func (zs *ZSet) sortNodes() {
	sort.Slice(zs.nodes, func(i, j int) bool {
		if zs.nodes[i].Score == zs.nodes[j].Score {
			return zs.nodes[i].Member < zs.nodes[j].Member
		}
		return zs.nodes[i].Score < zs.nodes[j].Score
	})
}

// Range 返回指定排名区间的成员 (包含 start 和 stop)
func (zs *ZSet) Range(start, stop int) []string {
	zs.mu.RLock()
	defer zs.mu.RUnlock()

	length := len(zs.nodes)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if start > stop || start >= length {
		return []string{}
	}
	if stop >= length {
		stop = length - 1
	}

	var res []string
	for i := start; i <= stop; i++ {
		res = append(res, zs.nodes[i].Member)
	}
	return res
}

// Score 获取成员的分数
func (zs *ZSet) Score(member string) (float64, bool) {
	zs.mu.RLock()
	defer zs.mu.RUnlock()
	score, ok := zs.dict[member]
	return score, ok
}

// Len 获取元素个数
func (zs *ZSet) Len() int {
	zs.mu.RLock()
	defer zs.mu.RUnlock()
	return len(zs.nodes)
}

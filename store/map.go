package store

import (
	"godis/utils"
	"sync"
	"time"
)

// Store 定义了基础 KV 存储引擎的接口
type Store interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Delete(key string)
	Len() int
	CountKeysInSlot(slot uint) int
	SetExpire(key string, ttl time.Duration) bool
	TTL(key string) time.Duration
}



func NewShardedMap(maxEntries int, policyType EvictionPolicyType) *ShardedMap {
	var policy EvictionPolicy
	switch policyType {
	case PolicyLRU:
		policy = NewLRUPolicy()
	case PolicyFIFO:
		policy = NewFIFOPolicy()
	case PolicyLFU:
		policy = NewLFUPolicy()
	default:
		policy = nil // No eviction
	}

	return &ShardedMap{
		data:       make(map[string]*Entry),
		maxEntries: maxEntries,
		policy:     policy,
	}
}

// ConcurrentMap 是一个基础的线程安全的字典实现。
type ConcurrentMap struct {
	mu         sync.RWMutex
	slots      map[uint]*ShardedMap
	maxEntries int
	policyType EvictionPolicyType
}

// NewConcurrentMap 初始化一个新的 ConcurrentMap
func NewConcurrentMap(maxEntries int, policyType EvictionPolicyType) *ConcurrentMap {
	return &ConcurrentMap{
		slots:      make(map[uint]*ShardedMap),
		maxEntries: maxEntries,
		policyType: policyType,
	}
}

func (m *ConcurrentMap) CountKeysInSlot(slot uint) int {
	sMap := m.GetSharedMapBySlot(slot)
	if sMap == nil {
		return 0
	}
	return sMap.Len()
}

// Get 获取键值
func (m *ConcurrentMap) Get(key string) (interface{}, bool) {
	sm := m.GetSharedMap(key)
	return sm.Get(key)
	
}

// Set 设置键值
func (m *ConcurrentMap) Set(key string, value interface{}) {
	sm := m.GetSharedMap(key)
	sm.Set(key,value)
}

// Delete 删除键值
func (m *ConcurrentMap) Delete(key string) {
	sm := m.GetSharedMap(key)
	sm.Delete(key)
}

// SetExpire 为键设置过期时间
func (m *ConcurrentMap) SetExpire(key string, ttl time.Duration) bool {
	sm := m.GetSharedMap(key)
	return sm.SetExpire(key, ttl)
}

// TTL 获取键的剩余过期时间
func (m *ConcurrentMap) TTL(key string) time.Duration {
	sm := m.GetSharedMap(key)
	return sm.TTL(key)
}

// Len 获取当前存储的元素数量
func (m *ConcurrentMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, sm := range m.slots {
		total += sm.Len()
	}
	return total
}

func (m *ConcurrentMap) GetSharedMap(key string) *ShardedMap {
	slot := utils.KeyHashSlot(key)
	return m.GetSharedMapBySlot(slot)
}

func (m *ConcurrentMap) GetSharedMapBySlot(slot uint) *ShardedMap {
	m.mu.RLock()
	sMap, ok := m.slots[slot]
	m.mu.RUnlock()

	if ok {
		return sMap
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check
	if sMap, ok = m.slots[slot]; ok {
		return sMap
	}

	
	shardLimit := 1024

	sMap = NewShardedMap(shardLimit, m.policyType)
	m.slots[slot] = sMap
	return sMap
}

package store

import "sync"

// Store 定义了基础 KV 存储引擎的接口
type Store interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Delete(key string)
	Len() int
}

// ConcurrentMap 是一个基础的线程安全的字典实现。
// 在分布式缓存乃至单机高并发场景中，你可以后续将其升级为分段锁 (Sharded Map) 以减少锁冲突。
type ConcurrentMap struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

// NewConcurrentMap 初始化一个新的 ConcurrentMap
func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		data: make(map[string]interface{}),
	}
}

// Get 获取键值
func (m *ConcurrentMap) Get(key string) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[key]
	return val, ok
}

// Set 设置键值
func (m *ConcurrentMap) Set(key string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Delete 删除键值
func (m *ConcurrentMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// Len 获取当前存储的元素数量
func (m *ConcurrentMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

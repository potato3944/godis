package store

import (
	"container/list"
	"sync"
	"time"
)

// Entry 存储 KV 及其元数据
type Entry struct {
	Key       string
	Value     any
	Accessed  time.Time     // 最后访问时间 (LRU)
	Created   time.Time     // 创建时间 (FIFO)
	Frequency int           // 访问频率 (LFU)
	Element   *list.Element // 指向链表节点的指针，方便快速删除
}

type ShardedMap struct {
	mu         sync.RWMutex
	data       map[string]*Entry
	maxEntries int
	policy     EvictionPolicy
}

func (sm *ShardedMap) Get(key string) (any, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	entry, ok := sm.data[key]
	if !ok {
		return nil, false
	}

	if sm.policy != nil {
		sm.policy.OnGet(entry)
	}
	return entry.Value, true
}

func (sm *ShardedMap) Set(key string, value any) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if entry, ok := sm.data[key]; ok {
		entry.Value = value
		if sm.policy != nil {
			sm.policy.OnSet(entry)
		}
		return
	}

	// Evict if necessary
	if sm.maxEntries > 0 && len(sm.data) >= sm.maxEntries && sm.policy != nil {
		if evicted := sm.policy.Evict(); evicted != nil {
			delete(sm.data, evicted.Key)
		}
	}

	entry := &Entry{
		Key:   key,
		Value: value,
	}
	sm.data[key] = entry
	if sm.policy != nil {
		sm.policy.OnSet(entry)
	}
}

func (sm *ShardedMap) Delete(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if entry, ok := sm.data[key]; ok {
		if sm.policy != nil {
			sm.policy.OnDelete(entry)
		}
		delete(sm.data, key)
	}
}


func (sm *ShardedMap) Len() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.data)
}

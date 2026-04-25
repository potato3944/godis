package store

import (
	"container/list"
	"time"
)

// EvictionPolicyType 定义了不同的淘汰策略类型
type EvictionPolicyType string

const (
	PolicyLRU  EvictionPolicyType = "LRU"
	PolicyLFU  EvictionPolicyType = "LFU"
	PolicyFIFO EvictionPolicyType = "FIFO"
	PolicyNone EvictionPolicyType = "NONE"
)



// EvictionPolicy 定义了淘汰策略需要实现的方法
type EvictionPolicy interface {
	OnGet(entry *Entry)
	OnSet(entry *Entry)
	OnDelete(entry *Entry)
	Evict() *Entry
}

// --- LRU Implementation ---

type LRUPolicy struct {
	list *list.List
}

func NewLRUPolicy() *LRUPolicy {
	return &LRUPolicy{list: list.New()}
}

func (p *LRUPolicy) OnGet(entry *Entry) {
	if entry.Element != nil {
		p.list.MoveToFront(entry.Element)
	}
	entry.Accessed = time.Now()
}

func (p *LRUPolicy) OnSet(entry *Entry) {
	if entry.Element == nil {
		entry.Element = p.list.PushFront(entry)
	} else {
		p.list.MoveToFront(entry.Element)
	}
	entry.Accessed = time.Now()
}

func (p *LRUPolicy) OnDelete(entry *Entry) {
	if entry.Element != nil {
		p.list.Remove(entry.Element)
		entry.Element = nil
	}
}

func (p *LRUPolicy) Evict() *Entry {
	el := p.list.Back()
	if el == nil {
		return nil
	}
	p.list.Remove(el)
	entry := el.Value.(*Entry)
	entry.Element = nil
	return entry
}

// --- FIFO Implementation ---

type FIFOPolicy struct {
	list *list.List
}

func NewFIFOPolicy() *FIFOPolicy {
	return &FIFOPolicy{list: list.New()}
}

func (p *FIFOPolicy) OnGet(entry *Entry) {
	// FIFO does nothing on access
}

func (p *FIFOPolicy) OnSet(entry *Entry) {
	if entry.Element == nil {
		entry.Element = p.list.PushBack(entry)
		entry.Created = time.Now()
	}
}

func (p *FIFOPolicy) OnDelete(entry *Entry) {
	if entry.Element != nil {
		p.list.Remove(entry.Element)
		entry.Element = nil
	}
}

func (p *FIFOPolicy) Evict() *Entry {
	el := p.list.Front()
	if el == nil {
		return nil
	}
	p.list.Remove(el)
	entry := el.Value.(*Entry)
	entry.Element = nil
	return entry
}
// --- LFU Implementation ---

type LFUPolicy struct {
	lists   map[int]*list.List
	minFreq int
}

func NewLFUPolicy() *LFUPolicy {
	return &LFUPolicy{
		lists:   make(map[int]*list.List),
		minFreq: 0,
	}
}

func (p *LFUPolicy) OnGet(entry *Entry) {
	oldFreq := entry.Frequency
	entry.Frequency++

	if p.lists[oldFreq] != nil && entry.Element != nil {
		p.lists[oldFreq].Remove(entry.Element)
		if p.lists[oldFreq].Len() == 0 && oldFreq == p.minFreq {
			p.minFreq++
		}
	}

	if p.lists[entry.Frequency] == nil {
		p.lists[entry.Frequency] = list.New()
	}
	entry.Element = p.lists[entry.Frequency].PushFront(entry)
}

func (p *LFUPolicy) OnSet(entry *Entry) {
	if entry.Element == nil {
		// New entry
		entry.Frequency = 1
		p.minFreq = 1
		if p.lists[1] == nil {
			p.lists[1] = list.New()
		}
		entry.Element = p.lists[1].PushFront(entry)
	} else {
		// Existing entry, same as OnGet
		p.OnGet(entry)
	}
}

func (p *LFUPolicy) OnDelete(entry *Entry) {
	if entry.Element != nil {
		if l, ok := p.lists[entry.Frequency]; ok {
			l.Remove(entry.Element)
		}
		entry.Element = nil
	}
}

func (p *LFUPolicy) Evict() *Entry {
	if p.minFreq == 0 {
		return nil
	}

	// Find the first non-empty list starting from minFreq
	for p.minFreq > 0 && (p.lists[p.minFreq] == nil || p.lists[p.minFreq].Len() == 0) {
		// If current minFreq is empty, we might need to search upwards.
		// However, in a healthy LFU, minFreq should point to a valid list.
		// If it doesn't, we search. This is rare but possible after deletions.
		found := false
		for f := p.minFreq + 1; f < p.minFreq+100; f++ { // Limit search range for safety
			if p.lists[f] != nil && p.lists[f].Len() > 0 {
				p.minFreq = f
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}

	l := p.lists[p.minFreq]
	el := l.Back()
	l.Remove(el)
	entry := el.Value.(*Entry)
	entry.Element = nil

	if l.Len() == 0 {
		// We'll find the next minFreq on the next Evict or OnGet call
	}

	return entry
}

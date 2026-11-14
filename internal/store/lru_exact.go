package store

import (
	"container/list"
	"sync"
)

// exactLRU keeps every key in a linked list, most recently used at the
// front. It exists only to compare against approximate LRU (ADR 012). It's
// the "textbook" LRU, and it shows why Redis doesn't use one:
//
//   - Every read moves a list node, so every GET becomes a write to one
//     shared structure. With 256 shard locks, this single mutex is what all
//     cores end up waiting on.
//   - Every key pays for a list element plus an index entry (~112 bytes).
//
// Lock order: callers already hold a shard lock when they call touch/remove,
// and the evictor takes the shard lock *before* calling remove. So the order
// is always shard lock → mu, which rules out deadlock.
type exactLRU struct {
	mu    sync.Mutex
	order list.List // front = most recent; values are lruItem
	index map[string]*list.Element
}

type lruItem struct {
	key  string
	slot uint16
}

func newExactLRU() *exactLRU { return &exactLRU{index: make(map[string]*list.Element)} }

func (l *exactLRU) touch(key string, slot uint16) {
	l.mu.Lock()
	if el, ok := l.index[key]; ok {
		l.order.MoveToFront(el)
	} else {
		l.index[key] = l.order.PushFront(lruItem{key, slot})
	}
	l.mu.Unlock()
}

func (l *exactLRU) remove(key string) {
	l.mu.Lock()
	if el, ok := l.index[key]; ok {
		l.order.Remove(el)
		delete(l.index, key)
	}
	l.mu.Unlock()
}

// oldest returns the least recently used key without removing it.
func (l *exactLRU) oldest() (lruItem, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	el := l.order.Back()
	if el == nil {
		return lruItem{}, false
	}
	return el.Value.(lruItem), true
}

func (l *exactLRU) reset() {
	l.mu.Lock()
	l.order.Init()
	l.index = make(map[string]*list.Element)
	l.mu.Unlock()
}

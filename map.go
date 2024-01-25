package shard

import (
	"sync"
)

// Fastest minimum mapShardCount for different cpu cores:
// 1 core: 16
// 2 ~ 15: 512
// 16 ~ 31: 1024
// 32: 2048
// However, the speed difference between using 512 and 2048 shards is not very significant, and the latter uses more memory.
const mapShardCount = 512

type MapOptionFunc func(*MapOption)

type MapOption struct {
	size uint
}

type Map[T any] struct {
	shards []MapShard[T]
	pool   sync.Pool
}

type MapShard[T any] struct {
	items map[string]T
	lock  sync.RWMutex
}

func MapSize(size uint) MapOptionFunc {
	return func(o *MapOption) {
		o.size = size
	}
}

func fnv1a32(key string) uint32 {
	hash := uint32(2166136261)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return hash
}

func NewMap[T any](funcs ...MapOptionFunc) *Map[T] {
	option := &MapOption{}
	for _, f := range funcs {
		f(option)
	}

	m := Map[T]{
		shards: make([]MapShard[T], mapShardCount),
		pool: sync.Pool{
			New: func() interface{} {
				return make(map[string]T, option.size)
			},
		},
	}
	for i := 0; i < mapShardCount; i++ {
		m.shards[i] = MapShard[T]{
			items: make(map[string]T, option.size),
		}
	}
	return &m
}

func (m *Map[T]) Get(key string) (value T, ok bool) {
	shard := &m.shards[fnv1a32(key)%mapShardCount]
	shard.lock.RLock()
	value, ok = shard.items[key]
	shard.lock.RUnlock()
	return
}

func (m *Map[T]) Set(key string, value T) {
	shard := &m.shards[fnv1a32(key)%mapShardCount]
	shard.lock.Lock()
	shard.items[key] = value
	shard.lock.Unlock()
	return
}

func (m *Map[T]) PopAll() []map[string]T {
	all := make([]map[string]T, 0, mapShardCount)
	for i := 0; i < mapShardCount; i++ {
		shard := &m.shards[i]
		shard.lock.Lock()
		if len(shard.items) > 0 {
			all = append(all, shard.items)
			shard.items = m.pool.Get().(map[string]T)
		}
		shard.lock.Unlock()
	}
	return all
}

func (m *Map[T]) Reuse(items map[string]T) {
	for k := range items {
		delete(items, k)
	}
	m.pool.Put(items)
}

package shard

import (
	"runtime"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/cpu"
)

const CacheLinePadSize = unsafe.Sizeof(cpu.CacheLinePad{})

var sliceShardCount = runtime.GOMAXPROCS(0)

type SliceOptionFunc func(*SliceOption)

type SliceOption struct {
	size     uint
	waitTime time.Duration
}

type Slice[T any] struct {
	shards   []SliceShard[T]
	pool     sync.Pool
	waitTime time.Duration
}

type SliceShard[T any] struct {
	items []T
	_     [CacheLinePadSize - unsafe.Sizeof([]T{})%CacheLinePadSize]byte
}

func SliceSize(size uint) SliceOptionFunc {
	return func(o *SliceOption) {
		o.size = size
	}
}

func WaitTime(waitTime time.Duration) SliceOptionFunc {
	return func(o *SliceOption) {
		o.waitTime = waitTime
	}
}

func NewSlice[T any](funcs ...SliceOptionFunc) *Slice[T] {
	option := &SliceOption{waitTime: time.Millisecond * 100} // default waitTime
	for _, f := range funcs {
		f(option)
	}

	s := Slice[T]{
		shards: make([]SliceShard[T], sliceShardCount),
		pool: sync.Pool{
			New: func() interface{} {
				return make([]T, 0, option.size)
			},
		},
		waitTime: option.waitTime,
	}

	for i := 0; i < sliceShardCount; i++ {
		s.shards[i].items = make([]T, 0, option.size)
	}
	return &s
}

func (s *Slice[T]) Append(item T) {
	shardID := procPin()
	shard := &s.shards[shardID]
	shard.items = append(shard.items, item)
	procUnpin()
}

func (s *Slice[T]) popAll() []SliceShard[T] {
	newShards := make([]SliceShard[T], sliceShardCount)
	for i := 0; i < sliceShardCount; i++ {
		newShards[i].items = s.pool.Get().([]T)
	}
	oldShards := s.shards
	s.shards = newShards
	return oldShards
}

func (s *Slice[T]) PopAll() [][]T {
	shards := s.popAll()
	if s.waitTime > 0 {
		time.Sleep(s.waitTime)
	}

	result := make([][]T, 0, len(shards))
	for i := 0; i < sliceShardCount; i++ {
		items := shards[i].items
		if len(items) > 0 {
			result = append(result, items)
		} else {
			s.Reuse(items)
		}
	}
	return result
}

func (s *Slice[T]) Reuse(items []T) {
	s.pool.Put(items[:0])
}

//go:noescape
//go:linkname procPin runtime.procPin
func procPin() int

//go:noescape
//go:linkname procUnpin runtime.procUnpin
func procUnpin()

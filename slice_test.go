package shard

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

const itemCountPerGoroutine = 1000000
const sliceWaitMS = 300

var sliceSize = itemCountPerGoroutine * sliceShardCount // make sure if all the items are inserted into one shard, it won't reallocate

func appendSlice(s *Slice[int]) {
	wg := sync.WaitGroup{}
	wg.Add(sliceShardCount)
	for i := 0; i < sliceShardCount; i++ {
		go func() {
			for j := 0; j < itemCountPerGoroutine; j++ {
				s.Append(j)
			}
		}()
		wg.Done()
	}
	wg.Wait()
}

func TestSliceAppend(t *testing.T) {
	s := NewSlice[int](SliceSize(uint(sliceSize)))
	appendSlice(s)

	count := 0
	for i := 0; i < sliceWaitMS; i++ {
		time.Sleep(time.Millisecond) // wait for cpu cache of s.shards
		count = 0

		for _, shard := range s.shards {
			count += len(shard.items)
		}

		if count == sliceSize {
			t.Logf("wait %d ms", i+1)
			return
		}
	}
	t.Errorf("count = %d, want %d", count, sliceSize)
}

func TestSlicePopAll(t *testing.T) {
	s := NewSlice[int](SliceSize(uint(sliceSize)))

outerLoop:
	for j := 0; j < 100; j++ {
		appendSlice(s)

		shards := s.popAll()
		if len(shards) != sliceShardCount {
			t.Fatalf("len(shards) = %d, want %d", len(shards), sliceShardCount)
		}

		count := 0
		for i := 0; i < sliceWaitMS; i++ {
			time.Sleep(time.Millisecond) // wait for cpu cache of s.shards
			count = 0

			for _, shard := range shards {
				count += len(shard.items)
			}

			for _, shard := range s.shards {
				count += len(shard.items)
			}

			if count == sliceSize {
				t.Logf("wait %d ms", i+1)

				for _, shard := range shards {
					s.Reuse(shard.items)
				}
				shards := s.popAll() // popAll() twice to make sure the shards are empty
				for _, shard := range shards {
					s.Reuse(shard.items)
				}
				continue outerLoop
			}
		}
		t.Fatalf("count = %d, want %d", count, sliceSize)
	}
}

func TestSliceAppendAndPopAll(t *testing.T) {
	s := NewSlice[int](SliceSize(uint(sliceSize)))

	stop := make(chan struct{})
	for j := 0; j < 10; j++ {
		go func() {
			ticker := time.NewTicker(time.Millisecond * 100)
			total := [][]SliceShard[int]{}
		loop:
			for {
				select {
				case <-stop:
					all := s.popAll()
					total = append(total, all)
					break loop
				case <-ticker.C:
					all := s.popAll()
					total = append(total, all)
				}
			}
			ticker.Stop()

			count := 0
			for i := 0; i < sliceWaitMS+1; i++ {
				count = 0
				for _, shards := range total {
					for _, shard := range shards {
						count += len(shard.items)
					}
				}
				for _, shard := range s.shards {
					count += len(shard.items)
				}
				if count == sliceSize {
					t.Logf("wait %d ms", i)
					break
				}
				if i == sliceWaitMS {
					t.Errorf("count = %d, want %d", count, sliceSize)
					return
				}
				time.Sleep(time.Millisecond) // wait for cpu cache of s.shards
			}

			s.popAll() // clears s.shards

			stop <- struct{}{} // notifies popAll() has done
		}()

		appendSlice(s)
		stop <- struct{}{} // notifies Append() has done
		<-stop             // waits for popAll()
	}
}

func TestSlicePopAllWait(t *testing.T) {
	s := NewSlice[int](SliceSize(uint(sliceSize)), WaitTime(time.Millisecond*sliceWaitMS))
	for i := 0; i < 10; i++ {
		appendSlice(s)
		shards := s.PopAll()

		count := 0
		for _, shard := range shards {
			count += len(shard)
		}
		for _, shard := range s.shards {
			count += len(shard.items)
		}

		if count != sliceSize {
			t.Fatalf("count = %d, want %d", count, sliceSize)
		} else {
			for _, shard := range shards {
				s.Reuse(shard)
			}
			shards := s.popAll() // popAll() twice to make sure the shards are empty
			for _, shard := range shards {
				s.Reuse(shard.items)
			}
		}
	}
}

func TestSliceReuse(t *testing.T) {
	s := NewSlice[int](SliceSize(uint(sliceSize)))
	appendSlice(s)

	shards := s.popAll()
	for {
		time.Sleep(time.Millisecond) // wait for cpu cache of s.shards

		count := 0
		for _, shard := range shards {
			count += len(shard.items)
		}
		for _, shard := range s.shards {
			count += len(shard.items)
		}
		if count == sliceSize {
			break
		}
	}

	for _, shard := range shards {
		s.Reuse(shard.items)
	}
	ptr := (*reflect.SliceHeader)(unsafe.Pointer(&shards[0].items)).Data

	s.popAll()
	for _, shard := range s.shards {
		if (*reflect.SliceHeader)(unsafe.Pointer(&shard.items)).Data == ptr {
			return
		}
	}
	t.Error("items not reused")
}

func BenchmarkSliceAppend(b *testing.B) {
	s := NewSlice[int](SliceSize(100000000))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Append(0)
		}
	})
}

func BenchmarkMutexSliceAppend(b *testing.B) {
	s := make([]int, 0, 100000000)
	l := sync.Mutex{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Lock()
			s = append(s, 0)
			l.Unlock()
		}
	})
}

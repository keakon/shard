package shard

import (
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

const (
	stringsCount = 1 << 15
	mapSize      = stringsCount / mapShardCount * 8
)

var strings = make([]string, stringsCount)

func init() {
	for i := 0; i < stringsCount; i++ {
		strings[i] = strconv.Itoa(i)
	}
}

func TestNewMap(t *testing.T) {
	NewMap[string]()
	NewMap[string](MapSize(1000))
}

func TestMapSet(t *testing.T) {
	m := NewMap[int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)
}

func TestMapGet(t *testing.T) {
	m := NewMap[int]()

	tests := []struct {
		key   string
		value int
	}{
		{"a", 1},
		{"b", -2},
		{"c", 3456789},
		{"d", 0},
	}

	for _, test := range tests {
		if _, ok := m.Get(test.key); ok {
			t.Errorf("key %s exists", test.key)
		}
		m.Set(test.key, test.value)
		if v, ok := m.Get(test.key); !ok || v != test.value {
			t.Errorf("got %d, excepted %d", v, test.value)
		}
	}
}

func TestMapPopAll(t *testing.T) {
	m := NewMap[int]()
	if len(m.PopAll()) != 0 {
		t.FailNow()
	}

	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)
	all := m.PopAll()
	if len(all) == 0 || len(all) > 3 {
		t.FailNow()
	}

	mergedMap := map[string]int{}
	for _, m := range all {
		for k, v := range m {
			mergedMap[k] = v
		}
	}
	if !reflect.DeepEqual(mergedMap, map[string]int{"a": 1, "b": 2, "c": 3}) {
		t.FailNow()
	}
}

func TestMapReuse(t *testing.T) {
	m := NewMap[int]()

	m.Set("a", 1)
	all := m.PopAll()
	if len(all) != 1 {
		t.FailNow()
	}
	items := all[0]
	m.Reuse(items)

	all = m.PopAll()
	if len(all) != 0 {
		t.FailNow()
	}

	m.Set("a", 2)
	all = m.PopAll()
	if len(all) != 1 {
		t.FailNow()
	}

	items["a"] = 3
	if v, ok := m.Get("a"); !ok || v != 3 {
		t.FailNow()
	}
}

func BenchmarkMapSet(b *testing.B) {
	m := NewMap[uint32](MapSize(mapSize))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := rand.Uint32()
		for pb.Next() {
			m.Set(strings[x%mapSize], x)
			x++
		}
	})
}

func BenchmarkMapGet(b *testing.B) {
	m := NewMap[uint64](MapSize(mapSize))
	for i := 0; i < mapSize; i++ {
		m.Set(strings[i], uint64(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := rand.Uint32()
		for pb.Next() {
			m.Get(strings[x%mapSize])
		}
	})
}

func BenchmarkMutexMapSet(b *testing.B) {
	m := make(map[string]uint32, mapSize)
	l := sync.Mutex{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := rand.Uint32()
		for pb.Next() {
			l.Lock()
			m[strings[x%mapSize]] = x
			l.Unlock()
			x++
		}
	})
}

func BenchmarkMutexMapGet(b *testing.B) {
	m := make(map[string]uint32, mapSize)
	l := sync.Mutex{}
	for i := 0; i < mapSize; i++ {
		m[strings[i]] = uint32(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := rand.Uint32()
		for pb.Next() {
			l.Lock()
			_ = m[strings[x%mapSize]]
			l.Unlock()
			x++
		}
	})
}

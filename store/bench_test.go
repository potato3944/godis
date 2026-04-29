package store

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkConcurrentMap_Set(b *testing.B) {
	m := NewConcurrentMap(0, PolicyNone)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key%d", i), i)
	}
}

func BenchmarkConcurrentMap_Get(b *testing.B) {
	m := NewConcurrentMap(0, PolicyNone)
	for i := 0; i < 10000; i++ {
		m.Set(fmt.Sprintf("key%d", i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(fmt.Sprintf("key%d", i%10000))
	}
}

func BenchmarkConcurrentMap_SetParallel(b *testing.B) {
	m := NewConcurrentMap(0, PolicyNone)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(fmt.Sprintf("key%d", i), i)
			i++
		}
	})
}

func BenchmarkConcurrentMap_GetParallel(b *testing.B) {
	m := NewConcurrentMap(0, PolicyNone)
	for i := 0; i < 10000; i++ {
		m.Set(fmt.Sprintf("key%d", i), i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Get(fmt.Sprintf("key%d", i%10000))
			i++
		}
	})
}

func BenchmarkConcurrentMap_LRU_Set(b *testing.B) {
	m := NewConcurrentMap(1000, PolicyLRU)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key%d", i), i)
	}
}

func BenchmarkConcurrentMap_FIFO_Set(b *testing.B) {
	m := NewConcurrentMap(1000, PolicyFIFO)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key%d", i), i)
	}
}

func BenchmarkConcurrentMap_LFU_Set(b *testing.B) {
	m := NewConcurrentMap(1000, PolicyLFU)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key%d", i), i)
	}
}

func BenchmarkZSet_Add(b *testing.B) {
	zs := NewZSet()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zs.Add(float64(i), fmt.Sprintf("member%d", i))
	}
}

func BenchmarkZSet_Range(b *testing.B) {
	zs := NewZSet()
	for i := 0; i < 1000; i++ {
		zs.Add(float64(i), fmt.Sprintf("member%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zs.Range(0, 100)
	}
}

func BenchmarkAOF_Append(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "godis_aof_bench")
	if err != nil {
		b.Fatal(err)
	}
	filename := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(filename)

	m := NewConcurrentMap(0, PolicyNone)
	aof, err := NewAOF(filename, m)
	if err != nil {
		b.Fatal(err)
	}
	defer aof.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aof.Append("SET", "key", "value")
	}
}

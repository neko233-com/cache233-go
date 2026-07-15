package cache233

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neko233-com/cache233-go/stats"
)

// ---------------------------------------------------------------------------
// Helpers for benchmarks
// ---------------------------------------------------------------------------

func newBenchCache(size int) *Cache[int, int] {
	return Must(&Options[int, int]{
		MaximumSize:      size,
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
	})
}

func newBenchCacheWithStats(size int) *Cache[int, int] {
	return Must(&Options[int, int]{
		MaximumSize:      size,
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
		StatsRecorder:    stats.NewCounter(),
	})
}

// ---------------------------------------------------------------------------
// 1. Read-heavy benchmark: 95% read, 5% write
// ---------------------------------------------------------------------------

func BenchmarkReadHeavy_Cache233(b *testing.B) {
	const size = 1024
	c := newBenchCacheWithStats(size)
	for i := 0; i < size; i++ {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 { // 5% writes
				c.Set(i%size, i)
			} else { // 95% reads
				c.GetIfPresent(i % size)
			}
			i++
		}
	})
}

func BenchmarkReadHeavy_SyncMap(b *testing.B) {
	const size = 1024
	var m sync.Map
	for i := 0; i < size; i++ {
		m.Store(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				m.Store(i%size, i)
			} else {
				m.Load(i % size)
			}
			i++
		}
	})
}

func BenchmarkReadHeavy_RWMutexMap(b *testing.B) {
	const size = 1024
	var mu sync.RWMutex
	m := make(map[int]int, size)
	for i := 0; i < size; i++ {
		m[i] = i
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				mu.Lock()
				m[i%size] = i
				mu.Unlock()
			} else {
				mu.RLock()
				_ = m[i%size]
				mu.RUnlock()
			}
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// 2. Write-heavy benchmark: 50% read, 50% write
// ---------------------------------------------------------------------------

func BenchmarkWriteHeavy_Cache233(b *testing.B) {
	const size = 1024
	c := newBenchCacheWithStats(size)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			if i%2 == 0 {
				c.Set(key, i)
			} else {
				c.GetIfPresent(key)
			}
			i++
		}
	})
}

func BenchmarkWriteHeavy_SyncMap(b *testing.B) {
	const size = 1024
	var m sync.Map

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			if i%2 == 0 {
				m.Store(key, i)
			} else {
				m.Load(key)
			}
			i++
		}
	})
}

func BenchmarkWriteHeavy_RWMutexMap(b *testing.B) {
	const size = 1024
	var mu sync.RWMutex
	m := make(map[int]int, size)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			if i%2 == 0 {
				mu.Lock()
				m[key] = i
				mu.Unlock()
			} else {
				mu.RLock()
				_ = m[key]
				mu.RUnlock()
			}
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// 3. Mixed with TTL benchmark
// ---------------------------------------------------------------------------

func BenchmarkMixedWithTTL_Cache233(b *testing.B) {
	const size = 1024
	c := Must(&Options[int, int]{
		MaximumSize:      size,
		ExpiryCalculator: ExpiryWriting[int, int](5 * time.Second),
		StatsRecorder:    stats.NewCounter(),
	})
	for i := 0; i < size; i++ {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			switch i % 4 {
			case 0: // 25% write
				c.Set(key, i)
			case 1: // 25% read hit
				c.GetIfPresent(key)
			case 2: // 25% invalidate
				c.Invalidate(key)
			case 3: // 25% compute
				c.Compute(key, func(old int, found bool) (int, ComputeOp) {
					return i, WriteOp
				})
			}
			i++
		}
	})
}

func BenchmarkMixedWithTTL_SyncMap(b *testing.B) {
	const size = 1024
	var m sync.Map
	for i := 0; i < size; i++ {
		m.Store(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			switch i % 4 {
			case 0:
				m.Store(key, i)
			case 1:
				m.Load(key)
			case 2:
				m.Delete(key)
			case 3:
				m.Store(key, i)
			}
			i++
		}
	})
}

func BenchmarkMixedWithTTL_RWMutexMap(b *testing.B) {
	const size = 1024
	var mu sync.RWMutex
	m := make(map[int]int, size)
	for i := 0; i < size; i++ {
		m[i] = i
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			switch i % 4 {
			case 0:
				mu.Lock()
				m[key] = i
				mu.Unlock()
			case 1:
				mu.RLock()
				_ = m[key]
				mu.RUnlock()
			case 2:
				mu.Lock()
				delete(m, key)
				mu.Unlock()
			case 3:
				mu.Lock()
				m[key] = i
				mu.Unlock()
			}
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// 4. Single-flight loader dedup benchmark
// ---------------------------------------------------------------------------

func BenchmarkSingleFlight_Cache233(b *testing.B) {
	const size = 64
	c := newBenchCache(size)
	loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
		return key * 10, nil
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(context.Background(), i%size, loader)
			i++
		}
	})
}

func BenchmarkSingleFlight_SyncMap(b *testing.B) {
	const size = 64
	var m sync.Map
	for i := 0; i < size; i++ {
		m.Store(i, i*10)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			if v, ok := m.Load(key); ok {
				_ = v
			} else {
				m.Store(key, key*10)
			}
			i++
		}
	})
}

func BenchmarkSingleFlight_RWMutexMap(b *testing.B) {
	const size = 64
	var mu sync.RWMutex
	m := make(map[int]int, size)
	for i := 0; i < size; i++ {
		m[i] = i * 10
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			mu.RLock()
			v, ok := m[key]
			mu.RUnlock()
			if !ok {
				mu.Lock()
				m[key] = key * 10
				mu.Unlock()
			} else {
				_ = v
			}
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// 5. Set + GetIfPresent (pure read/write, no TTL)
// ---------------------------------------------------------------------------

func BenchmarkSetGet_Cache233(b *testing.B) {
	c := newBenchCache(10240)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(i, i)
			c.GetIfPresent(i)
			i++
		}
	})
}

func BenchmarkSetGet_SyncMap(b *testing.B) {
	var m sync.Map

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			m.Load(i)
			i++
		}
	})
}

func BenchmarkSetGet_RWMutexMap(b *testing.B) {
	var mu sync.RWMutex
	m := make(map[int]int)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mu.Lock()
			m[i] = i
			mu.Unlock()
			mu.RLock()
			_ = m[i]
			mu.RUnlock()
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// 6. Compute operations benchmark
// ---------------------------------------------------------------------------

func BenchmarkCompute_Cache233(b *testing.B) {
	const size = 1024
	c := newBenchCache(size)
	for i := 0; i < size; i++ {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Compute(i%size, func(old int, found bool) (int, ComputeOp) {
				return old + 1, WriteOp
			})
			i++
		}
	})
}

func BenchmarkCompute_SyncMap(b *testing.B) {
	const size = 1024
	var m sync.Map
	for i := 0; i < size; i++ {
		m.Store(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			m.Store(key, i)
			i++
		}
	})
}

func BenchmarkCompute_RWMutexMap(b *testing.B) {
	const size = 1024
	var mu sync.RWMutex
	m := make(map[int]int, size)
	for i := 0; i < size; i++ {
		m[i] = i
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % size
			mu.Lock()
			m[key] = i
			mu.Unlock()
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// 7. Contention benchmark with many goroutines
// ---------------------------------------------------------------------------

func BenchmarkContention_Cache233(b *testing.B) {
	c := newBenchCache(1024)
	for i := 0; i < 1024; i++ {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % 1024
			if i%10 < 3 {
				c.Set(key, i)
			} else {
				c.GetIfPresent(key)
			}
			i++
		}
	})
}

func BenchmarkContention_SyncMap(b *testing.B) {
	var m sync.Map
	for i := 0; i < 1024; i++ {
		m.Store(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % 1024
			if i%10 < 3 {
				m.Store(key, i)
			} else {
				m.Load(key)
			}
			i++
		}
	})
}

func BenchmarkContention_RWMutexMap(b *testing.B) {
	var mu sync.RWMutex
	m := make(map[int]int, 1024)
	for i := 0; i < 1024; i++ {
		m[i] = i
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % 1024
			if i%10 < 3 {
				mu.Lock()
				m[key] = i
				mu.Unlock()
			} else {
				mu.RLock()
				_ = m[key]
				mu.RUnlock()
			}
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// 8. Invalidation benchmark
// ---------------------------------------------------------------------------

func BenchmarkInvalidate_Cache233(b *testing.B) {
	const size = 1024
	c := newBenchCacheWithStats(size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i%size, i)
		c.Invalidate(i % size)
	}
}

func BenchmarkInvalidate_SyncMap(b *testing.B) {
	const size = 1024
	var m sync.Map

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Store(i%size, i)
		m.Delete(i % size)
	}
}

func BenchmarkInvalidate_RWMutexMap(b *testing.B) {
	const size = 1024
	var mu sync.RWMutex
	m := make(map[int]int, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m[i%size] = i
		mu.Unlock()
		mu.Lock()
		delete(m, i%size)
		mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// 9. Size-limited cache eviction benchmark
// ---------------------------------------------------------------------------

func BenchmarkEviction_Cache233(b *testing.B) {
	c := newBenchCacheWithStats(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i, i)
	}
}

func BenchmarkEviction_SyncMap(b *testing.B) {
	var m sync.Map
	const limit = 1024

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Store(i%limit, i)
		if i > limit*2 {
			m.Delete((i - limit*2) % limit)
		}
	}
}

func BenchmarkEviction_RWMutexMap(b *testing.B) {
	var mu sync.RWMutex
	m := make(map[int]int)
	const limit = 1024

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m[i%limit] = i
		if i > limit*2 {
			delete(m, (i-limit*2)%limit)
		}
		mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// 10. Full lifecycle benchmark with TTL + stats
// ---------------------------------------------------------------------------

func BenchmarkFullLifecycle_Cache233(b *testing.B) {
	c := Must(&Options[int, int]{
		MaximumSize:      4096,
		ExpiryCalculator: ExpiryWriting[int, int](100 * time.Millisecond),
		StatsRecorder:    stats.NewCounter(),
	})
	loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
		return key, nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := i % 4096
		switch i % 5 {
		case 0:
			c.Set(key, i)
		case 1:
			c.GetIfPresent(key)
		case 2:
			c.Invalidate(key)
		case 3:
			c.Get(context.Background(), key, loader)
		case 4:
			c.Compute(key, func(old int, found bool) (int, ComputeOp) {
				return i, WriteOp
			})
		}
	}
}

// ---------------------------------------------------------------------------
// 11. Parallel Get with loader (single-flight contention)
// ---------------------------------------------------------------------------

func BenchmarkParallelGetWithLoader_Cache233(b *testing.B) {
	const size = 16
	c := newBenchCache(size)
	var loadCount atomic.Int64
	loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
		loadCount.Add(1)
		time.Sleep(5 * time.Millisecond) // simulate slow load
		return key * 10, nil
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(context.Background(), i%size, loader)
			i++
		}
	})
	b.ReportMetric(float64(loadCount.Load()), "loads")
}

// ---------------------------------------------------------------------------
// 12. Iterator benchmark
// ---------------------------------------------------------------------------

func BenchmarkIterator_All_Cache233(b *testing.B) {
	const size = 1024
	c := newBenchCache(size)
	for i := 0; i < size; i++ {
		c.Set(i, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range c.All() {
		}
	}
}

func BenchmarkIterator_SyncMap(b *testing.B) {
	const size = 1024
	var m sync.Map
	for i := 0; i < size; i++ {
		m.Store(i, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Range(func(key, value any) bool {
			return true
		})
	}
}

// ---------------------------------------------------------------------------
// 13. Memory footprint benchmark (small items)
// ---------------------------------------------------------------------------

func BenchmarkMemory_Cache233(b *testing.B) {
	c := newBenchCache(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i%1024, i)
	}
}

func BenchmarkMemory_SyncMap(b *testing.B) {
	var m sync.Map
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Store(i%1024, i)
	}
}

// ---------------------------------------------------------------------------
// 14. Scaling benchmark: different cache sizes
// ---------------------------------------------------------------------------

func BenchmarkScaling_Cache233(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096, 16384} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			c := newBenchCache(size)
			for i := 0; i < size; i++ {
				c.Set(i, i)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					c.GetIfPresent(i % size)
					i++
				}
			})
		})
	}
}

func BenchmarkScaling_SyncMap(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096, 16384} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			var m sync.Map
			for i := 0; i < size; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					m.Load(i % size)
					i++
				}
			})
		})
	}
}

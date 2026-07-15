package cache233

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/neko233-com/cache233-go/stats"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type testErr string

func (e testErr) Error() string { return string(e) }

func syncExecutor() func(fn func()) {
	return func(fn func()) { fn() }
}

// ---------------------------------------------------------------------------
// 1. Basic CRUD
// ---------------------------------------------------------------------------

func TestFull_Set_GetIfPresent(t *testing.T) {
	t.Parallel()

	t.Run("new_key_returns_true", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, string]{})
		v, ok := c.Set(1, "hello")
		require.True(t, ok)
		require.Equal(t, "hello", v)

		got, found := c.GetIfPresent(1)
		require.True(t, found)
		require.Equal(t, "hello", got)
	})

	t.Run("existing_key_returns_old_value", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, string]{})
		c.Set(1, "first")
		v, ok := c.Set(1, "second")
		require.False(t, ok)
		require.Equal(t, "first", v)

		got, found := c.GetIfPresent(1)
		require.True(t, found)
		require.Equal(t, "second", got)
	})

	t.Run("missing_key_returns_zero", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, string]{})
		got, found := c.GetIfPresent(999)
		require.False(t, found)
		require.Equal(t, "", got)
	})
}

func TestFull_SetIfAbsent(t *testing.T) {
	t.Parallel()

	t.Run("absent_inserts", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		v, ok := c.SetIfAbsent(1, 10)
		require.True(t, ok)
		require.Equal(t, 10, v)
	})

	t.Run("present_keeps_old", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.SetIfAbsent(1, 20)
		require.False(t, ok)
		require.Equal(t, 10, v)

		got, _ := c.GetIfPresent(1)
		require.Equal(t, 10, got)
	})
}

func TestFull_GetEntry(t *testing.T) {
	t.Parallel()

	c := Must(&Options[string, int]{
		ExpiryCalculator: ExpiryWriting[string, int](time.Hour),
	})
	c.Set("key", 42)

	entry, found := c.GetEntry("key")
	require.True(t, found)
	require.Equal(t, "key", entry.Key)
	require.Equal(t, 42, entry.Value)
	require.Equal(t, uint32(1), entry.Weight)

	_, found = c.GetEntry("missing")
	require.False(t, found)
}

func TestFull_GetEntryQuietly(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
	})
	c.Set(1, 100)

	entry, found := c.GetEntryQuietly(1)
	require.True(t, found)
	require.Equal(t, 100, entry.Value)

	_, found = c.GetEntryQuietly(999)
	require.False(t, found)
}

// ---------------------------------------------------------------------------
// 2. Compute operations
// ---------------------------------------------------------------------------

func TestFull_Compute(t *testing.T) {
	t.Parallel()

	t.Run("write_new", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		v, ok := c.Compute(1, func(old int, found bool) (int, ComputeOp) {
			require.False(t, found)
			return 42, WriteOp
		})
		require.True(t, ok)
		require.Equal(t, 42, v)

		got, _ := c.GetIfPresent(1)
		require.Equal(t, 42, got)
	})

	t.Run("write_existing", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.Compute(1, func(old int, found bool) (int, ComputeOp) {
			require.True(t, found)
			require.Equal(t, 10, old)
			return old + 5, WriteOp
		})
		require.True(t, ok)
		require.Equal(t, 15, v)
	})

	t.Run("cancel_on_missing", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		v, ok := c.Compute(1, func(old int, found bool) (int, ComputeOp) {
			return 0, CancelOp
		})
		require.False(t, ok)
		require.Equal(t, 0, v)

		_, found := c.GetIfPresent(1)
		require.False(t, found)
	})

	t.Run("cancel_on_existing", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.Compute(1, func(old int, found bool) (int, ComputeOp) {
			return 0, CancelOp
		})
		require.True(t, ok)
		require.Equal(t, 10, v)
	})

	t.Run("invalidate_existing", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.Compute(1, func(old int, found bool) (int, ComputeOp) {
			return 0, InvalidateOp
		})
		require.False(t, ok)
		require.Equal(t, 0, v)

		_, found := c.GetIfPresent(1)
		require.False(t, found)
	})

	t.Run("invalid_op_panics", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		require.Panics(t, func() {
			c.Compute(1, func(old int, found bool) (int, ComputeOp) {
				return 0, ComputeOp(99)
			})
		})
	})
}

func TestFull_ComputeIfAbsent(t *testing.T) {
	t.Parallel()

	t.Run("absent_inserts", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		v, ok := c.ComputeIfAbsent(1, func() (int, bool) {
			return 42, false
		})
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("present_returns_existing", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.ComputeIfAbsent(1, func() (int, bool) {
			panic("should not be called")
		})
		require.True(t, ok)
		require.Equal(t, 10, v)
	})

	t.Run("cancel_skips_insert", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		v, ok := c.ComputeIfAbsent(1, func() (int, bool) {
			return 0, true
		})
		require.False(t, ok)
		require.Equal(t, 0, v)

		_, found := c.GetIfPresent(1)
		require.False(t, found)
	})
}

func TestFull_ComputeIfPresent(t *testing.T) {
	t.Parallel()

	t.Run("write_updates", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.ComputeIfPresent(1, func(old int) (int, ComputeOp) {
			return old * 2, WriteOp
		})
		require.True(t, ok)
		require.Equal(t, 20, v)
	})

	t.Run("cancel_leaves_unchanged", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.ComputeIfPresent(1, func(old int) (int, ComputeOp) {
			return 0, CancelOp
		})
		require.True(t, ok)
		require.Equal(t, 10, v)
	})

	t.Run("invalidate_removes", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.ComputeIfPresent(1, func(old int) (int, ComputeOp) {
			return 0, InvalidateOp
		})
		require.False(t, ok)
		require.Equal(t, 0, v)
	})

	t.Run("missing_returns_zero", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		v, ok := c.ComputeIfPresent(999, func(old int) (int, ComputeOp) {
			panic("should not be called")
		})
		require.False(t, ok)
		require.Equal(t, 0, v)
	})
}

// ---------------------------------------------------------------------------
// 3. TTL / Expiry
// ---------------------------------------------------------------------------

func TestFull_ExpiryCreating(t *testing.T) {
	t.Parallel()

	fs := &fakeSource{}
	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryCreating[int, int](100 * time.Millisecond),
		Clock:            fs,
	})

	c.Set(1, 100)
	_, found := c.GetIfPresent(1)
	require.True(t, found)

	fs.Sleep(150 * time.Millisecond)
	c.CleanUp()

	_, found = c.GetIfPresent(1)
	require.False(t, found)
}

func TestFull_ExpiryWriting(t *testing.T) {
	t.Parallel()

	fs := &fakeSource{}
	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryWriting[int, int](100 * time.Millisecond),
		Clock:            fs,
	})

	c.Set(1, 100)
	fs.Sleep(80 * time.Millisecond)
	// Read does NOT reset expiry under ExpiryWriting
	_, found := c.GetIfPresent(1)
	require.True(t, found)

	fs.Sleep(80 * time.Millisecond) // total 160ms > 100ms
	c.CleanUp()

	_, found = c.GetIfPresent(1)
	require.False(t, found)
}

func TestFull_ExpiryAccessing(t *testing.T) {
	t.Parallel()

	fs := &fakeSource{}
	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryAccessing[int, int](100 * time.Millisecond),
		Clock:            fs,
	})

	c.Set(1, 100)
	fs.Sleep(80 * time.Millisecond)
	// Read resets expiry under ExpiryAccessing
	_, found := c.GetIfPresent(1)
	require.True(t, found)

	fs.Sleep(80 * time.Millisecond) // 80ms after last read, still fresh
	_, found = c.GetIfPresent(1)
	require.True(t, found)

	fs.Sleep(120 * time.Millisecond) // 120ms after last read, expired
	c.CleanUp()

	_, found = c.GetIfPresent(1)
	require.False(t, found)
}

func TestFull_ExpiryWithRealTime(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryCreating[int, int](50 * time.Millisecond),
	})

	c.Set(1, 100)
	_, found := c.GetIfPresent(1)
	require.True(t, found)

	time.Sleep(80 * time.Millisecond)
	_, found = c.GetIfPresent(1)
	require.False(t, found)
}

func TestFull_ExpiryFunc_Variants(t *testing.T) {
	t.Parallel()

	fs := &fakeSource{}
	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryCreatingFunc(func(entry Entry[int, int]) time.Duration {
			if entry.Key%2 == 0 {
				return 100 * time.Millisecond
			}
			return time.Hour
		}),
		Clock: fs,
	})

	c.Set(0, 0) // even key → 100ms
	c.Set(1, 1) // odd key → 1h

	fs.Sleep(150 * time.Millisecond)
	c.CleanUp()

	_, found := c.GetIfPresent(0)
	require.False(t, found) // expired
	_, found = c.GetIfPresent(1)
	require.True(t, found) // still alive
}

// ---------------------------------------------------------------------------
// 4. Loading: Get with Loader, LoaderFunc, BulkGet, SingleFlight
// ---------------------------------------------------------------------------

func TestFull_GetWithLoader(t *testing.T) {
	t.Parallel()

	t.Run("basic_load", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
			return key * 10, nil
		})

		v, err := c.Get(context.Background(), 5, loader)
		require.NoError(t, err)
		require.Equal(t, 50, v)

		// should be cached now
		got, found := c.GetIfPresent(5)
		require.True(t, found)
		require.Equal(t, 50, got)
	})

	t.Run("loader_error", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
			return 0, fmt.Errorf("db error")
		})

		v, err := c.Get(context.Background(), 1, loader)
		require.Error(t, err)
		require.Equal(t, 0, v)

		_, found := c.GetIfPresent(1)
		require.False(t, found)
	})

	t.Run("err_not_found_deletes_entry", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})

		loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
			return 0, ErrNotFound
		})

		// Key not in cache, loader returns ErrNotFound → should return ErrNotFound
		v, err := c.Get(context.Background(), 1, loader)
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, 0, v)

		_, found := c.GetIfPresent(1)
		require.False(t, found)
	})

	t.Run("err_not_found_removes_existing", func(t *testing.T) {
		t.Parallel()
		fs := &fakeSource{}
		c := Must(&Options[int, int]{
			ExpiryCalculator: ExpiryCreating[int, int](10 * time.Millisecond),
			Clock:            fs,
		})
		c.Set(1, 100)
		// Expire the entry
		fs.Sleep(20 * time.Millisecond)

		loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
			return 0, ErrNotFound
		})

		v, err := c.Get(context.Background(), 1, loader)
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, 0, v)
	})
}

func TestFull_BulkGet(t *testing.T) {
	t.Parallel()

	t.Run("basic_bulk", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		loader := BulkLoaderFunc[int, int](func(ctx context.Context, keys []int) (map[int]int, error) {
			result := make(map[int]int, len(keys))
			for _, k := range keys {
				result[k] = k * 100
			}
			return result, nil
		})

		result, err := c.BulkGet(context.Background(), []int{1, 2, 3}, loader)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Equal(t, 100, result[1])
		require.Equal(t, 200, result[2])
		require.Equal(t, 300, result[3])
	})

	t.Run("partial_result", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		loader := BulkLoaderFunc[int, int](func(ctx context.Context, keys []int) (map[int]int, error) {
			// Only return key 1, skip key 2
			return map[int]int{1: 10}, nil
		})

		result, err := c.BulkGet(context.Background(), []int{1, 2}, loader)
		require.NoError(t, err)
		require.Equal(t, 10, result[1])
		require.Equal(t, 0, result[2]) // missing key gets zero value
	})

	t.Run("deduplicates_keys", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		var loadCount atomic.Int32
		loader := BulkLoaderFunc[int, int](func(ctx context.Context, keys []int) (map[int]int, error) {
			loadCount.Add(1)
			result := make(map[int]int)
			for _, k := range keys {
				result[k] = k
			}
			return result, nil
		})

		_, err := c.BulkGet(context.Background(), []int{1, 1, 2, 2}, loader)
		require.NoError(t, err)
		// Duplicate keys should be deduplicated
		require.LessOrEqual(t, int(loadCount.Load()), 1)
	})
}

func TestFull_SingleFlight_Dedup(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})
	var loadCount atomic.Int32
	loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
		loadCount.Add(1)
		time.Sleep(30 * time.Millisecond)
		return key * 10, nil
	})

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	results := make([]int, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			v, err := c.Get(context.Background(), 42, loader)
			require.NoError(t, err)
			results[idx] = v
		}(i)
	}
	wg.Wait()

	// Only one load should have happened
	require.Equal(t, int32(1), loadCount.Load())
	for _, v := range results {
		require.Equal(t, 420, v)
	}
}

func TestFull_Get_AlreadyCached(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})
	c.Set(1, 100)

	var loadCount atomic.Int32
	loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
		loadCount.Add(1)
		return 0, nil
	})

	v, err := c.Get(context.Background(), 1, loader)
	require.NoError(t, err)
	require.Equal(t, 100, v)
	require.Equal(t, int32(0), loadCount.Load()) // loader not invoked
}

// ---------------------------------------------------------------------------
// 5. Invalidation
// ---------------------------------------------------------------------------

func TestFull_Invalidate(t *testing.T) {
	t.Parallel()

	t.Run("existing_key", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		c.Set(1, 10)
		v, ok := c.Invalidate(1)
		require.True(t, ok)
		require.Equal(t, 10, v)

		_, found := c.GetIfPresent(1)
		require.False(t, found)
	})

	t.Run("missing_key", func(t *testing.T) {
		t.Parallel()
		c := Must(&Options[int, int]{})
		v, ok := c.Invalidate(999)
		require.False(t, ok)
		require.Equal(t, 0, v)
	})
}

func TestFull_InvalidateAll(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		MaximumSize: 100,
		Executor:    syncExecutor(),
	})

	for i := 0; i < 50; i++ {
		c.Set(i, i)
	}
	require.Equal(t, 50, c.EstimatedSize())

	c.InvalidateAll()
	require.Equal(t, 0, c.EstimatedSize())
}

// ---------------------------------------------------------------------------
// 6. Iterators: All, Keys, Values
// ---------------------------------------------------------------------------

func TestFull_All_Iterator(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})
	c.Set(1, 10)
	c.Set(2, 20)
	c.Set(3, 30)

	collected := make(map[int]int)
	for k, v := range c.All() {
		collected[k] = v
	}
	require.Equal(t, map[int]int{1: 10, 2: 20, 3: 30}, collected)
}

func TestFull_Keys_Iterator(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})
	c.Set(1, 10)
	c.Set(2, 20)

	keys := make([]int, 0)
	for k := range c.Keys() {
		keys = append(keys, k)
	}
	require.Len(t, keys, 2)
	require.ElementsMatch(t, []int{1, 2}, keys)
}

func TestFull_Values_Iterator(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})
	c.Set(1, 10)
	c.Set(2, 20)

	vals := make([]int, 0)
	for v := range c.Values() {
		vals = append(vals, v)
	}
	require.Len(t, vals, 2)
	require.ElementsMatch(t, []int{10, 20}, vals)
}

func TestFull_Hottest_Coldest(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		MaximumSize: 10,
	})
	for i := 0; i < 5; i++ {
		c.Set(i, i)
	}

	hottest := make([]int, 0)
	for e := range c.Hottest() {
		hottest = append(hottest, e.Key)
	}
	require.Len(t, hottest, 5)

	coldest := make([]int, 0)
	for e := range c.Coldest() {
		coldest = append(coldest, e.Key)
	}
	require.Len(t, coldest, 5)
}

// ---------------------------------------------------------------------------
// 7. Size management
// ---------------------------------------------------------------------------

func TestFull_SetMaximum_GetMaximum(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{MaximumSize: 10})
	require.Equal(t, uint64(10), c.GetMaximum())

	c.SetMaximum(20)
	require.Equal(t, uint64(20), c.GetMaximum())
}

func TestFull_EstimatedSize(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{MaximumSize: 100})
	for i := 0; i < 50; i++ {
		c.Set(i, i)
	}
	require.Equal(t, 50, c.EstimatedSize())
}

func TestFull_WeightedSize(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		MaximumWeight: 100,
		Weigher: func(key int, value int) uint32 {
			return uint32(value)
		},
	})

	c.Set(1, 10)
	c.Set(2, 20)
	c.CleanUp() // trigger maintenance to update weighted size
	require.Equal(t, uint64(30), c.WeightedSize())
}

func TestFull_IsWeighted(t *testing.T) {
	t.Parallel()

	c1 := Must(&Options[int, int]{MaximumSize: 10})
	require.False(t, c1.IsWeighted())

	c2 := Must(&Options[int, int]{
		MaximumWeight: 100,
		Weigher:       func(key, value int) uint32 { return 1 },
	})
	require.True(t, c2.IsWeighted())
}

func TestFull_SetMaximum_Evicts(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		MaximumSize:      20,
		Executor:         syncExecutor(),
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
	})

	for i := 0; i < 20; i++ {
		c.Set(i, i)
	}
	require.Equal(t, 20, c.EstimatedSize())

	c.SetMaximum(10)
	// After reducing max, some entries should be evicted
	require.LessOrEqual(t, c.EstimatedSize(), 10)
}

// ---------------------------------------------------------------------------
// 8. Statistics
// ---------------------------------------------------------------------------

func TestFull_Stats_Recording(t *testing.T) {
	t.Parallel()

	counter := stats.NewCounter()
	c := Must(&Options[int, int]{
		StatsRecorder: counter,
	})

	// Misses
	_, found := c.GetIfPresent(1)
	require.False(t, found)

	// Set + Hits
	c.Set(2, 20)
	c.GetIfPresent(2)
	c.GetIfPresent(2)

	snap := counter.Snapshot()
	require.Equal(t, uint64(2), snap.Hits)
	require.Equal(t, uint64(1), snap.Misses)
}

func TestFull_Stats_NoRecording(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})
	require.False(t, c.IsRecordingStats())
	require.Equal(t, stats.Stats{}, c.Stats())
}

func TestFull_Stats_WithRecorder(t *testing.T) {
	t.Parallel()

	counter := stats.NewCounter()
	c := Must(&Options[int, int]{
		StatsRecorder: counter,
	})
	require.True(t, c.IsRecordingStats())

	for i := 0; i < 100; i++ {
		c.Set(i, i)
	}
	for i := 0; i < 100; i++ {
		c.GetIfPresent(i)
	}

	snap := c.Stats()
	require.Equal(t, uint64(100), snap.Hits)
	require.Equal(t, uint64(0), snap.Misses)
}

func TestFull_Stats_LoadSuccessAndFailure(t *testing.T) {
	t.Parallel()

	counter := stats.NewCounter()
	c := Must(&Options[int, int]{
		StatsRecorder: counter,
	})

	loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
		if key%2 == 0 {
			return key, nil
		}
		return 0, fmt.Errorf("odd key error")
	})

	for i := 0; i < 10; i++ {
		c.Get(context.Background(), i, loader)
	}

	snap := counter.Snapshot()
	require.Equal(t, uint64(5), snap.LoadSuccesses)
	require.Equal(t, uint64(5), snap.LoadFailures)
}

// ---------------------------------------------------------------------------
// 9. Edge cases
// ---------------------------------------------------------------------------

func TestFull_EmptyCache(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})

	_, found := c.GetIfPresent(1)
	require.False(t, found)

	require.Equal(t, 0, c.EstimatedSize())

	c.InvalidateAll() // no-op on empty

	count := 0
	for range c.All() {
		count++
	}
	require.Equal(t, 0, count)
}

func TestFull_ZeroValueCache(t *testing.T) {
	t.Parallel()

	c := Must(&Options[string, string]{})

	c.Set("", "")
	v, ok := c.GetIfPresent("")
	require.True(t, ok)
	require.Equal(t, "", v)
}

func TestFull_StopAllGoroutines(t *testing.T) {
	t.Parallel()

	fs := &fakeSource{}
	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryWriting[int, int](time.Second),
		Clock:            fs,
	})

	// Should not panic
	stopped := c.StopAllGoroutines()
	require.True(t, stopped)

	// Second call should return false
	stopped = c.StopAllGoroutines()
	require.False(t, stopped)
}

func TestFull_CleanUp(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		MaximumSize:      10,
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
	})

	for i := 0; i < 10; i++ {
		c.Set(i, i)
	}

	// Should not panic
	c.CleanUp()
	require.Equal(t, 10, c.EstimatedSize())
}

func TestFull_DeletionEvent(t *testing.T) {
	t.Parallel()

	var events []DeletionEvent[int, int]
	var mu sync.Mutex

	c := Must(&Options[int, int]{
		Executor: syncExecutor(),
		OnDeletion: func(e DeletionEvent[int, int]) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		},
	})

	c.Set(1, 10)
	c.Set(2, 20)

	// Invalidate triggers CauseInvalidation
	c.Invalidate(1)

	// Set on existing triggers CauseReplacement
	c.Set(2, 30)

	c.CleanUp()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, events, 2)
	require.Equal(t, CauseInvalidation, events[0].Cause)
	require.Equal(t, CauseReplacement, events[1].Cause)
}

func TestFull_DeletionCause_IsEviction(t *testing.T) {
	t.Parallel()

	require.False(t, CauseInvalidation.IsEviction())
	require.False(t, CauseReplacement.IsEviction())
	require.True(t, CauseOverflow.IsEviction())
	require.True(t, CauseExpiration.IsEviction())
}

func TestFull_DeletionCause_String(t *testing.T) {
	t.Parallel()

	require.Equal(t, "Invalidation", CauseInvalidation.String())
	require.Equal(t, "Replacement", CauseReplacement.String())
	require.Equal(t, "Overflow", CauseOverflow.String())
	require.Equal(t, "Expiration", CauseExpiration.String())
}

func TestFull_Options_Validation(t *testing.T) {
	t.Parallel()

	t.Run("both_max_invalid", func(t *testing.T) {
		_, err := New(&Options[int, int]{
			MaximumSize:   10,
			MaximumWeight: 20,
			Weigher:       func(k, v int) uint32 { return 1 },
		})
		require.Error(t, err)
	})

	t.Run("max_weight_without_weigher", func(t *testing.T) {
		_, err := New(&Options[int, int]{
			MaximumWeight: 20,
		})
		require.Error(t, err)
	})

	t.Run("weigher_without_max_weight", func(t *testing.T) {
		_, err := New(&Options[int, int]{
			Weigher: func(k, v int) uint32 { return 1 },
		})
		require.Error(t, err)
	})

	t.Run("negative_max_size", func(t *testing.T) {
		_, err := New(&Options[int, int]{
			MaximumSize: -1,
		})
		require.Error(t, err)
	})

	t.Run("valid_options", func(t *testing.T) {
		_, err := New(&Options[int, int]{
			MaximumSize: 100,
		})
		require.NoError(t, err)
	})
}

func TestFull_NoopLogger(t *testing.T) {
	t.Parallel()

	logger := &NoopLogger{}
	// should not panic
	logger.Warn(context.Background(), "warn", fmt.Errorf("err"))
	logger.Error(context.Background(), "err", fmt.Errorf("err"))
}

func TestFull_Entry_Methods(t *testing.T) {
	t.Parallel()

	now := time.Now()
	entry := Entry[string, int]{
		Key:               "k",
		Value:             42,
		Weight:            5,
		ExpiresAtNano:     now.Add(time.Hour).UnixNano(),
		RefreshableAtNano: now.Add(30 * time.Minute).UnixNano(),
		SnapshotAtNano:    now.UnixNano(),
	}

	require.Equal(t, "k", entry.Key)
	require.Equal(t, 42, entry.Value)
	require.Equal(t, uint32(5), entry.Weight)
	require.False(t, entry.HasExpired())
	require.Equal(t, time.Duration(time.Hour), entry.ExpiresAfter())
	require.Equal(t, time.Duration(30*time.Minute), entry.RefreshableAfter())
}

func TestFull_Entry_HasExpired(t *testing.T) {
	t.Parallel()

	now := time.Now()
	expired := Entry[int, int]{
		ExpiresAtNano: now.Add(-time.Second).UnixNano(),
		SnapshotAtNano: now.UnixNano(),
	}
	require.True(t, expired.HasExpired())

	fresh := Entry[int, int]{
		ExpiresAtNano: now.Add(time.Hour).UnixNano(),
		SnapshotAtNano: now.UnixNano(),
	}
	require.False(t, fresh.HasExpired())
}

func TestFull_NilOptions(t *testing.T) {
	t.Parallel()

	c, err := New[int, int](nil)
	require.NoError(t, err)
	require.NotNil(t, c)

	c.Set(1, 1)
	v, ok := c.GetIfPresent(1)
	require.True(t, ok)
	require.Equal(t, 1, v)
}

func TestFull_Invalidation_Cause_String(t *testing.T) {
	t.Parallel()

	require.Equal(t, "<unknown cache233.DeletionCause>", DeletionCause(0).String())
	require.Equal(t, "<unknown cache233.DeletionCause>", DeletionCause(99).String())
}

func TestFull_ComputeOp_String_Unknown(t *testing.T) {
	t.Parallel()

	require.Equal(t, "<unknown cache233.ComputeOp>", ComputeOp(99).String())
}

func TestFull_Stats_RequestAndLoadRatio(t *testing.T) {
	t.Parallel()

	s := stats.Stats{
		Hits:           70,
		Misses:         30,
		LoadSuccesses:  8,
		LoadFailures:   2,
		TotalLoadTime:  100 * time.Millisecond,
	}

	require.Equal(t, uint64(100), s.Requests())
	require.InDelta(t, 0.7, s.HitRatio(), 0.001)
	require.InDelta(t, 0.3, s.MissRatio(), 0.001)
	require.Equal(t, uint64(10), s.Loads())
	require.InDelta(t, 0.2, s.LoadFailureRatio(), 0.001)
	require.Equal(t, 10*time.Millisecond, s.AverageLoadPenalty())
}

func TestFull_Stats_PlusMinus(t *testing.T) {
	t.Parallel()

	a := stats.Stats{Hits: 10, Misses: 5}
	b := stats.Stats{Hits: 3, Misses: 2}

	sum := a.Plus(b)
	require.Equal(t, uint64(13), sum.Hits)
	require.Equal(t, uint64(7), sum.Misses)

	diff := a.Minus(b)
	require.Equal(t, uint64(7), diff.Hits)
	require.Equal(t, uint64(3), diff.Misses)

	// Underflow clamps to 0
	negative := b.Minus(a)
	require.Equal(t, uint64(0), negative.Hits)
	require.Equal(t, uint64(0), negative.Misses)
}

func TestFull_Stats_ZeroValues(t *testing.T) {
	t.Parallel()

	s := stats.Stats{}
	require.Equal(t, uint64(0), s.Requests())
	require.Equal(t, 1.0, s.HitRatio())
	require.Equal(t, 0.0, s.MissRatio())
	require.Equal(t, uint64(0), s.Loads())
	require.Equal(t, 0.0, s.LoadFailureRatio())
	require.Equal(t, time.Duration(0), s.AverageLoadPenalty())
}

func TestFull_Get_WithLoaderFunc_Adapter(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, string]{})
	loader := LoaderFunc[int, string](func(ctx context.Context, key int) (string, error) {
		return fmt.Sprintf("val-%d", key), nil
	})

	v, err := c.Get(context.Background(), 5, loader)
	require.NoError(t, err)
	require.Equal(t, "val-5", v)
}

func TestFull_BulkLoaderFunc_Adapter(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})
	loader := BulkLoaderFunc[int, int](func(ctx context.Context, keys []int) (map[int]int, error) {
		result := make(map[int]int)
		for _, k := range keys {
			result[k] = k * 10
		}
		return result, nil
	})

	result, err := c.BulkGet(context.Background(), []int{1, 2, 3}, loader)
	require.NoError(t, err)
	require.Equal(t, 10, result[1])
	require.Equal(t, 20, result[2])
	require.Equal(t, 30, result[3])
}

func TestFull_Concurrent_Set_Get(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		MaximumSize:      1000,
		Executor:         syncExecutor(),
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
	})

	const goroutines = 50
	const opsPerGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := id*opsPerGoroutine + i
				c.Set(key, key)
				c.GetIfPresent(key)
			}
		}(g)
	}
	wg.Wait()

	c.CleanUp()
	// After cleanup, size should be at or below maximum (best-effort eviction)
	require.LessOrEqual(t, c.EstimatedSize(), 1100) // some slack for best-effort
}

func TestFull_Concurrent_Invalidate(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{
		MaximumSize: 100,
		Executor:    syncExecutor(),
	})

	for i := 0; i < 100; i++ {
		c.Set(i, i)
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := id; i < 100; i += goroutines {
				c.Invalidate(i)
			}
		}(g)
	}
	wg.Wait()

	require.Equal(t, 0, c.EstimatedSize())
}

func TestFull_Unbounded_NoEviction(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{})

	for i := 0; i < 10000; i++ {
		c.Set(i, i)
	}
	require.Equal(t, 10000, c.EstimatedSize())
	require.Equal(t, uint64(math.MaxUint64), c.GetMaximum())
}

func TestFull_SetExpiresAfter(t *testing.T) {
	t.Parallel()

	fs := &fakeSource{}
	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
		Clock:            fs,
	})

	c.Set(1, 100)
	c.SetExpiresAfter(1, 50*time.Millisecond)

	fs.Sleep(100 * time.Millisecond)
	_, found := c.GetIfPresent(1)
	require.False(t, found)
}

func TestFull_Persistence_RoundTrip(t *testing.T) {
	t.Parallel()

	c1 := Must(&Options[int, int]{
		MaximumSize:      10,
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
	})

	for i := 0; i < 5; i++ {
		c1.Set(i, i*10)
	}

	var buf bytes.Buffer
	err := SaveCacheTo(c1, &buf)
	require.NoError(t, err)

	c2 := Must(&Options[int, int]{
		MaximumSize:      10,
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
	})

	err = LoadCacheFrom(c2, &buf)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		v, found := c2.GetIfPresent(i)
		require.True(t, found)
		require.Equal(t, i*10, v)
	}
}

func TestFull_Get_WithRefreshCalculator(t *testing.T) {
	t.Parallel()

	fs := &fakeSource{}
	var loadCount atomic.Int32
	c := Must(&Options[int, int]{
		ExpiryCalculator: ExpiryWriting[int, int](time.Hour),
		RefreshCalculator: RefreshWriting[int, int](50 * time.Millisecond),
		Clock:            fs,
	})

	loader := LoaderFunc[int, int](func(ctx context.Context, key int) (int, error) {
		loadCount.Add(1)
		return key * 10, nil
	})

	// First Get loads
	v, err := c.Get(context.Background(), 1, loader)
	require.NoError(t, err)
	require.Equal(t, 10, v)
	require.Equal(t, int32(1), loadCount.Load())

	// Advance past refresh time
	fs.Sleep(100 * time.Millisecond)

	// Get triggers async refresh, returns old value
	v, err = c.Get(context.Background(), 1, loader)
	require.NoError(t, err)
	require.Equal(t, 10, v)

	// Wait for refresh to complete
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(2), loadCount.Load())
}

func TestFull_SetMaximum_ZerosOut(t *testing.T) {
	t.Parallel()

	c := Must(&Options[int, int]{MaximumSize: 100})

	for i := 0; i < 50; i++ {
		c.Set(i, i)
	}

	c.SetMaximum(0)
	require.Equal(t, uint64(0), c.GetMaximum())
}

func TestFull_Stats_Requests_And_Loads_Overflow(t *testing.T) {
	t.Parallel()

	s := stats.Stats{
		Hits:          math.MaxUint64,
		Misses:        1,
		LoadSuccesses: math.MaxUint64,
		LoadFailures:  1,
	}

	// Should not panic, saturates to MaxUint64
	require.Equal(t, uint64(math.MaxUint64), s.Requests())
	require.Equal(t, uint64(math.MaxUint64), s.Loads())
}

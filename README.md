# cache233-go

A high-performance in-memory caching library for Go, based on [Otter v2](https://github.com/maypok86/otter).

## Features

- **High hit rates** via adaptive W-TinyLFU eviction policy
- **Excellent throughput** under high contention
- **Low memory overhead** across all cache capacities
- Size-based eviction when a maximum is exceeded
- Time-based expiration (access, write, or creation based)
- Automatic loading of entries via SingleFlight
- Asynchronous refresh when stale entries are accessed
- Write propagation to external resources
- Cache access statistics
- Persistence (save/load from file)

## Installation

```bash
go get github.com/neko233-com/cache233-go
```

## Requirements

Go 1.24 or later.

## Usage

```go
package main

import (
    "context"
    "time"

    "github.com/neko233-com/cache233-go"
    "github.com/neko233-com/cache233-go/stats"
)

func main() {
    ctx := context.Background()

    counter := stats.NewCounter()

    cache := cache233.Must(&cache233.Options[string, string]{
        MaximumSize:       10_000,
        ExpiryCalculator:  cache233.ExpiryAccessing[string, string](time.Second),
        RefreshCalculator: cache233.RefreshWriting[string, string](500 * time.Millisecond),
        StatsRecorder:     counter,
    })

    cache.Set("key", "value")

    loader := func(ctx context.Context, key string) (string, error) {
        return "loaded_value", nil
    }

    value, err := cache.Get(ctx, "key", cache233.LoaderFunc[string, string](loader))
    if err != nil {
        panic(err)
    }
    _ = value
}
```

## License

Apache License 2.0

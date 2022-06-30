# Gopher Celery

[![Documentation](https://godoc.org/github.com/marselester/gopher-celery?status.svg)](https://pkg.go.dev/github.com/marselester/gopher-celery)
[![Go Report Card](https://goreportcard.com/badge/github.com/marselester/gopher-celery)](https://goreportcard.com/report/github.com/marselester/gopher-celery)

**⚠️ This is still a draft.**

The objective of this project is to provide
the very basic mechanism to efficiently produce and consume Celery tasks on Go side.
Therefore there are no plans to support all the rich features the Python version provides,
such as tasks chains, etc.
Even task result backend has no practical value in the context of Gopher Celery,
so it wasn't taken into account.
Note, Celery has [no result backend](https://docs.celeryq.dev/en/stable/userguide/tasks.html?#result-backends)
enabled by default (it incurs overhead).

Typically one would want to use Gopher Celery when certain tasks on Python side
take too long to complete or there is a big volume of tasks requiring lots of Python workers
(expensive infrastructure).

This project offers a little bit more convenient API of https://github.com/gocelery/gocelery including:

- smaller API surface
- multiple queues support
- running workers on demand
- support for protocol v1 and v2

## Usage

The Celery app can be used as either a producer or consumer (worker).
To send tasks to a queue for a worker to consume, use `Delay` method.
In order to process a task you should register it using `Register` method.

```python
def mytask(a, b):
    println(a + b)
```

For example, whenever a task `mytask` is popped from `important` queue,
the Go function is executed with args and kwargs obtained from the task message.
By default Redis broker (localhost) is used with json task message serialization.

```go
app := celery.NewApp()
app.Register(
	"myproject.apps.myapp.tasks.mytask",
	"important",
	func(ctx context.Context, p *celery.TaskParam) {
		p.NameArgs("a", "b")
		fmt.Println(p.MustInt("a") + p.MustInt("b"))
	},
)
if err := app.Run(context.Background()); err != nil {
	log.Printf("celery worker error: %v", err)
}
```

Here is an example of sending `mytask` task to `important` queue with `a=2`, `b=3` arguments.
If a task is processed on Python side,
you don't need to register the task or run the app.

```go
app := celery.NewApp()
err := app.Delay(
	"myproject.apps.myapp.tasks.mytask",
	"important",
	2,
	3,
)
if err != nil {
	log.Printf("failed to send mytask")
}
```

Most likely Redis won't be running on localhost when the service is deployed,
so you would need to pass a connection pool.

<details>

<summary>Example</summary>

```go
package main

import (
	"context"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gomodule/redigo/redis"
	celery "github.com/marselester/gopher-celery"
	celeryredis "github.com/marselester/gopher-celery/redis"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	pool := redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(
				"redis://my-redis",
				redis.DialConnectTimeout(5*time.Second),
			)
			if err != nil {
				level.Error(logger).Log("msg", "Redis dial failed", "err", err)
			}
			return c, err
		},
		// Check the health of an idle connection before using.
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		// Maximum number of idle connections in the pool.
		MaxIdle: 3,
		// Close connections after remaining idle for given duration.
		IdleTimeout: 5 * time.Minute,
	}
	c := pool.Get()
	if _, err := c.Do("PING"); err != nil {
		level.Error(logger).Log("msg", "Redis connection failed", "err", err)
		return
	}
	c.Close()

	broker := celeryredis.NewBroker(
		celeryredis.WithPool(&pool),
	)
	app := celery.NewApp(
		celery.WithBroker(broker),
		celery.WithLogger(logger),
		celery.WithMaxWorkers(celery.DefaultMaxWorkers),
	)
	// Use the app...
}
```

</details>

## Testing

Run the tests.

```sh
$ go test ./...
```

Benchmarks help to spot performance changes as the project evolves
and also compare performance of serializers.
For example, based on the results below the protocol v2 is faster than v1 when encoding args:

- 350 nanoseconds mean time, 3 allocations (248 bytes) with 0% variation across the samples
- 1.21 microseconds mean time, 4 allocations (672 bytes) with 0% variation across the samples

It is recommended to run benchmarks multiple times and check
how stable they are using [Benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) tool.

```sh
$ go test -bench=. -benchmem -count=10 ./internal/... | tee bench-new.txt
```

<details>

<summary>

```sh
$ benchstat bench-old.txt
```

</summary>

```
name                                  time/op
JSONSerializerEncode_v2NoParams-12    2.97ns ± 1%
JSONSerializerEncode_v2Args-12         350ns ± 0%
JSONSerializerEncode_v2Kwargs-12       582ns ± 0%
JSONSerializerEncode_v2ArgsKwargs-12   788ns ± 1%
JSONSerializerEncode_v1NoParams-12    1.12µs ± 1%
JSONSerializerEncode_v1Args-12        1.21µs ± 0%
JSONSerializerEncode_v1Kwargs-12      1.68µs ± 0%
JSONSerializerEncode_v1ArgsKwargs-12  1.77µs ± 0%

name                                  alloc/op
JSONSerializerEncode_v2NoParams-12     0.00B
JSONSerializerEncode_v2Args-12          248B ± 0%
JSONSerializerEncode_v2Kwargs-12        472B ± 0%
JSONSerializerEncode_v2ArgsKwargs-12    528B ± 0%
JSONSerializerEncode_v1NoParams-12      672B ± 0%
JSONSerializerEncode_v1Args-12          672B ± 0%
JSONSerializerEncode_v1Kwargs-12      1.00kB ± 0%
JSONSerializerEncode_v1ArgsKwargs-12  1.00kB ± 0%

name                                  allocs/op
JSONSerializerEncode_v2NoParams-12      0.00
JSONSerializerEncode_v2Args-12          3.00 ± 0%
JSONSerializerEncode_v2Kwargs-12        7.00 ± 0%
JSONSerializerEncode_v2ArgsKwargs-12    8.00 ± 0%
JSONSerializerEncode_v1NoParams-12      4.00 ± 0%
JSONSerializerEncode_v1Args-12          4.00 ± 0%
JSONSerializerEncode_v1Kwargs-12        10.0 ± 0%
JSONSerializerEncode_v1ArgsKwargs-12    10.0 ± 0%
```

</details>

The old and new stats are compared as follows.

```sh
$ benchstat bench-old.txt bench-new.txt
```

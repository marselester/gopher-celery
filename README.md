# Gopher Celery

[![Documentation](https://godoc.org/github.com/marselester/gopher-celery?status.svg)](https://pkg.go.dev/github.com/marselester/gopher-celery)
[![Go Report Card](https://goreportcard.com/badge/github.com/marselester/gopher-celery)](https://goreportcard.com/report/github.com/marselester/gopher-celery)

**⚠️ This is still a draft.**

The objective of this project is to provide
a little bit more convenient API of https://github.com/gocelery/gocelery including:

- smaller API surface
- multiple queues support
- running workers on demand
- support for protocol v1 and v2

The Celery app can be used as either a producer or consumer (worker).
To send tasks to a queue for a worker to consume, use `Delay` method.
In order to process a task you should register it using `Register` method.

For example, whenever a task `mytask` is popped from `important` queue,
the Go function is executed with args and kwargs obtained from the task message.
By default Redis broker (localhost) is used with json task message serialization.

```go
app := celery.NewApp()
app.Register(
	"myproject.apps.myapp.tasks.mytask",
	"important",
	func(p *celery.TaskParam) {},
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

	app := celery.NewApp(
		celery.WithRedisBroker(&pool),
		celery.WithLogger(logger),
		celery.WithMaxWorkers(celery.DefaultMaxWorkers),
	)
	// Use the app...
}
```

</details>

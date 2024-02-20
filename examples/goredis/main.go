// Program goredis shows how to use github.com/redis/go-redis.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	celery "github.com/dryarullin/gopher-celery"
	celeryredis "github.com/dryarullin/gopher-celery/goredis"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer func() {
		if err := c.Close(); err != nil {
			level.Error(logger).Log("msg", "failed to close Redis client", "err", err)
		}
	}()

	if _, err := c.Ping(ctx).Result(); err != nil {
		level.Error(logger).Log("msg", "Redis connection failed", "err", err)
		return
	}

	broker := celeryredis.NewBroker(
		celeryredis.WithClient(c),
	)
	app := celery.NewApp(
		celery.WithBroker(broker),
		celery.WithLogger(logger),
	)
	app.Register(
		"myproject.mytask",
		"important",
		func(ctx context.Context, p *celery.TaskParam) error {
			p.NameArgs("a", "b")
			fmt.Printf("received a=%s b=%s\n", p.MustString("a"), p.MustString("b"))
			return nil
		},
	)

	logger.Log("msg", "waiting for tasks...")
	err := app.Run(ctx)
	logger.Log("msg", "program stopped", "err", err)
}

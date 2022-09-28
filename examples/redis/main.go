// Program redis shows how to pass a Redis connection pool to the broker.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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
				"redis://localhost",
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
		celeryredis.WithBrokerPool(&pool),
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	logger.Log("msg", "waiting for tasks...")
	err := app.Run(ctx)
	logger.Log("msg", "program stopped", "err", err)
}

// Program redis shows how to pass a Redis connection pool to the broker.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	celery "github.com/dryarullin/gopher-celery"
	celeryredis "github.com/dryarullin/gopher-celery/redis"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gomodule/redigo/redis"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	pool := redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(
				"redis://localhost",
				redis.DialConnectTimeout(5*time.Second),
				// The Conn.Do method sets a write deadline,
				// writes the command arguments to the network connection,
				// sets the read deadline and reads the response from the network connection
				// https://github.com/gomodule/redigo/issues/320.
				//
				// Note, the read timeout should be big enough for BRPOP to finish
				// or else the broker returns i/o timeout error.
				redis.DialWriteTimeout(5*time.Second),
				redis.DialReadTimeout(10*time.Second),
			)
			if err != nil {
				level.Error(logger).Log("msg", "Redis dial failed", "err", err)
			}
			return c, err
		},
		// Check the health of an idle connection before using it.
		// It PINGs connections that have been idle more than a minute.
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
		// Maximum number of idle connections in the pool.
		MaxIdle: 3,
		// Close connections after remaining idle for given duration.
		IdleTimeout: 5 * time.Minute,
	}
	defer func() {
		if err := pool.Close(); err != nil {
			level.Error(logger).Log("msg", "failed to close Redis connection pool", "err", err)
		}
	}()

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

// Program consumer receives "myproject.mytask" tasks from "important" queue.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	celery "github.com/dryarullin/gopher-celery"
	"github.com/go-kit/log"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	app := celery.NewApp(
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

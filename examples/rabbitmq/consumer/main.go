// Program consumer receives "myproject.mytask" tasks from "important" queue.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/go-kit/log"
	celery "github.com/lagerstrom/gopher-celery"
	celeryrabbitmq "github.com/lagerstrom/gopher-celery/rabbitmq"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	broker := celeryrabbitmq.NewBroker(celeryrabbitmq.WithAmqpUri("amqp://guest:guest@localhost:5672/"))
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

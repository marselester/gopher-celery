// Program producer sends two "myproject.mytask" tasks to "important" queue.
package main

import (
	"os"

	"github.com/go-kit/log"
	celery "github.com/roncemer/gopher-celery-with-rabbitmq-broker"
    celeryrabbitmq "github.com/roncemer/gopher-celery-with-rabbitmq-broker/rabbitmq"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

    broker := celeryrabbitmq.NewBroker(celeryrabbitmq.WithAmqpUri("amqp://guest:guest@localhost:5672/"))
	app := celery.NewApp(
        celery.WithBroker(broker),
		celery.WithLogger(logger),
		celery.WithTaskProtocol(2),
	)
	err := app.Delay("myproject.mytask", "important", "fizz", "bazz")
	logger.Log("msg", "task was sent using protocol v2", "err", err)

	app = celery.NewApp(
		celery.WithLogger(logger),
		celery.WithTaskProtocol(1),
	)
	err = app.Delay("myproject.mytask", "important", "fizz", "bazz")
	logger.Log("msg", "task was sent using protocol v1", "err", err)
}

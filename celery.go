// Package celery helps to work with Celery (place tasks in queues and execute them).
package celery

import (
	"context"

	"github.com/go-kit/log"
	"golang.org/x/sync/errgroup"

	"github.com/marselester/gopher-celery/internal/protocol"
	"github.com/marselester/gopher-celery/internal/redis"
)

// Broker is responsible for receiving and sending task messages.
// For example, it knows how to read a message from a given queue in Redis.
// The messages can be in defferent formats depending on Celery protocol version.
type Broker interface {
	// Send puts a message to a queue.
	Send(msg []byte, queue string) error
	// Receive returns a raw message from one of the queues.
	// It blocks until there is a message available for consumption.
	Receive() ([]byte, error)
}

// MessageSerializer encodes/decodes a task message, e.g., json, yaml, msgpack.
type MessageSerializer interface {
	// Encode encodes the message into the given byte slice.
	Encode(m *protocol.Message, raw []byte) error
	// Decode decodes the raw message.
	Decode(raw []byte) (*protocol.Message, error)
}

// NewApp creates a Celery app.
// The default broker is Redis assumed to run on localhost.
// The default message serializer is json.
func NewApp(options ...Option) *App {
	app := App{
		conf: Config{
			logger:     log.NewNopLogger(),
			serializer: &protocol.JSONSerializer{},
		},
		taskPath:  make(map[string]interface{}),
		taskQueue: make(map[string]string),
		sem:       make(chan struct{}, DefaultMaxWorkers),
	}

	for _, opt := range options {
		opt(&app.conf)
	}
	return &app
}

// App is a Celery app to produce or consume tasks asynchronously.
type App struct {
	// conf represents app settings.
	conf Config

	// broker is the app's broker, e.g., Redis.
	broker Broker
	// taskPath maps a Celery task path to a task itself, e.g.,
	// "myproject.apps.myapp.tasks.mytask": func(){}.
	taskPath map[string]interface{}
	// taskQueue helps to determine which queue a task belongs to, e.g.,
	// "myproject.apps.myapp.tasks.mytask": "important".
	taskQueue map[string]string
	// sem is a semaphore that limits number of workers.
	sem chan struct{}
}

// Register associates the task with given Python path and queue.
// For example, when "myproject.apps.myapp.tasks.mytask"
// is seen in "important" queue, the func(){} task is executed.
//
// Note, the method is not concurrency safe.
// The tasks mustn't be registered after the app starts processing tasks.
func (a *App) Register(path, queue string, task interface{}) {
	a.taskPath[path] = task
	a.taskQueue[path] = queue
}

// Delay places the task associated with given Python path into queue.
func (a *App) Delay(path, queue string, payload interface{}) error {
	return nil
}

// Run launches the workers that process the tasks.
// The caller mustn't register any new tasks.
func (a *App) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	qq := make([]string, 0, len(a.taskQueue))
	for k := range a.taskQueue {
		qq = append(qq, a.taskQueue[k])
	}
	a.broker = redis.NewBroker(qq, redis.WithPool(a.conf.pool))

	msgs := make(chan *protocol.Message, 1)
	g.Go(func() error {
		defer close(msgs)

		// One goroutine fetching and decoding tasks from queues
		// shouldn't be a bottleneck since the worker goroutines
		// usually take seconds/minutes to complete.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				rawMsg, err := a.broker.Receive()
				if err != nil {
					a.conf.logger.Log("failed to receive a raw task message", "err", err)
					continue
				}
				// No messages in the broker so far.
				if rawMsg == nil {
					continue
				}

				m, err := a.conf.serializer.Decode(rawMsg)
				if err != nil {
					a.conf.logger.Log("failed to decode raw task message", "raw", "rawMsg", "err", err)
					continue
				}

				msgs <- m
			}
		}
	})

	go func() {
		// Start a worker when there is a task.
		for m := range msgs {
			select {
			// Acquire a semaphore by sending a token.
			case a.sem <- struct{}{}:
			// Stop processing tasks.
			case <-ctx.Done():
				return
			}

			m := m
			g.Go(func() error {
				// Release a semaphore by discarding a token.
				defer func() { <-a.sem }()

				if err := a.executeTask(m); err != nil {
					a.conf.logger.Log("msg", "task failed", "msg", m, "err", err)
				}
				return nil
			})
		}
	}()

	return g.Wait()
}

func (a *App) executeTask(m *protocol.Message) error {
	return nil
}

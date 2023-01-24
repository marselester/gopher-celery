// Package celery helps to work with Celery (place tasks in queues and execute them).
package celery

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/marselester/gopher-celery/protocol"
	"github.com/marselester/gopher-celery/redis"
)

// TaskF represents a Celery task implemented by the client.
// The error doesn't affect anything, it's logged though.
type TaskF func(ctx context.Context, p *TaskParam) error

// Middleware is a chainable behavior modifier for tasks.
// For example, a caller can collect task metrics.
type Middleware func(next TaskF) TaskF

// Broker is responsible for receiving and sending task messages.
// For example, it knows how to read a message from a given queue in Redis.
// The messages can be in defferent formats depending on Celery protocol version.
type Broker interface {
	// Send puts a message to a queue.
	Send(msg []byte, queue string) error
	// Observe sets the queues from which the tasks should be received.
	Observe(queues []string)
	// Receive returns a raw message from one of the queues.
	// It blocks until there is a message available for consumption.
	Receive() ([]byte, error)
}

// AsyncParam represents parameters for sending a task message.
type AsyncParam struct {
	// Args is a list of arguments.
	// It will be an empty list if not provided.
	Args []interface{}
	// Kwargs is a dictionary of keyword arguments.
	// It will be an empty dictionary if not provided.
	Kwargs map[string]interface{}
	// Expires is an expiration date.
	// If not provided the message will never expire.
	Expires time.Time
}

// NewApp creates a Celery app.
// The default broker is Redis assumed to run on localhost.
// When producing tasks the default message serializer is json and protocol is v2.
func NewApp(options ...Option) *App {
	app := App{
		conf: Config{
			logger:     log.NewNopLogger(),
			registry:   protocol.NewSerializerRegistry(),
			mime:       protocol.MimeJSON,
			protocol:   protocol.V2,
			maxWorkers: DefaultMaxWorkers,
		},
		task:      &sync.Map{},
		taskQueue: &sync.Map{},
	}

	for _, opt := range options {
		opt(&app.conf)
	}
	app.sem = make(chan struct{}, app.conf.maxWorkers)

	if app.conf.broker == nil {
		app.conf.broker = redis.NewBroker()
	}

	return &app
}

// App is a Celery app to produce or consume tasks asynchronously.
type App struct {
	// conf represents app settings.
	conf Config

	// task maps a Celery task path to a task itself, e.g.,
	// "myproject.apps.myapp.tasks.mytask": TaskF.
	task *sync.Map //map[string]TaskF
	// taskQueue helps to determine which queue a task belongs to, e.g.,
	// "myproject.apps.myapp.tasks.mytask": "important".
	taskQueue *sync.Map //map[string]string
	// sem is a semaphore that limits number of workers.
	sem chan struct{}
	// queueCount is a number of queues to observe.
	queueCount atomic.Uint32
}

// Register associates the task with given Python path and queue.
// Note that you cannot have same task name for different queues.
// For example, when "myproject.apps.myapp.tasks.mytask"
// is seen in "important" queue, the TaskF task is executed.
func (a *App) Register(path, queue string, task TaskF) {
	a.task.Store(path, task)
	a.taskQueue.Store(path, queue)
	a.observe()
}

// observe sets the queues from which the tasks should be received.
func (a *App) Unregister(path string) {
	a.task.Delete(path)
	a.taskQueue.Delete(path)
	a.observe()
}

// ApplyAsync sends a task message.
func (a *App) ApplyAsync(path, queue string, p *AsyncParam) error {
	m := protocol.Task{
		ID:      uuid.NewString(),
		Name:    path,
		Args:    p.Args,
		Kwargs:  p.Kwargs,
		Expires: p.Expires,
	}
	rawMsg, err := a.conf.registry.Encode(queue, a.conf.mime, a.conf.protocol, &m)
	if err != nil {
		return fmt.Errorf("failed to encode task message: %w", err)
	}

	if err = a.conf.broker.Send(rawMsg, queue); err != nil {
		return fmt.Errorf("failed to send task message to broker: %w", err)
	}
	return nil
}

// Delay is a shortcut to send a task message,
// i.e., it places the task associated with given Python path into queue.
func (a *App) Delay(path, queue string, args ...interface{}) error {
	m := protocol.Task{
		ID:   uuid.NewString(),
		Name: path,
		Args: args,
	}
	rawMsg, err := a.conf.registry.Encode(queue, a.conf.mime, a.conf.protocol, &m)
	if err != nil {
		return fmt.Errorf("failed to encode task message: %w", err)
	}

	if err = a.conf.broker.Send(rawMsg, queue); err != nil {
		return fmt.Errorf("failed to send task message to broker: %w", err)
	}
	return nil
}

// Run launches the workers that process the tasks received from the broker.
// The call is blocking until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	a.observe()

	msgs := make(chan *protocol.Task, 1)
	g.Go(func() error {
		defer close(msgs)

		// One goroutine fetching and decoding tasks from queues
		// shouldn't be a bottleneck since the worker goroutines
		// usually take seconds/minutes to complete.
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				if a.queueCount.Load() == 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				rawMsg, err := a.conf.broker.Receive()
				if err != nil {
					level.Error(a.conf.logger).Log("failed to receive a raw task message: %w", err)
					continue
				}
				// No messages in the broker so far.
				if rawMsg == nil {
					continue
				}

				m, err := a.conf.registry.Decode(rawMsg)
				if err != nil {
					level.Error(a.conf.logger).Log("msg", "failed to decode task message", "rawmsg", rawMsg, "err", err)
					continue
				}

				msgs <- m
			}
		}
	})

	go func() {
		// Start a worker when there is a task.
		for m := range msgs {
			level.Debug(a.conf.logger).Log("msg", "task received", "name", m.Name)

			_, ok := a.task.Load(m.Name)
			if !ok {
				level.Debug(a.conf.logger).Log("msg", "unregistered task", "name", m.Name)
				continue
			}
			if m.IsExpired() {
				level.Debug(a.conf.logger).Log("msg", "task message expired", "name", m.Name)
				continue
			}

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

				if err := a.executeTask(ctx, m); err != nil {
					level.Error(a.conf.logger).Log("msg", "task failed", "taskmsg", m, "err", err)
				} else {
					level.Debug(a.conf.logger).Log("msg", "task succeeded", "name", m.Name)
				}
				return nil
			})
		}
	}()

	return g.Wait()
}

type contextKey int

const (
	// ContextKeyTaskName is a context key to access task names.
	ContextKeyTaskName contextKey = iota
)

// executeTask calls the task function with args and kwargs from the message.
// If the task panics, the stack trace is returned as an error.
func (a *App) executeTask(ctx context.Context, m *protocol.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected task error: %v: %s", r, debug.Stack())
		}
	}()

	task, ok := a.task.Load(m.Name)
	if !ok {
		return fmt.Errorf("unregistered task: %s", m.Name)
	}
	taskF, ok := task.(TaskF)
	if !ok {
		return fmt.Errorf("invalid task: %s", m.Name)
	}
	// Use middlewares if a client provided them.
	if a.conf.chain != nil {
		taskF = a.conf.chain(taskF)
	}

	ctx = context.WithValue(ctx, ContextKeyTaskName, m.Name)
	p := NewTaskParam(m.Args, m.Kwargs)
	return taskF(ctx, p)
}

// update broker with new queues
func (a *App) observe() {
	qq := make([]string, 0)
	a.taskQueue.Range(func(k, val any) bool {
		sval, ok := val.(string)
		if !ok {
			level.Error(a.conf.logger).Log("could not convert task name to string")
			return true
		}
		qq = append(qq, sval)
		return true
	})
	a.conf.broker.Observe(qq)
	level.Debug(a.conf.logger).Log("msg", "observing queues", "queues", qq)
	a.queueCount.Store(uint32(len(qq)))
}

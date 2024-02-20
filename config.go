package celery

import (
	"github.com/go-kit/log"

	"github.com/dryarullin/gopher-celery/protocol"
)

// DefaultMaxWorkers is the default upper limit of goroutines
// allowed to process Celery tasks.
// Note, the workers are launched only when there are tasks to process.
//
// Let's say it takes ~5s to process a task on average,
// so 1000 goroutines should be able to handle 200 tasks per second
// (X = N / R = 1000 / 5) according to Little's law N = X * R.
const DefaultMaxWorkers = 1000

// Option sets up a Config.
type Option func(*Config)

// WithCustomTaskSerializer registers a custom serializer where
// mime is the mime-type describing the serialized structure, e.g., application/json,
// and encoding is the content encoding which is usually utf-8 or binary.
func WithCustomTaskSerializer(serializer protocol.Serializer, mime, encoding string) Option {
	return func(c *Config) {
		c.registry.Register(serializer, mime, encoding)
	}
}

// WithTaskSerializer sets a serializer mime-type, e.g.,
// the message's body is encoded in JSON when a task is sent to the broker.
// It is equivalent to CELERY_TASK_SERIALIZER in Python.
func WithTaskSerializer(mime string) Option {
	return func(c *Config) {
		c.mime = mime
	}
}

// WithTaskProtocol sets the default task message protocol version used to send tasks.
// It is equivalent to CELERY_TASK_PROTOCOL in Python.
func WithTaskProtocol(version int) Option {
	return func(c *Config) {
		switch version {
		case protocol.V1, protocol.V2:
			c.protocol = version
		default:
			c.protocol = protocol.V2
		}
	}
}

// WithBroker allows a caller to replace the default broker.
func WithBroker(broker Broker) Option {
	return func(c *Config) {
		c.broker = broker
	}
}

// WithLogger sets a structured logger.
func WithLogger(logger log.Logger) Option {
	return func(c *Config) {
		c.logger = logger
	}
}

// WithMaxWorkers sets an upper limit of goroutines
// allowed to process Celery tasks.
func WithMaxWorkers(n int) Option {
	return func(c *Config) {
		c.maxWorkers = n
	}
}

// WithMiddlewares sets a chain of task middlewares.
// The first middleware is treated as the outermost middleware.
func WithMiddlewares(chain ...Middleware) Option {
	return func(c *Config) {
		c.chain = func(next TaskF) TaskF {
			for i := len(chain) - 1; i >= 0; i-- {
				next = chain[i](next)
			}
			return next
		}
	}
}

// Config represents Celery settings.
type Config struct {
	logger     log.Logger
	broker     Broker
	registry   *protocol.SerializerRegistry
	mime       string
	protocol   int
	maxWorkers int
	chain      Middleware
}

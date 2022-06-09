package celery

import (
	"github.com/go-kit/log"
	"github.com/gomodule/redigo/redis"

	"github.com/marselester/gopher-celery/internal/protocol"
)

// The task message serializers.
const (
	SerializerJSON = "json"
)

// Supported protocol versions.
const (
	ProtocolV1 = 1
	ProtocolV2 = 2
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

// WithTaskSerializer sets a serializer format, e.g.,
// the message's body is encoded in JSON when a task is sent to the broker.
// It is equivalent to CELERY_TASK_SERIALIZER in Python.
func WithTaskSerializer(format string) Option {
	return func(c *Config) {
		switch format {
		case SerializerJSON:
			c.format = format
		default:
			c.format = SerializerJSON
		}
	}
}

// WithTaskProtocol sets the default task message protocol version used to send tasks.
// It is equivalent to CELERY_TASK_PROTOCOL in Python.
func WithTaskProtocol(version int) Option {
	return func(c *Config) {
		switch version {
		case 1, 2:
			c.protocol = version
		default:
			c.protocol = 2
		}
	}
}

// WithRedisBroker sets Redis as a broker.
func WithRedisBroker(pool *redis.Pool) Option {
	return func(c *Config) {
		c.pool = pool
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

// Config represents Celery settings.
type Config struct {
	logger     log.Logger
	pool       *redis.Pool
	registry   *protocol.SerializerRegistry
	format     string
	protocol   int
	maxWorkers int
}

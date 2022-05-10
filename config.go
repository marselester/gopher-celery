package celery

import (
	"github.com/go-kit/log"
	"github.com/gomodule/redigo/redis"

	"github.com/marselester/gopher-celery/internal/protocol"
)

type SerializerFormat string

// The task message serializers.
const (
	SerializerJSON SerializerFormat = "json"
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

// WithSerializer sets a serializer format, e.g., json.
// It is equivalent to CELERY_TASK_SERIALIZER in Python.
func WithSerializer(format SerializerFormat) Option {
	return func(c *Config) {
		switch format {
		case "json":
			c.serializer = &protocol.JSONSerializer{}
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
	serializer MessageSerializer
	maxWorkers int
}

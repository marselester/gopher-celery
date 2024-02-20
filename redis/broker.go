// Package redis implements a Celery broker using Redis
// and github.com/gomodule/redigo.
package redis

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/dryarullin/gopher-celery/internal/broker"
)

// DefaultReceiveTimeout defines how many seconds the broker's Receive command
// should block waiting for results from Redis.
const DefaultReceiveTimeout = 5

// BrokerOption sets up a Broker.
type BrokerOption func(*Broker)

// WithReceiveTimeout sets a timeout of how long the broker's Receive command
// should block waiting for results from Redis.
// Larger the timeout, longer the client will have to wait for Celery app to exit.
// Smaller the timeout, more BRPOP commands would have to be sent to Redis.
//
// Note, the read timeout you specified with redis.DialReadTimeout() method
// should be bigger than the receive timeout.
// Otherwise redigo would return i/o timeout error.
func WithReceiveTimeout(timeout time.Duration) BrokerOption {
	return func(br *Broker) {
		sec := int(timeout.Seconds())
		if sec <= 0 {
			sec = 1
		}
		br.receiveTimeout = sec
	}
}

// WithPool sets Redis connection pool.
func WithPool(pool *redis.Pool) BrokerOption {
	return func(br *Broker) {
		br.pool = pool
	}
}

// NewBroker creates a broker backed by Redis.
// By default it connects to localhost.
func NewBroker(options ...BrokerOption) *Broker {
	br := Broker{
		receiveTimeout: DefaultReceiveTimeout,
	}
	for _, opt := range options {
		opt(&br)
	}

	if br.pool == nil {
		br.pool = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.DialURL("redis://localhost")
			},
		}
	}
	return &br
}

// Broker is a Redis broker that sends/receives messages from specified queues.
type Broker struct {
	pool           *redis.Pool
	queues         []string
	receiveTimeout int
}

// Send inserts the specified message at the head of the queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
	conn := br.pool.Get()
	defer conn.Close()

	_, err := conn.Do("LPUSH", q, m)
	return err
}

// Observe sets the queues from which the tasks should be received.
// Note, the method is not concurrency safe.
func (br *Broker) Observe(queues []string) {
	br.queues = queues
}

// Receive fetches a Celery task message from a tail of one of the queues in Redis.
// After a timeout it returns nil, nil.
//
// Celery relies on BRPOP command to process messages fairly, see https://github.com/celery/kombu/issues/166.
// Redis BRPOP is a blocking list pop primitive.
// It blocks the connection when there are no elements to pop from any of the given lists.
// An element is popped from the tail of the first list that is non-empty,
// with the given keys being checked in the order that they are given,
// see https://redis.io/commands/brpop/.
//
// Note, the method is not concurrency safe.
func (br *Broker) Receive() ([]byte, error) {
	conn := br.pool.Get()
	defer conn.Close()

	// See the discussion regarding timeout and Context cancellation
	// https://github.com/gomodule/redigo/issues/207#issuecomment-283815775.
	res, err := redis.ByteSlices(conn.Do(
		"BRPOP",
		redis.Args{}.AddFlat(br.queues).Add(br.receiveTimeout)...,
	))
	if err == redis.ErrNil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to BRPOP %v: %w", br.queues, err)
	}

	// Put the Celery queue name to the end of the slice for fair processing.
	q := string(res[0])
	b := res[1]
	broker.Move2back(br.queues, q)
	return b, nil
}

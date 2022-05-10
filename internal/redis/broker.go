// Package redis implements a Celery broker using Redis.
package redis

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/gomodule/redigo/redis"
)

// DefaultReceiveTimeout defines how many seconds the broker's Receive command
// should block waiting for results from Redis.
// Larger the timeout, longer the client will have to wait for Celery app to exit.
const DefaultReceiveTimeout = 5

// Option sets up a Broker.
type Option func(*Broker)

// WithLogger sets a structured logger.
func WithLogger(logger log.Logger) Option {
	return func(c *Broker) {
		c.logger = logger
	}
}

// WithPool sets Redis connection pool.
func WithPool(pool *redis.Pool) Option {
	return func(c *Broker) {
		c.pool = pool
	}
}

// NewBroker creates a broker backed by Redis.
// By default it connects to localhost.
func NewBroker(queues []string, options ...Option) *Broker {
	br := Broker{
		queues:         queues,
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
	logger         log.Logger
	pool           *redis.Pool
	queues         []string
	receiveTimeout int
}

// Send inserts the specified message at the head of the queue using LPUSH command.
func (br *Broker) Send(m []byte, q string) error {
	return nil
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
	move2back(br.queues, q)
	return b, nil
}

// move2back moves item v to the end of the slice ss.
// For example, given slice [a, b, c, d, e, f] and item c,
// the result is [a, b, d, e, f, c].
// The running time is linear in the worst case.
func move2back(ss []string, v string) {
	n := len(ss)
	if n <= 1 {
		return
	}
	// Nothing to do when an item is already at the end of the slice.
	if ss[n-1] == v {
		return
	}

	var found bool
	i := 0
	for ; i < n; i++ {
		if ss[i] == v {
			found = true
			break
		}
	}
	if !found {
		return
	}

	// Swap the found item with the last item in the slice,
	// and then swap the neighbors starting from the found index i till the n-2:
	// the last item is already in its place,
	// and the one before it shouldn't be swapped with the last item.
	ss[i], ss[n-1] = ss[n-1], ss[i]
	for ; i < n-2; i++ {
		ss[i], ss[i+1] = ss[i+1], ss[i]
	}
}

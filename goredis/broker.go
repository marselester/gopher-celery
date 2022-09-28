// Package goredis implements a Celery broker using GoRedis.
package goredis

import (
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/marselester/gopher-celery/internal/brokertools"
	"time"
)

// DefaultReceiveTimeout defines how many seconds the broker's Receive command
// should block waiting for results from Redis.
// Larger the timeout, longer the client will have to wait for Celery app to exit.
const DefaultReceiveTimeout = 5

// BrokerOption sets up a Broker.
type BrokerOption func(*Broker)

// WithBrokerClient sets Redis connection pool.
func WithBrokerClient(pool *redis.Client) BrokerOption {
	return func(c *Broker) {
		c.pool = pool
	}
}

// NewBroker creates a broker backed by Redis.
// By default, it connects to localhost.
func NewBroker(options ...BrokerOption) *Broker {
	br := Broker{
		receiveTimeout: DefaultReceiveTimeout * time.Second,
		ctx:            context.Background(),
	}
	for _, opt := range options {
		opt(&br)
	}

	if br.pool == nil {
		// should we provide a way to pass/override redis.Options here?
		br.pool = redis.NewClient(&redis.Options{})
	}
	return &br
}

// Broker is a Redis broker that sends/receives messages from specified queues.
type Broker struct {
	pool           *redis.Client
	queues         []string
	receiveTimeout time.Duration
	ctx            context.Context
}

// Send inserts the specified message at the head of the queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
	conn := br.pool.Conn()
	defer conn.Close()

	res := conn.LPush(br.ctx, q, m)
	return res.Err()
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
	conn := br.pool.Conn()
	defer conn.Close()

	res := conn.BRPop(br.ctx, br.receiveTimeout, br.queues...)
	// Put the Celery queue name to the end of the slice for fair processing.
	q := res.Val()[0]
	b := res.Val()[1]
	brokertools.Move2back(br.queues, q)
	return []byte(b), res.Err()
}

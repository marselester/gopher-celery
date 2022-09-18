package goredis

import (
	"github.com/go-redis/redis/v9"
	"time"
)

// DefaultReceiveTimeout defines how many seconds the broker's Receive command
// should block waiting for results from Redis.
// Larger the timeout, longer the client will have to wait for Celery app to exit.
const DefaultReceiveTimeout = 5

// Option sets up a Broker.
type Option func(*Broker)

// WithPool sets Redis connection pool.
func WithPool(pool *redis.Client) Option {
	return func(c *Broker) {
		c.pool = pool
	}
}

// NewBroker creates a broker backed by Redis.
// By default it connects to localhost.
func NewBroker(redisOptions *redis.Options, options ...Option) *Broker {
	br := Broker{
		receiveTimeout: DefaultReceiveTimeout,
	}
	for _, opt := range options {
		opt(&br)
	}

	if br.pool == nil {
		br.pool = redis.NewClient(redisOptions)
	}
	return &br
}

// Broker is a Redis broker that sends/receives messages from specified queues.
type Broker struct {
	pool           *redis.Client
	queues         []string
	receiveTimeout int
}

// Send inserts the specified message at the head of the queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) {
	conn := br.pool.Conn()
	defer conn.Close()

	_ = conn.LPush(nil, q, m)
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

	res := conn.BRPop(nil, time.Duration(br.receiveTimeout), br.queues...)
	// Put the Celery queue name to the end of the slice for fair processing.
	q := res.Val()[0]
	b := res.Val()[1]
	move2back(br.queues, q)
	return []byte(b), nil
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

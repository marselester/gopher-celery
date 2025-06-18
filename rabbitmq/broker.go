// Package rabbitmq implements a Celery broker using RabbitMQ
// and github.com/rabbitmq/amqp091-go.
package rabbitmq

import (
    "context"
    "log"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"

    "github.com/roncemer/gopher-celery-with-rabbitmq-broker/internal/broker"
)

// DefaultAmqpUri defines the default AMQP URI which is used to connect to RabbitMQ.
const DefaultAmqpUri = "amqp://guest:guest@localhost:5672/"

// DefaultReceiveTimeout defines how many seconds the broker's Receive command
// should block waiting for results from RabbitMQ.
const DefaultReceiveTimeout = 5

// BrokerOption sets up a Broker.
type BrokerOption func(*Broker)

// Broker is a RabbitMQ broker that sends/receives messages from specified queues.
type Broker struct {
    amqpUri        string
    conn           *amqp.Connection
    channel        *amqp.Channel
    queues         []string
    receiveTimeout time.Duration
    ctx            context.Context
}

// WithAmqpUri sets the AMQP connection URI to RabbitMQ.
func WithAmqpUri(amqpUri string) BrokerOption {
    return func(br *Broker) {
        br.amqpUri = amqpUri
    }
}

// WithReceiveTimeout sets a timeout of how long the broker's Receive command
// should block waiting for results from RabbitMQ.
// Larger the timeout, longer the client will have to wait for Celery app to exit.
// Smaller the timeout, more Get commands would have to be sent to RabbitMQ.
func WithReceiveTimeout(timeout time.Duration) BrokerOption {
    return func(br *Broker) {
        br.receiveTimeout = timeout
    }
}

// WithClient sets RabbitMQ client representing a connection to RabbitMQ.
func WithClient(c *amqp.Connection) BrokerOption {
    return func(br *Broker) {
        br.conn = c
    }
}

// NewBroker creates a broker backed by RabbitMQ.
// By default, it connects to localhost.
func NewBroker(options ...BrokerOption) *Broker {
    br := Broker{
        amqpUri:        DefaultAmqpUri,
        receiveTimeout: DefaultReceiveTimeout * time.Second,
        ctx:            context.Background(),
    }
    for _, opt := range options {
        opt(&br)
    }

    if br.conn == nil {
        br.channel = nil
        conn, err := amqp.Dial(br.amqpUri)
        br.conn = conn
        if err != nil {
            log.Panicf("Failed to connect to RabbitMQ: %s", err)
            return nil
        }
    }

    if br.channel == nil {
        channel, err := br.conn.Channel()
        br.channel = channel
        if err != nil {
            log.Panicf("Failed to open a channel: %s", err)
            return nil
        }
    }

    return &br
}

// Send inserts the specified message at the head of the queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
    err := br.channel.PublishWithContext(
        br.ctx,
        "",     // exchange
        q,      // routing key
        false,  // mandatory
        false,  // immediate
        amqp.Publishing{
            ContentType: "application/octet-stream",
            Body:        m,
        })
    if err != nil {
        log.Panicf("Failed to publish a message: %s", err)
    }
    return err
}

// Observe sets the queues from which the tasks should be received.
// Note, the method is not concurrency safe.
func (br *Broker) Observe(queues []string) {
    br.queues = queues
    for _, queue := range queues {
        _, err := br.channel.QueueDeclare(
            queue,   // name
            true,    // durable
            false,   // delete when unused
            false,   // exclusive
            false,   // no-wait
            nil,     // arguments
        )
        if err != nil {
            log.Panicf("Failed to declare a queue: %s", err)
        }
    }
}

// Receive fetches a Celery task message from a tail of one of the queues in RabbitMQ.
// After a timeout it returns nil, nil.
func (br *Broker) Receive() ([]byte, error) {
    //ctx, cancel := context.WithTimeout(context.Background(), br.receiveTimeout)
    //br.ctx = ctx
    //defer cancel()

    const retryIntervalMs = 100
    queue := br.queues[0]

    startTime := time.Now()
    timeoutTime := startTime.Add(br.receiveTimeout)
    msg, ok, err := br.channel.Get(queue, true)
    if err != nil {
        log.Panicf("Failed to g a message: %s", err)
        return nil, nil
    }
    for !ok {
        time.Sleep(retryIntervalMs * time.Millisecond)
        if time.Now().After(timeoutTime) {
            break
        }
        msg, ok, err = br.channel.Get(queue, true)
        if err != nil {
            log.Panicf("Failed to g a message: %s", err)
            return nil, nil
        }
    }

    // Put the Celery queue name to the end of the slice for fair processing.
    broker.Move2back(br.queues, queue)

    if ok {
        return []byte(msg.Body), nil
    }
    return nil, nil
}

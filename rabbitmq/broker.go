// Package rabbitmq implements a Celery broker using RabbitMQ
// and github.com/rabbitmq/amqp091-go.
package rabbitmq

import (
	"encoding/base64"
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/marselester/gopher-celery/internal/broker"
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
	receiveTimeout time.Duration
	rawMode        bool
	queues         []string
	conn           *amqp.Connection
	channel        *amqp.Channel
	delivery       map[string]<-chan amqp.Delivery
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
func NewBroker(options ...BrokerOption) (*Broker, error) {
	br := Broker{
		amqpUri:        DefaultAmqpUri,
		receiveTimeout: DefaultReceiveTimeout * time.Second,
		rawMode:        false,
		delivery:       make(map[string]<-chan amqp.Delivery),
	}
	for _, opt := range options {
		opt(&br)
	}

	if br.conn == nil {
		conn, err := amqp.Dial(br.amqpUri)
		if err != nil {
			return nil, err
		}

		br.conn = conn
	}

	channel, err := br.conn.Channel()
	if err != nil {
		return nil, err
	}

	br.channel = channel

	return &br, nil
}

// Send inserts the specified message at the head of the queue.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
	var (
		headers         map[string]interface{}
		body            []byte
		contentType     string
		contentEncoding string
		deliveryMode    uint8
		correlationId   string
		replyTo         string
	)

	if br.rawMode {
		headers = make(amqp.Table)
		body = m
		contentType = "application/json"
		contentEncoding = "utf-8"
		deliveryMode = 2
		correlationId = ""
		replyTo = ""
	} else {
		var msgmap map[string]interface{}
		err := json.Unmarshal(m, &msgmap)
		if err != nil {
			return err
		}

		headers = msgmap["headers"].(map[string]interface{})
		body, err = base64.StdEncoding.DecodeString(msgmap["body"].(string))
		if err != nil {
			return err
		}
		contentType = msgmap["content-type"].(string)
		contentEncoding = msgmap["content-encoding"].(string)

		properties_in := msgmap["properties"].(map[string]interface{})
		deliveryMode = uint8(properties_in["delivery_mode"].(float64))
		correlationId = properties_in["correlation_id"].(string)
		replyTo = properties_in["reply_to"].(string)
	}

	return br.channel.Publish(
		"",    // exchange
		q,     // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         headers,
			ContentType:     contentType,
			ContentEncoding: contentEncoding,
			DeliveryMode:    deliveryMode,
			CorrelationId:   correlationId,
			ReplyTo:         replyTo,
			Body:            body,
		})
}

// Observe sets the queues from which the tasks should be received.
// Note, the method is not concurrency safe.
func (br *Broker) Observe(queues []string) error {
	br.queues = queues

	var (
		durable    = true
		autoDelete = false
		exclusive  = false
		noWait     = false
	)
	for _, queue := range queues {
		// Check whether the queue exists.
		// If the queue doesn't exist, attempt to create it.
		_, err := br.channel.QueueDeclarePassive(
			queue,
			durable,
			autoDelete,
			exclusive,
			noWait,
			nil,
		)
		if err != nil {
			// QueueDeclarePassive() will close the channel if the queue does not exist,
			// so we have to create a new channel when this happens.
			if br.channel.IsClosed() {
				channel, err := br.conn.Channel()
				if err != nil {
					return err
				}

				br.channel = channel
			}

			_, err = br.channel.QueueDeclare(
				queue,
				durable,
				autoDelete,
				exclusive,
				noWait,
				nil,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Receive fetches a Celery task message from a tail of one of the queues in RabbitMQ.
// After a timeout it returns nil, nil.
func (br *Broker) Receive() ([]byte, error) {
	queue := br.queues[0]
	// Put the Celery queue name to the end of the slice for fair processing.
	broker.Move2back(br.queues, queue)

	var err error

	delivery, deliveryExists := br.delivery[queue]
	if !deliveryExists {
		delivery, err = br.channel.Consume(
			queue, // queue
			"",    // consumer
			true,  // autoAck
			false, // exclusive
			false, // noLocal (ignored)
			false, // noWait
			nil,   // args
		)
		if err != nil {
			return nil, err
		}

		br.delivery[queue] = delivery
	}

	select {
	case msg := <-delivery:
		if br.rawMode {
			return msg.Body, nil
		}

		// Marshal msg from RabbitMQ Celery format to internal Celery format.

		properties := make(map[string]interface{})
		properties["correlation_id"] = msg.CorrelationId
		properties["reply_to"] = msg.ReplyTo
		properties["delivery_mode"] = msg.DeliveryMode
		properties["delivery_info"] = map[string]interface{}{
			"exchange":    msg.Exchange,
			"routing_key": msg.RoutingKey,
		}
		properties["priority"] = msg.Priority
		properties["body_encoding"] = "base64"
		properties["delivery_tag"] = msg.DeliveryTag

		imsg := make(map[string]interface{})
		imsg["body"] = msg.Body
		imsg["content-encoding"] = msg.ContentEncoding
		imsg["content-type"] = msg.ContentType
		imsg["headers"] = msg.Headers
		imsg["properties"] = properties

		var result []byte
		result, err := json.Marshal(imsg)
		if err != nil {
			return nil, err
		}

		return result, nil

	case <-time.After(br.receiveTimeout):
		// Receive timeout
		return nil, nil
	}
}

// Package rabbitmq implements a Celery broker using RabbitMQ
// and github.com/rabbitmq/amqp091-go.
package rabbitmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
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
		rawMode:        false,
		delivery:       make(map[string]<-chan amqp.Delivery),
		ctx:            context.Background(),
	}
	for _, opt := range options {
		opt(&br)
	}

	if br.conn == nil {
		conn, err := amqp.Dial(br.amqpUri)
		if err != nil {
			log.Panicf("Failed to connect to RabbitMQ: %s", err)
			return nil
		}
		br.conn = conn
	}

	channel, err := br.conn.Channel()
	if err != nil {
		log.Panicf("Failed to open a channel: %s", err)
		return nil
	}
	br.channel = channel

	return &br
}

// Send inserts the specified message at the head of the queue.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
	var headers map[string]interface{}
	var body []byte
	var contentType string
	var contentEncoding string
	var deliveryMode uint8
	var correlationId string
	var replyTo string

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

	err := br.channel.PublishWithContext(
		br.ctx,
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
	return err
}

// Observe sets the queues from which the tasks should be received.
// Note, the method is not concurrency safe.
func (br *Broker) Observe(queues []string) {
	br.queues = queues
	for _, queue := range queues {
		_, err := br.channel.QueueDeclare(
			queue, // name
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			log.Panicf("Failed to declare a queue: %s", err)
		} else {
			for _, queue := range queues {
				delivery, err := br.channel.Consume(
					queue, // queue
					"",    // consumer
					true,  // autoAck
					false, // exclusive
					false, // noLocal (ignored)
					false, // noWait
					nil,   // args
				)
				if err != nil {
					err_str := fmt.Errorf("%w", err)
					log.Panicf("channel.Consume() failed for queue %s: %s", queue, err_str)
				} else {
					br.delivery[queue] = delivery
				}
			}
		}
	}
}

// Receive fetches a Celery task message from a tail of one of the queues in RabbitMQ.
// After a timeout it returns nil, nil.
func (br *Broker) Receive() ([]byte, error) {
	queue := br.queues[0]
	// Put the Celery queue name to the end of the slice for fair processing.
	broker.Move2back(br.queues, queue)

	delivery := br.delivery[queue]

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
		delivery_info := make(map[string]interface{})
		properties["delivery_info"] = delivery_info
		delivery_info["exchange"] = msg.Exchange
		delivery_info["routing_key"] = msg.RoutingKey
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
			err_str := fmt.Errorf("%w", err)
			log.Printf("json encode: %s", err_str)
			return nil, err
		}

		return result, nil

	case <-time.After(br.receiveTimeout):
		// Receive timeout
		return nil, nil
	}
}

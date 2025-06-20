// Package rabbitmq implements a Celery broker using RabbitMQ
// and github.com/rabbitmq/amqp091-go.
package rabbitmq

import (
    "context"
    "encoding/json"
    "fmt"
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
    receiveTimeout time.Duration
    rawMode        bool
    queues         []string
    conn           *amqp.Connection
    channel        *amqp.Channel
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

// WithRawMode sets rawMode, which, if true, disables marshaling of data structures between internal and RabbitMQ Celery formats.
// Note that rawMode defaults to false, and should only be set to true inside the broker unit tests.
func WithRawMode(rawMode bool) BrokerOption {
    return func(br *Broker) {
        br.rawMode = rawMode
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

// Marshal data from m in internal Celery format to body in RabbitMQ Celery format.
func marshalInternalFormatToRabbitMQCeleryMessage(m []byte) ([]byte) {
    // TODO: Write the data marshaling from m to body here.
    return m
}

// Send inserts the specified message at the head of the queue using LPUSH command.
// Note, the method is safe to call concurrently.
func (br *Broker) Send(m []byte, q string) error {
    var body []byte

    if br.rawMode {
        body = m
    } else {
        body = marshalInternalFormatToRabbitMQCeleryMessage(m)
    }

    err := br.channel.PublishWithContext(
        br.ctx,
        "",     // exchange
        q,      // routing key
        false,  // mandatory
        false,  // immediate
        amqp.Publishing{
            ContentType: "application/json",
            ContentEncoding: "utf-8",
            Body:        body,
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

type inboundMessageBody struct {
    ID          string                  `json:"id"`
    Task        string                  `json:"task"`
    Args        []interface{}           `json:"args"`
    Kwargs      map[string]interface{}  `json:"kwargs"`
    Expires     interface{}             `json:"expires"`    // string | nil
    Retries     int                     `json:"retries"`
    ETA         interface{}             `json:"eta"`        // string | nil
    UTC         bool                    `json:"utc"`
}

type inboundMessage struct {
	Body            []byte                 `json:"body"`
	ContentEncoding string                 `json:"content-encoding"`
	ContentType     string                 `json:"content-type"`
	Headers         map[string]string      `json:"headers"`
}

// Marshal msg from RabbitMQ Celery format to internal Celery format.
func marshalRabbitMQCeleryMessageToInternalFormat(msg amqp.Delivery) ([]byte) {
    var argsarr []interface{}
    var err error

    err = json.Unmarshal([]byte(msg.Body), &argsarr)
    if err != nil {
        err_str := fmt.Errorf("%w", err)
        log.Printf("json decode: %s", err_str)
        return nil
    }

    body := inboundMessageBody {
        ID: msg.Headers["id"].(string),
        Task: msg.Headers["task"].(string),
        Args: argsarr[0].([]interface{}),
        Kwargs: argsarr[1].(map[string]interface{}),
    }

    expires, ok := msg.Headers["expires"]
    if ok {
        body.Expires = expires
    } else {
        body.Expires = nil
    }

    retries, ok := msg.Headers["retries"]
    if ok {
        body.Retries = int(retries.(int32))
    } else {
        body.Retries = 0
    }

    eta, ok := msg.Headers["eta"]
    if ok {
        body.ETA = eta
    } else {
        body.ETA = nil
    }

    utc, ok := msg.Headers["utc"]
    if ok {
        body.UTC = utc.(bool)
    } else {
        body.UTC = true
    }

    //log.Printf("body: %T %v", body, body)

    body_json, err := json.Marshal(body)
    if err != nil {
        err_str := fmt.Errorf("%w", err)
        log.Printf("json encode: %s", err_str)
        return nil
    }
 
    //log.Printf("body_json: %T %v %s", body_json, body_json, body_json)

    imsg := inboundMessage {
        Body: body_json,
        ContentEncoding: "utf-8",
        ContentType: "application/json",
        //Headers: {},
    }

    //log.Printf("imsg: %T %v", imsg, imsg)

    result, err := json.Marshal(imsg)
    if err != nil {
        err_str := fmt.Errorf("%w", err)
        log.Printf("json encode: %s", err_str)
        return nil
    }

    return result
}

// Receive fetches a Celery task message from a tail of one of the queues in RabbitMQ.
// After a timeout it returns nil, nil.
func (br *Broker) Receive() ([]byte, error) {

    const retryIntervalMs = 100
    queue := br.queues[0]

    try_receive := func() (msg amqp.Delivery, ok bool, err error) {
        my_msg, my_ok, my_err := br.channel.Get(queue, true)
        if my_err != nil {
            log.Printf("Failed to g a message: %s", my_err)
        }
        return my_msg, my_ok, my_err
    }

    startTime := time.Now()
    timeoutTime := startTime.Add(br.receiveTimeout)
    msg, ok, err := try_receive()
    if err != nil {
        return nil, nil
    }
    for !ok {
        time.Sleep(retryIntervalMs * time.Millisecond)
        if time.Now().After(timeoutTime) {
            break
        }
        msg, ok, err = try_receive()
        if err != nil {
            return nil, nil
        }
    }

    // Put the Celery queue name to the end of the slice for fair processing.
    broker.Move2back(br.queues, queue)

    if ok {
        //log.Printf("msg.Body: %T %v", msg.Body, msg.Body)

        if br.rawMode {
            return msg.Body, nil
        }

        return marshalRabbitMQCeleryMessageToInternalFormat(msg), nil
    }
    return nil, nil
}

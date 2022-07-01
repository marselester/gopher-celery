// Package protocol provides means to encode/decode task messages
// as described in https://github.com/celery/celery/blob/master/docs/internals/protocol.rst.
package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Task represents a task message that provides essential params to run a task.
type Task struct {
	// ID id a unique id of the task in UUID v4 format (required).
	ID string
	// Name is a name of the task (required).
	Name string
	// Args is a list of arguments.
	// It will be an empty list if not provided.
	Args []interface{}
	// Kwargs is a dictionary of keyword arguments.
	// It will be an empty dictionary if not provided.
	Kwargs map[string]interface{}
	// Expires is an expiration date in ISO 8601 format.
	// If not provided the message will never expire.
	// The message will be expired when the message is received and the expiration date has been exceeded.
	Expires time.Time
}

// IsExpired returns true if the message is expired
// and shouldn't be processed.
func (t *Task) IsExpired() bool {
	return !t.Expires.IsZero() && t.Expires.Before(time.Now())
}

// The mime-type describing the serializers.
const (
	MimeJSON = "application/json"
)

// Supported protocol versions.
const (
	V1 = 1
	V2 = 2
)

// Serializer encodes/decodes Celery tasks (message's body param to be precise).
// See https://docs.celeryq.dev/projects/kombu/en/latest/userguide/serialization.html.
type Serializer interface {
	// Decode decodes the message body s into task t
	// using protocol p which could be version 1 or 2.
	Decode(p int, s string, t *Task) error
	// Encode encodes task t using protocol p and returns the message body s.
	Encode(p int, t *Task) (s string, err error)
}

// NewSerializerRegistry creates a registry of serializers.
func NewSerializerRegistry() *SerializerRegistry {
	js := NewJSONSerializer()
	r := SerializerRegistry{
		pool: sync.Pool{New: func() interface{} {
			return &bytes.Buffer{}
		}},
		serializers: make(map[string]Serializer),
		encoding:    make(map[string]string),
		uuid4:       uuid.NewString,
	}
	r.Register(js, "json", "utf-8")
	r.Register(js, "application/json", "utf-8")

	var (
		host string
		err  error
	)
	if host, err = os.Hostname(); err != nil {
		host = "unknown"
	}
	r.origin = fmt.Sprintf("%d@%s", os.Getpid(), host)

	return &r
}

// SerializerRegistry encodes/decodes task messages using registered serializers.
// Celery relies on JSON format to store message metadata
// such as content type and headers.
// Task details (args, kwargs) are encoded in message body in base64 and JSON by default.
// The encoding is indicated by body_encoding and content-type message params.
// Therefore a client doesn't have to specify the formats since the registry can
// pick an appropriate decoder based on the aforementioned params.
type SerializerRegistry struct {
	pool sync.Pool
	// serializers helps to look up a serializer by a content-type,
	// see also https://github.com/celery/kombu/blob/master/kombu/serialization.py#L388.
	serializers map[string]Serializer
	// encoding maps content-type to its encoding, e.g., application/json uses utf-8 encoding.
	encoding map[string]string
	// uuid4 returns uuid v4, e.g., 0ad73c66-f4c9-4600-bd20-96746e720eed.
	uuid4 func() string
	// origin is a pid@host used in encoding task messages.
	origin string
}

// Register registers a custom serializer where
// mime is the mime-type describing the serialized structure, e.g., application/json,
// and encoding is the content encoding which is usually utf-8 or binary.
func (r *SerializerRegistry) Register(serializer Serializer, mime, encoding string) {
	r.serializers[mime] = serializer
	r.encoding[mime] = encoding
}

type inboundMessage struct {
	Body        string                 `json:"body"`
	ContentType string                 `json:"content-type"`
	Header      inboundMessageV2Header `json:"headers"`
}
type inboundMessageV2Header struct {
	ID      string    `json:"id"`
	Task    string    `json:"task"`
	Expires time.Time `json:"expires"`
}

// Decode decodes the raw message and returns a task info.
// If the header doesn't contain a task name, then protocol v1 is assumed.
// Otherwise the protocol v2 is used.
func (r *SerializerRegistry) Decode(raw []byte) (*Task, error) {
	var m inboundMessage
	err := json.Unmarshal(raw, &m)
	if err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}

	var (
		prot int
		t    Task
	)
	// Protocol version is detected by the presence of a task message header.
	if m.Header.Task == "" {
		prot = V1
	} else {
		prot = V2
		t.ID = m.Header.ID
		t.Name = m.Header.Task
		t.Expires = m.Header.Expires
	}

	ser := r.serializers[m.ContentType]
	if ser == nil {
		return nil, fmt.Errorf("unregistered serializer: %s", m.ContentType)
	}
	if err = ser.Decode(prot, m.Body, &t); err != nil {
		return nil, fmt.Errorf("parsing body v%d: %w", prot, err)
	}
	if t.Name == "" {
		return nil, fmt.Errorf("missing task name")
	}

	return &t, err
}

// Encode encodes the task message.
func (r *SerializerRegistry) Encode(queue, mime string, prot int, t *Task) ([]byte, error) {
	if prot != V1 && prot != V2 {
		return nil, fmt.Errorf("unknown protocol version %d", prot)
	}

	ser := r.serializers[mime]
	if ser == nil {
		return nil, fmt.Errorf("unregistered serializer %s", mime)
	}
	if r.encoding[mime] == "" {
		return nil, fmt.Errorf("unregistered serializer encoding %s", mime)
	}

	body, err := ser.Encode(prot, t)
	if err != nil {
		return nil, fmt.Errorf("%s encode %d: %w", mime, prot, err)
	}

	if prot == V1 {
		return r.encodeV1(body, queue, mime, t)
	} else {
		return r.encodeV2(body, queue, mime, t)
	}
}

// jsonEmptyMap helps to reduce allocs when encoding empty maps in json.
var jsonEmptyMap = json.RawMessage("{}")

type outboundMessageV1 struct {
	Body            string                  `json:"body"`
	ContentEncoding string                  `json:"content-encoding"`
	ContentType     string                  `json:"content-type"`
	Header          json.RawMessage         `json:"headers"`
	Property        outboundMessageProperty `json:"properties"`
}

type outboundMessageProperty struct {
	DeliveryInfo  outboundMessageDeliveryInfo `json:"delivery_info"`
	CorrelationID string                      `json:"correlation_id"`
	ReplyTo       string                      `json:"reply_to"`
	BodyEncoding  string                      `json:"body_encoding"`
	DeliveryTag   string                      `json:"delivery_tag"`
	DeliveryMode  int                         `json:"delivery_mode"`
	// Priority is a number between 0 and 255, where 255 is the highest priority in RabbitMQ
	// and 0 is the highest in Redis.
	Priority int `json:"priority"`
}
type outboundMessageDeliveryInfo struct {
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
}

func (r *SerializerRegistry) encodeV1(body, queue, mime string, t *Task) ([]byte, error) {
	buf := r.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		r.pool.Put(buf)
	}()

	m := outboundMessageV1{
		Body:            body,
		ContentEncoding: r.encoding[mime],
		ContentType:     mime,
		Header:          jsonEmptyMap,
		Property: outboundMessageProperty{
			BodyEncoding:  "base64",
			CorrelationID: t.ID,
			ReplyTo:       r.uuid4(),
			DeliveryInfo: outboundMessageDeliveryInfo{
				Exchange:   queue,
				RoutingKey: queue,
			},
			DeliveryMode: 2,
			DeliveryTag:  r.uuid4(),
		},
	}

	if err := json.NewEncoder(buf).Encode(&m); err != nil {
		return nil, fmt.Errorf("json encode: %w", err)
	}

	return buf.Bytes(), nil
}

type outboundMessageV2 struct {
	Body            string                  `json:"body"`
	ContentEncoding string                  `json:"content-encoding"`
	ContentType     string                  `json:"content-type"`
	Header          outboundMessageV2Header `json:"headers"`
	Property        outboundMessageProperty `json:"properties"`
}
type outboundMessageV2Header struct {
	// Lang enables support for multiple languages.
	// Worker may redirect the message to a worker that supports the language.
	Lang string `json:"lang"`
	ID   string `json:"id"`
	// RootID helps to keep track of workflows.
	RootID string `json:"root_id"`
	Task   string `json:"task"`
	// Origin is the name of the node sending the task,
	// '@'.join([os.getpid(), socket.gethostname()]).
	Origin  string  `json:"origin"`
	Expires *string `json:"expires"`
	// Retries is a current number of times this task has been retried.
	// It's always set to zero.
	Retries int `json:"retries"`
}

func (r *SerializerRegistry) encodeV2(body, queue, mime string, t *Task) ([]byte, error) {
	buf := r.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		r.pool.Put(buf)
	}()

	m := outboundMessageV2{
		Body:            body,
		ContentEncoding: r.encoding[mime],
		ContentType:     mime,
		Header: outboundMessageV2Header{
			Lang:   "go",
			ID:     t.ID,
			RootID: t.ID,
			Task:   t.Name,
			Origin: r.origin,
		},
		Property: outboundMessageProperty{
			BodyEncoding:  "base64",
			CorrelationID: t.ID,
			ReplyTo:       r.uuid4(),
			DeliveryInfo: outboundMessageDeliveryInfo{
				Exchange:   queue,
				RoutingKey: queue,
			},
			DeliveryMode: 2,
			DeliveryTag:  r.uuid4(),
		},
	}
	if !t.Expires.IsZero() {
		s := t.Expires.Format(time.RFC3339)
		m.Header.Expires = &s
	}

	if err := json.NewEncoder(buf).Encode(&m); err != nil {
		return nil, fmt.Errorf("json encode: %w", err)
	}

	return buf.Bytes(), nil
}

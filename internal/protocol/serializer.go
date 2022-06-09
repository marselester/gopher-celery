// Package protocol provides means to encode/decode task messages
// as described in https://github.com/celery/celery/blob/master/docs/internals/protocol.rst.
package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

// Task represents a task message that provides essential params to run a task.
type Task struct {
	ID      string
	Name    string
	Args    []interface{}
	Kwargs  map[string]interface{}
	Expires time.Time
}

// IsExpired returns true if the message is expired
// and shouldn't be processed.
func (t *Task) IsExpired() bool {
	return !t.Expires.IsZero() && t.Expires.Before(time.Now())
}

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
	var js JSONSerializer
	r := SerializerRegistry{
		serializers: map[string]Serializer{
			"json":             &js,
			"application/json": &js,
		},
	}
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
	serializers map[string]Serializer
}

// Register registers the serializer,
// see https://github.com/celery/kombu/blob/master/kombu/serialization.py#L291.
func (r *SerializerRegistry) Register(name string, serializer Serializer) {
	r.serializers[name] = serializer
}

type message struct {
	Body        string          `json:"body"`
	ContentType string          `json:"content-type"`
	Header      messageV2Header `json:"headers"`
}
type messageV2Header struct {
	ID      string    `json:"id"`
	Task    string    `json:"task"`
	Expires time.Time `json:"expires"`
}
type messageV1Body struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Expires time.Time              `json:"expires"`
}

// Decode decodes the raw message and returns a task info.
// If the header doesn't contain a task name, then protocol v1 is assumed.
// Otherwise the protocol v2 is used.
func (r *SerializerRegistry) Decode(raw []byte) (*Task, error) {
	var m message
	err := json.Unmarshal(raw, &m)
	if err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}

	var (
		prot int
		t    Task
	)
	if m.Header.Task == "" {
		prot = 1
	} else {
		prot = 2
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
func (r *SerializerRegistry) Encode(format string, prot int, t *Task) ([]byte, error) {
	ser := r.serializers[format]
	if ser == nil {
		return nil, fmt.Errorf("unregistered serializer")
	}
	return nil, nil
}

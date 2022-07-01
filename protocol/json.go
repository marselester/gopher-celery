package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// NewJSONSerializer returns JSONSerializer.
func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{
		pool: sync.Pool{New: func() interface{} {
			return &bytes.Buffer{}
		}},
		now: time.Now,
	}
}

// JSONSerializer encodes/decodes a task messages in JSON format.
// The zero value is not usable.
type JSONSerializer struct {
	pool sync.Pool
	now  func() time.Time
}

// jsonInboundV1Body helps to decode a message body v1.
type jsonInboundV1Body struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Expires time.Time              `json:"expires"`
}

// Decode parses the JSON-encoded message body s
// depending on Celery protocol version (v1 or v2).
// The task t is updated with the decoded params.
func (ser *JSONSerializer) Decode(p int, s string, t *Task) error {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("base64 decode: %w", err)
	}

	switch p {
	case V1:
		var body jsonInboundV1Body
		if err := json.Unmarshal(b, &body); err != nil {
			return fmt.Errorf("json decode: %w", err)
		}

		t.ID = body.ID
		t.Name = body.Task
		t.Args = body.Args
		t.Kwargs = body.Kwargs
		t.Expires = body.Expires
	case V2:
		var a [3]interface{}
		if err := json.Unmarshal(b, &a); err != nil {
			return fmt.Errorf("json decode: %w", err)
		}
		args, ok := a[0].([]interface{})
		if !ok {
			return fmt.Errorf("expected args: %v", a[0])
		}
		kwargs, ok := a[1].(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected kwargs: %v", a[1])
		}

		t.Args = args
		t.Kwargs = kwargs
	default:
		return fmt.Errorf("unknown protocol version %d", p)
	}

	return nil
}

// Encode encodes task t using protocol version p and returns the message body s.
func (ser *JSONSerializer) Encode(p int, t *Task) (s string, err error) {
	if p == V1 {
		return ser.encodeV1(t)
	}
	return ser.encodeV2(t)
}

// jsonOutboundV1Body is an auxiliary task struct to encode the message body v1 in json.
type jsonOutboundV1Body struct {
	ID      string          `json:"id"`
	Task    string          `json:"task"`
	Args    []interface{}   `json:"args"`
	Kwargs  json.RawMessage `json:"kwargs"`
	Expires *string         `json:"expires"`
	// Retries is a current number of times this task has been retried.
	// It's always set to zero.
	Retries int `json:"retries"`
	// ETA is an estimated time of arrival in ISO 8601 format, e.g., 2009-11-17T12:30:56.527191.
	// If not provided the message isn't scheduled, but will be executed ASAP.
	ETA string `json:"eta"`
	// UTC indicates to use the UTC timezone or the current local timezone.
	UTC bool `json:"utc"`
}

func (ser *JSONSerializer) encodeV1(t *Task) (s string, err error) {
	v := jsonOutboundV1Body{
		ID:     t.ID,
		Task:   t.Name,
		Args:   t.Args,
		Kwargs: jsonEmptyMap,
		ETA:    ser.now().Format(time.RFC3339),
		UTC:    true,
	}
	if t.Args == nil {
		v.Args = make([]interface{}, 0)
	}
	if t.Kwargs != nil {
		if v.Kwargs, err = json.Marshal(t.Kwargs); err != nil {
			return "", fmt.Errorf("kwargs json encode: %w", err)
		}
	}
	if !t.Expires.IsZero() {
		s := t.Expires.Format(time.RFC3339)
		v.Expires = &s
	}

	buf := ser.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		ser.pool.Put(buf)
	}()

	if err = json.NewEncoder(buf).Encode(&v); err != nil {
		return "", fmt.Errorf("json encode: %w", err)
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

const (
	// jsonV2opts represents blank task options in protocol v2.
	// They are blank because none of those features are supported here.
	jsonV2opts = `{"callbacks":null,"errbacks":null,"chain":null,"chord":null}`
	// jsonV2noparams is a base64+json encoded task with no args/kwargs when protocol v2 is used.
	// It helps to reduce allocs.
	jsonV2noparams = "W1tdLCB7fSwgeyJjYWxsYmFja3MiOiBudWxsLCAiZXJyYmFja3MiOiBudWxsLCAiY2hhaW4iOiBudWxsLCAiY2hvcmQiOiBudWxsfV0="
)

func (ser *JSONSerializer) encodeV2(t *Task) (s string, err error) {
	if t.Args == nil && t.Kwargs == nil {
		return jsonV2noparams, nil
	}

	buf := ser.pool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		ser.pool.Put(buf)
	}()

	buf.WriteRune('[')
	{
		js := json.NewEncoder(buf)
		if t.Args == nil {
			buf.WriteString("[]")
		} else if err = js.Encode(t.Args); err != nil {
			return "", fmt.Errorf("args json encode: %w", err)
		}

		buf.WriteRune(',')

		if t.Kwargs == nil {
			buf.WriteString("{}")
		} else if err = js.Encode(t.Kwargs); err != nil {
			return "", fmt.Errorf("kwargs json encode: %w", err)
		}

		buf.WriteRune(',')

		buf.WriteString(jsonV2opts)
	}
	buf.WriteRune(']')

	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

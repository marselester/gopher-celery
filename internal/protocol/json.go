package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

type messageV1 struct {
	Body string `json:"body"`
}
type messageV1Body struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Expires time.Time              `json:"expires"`
}

type messageV2 struct {
	Body   string          `json:"body"`
	Header messageV2Header `json:"headers"`
}
type messageV2Header struct {
	ID      string    `json:"id"`
	Task    string    `json:"task"`
	Expires time.Time `json:"expires"`
}

// JSONSerializer encodes/decodes task messages in json format.
type JSONSerializer struct{}

// Encode encodes the message into the given byte slice.
func (s *JSONSerializer) Encode(m *Message, raw []byte) error {
	return nil
}

// Decode decodes the raw message.
// Firstly it attemts to decode the message using protocol v2.
// If that didn't work, it falls back to protocol v1.
func (s *JSONSerializer) Decode(raw []byte) (*Message, error) {
	m, errV2 := s.decodeV2(raw)
	if errV2 == nil {
		return m, nil
	}

	m, errV1 := s.decodeV1(raw)
	if errV1 == nil {
		return m, nil
	}

	return nil, fmt.Errorf("unsupported protocol: v2 %v, v1 %v", errV2, errV1)
}

// decodeV1 decodes the raw message using protocol v1.
func (s *JSONSerializer) decodeV1(raw []byte) (*Message, error) {
	var m1 messageV1
	if err := json.Unmarshal(raw, &m1); err != nil {
		return nil, fmt.Errorf("json decode failed: %w", err)
	}

	b, err := base64.StdEncoding.DecodeString(m1.Body)
	if err != nil {
		return nil, fmt.Errorf("base64 body decode failed: %w", err)
	}

	var body messageV1Body
	if err := json.Unmarshal(b, &body); err != nil {
		return nil, fmt.Errorf("json body decode failed: %w", err)
	}

	if body.Task == "" {
		return nil, fmt.Errorf("missing task in body")
	}

	m := Message{
		ID:      body.ID,
		Task:    body.Task,
		Args:    body.Args,
		Kwargs:  body.Kwargs,
		Expires: body.Expires,
	}
	return &m, nil
}

// decodeV2 decodes the raw message using protocol v2.
// It doesn't attempt to base64 decode the message body
// if task is blank which could indicate that it is not protocol v2 at all.
func (s *JSONSerializer) decodeV2(raw []byte) (*Message, error) {
	var m2 messageV2
	if err := json.Unmarshal(raw, &m2); err != nil {
		return nil, fmt.Errorf("json decode failed: %w", err)
	}

	if m2.Header.Task == "" {
		return nil, fmt.Errorf("missing task in header")
	}

	b, err := base64.StdEncoding.DecodeString(m2.Body)
	if err != nil {
		return nil, fmt.Errorf("base64 body decode failed: %w", err)
	}

	var a [3]interface{}
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, fmt.Errorf("json body decode failed: %w", err)
	}
	args, ok := a[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected args: %v", a[0])
	}
	kwargs, ok := a[1].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected kwargs: %v", a[1])
	}

	m := Message{
		ID:      m2.Header.ID,
		Task:    m2.Header.Task,
		Args:    args,
		Kwargs:  kwargs,
		Expires: m2.Header.Expires,
	}
	return &m, nil
}

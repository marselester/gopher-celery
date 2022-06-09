package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// JSONSerializer encodes/decodes a task messages in JSON format.
type JSONSerializer struct{}

// Decode parses the JSON-encoded message body s
// depending on Celery protocol version (v1 or v2).
// The task t is updated with the decoded params.
func (ser *JSONSerializer) Decode(p int, s string, t *Task) error {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("base64 decode: %w", err)
	}

	switch p {
	case 1:
		var body messageV1Body
		if err := json.Unmarshal(b, &body); err != nil {
			return fmt.Errorf("json decode: %w", err)
		}

		t.ID = body.ID
		t.Name = body.Task
		t.Args = body.Args
		t.Kwargs = body.Kwargs
		t.Expires = body.Expires
	case 2:
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
	}

	return nil
}

// Encode encodes task t using protocol version p and returns the message body s.
func (ser *JSONSerializer) Encode(p int, t *Task) (s string, err error) {
	return "", nil
}

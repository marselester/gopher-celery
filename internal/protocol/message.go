// Package protocol provides means to encode/decode task messages
// as described in https://github.com/celery/celery/blob/master/docs/internals/protocol.rst.
package protocol

import "time"

// Message represents a task message.
type Message struct {
	ID      string
	Task    string
	Args    []interface{}
	Kwargs  map[string]interface{}
	ETA     string
	Expires time.Time
}

// Package protocol provides means to encode/decode task messages
// as described in https://github.com/celery/celery/blob/master/docs/internals/protocol.rst.
package protocol

import "time"

// Message represents a task message that provides essential params to run a task.
type Message struct {
	ID      string
	Task    string
	Args    []interface{}
	Kwargs  map[string]interface{}
	Expires time.Time
}

// IsExpired returns true if the message is expired
// and shouldn't be processed.
func (m *Message) IsExpired() bool {
	return !m.Expires.IsZero() && m.Expires.Before(time.Now())
}

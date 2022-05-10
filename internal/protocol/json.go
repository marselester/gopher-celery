package protocol

// JSONSerializer encodes/decodes task messages in json format.
type JSONSerializer struct{}

// Encode encodes the message into the given byte slice.
func (s *JSONSerializer) Encode(m *Message, raw []byte) error {
	return nil
}

// Decode decodes the raw message.
func (s *JSONSerializer) Decode(raw []byte) (*Message, error) {
	return nil, nil
}

package rabbitmq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestReceive(t *testing.T) {

	q := "rabbitmqq"

	tests := map[string]struct {
		input []string
		want  []byte
		err   error
	}{
		"timeout": {
			input: nil,
			want:  nil,
			err:   nil,
		},
		"one-msg": {
			input: []string{"{}"},
			want:  []byte("{}"),
			err:   nil,
		},
		"oldest-msg": {
			input: []string{"1", "2"},
			want:  []byte("1"),
			err:   nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			br := NewBroker(WithReceiveTimeout(time.Second), WithRawMode(true))
			br.Observe([]string{q})

			t.Cleanup(func() {
				br.channel.QueueDelete(q, false, false, false)
			})

			for _, m := range tc.input {
				if err := br.Send([]byte(m), q); err != nil {
					t.Fatal(err)
				}
			}

			got, err := br.Receive()
			if err != tc.err {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Error(diff, string(got))
			}
		})
	}
}

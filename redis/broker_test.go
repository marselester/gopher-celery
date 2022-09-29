package redis

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestReceive(t *testing.T) {
	q := "redigoq"
	br := NewBroker(WithReceiveTimeout(time.Second))
	br.Observe([]string{q})

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
			t.Cleanup(func() {
				c := br.pool.Get()
				defer c.Close()

				if _, err := c.Do("DEL", q); err != nil {
					t.Fatal(err)
				}
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

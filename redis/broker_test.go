package redis

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMove2back(t *testing.T) {
	tests := map[string]struct {
		input []string
		v     string
		want  []string
	}{
		"start": {
			input: []string{"a", "b", "c", "d", "e", "f"},
			v:     "a",
			want:  []string{"b", "c", "d", "e", "f", "a"},
		},
		"middle": {
			input: []string{"a", "b", "c", "d", "e", "f"},
			v:     "c",
			want:  []string{"a", "b", "d", "e", "f", "c"},
		},
		"end": {
			input: []string{"a", "b", "c", "d", "e", "f"},
			v:     "f",
			want:  []string{"a", "b", "c", "d", "e", "f"},
		},
		"nil": {
			input: nil,
			v:     "f",
			want:  nil,
		},
		"one-item": {
			input: []string{"a"},
			v:     "a",
			want:  []string{"a"},
		},
		"two-items-start": {
			input: []string{"a", "b"},
			v:     "a",
			want:  []string{"b", "a"},
		},
		"two-items-end": {
			input: []string{"a", "b"},
			v:     "b",
			want:  []string{"a", "b"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			move2back(tc.input, tc.v)
			if diff := cmp.Diff(tc.want, tc.input); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func BenchmarkMove2back(b *testing.B) {
	ss := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
	for n := 0; n < b.N; n++ {
		move2back(ss, "a")
	}
}

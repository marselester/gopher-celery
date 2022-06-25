package protocol

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestJSONSerializerEncode(t *testing.T) {
	tests := map[string]struct {
		task    Task
		version int
		body    string
	}{
		"v2_noparams": {
			task: Task{
				ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name: "myproject.apps.myapp.tasks.mytask",
			},
			version: 2,
			body:    `[[], {}, {"callbacks": null, "errbacks": null, "chain": null, "chord": null}]`,
		},
		"v2_args": {
			task: Task{
				ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name: "myproject.apps.myapp.tasks.mytask",
				Args: []interface{}{"fizz"},
			},
			version: 2,
			body:    `[["fizz"],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]`,
		},
		"v2_argskwargs": {
			task: Task{
				ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name:   "myproject.apps.myapp.tasks.mytask",
				Args:   []interface{}{"fizz"},
				Kwargs: map[string]interface{}{"b": "bazz"},
			},
			version: 2,
			body:    `[["fizz"],{"b":"bazz"},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]`,
		},
		"v1_noparams": {
			task: Task{
				ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name: "myproject.apps.myapp.tasks.mytask",
			},
			version: 1,
			body:    `{"id":"0ad73c66-f4c9-4600-bd20-96746e720eed","task":"myproject.apps.myapp.tasks.mytask","args":[],"kwargs":{},"expires":null,"retries":0,"eta":"2009-11-17T12:30:56Z","utc":true}`,
		},
		"v1_args": {
			task: Task{
				ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name: "myproject.apps.myapp.tasks.mytask",
				Args: []interface{}{"fizz"},
			},
			version: 1,
			body:    `{"id":"0ad73c66-f4c9-4600-bd20-96746e720eed","task":"myproject.apps.myapp.tasks.mytask","args":["fizz"],"kwargs":{},"expires":null,"retries":0,"eta":"2009-11-17T12:30:56Z","utc":true}`,
		},
		"v1_argskwargs": {
			task: Task{
				ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name:   "myproject.apps.myapp.tasks.mytask",
				Args:   []interface{}{"fizz"},
				Kwargs: map[string]interface{}{"b": "bazz"},
			},
			version: 1,
			body:    `{"id":"0ad73c66-f4c9-4600-bd20-96746e720eed","task":"myproject.apps.myapp.tasks.mytask","args":["fizz"],"kwargs":{"b":"bazz"},"expires":null,"retries":0,"eta":"2009-11-17T12:30:56Z","utc":true}`,
		},
	}

	s := NewJSONSerializer()
	s.now = func() time.Time {
		// 2009-11-17T12:30:56Z
		return time.Date(2009, 11, 17, 12, 30, 56, 0, time.UTC)
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			b64, err := s.Encode(tc.version, &tc.task)
			if err != nil {
				t.Fatal(err)
			}
			gotb64, err := base64.StdEncoding.DecodeString(b64)
			if err != nil {
				t.Fatal(err)
			}

			got := string(
				bytes.Replace(gotb64, []byte("\n"), nil, -1),
			)
			if diff := cmp.Diff(tc.body, got); diff != "" {
				t.Error(diff, got)
			}
		})
	}
}

func BenchmarkJSONSerializerEncode_v2NoParams(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(2, &task)
	}
}

func BenchmarkJSONSerializerEncode_v2Args(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(2, &task)
	}
}

func BenchmarkJSONSerializerEncode_v2Kwargs(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name:   "myproject.apps.myapp.tasks.mytask",
		Kwargs: map[string]interface{}{"b": "bazz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(2, &task)
	}
}

func BenchmarkJSONSerializerEncode_v2ArgsKwargs(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name:   "myproject.apps.myapp.tasks.mytask",
		Args:   []interface{}{"fizz"},
		Kwargs: map[string]interface{}{"b": "bazz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(2, &task)
	}
}

func BenchmarkJSONSerializerEncode_v1NoParams(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(1, &task)
	}
}

func BenchmarkJSONSerializerEncode_v1Args(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(1, &task)
	}
}

func BenchmarkJSONSerializerEncode_v1Kwargs(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name:   "myproject.apps.myapp.tasks.mytask",
		Kwargs: map[string]interface{}{"b": "bazz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(1, &task)
	}
}

func BenchmarkJSONSerializerEncode_v1ArgsKwargs(b *testing.B) {
	s := NewJSONSerializer()
	task := Task{
		ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name:   "myproject.apps.myapp.tasks.mytask",
		Args:   []interface{}{"fizz"},
		Kwargs: map[string]interface{}{"b": "bazz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s.Encode(1, &task)
	}
}

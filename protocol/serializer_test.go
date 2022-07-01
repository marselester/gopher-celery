package protocol

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/google/go-cmp/cmp"
)

func TestSerializerRegistryDecode(t *testing.T) {
	tests := map[string]Task{
		"v2_argskwargs.json": {
			ID:      "0ad73c66-f4c9-4600-bd20-96746e720eed",
			Name:    "myproject.apps.myapp.tasks.mytask",
			Args:    []interface{}{"fizz"},
			Kwargs:  map[string]interface{}{"b": "bazz"},
			Expires: time.Time{},
		},
		"v2_noparams.json": {
			ID:      "3802f860-8d3c-4dad-b18c-597fb2ac728b",
			Name:    "myproject.apps.myapp.tasks.mytask",
			Args:    []interface{}{},
			Kwargs:  map[string]interface{}{},
			Expires: time.Time{},
		},
		"v1_noparams.json": {
			ID:      "0d09a6dd-99fc-436a-a41a-0dcaa4875459",
			Name:    "myproject.apps.myapp.tasks.mytask",
			Args:    []interface{}{},
			Kwargs:  map[string]interface{}{},
			Expires: time.Time{},
		},
	}

	r := NewSerializerRegistry()
	for testfile, want := range tests {
		t.Run(testfile, func(t *testing.T) {
			filename := filepath.Join("testdata", testfile)
			content, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}

			got, err := r.Decode(content)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(&want, got); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestSerializerRegistryEncode(t *testing.T) {
	tests := map[string]struct {
		task    Task
		queue   string
		format  string
		version int

		msg string
	}{
		"v1_argskwargs": {
			task: Task{
				ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name:   "myproject.apps.myapp.tasks.mytask",
				Args:   []interface{}{"fizz"},
				Kwargs: map[string]interface{}{"b": "bazz"},
			},
			queue:   "important",
			format:  "application/json",
			version: 1,
			msg: `
{
    "body": "",
    "content-encoding": "utf-8",
    "content-type": "application/json",
    "headers": {},
    "properties": {
        "delivery_info": {
            "exchange": "important",
            "routing_key": "important"
        },
        "correlation_id": "0ad73c66-f4c9-4600-bd20-96746e720eed",
        "reply_to": "967ff33a-e83c-4225-99ea-1d945c62526a",
        "body_encoding": "base64",
        "delivery_tag": "967ff33a-e83c-4225-99ea-1d945c62526a",
        "delivery_mode": 2,
        "priority": 0
    }
}`,
		},
		"v2_argskwargs": {
			task: Task{
				ID:     "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name:   "myproject.apps.myapp.tasks.mytask",
				Args:   []interface{}{"fizz"},
				Kwargs: map[string]interface{}{"b": "bazz"},
			},
			queue:   "important",
			format:  "application/json",
			version: 2,
			msg: `
{
    "body": "",
    "content-encoding": "utf-8",
    "content-type": "application/json",
    "headers": {
        "lang": "go",
        "id": "0ad73c66-f4c9-4600-bd20-96746e720eed",
        "root_id": "0ad73c66-f4c9-4600-bd20-96746e720eed",
        "task": "myproject.apps.myapp.tasks.mytask",
        "origin": "123@home",
        "expires": null,
        "retries": 0
    },
    "properties": {
        "delivery_info": {
            "exchange": "important",
            "routing_key": "important"
        },
        "correlation_id": "0ad73c66-f4c9-4600-bd20-96746e720eed",
        "reply_to": "967ff33a-e83c-4225-99ea-1d945c62526a",
        "body_encoding": "base64",
        "delivery_tag": "967ff33a-e83c-4225-99ea-1d945c62526a",
        "delivery_mode": 2,
        "priority": 0
    }
}`,
		},
		"v2_noparams": {
			task: Task{
				ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
				Name: "myproject.apps.myapp.tasks.mytask",
			},
			queue:   "important",
			format:  "application/json",
			version: 2,
			msg: `
{
    "body": "",
    "content-encoding": "utf-8",
    "content-type": "application/json",
    "headers": {
        "lang": "go",
        "id": "0ad73c66-f4c9-4600-bd20-96746e720eed",
        "root_id": "0ad73c66-f4c9-4600-bd20-96746e720eed",
        "task": "myproject.apps.myapp.tasks.mytask",
        "origin": "123@home",
        "expires": null,
        "retries": 0
    },
    "properties": {
        "delivery_info": {
            "exchange": "important",
            "routing_key": "important"
        },
        "correlation_id": "0ad73c66-f4c9-4600-bd20-96746e720eed",
        "reply_to": "967ff33a-e83c-4225-99ea-1d945c62526a",
        "body_encoding": "base64",
        "delivery_tag": "967ff33a-e83c-4225-99ea-1d945c62526a",
        "delivery_mode": 2,
        "priority": 0
    }
}`,
		},
	}

	r := NewSerializerRegistry()
	// Suppress body encoding to simplify testing.
	r.serializers["application/json"] = &mockSerializer{}
	r.uuid4 = func() string {
		return "967ff33a-e83c-4225-99ea-1d945c62526a"
	}
	r.origin = "123@home"

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			b, err := r.Encode(tc.queue, tc.format, tc.version, &tc.task)
			if err != nil {
				t.Fatal(err)
			}

			got := string(
				bytes.ReplaceAll(b, []byte("\n"), nil),
			)
			want := strings.Map(func(r rune) rune {
				if !unicode.IsSpace(r) {
					return r
				}
				return -1
			}, tc.msg)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Error(diff, want, got)
			}
		})
	}
}

func BenchmarkSerializerRegistryEncode_v1Args(b *testing.B) {
	r := NewSerializerRegistry()
	r.serializers["application/json"] = &mockSerializer{}

	task := Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		r.Encode("important", "application/json", 1, &task)
	}
}

func BenchmarkSerializerRegistryEncode_v2Args(b *testing.B) {
	r := NewSerializerRegistry()
	r.serializers["application/json"] = &mockSerializer{}

	task := Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		r.Encode("important", "application/json", 2, &task)
	}
}

type mockSerializer struct {
	DecodeF func(p int, s string, t *Task) error
	EncodeF func(p int, t *Task) (s string, err error)
}

func (ser *mockSerializer) Decode(p int, s string, t *Task) error {
	if ser.DecodeF == nil {
		return nil
	}
	return ser.DecodeF(p, s, t)
}

func (ser *mockSerializer) Encode(p int, t *Task) (string, error) {
	if ser.EncodeF == nil {
		return "", nil
	}
	return ser.EncodeF(p, t)
}

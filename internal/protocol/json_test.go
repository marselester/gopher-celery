package protocol

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestJSONSerializerDecode(t *testing.T) {
	tests := map[string]Message{
		"v2_argskwargs.json": {
			ID:      "0ad73c66-f4c9-4600-bd20-96746e720eed",
			Task:    "myproject.apps.myapp.tasks.mytask",
			Args:    []interface{}{"fizz"},
			Kwargs:  map[string]interface{}{"b": "bazz"},
			Expires: time.Time{},
		},
		"v2_noparams.json": {
			ID:      "3802f860-8d3c-4dad-b18c-597fb2ac728b",
			Task:    "myproject.apps.myapp.tasks.mytask",
			Args:    []interface{}{},
			Kwargs:  map[string]interface{}{},
			Expires: time.Time{},
		},
		"v1_noparams.json": {
			ID:      "0d09a6dd-99fc-436a-a41a-0dcaa4875459",
			Task:    "myproject.apps.myapp.tasks.mytask",
			Args:    []interface{}{},
			Kwargs:  map[string]interface{}{},
			Expires: time.Time{},
		},
	}

	s := JSONSerializer{}
	for testfile, want := range tests {
		t.Run(testfile, func(t *testing.T) {
			filename := filepath.Join("testdata", testfile)
			content, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}

			got, err := s.Decode(content)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(&want, got); diff != "" {
				t.Error(diff)
			}
		})
	}
}

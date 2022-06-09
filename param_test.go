package celery

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func ExampleTaskParam() {
	var (
		args   = []interface{}{2}
		kwargs = map[string]interface{}{"b": 3}
	)
	p := NewTaskParam(args, kwargs)
	p.NameArgs("a", "b")

	fmt.Println(p.Get("a"))
	fmt.Println(p.Get("b"))
	fmt.Println(p.Get("c"))
	// Output:
	// 2 true
	// 3 true
	// <nil> false
}

func TestTaskParamGet(t *testing.T) {
	tests := map[string]struct {
		p        *TaskParam
		argNames []string
		pname    string
		want     interface{}
		exists   bool
	}{
		"no-params": {
			p:      NewTaskParam(nil, nil),
			pname:  "a",
			want:   nil,
			exists: false,
		},
		"found-kwarg": {
			p: NewTaskParam(
				nil,
				map[string]interface{}{"b": 3},
			),
			pname:  "b",
			want:   3,
			exists: true,
		},
		"unnamed-arg": {
			p: NewTaskParam(
				[]interface{}{2},
				nil,
			),
			pname:  "a",
			want:   nil,
			exists: false,
		},
		"found-named-arg": {
			p: NewTaskParam(
				[]interface{}{2},
				nil,
			),
			pname:    "a",
			argNames: []string{"a"},
			want:     2,
			exists:   true,
		},
		"args-lt-names": {
			p: NewTaskParam(
				[]interface{}{2},
				nil,
			),
			argNames: []string{"a", "b", "c"},
			pname:    "c",
			want:     nil,
			exists:   false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.p.NameArgs(tc.argNames...)

			got, ok := tc.p.Get(tc.pname)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf(diff)
			}
			if tc.exists != ok {
				t.Errorf("expected %t got %t", tc.exists, ok)
			}
		})
	}
}

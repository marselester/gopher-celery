package celery

import (
	"strings"
	"testing"

	"github.com/marselester/gopher-celery/internal/protocol"
)

func TestExecuteTaskPanic(t *testing.T) {
	a := NewApp()
	a.Register(
		"myproject.apps.myapp.tasks.mytask",
		"important",
		func(args []interface{}, kwargs map[string]interface{}) {
			_ = args[100]
		},
	)

	m := protocol.Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
		Kwargs: map[string]interface{}{
			"b": "bazz",
		},
	}

	want := "unexpected task error"
	err := a.executeTask(&m)
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("expected %q got %q", want, err)
	}
}

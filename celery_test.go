package celery

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"

	"github.com/marselester/gopher-celery/protocol"
)

func TestExecuteTaskPanic(t *testing.T) {
	a := NewApp()
	a.Register(
		"myproject.apps.myapp.tasks.mytask",
		"important",
		func(ctx context.Context, p *TaskParam) error {
			_ = p.Args()[100]
			return nil
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
	err := a.executeTask(context.Background(), &m)
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("expected %q got %q", want, err)
	}
}

func TestProduceAndConsume(t *testing.T) {
	app := NewApp(WithLogger(log.NewJSONLogger(os.Stderr)))
	err := app.Delay(
		"myproject.apps.myapp.tasks.mytask",
		"important",
		2,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}

	// The test finishes either when ctx times out or the task finishes.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	var sum int
	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		"important",
		func(ctx context.Context, p *TaskParam) error {
			defer cancel()

			p.NameArgs("a", "b")
			sum = p.MustInt("a") + p.MustInt("b")
			return nil
		},
	)
	if err := app.Run(ctx); err != nil {
		t.Error(err)
	}

	var (
		want = 5
	)
	if want != sum {
		t.Errorf("expected sum %d got %d", want, sum)
	}
}

func TestProduceAndConsume_100times(t *testing.T) {
	app := NewApp(WithLogger(log.NewJSONLogger(os.Stderr)))
	for i := 0; i < 100; i++ {
		err := app.Delay(
			"myproject.apps.myapp.tasks.mytask",
			"important",
			2,
			3,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	// The test finishes either when ctx times out or all the tasks finish.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	var sum int32
	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		"important",
		func(ctx context.Context, p *TaskParam) error {
			p.NameArgs("a", "b")
			atomic.AddInt32(
				&sum,
				int32(p.MustInt("a")+p.MustInt("b")),
			)
			return nil
		},
	)
	if err := app.Run(ctx); err != nil {
		t.Error(err)
	}

	var want int32 = 500
	if want != sum {
		t.Errorf("expected sum %d got %d", want, sum)
	}
}

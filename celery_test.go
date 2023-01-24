package celery

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"golang.org/x/sync/errgroup"

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

func TestExecuteTaskMiddlewares(t *testing.T) {
	// The middlewares are called in the order they were defined, e.g., A -> B -> task.
	tests := map[string]struct {
		middlewares []Middleware
		want        string
	}{
		"A-B-task": {
			middlewares: []Middleware{
				func(next TaskF) TaskF {
					return func(ctx context.Context, p *TaskParam) error {
						err := next(ctx, p)
						return fmt.Errorf("A -> %w", err)
					}
				},
				func(next TaskF) TaskF {
					return func(ctx context.Context, p *TaskParam) error {
						err := next(ctx, p)
						return fmt.Errorf("B -> %w", err)
					}
				},
			},
			want: "A -> B -> task",
		},
		"A-task": {
			middlewares: []Middleware{
				func(next TaskF) TaskF {
					return func(ctx context.Context, p *TaskParam) error {
						err := next(ctx, p)
						return fmt.Errorf("A -> %w", err)
					}
				},
			},
			want: "A -> task",
		},
		"empty chain": {
			middlewares: []Middleware{},
			want:        "task",
		},
		"nil chain": {
			middlewares: nil,
			want:        "task",
		},
		"nil middleware panic": {
			middlewares: []Middleware{nil},
			want:        "unexpected task error",
		},
	}

	ctx := context.Background()
	m := protocol.Task{
		ID:   "0ad73c66-f4c9-4600-bd20-96746e720eed",
		Name: "myproject.apps.myapp.tasks.mytask",
		Args: []interface{}{"fizz"},
		Kwargs: map[string]interface{}{
			"b": "bazz",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := NewApp(
				WithMiddlewares(tc.middlewares...),
			)
			a.Register(
				"myproject.apps.myapp.tasks.mytask",
				"important",
				func(ctx context.Context, p *TaskParam) error {
					return fmt.Errorf("task")
				},
			)

			err := a.executeTask(ctx, &m)
			if !strings.HasPrefix(err.Error(), tc.want) {
				t.Errorf("expected %q got %q", tc.want, err)
			}
		})
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

	want := 5
	if want != sum {
		t.Errorf("expected sum %d got %d", want, sum)
	}
}

func TestProduceAndConsumeAfterRun(t *testing.T) {
	app := NewApp(WithLogger(log.NewJSONLogger(os.Stderr)))
	delayImportantTask(t, app, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	t.Cleanup(cancel)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := app.Run(ctx); err != nil {
			t.Error(err)
			return err
		}
		return nil
	})
	time.Sleep(time.Millisecond * 50)
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

	g.Wait()

	want := 5
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

func TestUnregister(t *testing.T) {
	app := NewApp(WithLogger(log.NewJSONLogger(os.Stderr)))
	delayImportantTask(t, app, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	var cnt int32
	app.Register(
		"myproject.apps.myapp.tasks.mytask",
		"important",
		func(ctx context.Context, p *TaskParam) error {
			p.NameArgs("a", "b")
			atomic.StoreInt32(
				&cnt,
				int32(p.MustInt("a")+p.MustInt("b")),
			)
			return nil
		},
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := app.Run(ctx); err != nil {
			t.Error(err)
			return err
		}
		return nil
	})

	time.AfterFunc(50*time.Millisecond, func() {
		if 5 != cnt {
			t.Errorf("expected cnt %d got %d", 5, cnt)
		}

		app.Unregister("myproject.apps.myapp.tasks.mytask")

		delayImportantTask(t, app, 5, 5)

		time.Sleep(50 * time.Millisecond)

		if 5 != cnt { // cnt should not be changed.
			t.Errorf("expected cnt %d got %d", 5, cnt)
		}

		actual := app.queueCount.Load()
		if actual != 0 {
			t.Errorf("expected queue count %d got %d", 0, actual)
		}
	})

	g.Wait()
}

func delayImportantTask(t *testing.T, app *App, params ...interface{}) {
	err := app.Delay(
		"myproject.apps.myapp.tasks.mytask",
		"important",
		params...,
	)
	if err != nil {
		t.Fatal(err)
	}
}

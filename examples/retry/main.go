// Program retry shows how a task's business logic can be retried
// without rescheduling the task itself.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/dryarullin/backoff"
	celery "github.com/dryarullin/gopher-celery"
	"github.com/go-kit/log"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	r := requestor{
		maxRetries: 2,
		logger:     logger,
	}

	app := celery.NewApp(
		celery.WithLogger(logger),
	)
	app.Register(
		"myproject.mytask",
		"important",
		r.request,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err := app.Delay("myproject.mytask", "important", "fizz", "bazz")
	logger.Log("msg", "task was sent", "err", err)

	logger.Log("msg", "waiting for tasks...")
	err = app.Run(ctx)
	logger.Log("msg", "program stopped", "err", err)
}

type requestor struct {
	maxRetries int
	logger     log.Logger
}

func (rq *requestor) request(ctx context.Context, p *celery.TaskParam) error {
	// Make 3 delivery attempts with exponential backoff (1st attempt and 2 retries).
	// The 5 seconds multiplier increases the wait intervals.
	// Max waiting time between attempts is 15 seconds.
	r := backoff.NewDecorrJitter(
		backoff.WithMaxRetries(rq.maxRetries),
		backoff.WithMultiplier(5*time.Second),
		backoff.WithMaxWait(15*time.Second),
	)

	return backoff.Run(ctx, r, func(attempt int) (err error) {
		if err = rq.work(); err != nil {
			rq.logger.Log("msg", "request failed", "attempt", attempt, "err", err)
		} else {
			rq.logger.Log("msg", "request succeeded", "attempt", attempt)
		}
		return err
	})
}

// work is a dummy function that imitates work.
func (rq *requestor) work() error {
	return fmt.Errorf("uh oh")
}

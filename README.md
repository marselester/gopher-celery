# Gopher Celery 🥬

[![Documentation](https://godoc.org/github.com/dryarullin/gopher-celery?status.svg)](https://pkg.go.dev/github.com/dryarullin/gopher-celery)
[![Go Report Card](https://goreportcard.com/badge/github.com/dryarullin/gopher-celery)](https://goreportcard.com/report/github.com/dryarullin/gopher-celery)

The objective of this project is to provide
the very basic mechanism to efficiently produce and consume Celery tasks on Go side.
Therefore there are no plans to support all the rich features the Python version provides,
such as tasks chains, etc.
Even task result backend has no practical value in the context of Gopher Celery,
so it wasn't taken into account.
Note, Celery has [no result backend](https://docs.celeryq.dev/en/stable/userguide/tasks.html?#result-backends)
enabled by default (it incurs overhead).

Typically one would want to use Gopher Celery when certain tasks on Python side
take too long to complete or there is a big volume of tasks requiring lots of Python workers
(expensive infrastructure).

This project offers a little bit more convenient API of https://github.com/gocelery/gocelery
including support for Celery protocol v2.

## Usage

The Celery app can be used as either a producer or consumer (worker).
To send tasks to a queue for a worker to consume, use `Delay` method.
In order to process a task you should register it using `Register` method.

```python
def mytask(a, b):
    print(a + b)
```

For example, whenever a task `mytask` is popped from `important` queue,
the Go function is executed with args and kwargs obtained from the task message.
By default Redis broker (localhost) is used with json task message serialization.

```go
app := celery.NewApp()
app.Register(
	"myproject.apps.myapp.tasks.mytask",
	"important",
	func(ctx context.Context, p *celery.TaskParam) error {
		p.NameArgs("a", "b")
		// Methods prefixed with Must panic if they can't find an argument name
		// or can't cast it to the corresponding type.
		// The panic doesn't affect other tasks execution; it's logged.
		fmt.Println(p.MustInt("a") + p.MustInt("b"))
		// Non-nil errors are logged.
		return nil
	},
)
if err := app.Run(context.Background()); err != nil {
	log.Printf("celery worker error: %v", err)
}
```

Here is an example of sending `mytask` task to `important` queue with `a=2`, `b=3` arguments.
If a task is processed on Python side,
you don't need to register the task or run the app.

```go
app := celery.NewApp()
err := app.Delay(
	"myproject.apps.myapp.tasks.mytask",
	"important",
	2,
	3,
)
if err != nil {
	log.Printf("failed to send mytask: %v", err)
}
```

More examples can be found in [the examples](examples) dir.
Note, you'll need a Redis server to run them.

```sh
$ redis-server
$ cd ./examples/
```

<details>

<summary>Sending tasks from Go and receiving them on Python side.</summary>

```sh
$ go run ./producer/
{"err":null,"msg":"task was sent using protocol v2"}
{"err":null,"msg":"task was sent using protocol v1"}
$ celery --app myproject worker --queues important --loglevel=debug --without-heartbeat --without-mingle
...
[... WARNING/ForkPoolWorker-1] received a=fizz b=bazz
[... WARNING/ForkPoolWorker-8] received a=fizz b=bazz
```

</details>

<details>

<summary>Sending tasks from Python and receiving them on Go side.</summary>

```sh
$ python producer.py
$ go run ./consumer/
{"msg":"waiting for tasks..."}
received a=fizz b=bazz
received a=fizz b=bazz
```

</details>

<details>

Most likely your Redis server won't be running on localhost when the service is deployed,
so you would need to pass a connection pool to the broker.

<summary>Redis connection pool.</summary>

```sh
$ go run ./producer/
{"err":null,"msg":"task was sent using protocol v2"}
{"err":null,"msg":"task was sent using protocol v1"}
$ go run ./redis/
```

</details>

<details>

<summary>Prometheus task metrics.</summary>

```sh
$ go run ./producer/
$ go run ./metrics/
$ curl http://0.0.0.0:8080/metrics
# HELP task_duration_seconds How long it took in seconds to process a task.
# TYPE task_duration_seconds histogram
task_duration_seconds_bucket{task="myproject.mytask",le="0.016"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="0.032"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="0.064"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="0.128"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="0.256"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="0.512"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="1.024"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="2.048"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="4.096"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="8.192"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="16.384"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="32.768"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="60"} 2
task_duration_seconds_bucket{task="myproject.mytask",le="+Inf"} 2
task_duration_seconds_sum{task="myproject.mytask"} 7.2802e-05
task_duration_seconds_count{task="myproject.mytask"} 2
# HELP tasks_total How many Celery tasks processed, partitioned by task name and error.
# TYPE tasks_total counter
tasks_total{error="false",task="myproject.mytask"} 2
```

</details>

<details>

Although there is no built-in support for task retries (publishing a task back to Redis),
you can still retry the operation within the same goroutine.

<summary>Task retries.</summary>

```sh
$ go run ./retry/
...
{"attempt":1,"err":"uh oh","msg":"request failed","ts":"2022-08-07T23:42:23.401191Z"}
{"attempt":2,"err":"uh oh","msg":"request failed","ts":"2022-08-07T23:42:28.337204Z"}
{"attempt":3,"err":"uh oh","msg":"request failed","ts":"2022-08-07T23:42:37.279873Z"}
```

</details>

## Testing

Tests require a Redis server running locally.

```sh
$ go test -v -count=1 ./...
```

Benchmarks help to spot performance changes as the project evolves
and also compare performance of serializers.
For example, based on the results below the protocol v2 is faster than v1 when encoding args:

- 350 nanoseconds mean time, 3 allocations (248 bytes) with 0% variation across the samples
- 1.21 microseconds mean time, 4 allocations (672 bytes) with 0% variation across the samples

It is recommended to run benchmarks multiple times and check
how stable they are using [Benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) tool.

```sh
$ go test -bench=. -benchmem -count=10 ./internal/... | tee bench-new.txt
```

<details>

<summary>

```sh
$ benchstat bench-old.txt
```

</summary>

```
name                                  time/op
JSONSerializerEncode_v2NoParams-12    2.97ns ± 1%
JSONSerializerEncode_v2Args-12         350ns ± 0%
JSONSerializerEncode_v2Kwargs-12       582ns ± 0%
JSONSerializerEncode_v2ArgsKwargs-12   788ns ± 1%
JSONSerializerEncode_v1NoParams-12    1.12µs ± 1%
JSONSerializerEncode_v1Args-12        1.21µs ± 0%
JSONSerializerEncode_v1Kwargs-12      1.68µs ± 0%
JSONSerializerEncode_v1ArgsKwargs-12  1.77µs ± 0%

name                                  alloc/op
JSONSerializerEncode_v2NoParams-12     0.00B
JSONSerializerEncode_v2Args-12          248B ± 0%
JSONSerializerEncode_v2Kwargs-12        472B ± 0%
JSONSerializerEncode_v2ArgsKwargs-12    528B ± 0%
JSONSerializerEncode_v1NoParams-12      672B ± 0%
JSONSerializerEncode_v1Args-12          672B ± 0%
JSONSerializerEncode_v1Kwargs-12      1.00kB ± 0%
JSONSerializerEncode_v1ArgsKwargs-12  1.00kB ± 0%

name                                  allocs/op
JSONSerializerEncode_v2NoParams-12      0.00
JSONSerializerEncode_v2Args-12          3.00 ± 0%
JSONSerializerEncode_v2Kwargs-12        7.00 ± 0%
JSONSerializerEncode_v2ArgsKwargs-12    8.00 ± 0%
JSONSerializerEncode_v1NoParams-12      4.00 ± 0%
JSONSerializerEncode_v1Args-12          4.00 ± 0%
JSONSerializerEncode_v1Kwargs-12        10.0 ± 0%
JSONSerializerEncode_v1ArgsKwargs-12    10.0 ± 0%
```

</details>

The old and new stats are compared as follows.

```sh
$ benchstat bench-old.txt bench-new.txt
```

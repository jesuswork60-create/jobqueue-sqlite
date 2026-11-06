# jobqueue-sqlite

A persistent background job queue that keeps everything in one SQLite file. There is no broker and no server to run: your program writes jobs into the database, and one or more worker processes on the same machine pick them up. It needs Python 3.11 or newer and has no dependencies outside the standard library.

*It covers the parts a small queue tends to need sooner or later:*

- Jobs have a queue name, a priority, a run time and a JSON payload.
- Workers hold a lease on a job. If a worker dies, the lease expires and the job goes back in the queue.
- Failed jobs retry with fixed, exponential or jittered backoff, then land in a dead letter list you can inspect and requeue.
- Unique keys stop the same piece of work being queued twice while it is still active.
- Recurring jobs run on an interval or a cron expression, with a choice of what happens to fires that were missed.
- A worker can run handlers one at a time or in a thread pool, and it stops gracefully on SIGINT or SIGTERM.
- `jobqueue status` shows queue depth and per-handler timings, and the same figures are available as a Python object.

## Install

From a checkout:

```
pip install .
```

That gives you the `jobqueue` command. You can also skip installing and run it with `PYTHONPATH=src python3 -m jobqueue`.

## Using it from Python

Register handlers, enqueue jobs and run a worker. Normally the worker is a separate long-running process, but it all fits in one script for a first try:

```python
from jobqueue import HandlerRegistry, JobQueue, Worker

registry = HandlerRegistry()


@registry.handler("mail.send", max_attempts=3)
def send_mail(payload):
    return f"sent to {payload['to']}"


with JobQueue("app.db") as queue:
    job = registry.enqueue(queue, "mail.send", {"to": "someone@example.com"}).job
    Worker(queue, registry).run(until_idle=True)
    print(queue.get(job.id).state, queue.get(job.id).result)
```

This prints `succeeded sent to someone@example.com`.

A handler takes `(payload)` or `(payload, ctx)`. Whatever it returns is stored as the job's result, so it has to fit in JSON. Raising an exception means a retry. Raise `Retry(delay=30)` to choose the wait yourself, or `Abandon("reason")` to send the job straight to the dead letters. In a long handler on a single-threaded worker, call `ctx.heartbeat()` now and then to keep the lease.

The queue can also be driven by hand, without a `Worker`, using `lease`, `heartbeat`, `complete`, `fail` and `release`:

```python
from jobqueue import JobQueue

with JobQueue("manual.db") as queue:
    queue.enqueue("report.build", {"month": "2026-01"}, unique_key="report-2026-01")
    job = queue.lease("my-worker", lease_seconds=120)[0]
    queue.complete(job.id, job.lease_token, {"pages": 12})
```

The token proves that you still hold the lease. If it has run out, or someone cancelled the job, those calls raise `LeaseLost` and change nothing.

Recurring jobs are added through `queue.schedules`, and any running worker turns due schedules into jobs:

```python
queue.schedules.add("nightly-report", "report.build", cron="0 3 * * *")
queue.schedules.add("tidy", "files.tidy", every="15m", misfire="catch_up")
```

## Using the command line

The database comes from `--db`, then the `JOBQUEUE_DB` environment variable, then `jobqueue.db` in the current directory. A few demo handlers are built in, so this works straight away:

```
$ jobqueue --db demo.db enqueue demo.echo --payload '{"hello": "world"}'
enqueued job 1 on queue default
$ jobqueue --db demo.db enqueue demo.flaky --backoff fixed:0
enqueued job 2 on queue default
$ jobqueue --db demo.db enqueue demo.fail --max-attempts 1
enqueued job 3 on queue default
$ jobqueue --db demo.db work --burst
job 1 demo.echo succeeded in 0ms
job 2 demo.flaky retried in 1ms
job 3 demo.fail dead in 0ms
job 2 demo.flaky succeeded in 0ms
worker finished: 2 succeeded, 1 retried, 1 dead, 0 lost, 0 released
$ jobqueue --db demo.db dead
ID  QUEUE    HANDLER    TRIES  DIED AT               LAST ERROR
3   default  demo.fail  1/1    2026-11-05T05:44:05Z  RuntimeError: this handler always fails (payl...
$ jobqueue --db demo.db retry --all
requeued 1 dead job(s)
```

`--burst` makes the worker exit once nothing is ready; without it, the worker keeps polling until it gets a signal. To run your own handlers, point it at a registry with `--handlers module:attribute`. The module is imported with the working directory on the path. There is a small one in `examples/invoices.py`, with instructions at the top of the file.

The other commands are `list`, `show`, `status`, `cancel`, `purge` and `schedule add|list|remove|pause|resume`. Most of the read commands take `--json`, and `jobqueue <command> --help` lists the options.

## How a few things behave

**Attempts.** An attempt is counted when a job is leased, so a worker that dies mid-job still uses one up. A job dies when a failure happens with `max_attempts` used. `release` gives the attempt back; a worker uses it when a handler is interrupted by `KeyboardInterrupt` or `SystemExit`.

**Backoff.** The wait before a retry comes from the first of these that exists: the delay in `Retry(delay=...)`, the backoff registered with the handler, the backoff on the job, the queue's default. Backoffs on a job are stored as text such as `fixed:30`, `exp:base=2,factor=2,max=600` or `jitter:full:exp:base=1,max=60`, and durations accept `90`, `5m` or `1h30m`.

**Missed schedule fires.** A schedule with `misfire="coalesce"` (the default) makes one job however many fires it missed. With `"catch_up"` it makes one job per missed fire, up to 100 in one pass. Each fire can only produce one job, even if several workers notice it together. Pausing a schedule and resuming it skips what was missed during the pause, in both modes.

**Cron.** Five fields, always in UTC, with month and weekday names, `@daily` style aliases, and the traditional rule that a restricted day of month and a restricted weekday match if either one does. A field starting with `*`, such as `*/2`, doesn't count as restricted. An expression that can never fire, such as `0 0 30 2 *`, is refused when you add it.

## Running the tests

```
PYTHONPATH=src python3 -m unittest discover -s tests
```

## Known limitations

- Delivery is at least once. A handler that outlives its lease can run alongside its replacement, and a crash just after a handler finishes runs it again, so handlers should be safe to repeat.
- It works on one machine only. WAL mode relies on shared memory, so the database must not sit on a network filesystem, and all writers take turns on a single write lock.
- Handlers can't be killed or timed out, because they run in threads. In pool mode a stuck handler has its lease renewed for as long as the worker lives, so it is never retried. After `--drain-timeout` the worker stops waiting for it, but the process can't exit until the thread returns.
- Only pool mode (`concurrency` above 1) renews leases by itself. With a single-threaded worker, a handler that runs longer than the lease has to call `ctx.heartbeat()`.
- `Retry` always uses up an attempt, so a job that waits on something outside its control will eventually die however it is scheduled.
- Handler defaults only apply through `registry.enqueue`. The `jobqueue enqueue` command doesn't know about them, so a job queued that way gets the queue's defaults, including its `max_attempts`.
- A backoff object with no text form can't be stored on a job. Workers still apply it to failures through the handler registration, but a reclaimed lease falls back to the job's or queue's backoff.
- Workers lease any job on their queues, including handlers they don't have, and a missing handler counts as a failure. Mixed workers need separate queue names.
- A schedule doesn't check whether its previous job has finished, so a slow job can overlap with its next fire. Nothing fires unless a worker is running, and any worker promotes all schedules, whatever queues it was told to serve.
- Queues can't be paused, and there are no per-queue rate limits or fairness between queues. A steady stream of high-priority jobs starves lower ones.
- Unique keys only cover jobs still queued or leased. Once a job finishes, the same key can be queued again.
- Time comes from the system clock, so a large clock change can expire leases early or delay jobs.
- Every poll takes the write lock, even when there's nothing to do, and leasing across all queues sorts the ready jobs rather than walking an index. Many idle workers or a very large backlog will be noticeably slower.
- `stats()` loads attempt durations for the window into memory to work out percentiles. Pruning a job deletes its attempt history too, so pruned work drops out of the figures.
- It has only been exercised by its own test suite, not under real long-term load.

## License

MIT. See `LICENSE`.

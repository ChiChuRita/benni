---
"beni": minor
---

Add `queue` to `beni/primitives` — a job queue built for AI work.

Model calls run for minutes, stream their output, cost money per attempt, and
get cancelled mid-flight. `queue` treats those as the design rather than as
configuration:

- **Heartbeat leases, not idle timers.** A reserved job is owned for `leaseMs`
  and renewed while the handler runs, so a ten-minute generation is ordinary.
  A crashed worker's lease lapses and the job is reclaimed or dead-lettered.
- **A resumable output stream per job.** `job.emit(token)` appends to a capped
  per-job stream *and* renews the lease in one round trip, so
  `queue.watch(id, { after })` is a resumable feed — a client that drops
  mid-generation replays from its last entry id instead of paying twice. A
  retried attempt emits `restarted` so watchers discard the failed generation.
- **First-class cancellation.** `queue.cancel(id)` aborts the handler's
  `AbortSignal`, stopping the in-flight provider call, and settles the job
  `cancelled` rather than `failed`.
- **Retries that match provider failures.** Exponential backoff with full
  jitter by default; `RetryJobError` carries a provider `Retry-After`, and
  `TerminalJobError` dead-letters without burning attempts.
- **Idempotency keys** that collapse duplicate requests onto one job and keep
  serving its result after it completes.

Job lifecycle lives in sorted sets (delays, priority, backoff, dead-lettering);
streams carry output. Every key shares one hash tag, so a queue occupies a
single Redis Cluster slot. `enqueue`/`get`/`cancel`/`wait`/`watch`/`stats` need
only `EVALSHA` and stream reads and run on `beni/upstash` at the edge;
`worker()` needs a persistent process and blocks on a doorbell list where the
adapter provides a dedicated connection, falling back to polling where it does
not.

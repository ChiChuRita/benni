---
"benni": minor
---

`worker.stop({ timeoutMs })` bounds a queue worker's shutdown and hands unfinished jobs straight back to the queue.

`stop()` waited for every in-flight job however long it took. On a platform that sends SIGTERM and then SIGKILL, the killed job sat in `leases` until its lease expired, up to `leaseMs`, before anyone re-ran it, and that re-run consumed an attempt. With a timeout, once it elapses, every job still running has its signal aborted with the new `WorkerStoppedError` and is requeued at once: at the front of its priority band, with its attempt refunded. `timeoutMs: 0` requeues immediately. A job cancelled during the run is settled `cancelled` instead of requeued.

```ts
process.on("SIGTERM", () => void worker.stop({ timeoutMs: 20_000 }));
```

A requeued job re-runs from the top and is paid for again; its stream restarts with a `restarted` marker even though its attempt number did not grow. Only a job that finishes within the timeout survives a deploy without a second bill. The default is unchanged: no limit.

Also: a job reserved while `stop()` was already under way is now handed back, attempt refunded, instead of being started.

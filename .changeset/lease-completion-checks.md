---
"benni": patch
---

`lock` and `semaphore` no longer report a successful `run()` when the lease was lost, and a throwing `onRenewError` can no longer take the process down.

Lease renewal shipped with its completion check reading one flag, `lease.lost`, which is set only from inside the renewal interval. That left the interval as the single witness to a lost lease, and an interval is not guaranteed to run. A body that blocks the event loop past its TTL (synchronous CPU work, a blocking native call) starves it completely, and because a timer is a macrotask while resuming from `await fn(handle)` is a microtask, the completion check won that race and resolved as though the critical section had been exclusive throughout. A 600ms synchronous body under `ttlMs: 200` returned its value with the lock key already gone from Redis. For the semaphore the same path is real over-admission: the member is pruned by the next `acquire` and another caller is let in on top of a body still running.

Completion now consults the deadline in the same turn the body finished, and the result of the final `release` (which runs the same token check `extend` does, so a `false` reply is Redis saying the lease had already moved on), alongside the flag. A lease given up deliberately inside the body is still not a loss, and `heartbeatMs: false` still means the documented opt-out rather than a failure. A healthy body that outlives many TTLs while renewals keep succeeding still resolves normally.

Three smaller fixes in the same area:

- **A throwing `onRenewError` is contained.** The hook ran inside the rejection handler of a promise the tick discards, so a throw from it became an unobserved rejection, which is fatal in default Node. A telemetry callback must not be able to kill the process. The hook is also no longer called for a renewal that settles after `run()` has already returned.
- **The interval is torn down when the lease is lost**, instead of staying armed and early-returning on every future tick. It was `unref()`ed so it never held Node open, but a body that ignores the abort signal and never settles used to leave it spinning.
- **An explicitly passed `heartbeatMs` must be at most half the lease.** Above that the first tick can land at or after expiry, so the lease lapses before renewal is ever attempted, on an uncontended lock. The old behavior was silent and load dependent: it passed for a body that finished before the first tick and failed for a slower one. The derived default is unchanged and still applies to a lease too small for any ratio to hold.

Found by an independent review of the renewal work, with each case reproduced before the fix and kept as a regression test.

---
"benni": minor
---

`lock().run()` now renews the lock while the critical section runs, and reports a lost lease instead of hiding it.

Before, `run()` acquired with `ttlMs` and never renewed, so a body that outlived the TTL silently lost mutual exclusion: the key expired, another caller acquired it, and the original body kept running as if it still held the lock. It now renews on an interval, following the same policy the `queue` primitive already used for job leases.

- `run()` renews the lock every `heartbeatMs`, which defaults to a quarter of the effective `ttlMs` (matching the queue's `leaseMs` 60000 / `heartbeatMs` 15000 ratio). Pass `heartbeatMs: false` to opt out and keep the previous behaviour.
- New `LockLeaseLostError`. If renewal finds the lock gone, `run()` rejects with it even when the body resolved, because a body that finished without the lock did not finish under mutual exclusion.
- `LockHandle` gained a `signal` (an `AbortSignal`) that aborts with that error, so a body can pass it to `fetch` or the AI SDK and stop as soon as the work stops being exclusive. A manual `extend()` that resolves `false` aborts it too.
- New `onRenewError` hook on `run()`, called when a renewal round trip fails. A failed round trip is not treated as a lost lock: the next tick retries, and the lease is only declared lost once Redis reports another token owns the key or the TTL window has passed with no successful renewal.
- The renewal timer is unref'd and always cleared, so it can never keep a process alive.

The fail-fast default is unchanged: `retries` still defaults to `0`, so a contended `acquire()` resolves `null` and a contended `run()` throws `LockNotAcquiredError` rather than waiting. That contract is now spelled out on `lock()`, `acquire()`, and `run()`, along with the `retries` / `retryDelayMs` pair to pass when the intent is to serialize concurrent callers instead.

`acquire()`, `run()`, `LockHandle.release()`, `LockHandle.extend()`, and `LockNotAcquiredError` are unchanged.

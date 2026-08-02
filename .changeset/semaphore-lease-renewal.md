---
"benni": minor
---

`semaphore().run()` now renews its lease while the critical section runs, and reports a lost slot instead of hiding it.

Before, `run()` acquired with `leaseMs` and never renewed, so a body that outlived the lease silently lost its slot: the lease lapsed, the next acquire pruned it and admitted another caller, and the original body kept running as if it were still inside the limit. That is over-admission, the one thing a semaphore exists to prevent, and a `limit: 20` guarding a provider quota would quietly run 21 in flight. It now renews on an interval, matching `lock().run()` and the `queue` primitive's job leases.

- `run()` renews the lease every `heartbeatMs`, which defaults to a quarter of the effective `leaseMs` (15s at the default `leaseMs` of 60000, the same ratio the queue uses). Pass `heartbeatMs: false` to opt out and keep the previous behaviour.
- New `SemaphoreLeaseLostError`, carrying `key` and `limit`. If renewal finds the slot gone, `run()` rejects with it even when the body resolved, because a body that finished without a slot did not finish under the bound it was written against.
- `SemaphoreHandle` gained a `signal` (an `AbortSignal`) that aborts with that error, so a body can pass it to `fetch` or the AI SDK and stop as soon as the pool stops accounting for it. A manual `extend()` that resolves `false` aborts it too.
- New `onRenewError` hook on `run()`, called when a renewal round trip fails. A failed round trip is not treated as a lost slot: the next tick retries, and the lease is only declared lost once Redis reports the slot is no longer ours, or a full `leaseMs` has passed with no successful renewal.
- New `SemaphoreRunOptions` type, the third argument to `run()`, widening `SemaphoreAcquireOptions`.
- Renewal options are validated before a slot is taken, so a bad `heartbeatMs` cannot hold a slot until its lease lapses. The renewal timer is unref'd and always cleared, so it can never keep a process alive.

The fail-fast default is unchanged: `retries` still defaults to `0`, so a full pool makes `acquire()` resolve `null` and `run()` throw `SemaphoreNotAcquiredError` rather than waiting. That contract is now spelled out on `semaphore()`, `acquire()`, `run()`, and `retries`, along with the `retries` / `retryDelayMs` pair to pass when the intent is to queue callers instead.

`acquire()`, `count()`, `SemaphoreHandle.release()`, `SemaphoreHandle.extend()`, and `SemaphoreNotAcquiredError` are otherwise unchanged.

The lock and semaphore documentation pages are rewritten around the shared lease model, and both now state that an `acquire()`d handle is never renewed in the background.

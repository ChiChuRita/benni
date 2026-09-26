---
"benni": minor
---

`idempotency` renews its claim while the handler runs, tells the handler when the claim is lost, rejects a key reused with a different request, and documents exactly what it guarantees.

The running marker was set once with `PX runningTtlMs` (30 seconds by default) and never renewed. A 35-second charge lost its claim at 30 seconds, a client retry found the key free, and the card was charged twice: the very failure the primitive exists to prevent, and it was sold as "exactly-once".

- **The claim is renewed** every quarter of `runningTtlMs` while the handler runs, through the same renewal machinery `lock.run()` uses. `runningTtlMs` is now a crash-recovery window (how long a handler that *died* blocks retries), not a bound on the handler.
- **The handler receives `{ signal }`.** It aborts with the new `IdempotencyLeaseLostError` once the claim is known to be lost (renewal failed for a whole `runningTtlMs`, or found another caller's marker), so an effect not yet committed can stop. If the handler resolves anyway, `run` throws `IdempotencyNotRecordedError` with that error as its `cause`, since another caller may be running it too. Handlers that ignore the argument are unaffected.
- **`fingerprint`**, a new per-call option, is the Stripe rule that one key names one request. Pass a digest of the body; a later call with the same key and a different fingerprint throws the new `IdempotencyFingerprintMismatchError` instead of replaying or waiting. It is compared only when both calls passed one, and records written before this release still replay.

  ```ts
  const { value } = await once.run(key, ({ signal }) => charge(order, { signal }), {
    fingerprint: await sha256(body)
  });
  ```

- **The docs state the real guarantee**: at most one caller runs the handler for a key at a time, and a success is replayed for `ttlMs`. A throw releases the key, a crash lets the next caller run after `runningTtlMs`, and a lost claim (including a failover that drops it) can run the handler concurrently. The "exactly-once" and "at most once" claims are gone from the JSDoc and the docs page, which also gains a section on how this differs from the queue's `idempotencyKey` and `budget`'s settle-once marker.

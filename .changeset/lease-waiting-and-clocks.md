---
"benni": minor
---

`lock` and `semaphore` can bound how long they wait, spread their retries, and keep lease deadlines on a clock that cannot be stepped.

- **`waitTimeoutMs`** on `acquire()` and `run()` bounds the total wait for a lock or a slot. Passed alone it means "retry until then"; with `retries`, whichever runs out first ends the wait, and a final attempt lands at the deadline rather than after it. The name matches `idempotency`'s option for the same concept.

  ```ts
  await locks.run("order:42", processOrder, { waitTimeoutMs: 2_000 });
  await slots.run("openai", callModel, { waitTimeoutMs: 2_000 });
  ```

- **Retry delays are jittered** uniformly over half to one and a half times `retryDelayMs`. The mean is unchanged, so `retries × retryDelayMs` still reads as the expected wait, but callers that collided once no longer wake in lockstep and collide again.
- **`extend()` with no argument re-applies this acquisition's TTL**, not the store default. `acquire("x", { ttlMs: 5_000 })` followed by `handle.extend()` used to extend to the store's 30 seconds (or 60 for a semaphore).
- **Local lease deadlines use `performance.now()`**, not `Date.now()`. An NTP step forwards used to declare a healthy, renewed lease lost and throw away its body's result; a step backwards made an expired lease look live for the size of the step. Ownership is still decided by the server's clock, as before.

The renewal machinery `lock` and `semaphore` shared by copy is now one internal module, which `idempotency` and `cache` also use (see their changesets).

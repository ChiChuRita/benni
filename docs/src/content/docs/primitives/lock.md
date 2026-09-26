---
title: "Distributed Lock"
description: "A distributed lock over Redis: acquire with SET NX PX, renew the lease while your critical section runs, release atomically, and hand every holder a fencing token for the stores it writes to."
---

`lock` is a distributed lock built the correct way: acquire with `SET key token NX PX ttl`, and release with an atomic check-and-delete Lua so a caller can **never** delete a lock that already expired and was re-acquired by someone else, the classic footgun of a naive `DEL`.

```ts
// schema.ts
import { lock } from "benni/schema";

export const orderLocks = lock("order", { ttlMs: 10_000 });
```

```ts
// app.ts
await redis.query.orderLocks.run("42", async () => {
  // critical section: the lock is renewed while this runs, and released
  // automatically, even if this throws
});
```

Declared as a schema value it lands in [`redis.query`](/benni/core-concepts/schema-registry/) and needs no client of its own. Where you hold a client but no handle, `benni/primitives` exports the same lock in its client-taking form, over the same keys:

```ts
import { lock } from "benni/primitives";

const locks = lock({ client, prefix: "order", ttlMs: 10_000 });
await locks.run("42", async () => { /* ... */ });
```

`client` accepts a `RedisClient`, a promise of one, a factory, or a Benni handle, so it works over every adapter, including [`benni/upstash`](/benni/runtime/edge/) on the edge (it needs only `EVALSHA`, no persistent connection).

Two defaults decide how it behaves under pressure, and both are worth reading before you ship: acquisition **fails fast**, and `run` **renews the lease** while your body is in flight. One assumption decides what it can promise at all: it is a lock on **one Redis primary**, and [a failover can grant it twice](#one-primary-no-redlock).

## Acquiring Fails Fast

`retries` defaults to `0`. A caller that finds the lock held does not wait: `acquire` resolves `null` and `run` throws `LockNotAcquiredError` immediately.

Concretely, six concurrent callers on the same id means one runs and **five throw**. That is the right default for a request handler (return 409 rather than pile up requests behind a lock), and the wrong one if what you meant was to serialize concurrent work.

Catch the error when "someone else is doing it" is a real answer:

```ts
import { LockNotAcquiredError } from "benni/primitives";

try {
  await locks.run("order:42", processOrder);
} catch (error) {
  if (error instanceof LockNotAcquiredError) {
    // someone else holds error.key, so back off, reschedule, or return 409
    return new Response("Already processing", { status: 409 });
  }
  throw error;
}
```

Pass retries when every caller must eventually run:

```ts
// Each caller waits its turn behind the holder: all six run, one at a time.
await locks.run("order:42", processOrder, {
  retries: 100,
  retryDelayMs: 50
});
```

Or bound the wait by time rather than by count. `waitTimeoutMs` alone means "keep retrying until then"; with `retries` as well, whichever runs out first stops the wait, and a last attempt is made at the deadline rather than after it:

```ts
// Wait up to two seconds for the holder, then throw LockNotAcquiredError.
await locks.run("order:42", processOrder, { waitTimeoutMs: 2_000 });
```

Each wait between attempts is jittered over half to one and a half times `retryDelayMs`, so callers that collided once do not wake in lockstep and collide again.

Retries are a bounded spin, not a fair queue: callers do not get the lock in arrival order, and a heavily contended lock can starve an unlucky one. If strict ordering matters, that is a job for the [queue](/benni/primitives/queue/).

## Lease Renewal

The TTL is a safety net for crashes: if your process dies mid-section, the lock expires instead of deadlocking forever. But a TTL that expires while your body is still running is not a safety net, it is a silent correctness bug. The key lapses, another caller acquires it, and your body keeps running as though it were still exclusive. Two writers, one critical section, no error anywhere.

So `run` renews the lock while `fn` is in flight, every `heartbeatMs`:

```ts
const locks = lock(client, { ttlMs: 10_000 }); // renewed every 2.5s

await locks.run("report:nightly", async () => {
  await generateReport(); // may take minutes; the lock is held throughout
});
```

`heartbeatMs` defaults to a quarter of the effective `ttlMs` (`Math.max(1, Math.floor(ttlMs / 4))`, the same ratio the [queue](/benni/primitives/queue/) uses for job leases). A quarter means three renewals in a row can fail outright before the lock could lapse, so a blip on the wire is survivable rather than fatal.

Set it yourself when you want a different margin, and pass `false` to opt out entirely:

```ts
// Renew more often: a tighter margin against a flaky connection.
await locks.run("order:42", processOrder, { heartbeatMs: 1_000 });

// Opt out: the lock expires ttlMs after it was taken, whatever fn is doing.
// This is the pre-renewal behaviour, kept reachable for short bodies that
// genuinely cannot outlive their TTL.
await locks.run("order:42", processOrder, { heartbeatMs: false });
```

A `heartbeatMs` you pass yourself must be **at most half of `ttlMs`**, otherwise the call throws `ValidationError` before the lock is taken. At a half, one renewal can still fail before the lock could lapse; above it, the first tick can arrive at or after expiry and the lock lapses before renewal ever runs. That misconfiguration used to be silent and load dependent: a short body finished before the first tick and looked fine, while a long one failed with `LockLeaseLostError` on an uncontended lock. The check is on the value you supply, not on the derived default, which stays intact for a `ttlMs` so small that no ratio could hold.

```ts
// Throws: ValidationError, lock heartbeatMs must be at most half of ttlMs (1000)
await locks.run("order:42", processOrder, { ttlMs: 1_000, heartbeatMs: 2_000 });
```

A body that finishes inside the first interval costs no extra round trips, so renewal is free for the short critical sections that never needed it.

## When The Lock Is Lost

Renewal can fail for a real reason: the lock expired and someone else took it, or a human ran `DEL`. When that happens `run` rejects with `LockLeaseLostError`, which carries the contested `.key`.

It rejects **even when `fn` resolved**. A body that finished without the lock did not finish under the mutual exclusion it was written against, and resolving would hide exactly that:

```ts
import { LockLeaseLostError } from "benni/primitives";

try {
  const receipt = await locks.run("order:42", chargeCard);
  // reached only if the lock was held for the whole call
  return receipt;
} catch (error) {
  if (error instanceof LockLeaseLostError) {
    // chargeCard may have completed, but not exclusively: reconcile rather
    // than assume either outcome
    return reconcile(error.key);
  }
  throw error;
}
```

### Detection Is Two-Pronged

A lost lease is not the same thing as a failed round trip, and conflating them would make every network hiccup fatal. `run` declares the lock lost only when:

1. **`extend` reports it is gone.** The renewal Lua checks the token, so a `0` reply means the key is missing or now owned by somebody else. This is immediate and definitive.
2. **A full `ttlMs` has passed with no successful renewal.** This catches the cases the first prong cannot see: renewals that keep rejecting, and a renewal that hangs and never answers at all. Silence is treated as loss, because after `ttlMs` the key has demonstrably lapsed.

That deadline is read both from the renewal tick and again in the same turn your body finishes, which matters because a tick is not guaranteed to run. A body that blocks the event loop (synchronous CPU work, a blocking native call) past its TTL starves the interval entirely, and a timer is a macrotask while resuming from `await fn(handle)` is a microtask, so the completion check would otherwise win the race and report success for a lock that had already expired. The final `release` is consulted for the same reason: it runs the same token check `extend` does, so a `false` reply is Redis saying the lock had already moved on.

One failed renewal is not a loss. The next tick simply retries. To see those failures, pass `onRenewError`:

```ts
await locks.run("order:42", processOrder, {
  onRenewError: (error) => {
    // A renewal round trip failed: a dropped connection, a timeout. The next
    // tick retries and the lock may well survive, so this is telemetry, not a
    // failure. Without the hook these errors are swallowed.
    logger.warn({ error }, "lock renewal round trip failed");
  }
});
```

### `handle.signal`

Rejecting after the fact is a correct report, but it is late: the body already did the work. `handle.signal` is an `AbortSignal` that aborts with the `LockLeaseLostError` the moment the lock is known to be gone, so the work can stop instead of finishing unprotected.

It composes with anything that takes a signal:

```ts
await locks.run("order:42", async (handle) => {
  // fetch rejects as soon as the lock is lost
  const res = await fetch(url, { signal: handle.signal });

  // so does an AI SDK call
  const { text } = await generateText({
    model,
    prompt,
    abortSignal: handle.signal
  });

  return { res, text };
});
```

If `fn` rejects with the abort reason itself (as `fetch` does), that error propagates unchanged rather than being replaced.

## Fencing Tokens

Renewal narrows the window in which a lock can be lost, and `signal` stops work once it is, but neither can stop a holder that is *paused*: a long GC, a descheduled container, a laptop lid. It wakes up after its lease lapsed and someone else took the lock, and its next write lands on top of theirs before any renewal gets a chance to notice.

The fix has to live where the write lands. Every acquisition carries `handle.fence`, a number that strictly increases with every grant of that lock id, across all processes (it is an `INCR` in the same script as the `SET NX`). Send it with each write, and have the store refuse a fence lower than one it has already accepted:

```ts
await locks.run("order:42", async ({ fence }) => {
  const order = await db.orders.get("42");
  // 0 rows updated means a newer holder already wrote: back off.
  const { rowCount } = await db.query(
    "UPDATE orders SET status = $1, fence = $2 WHERE id = $3 AND fence < $2",
    ["shipped", fence, order.id]
  );
  if (rowCount === 0) throw new Error("superseded by a newer lock holder");
});
```

The same pattern works for anything with a conditional write: a version column, an object store's `If-Match`, a Redis script that compares first. The counter lives at `{<prefix>:<id>}:fence` (the braces keep it in the lock's Cluster slot) and is kept forever, one small key per lock id, because a counter that expired would restart below fences a store has already accepted. An id containing a `}` that does not close a hash tag (`"a}b"`) cannot share a slot with its counter, so it is rejected with a `ValidationError` before anything is sent.

## One Primary, No Redlock

The lock is exactly as strong as the Redis primary it lives on. Replication is asynchronous, so if the primary fails over after acknowledging your `SET` but before a replica received it, the promoted replica has never heard of your lock and will grant it to the next caller while you are still inside. Nothing a client does over one Redis deployment can close that gap, and this library does not implement Redlock.

What that means in practice:

- For **efficiency** (don't do the same work twice, don't hammer a provider), this lock is the right tool: a double grant during a failover costs one duplicate job.
- For **correctness** (two writers must never interleave), pair it with a fencing check at the store, as above. The fence counter can fail over too, but a store that rejects any fence at or below the highest it has seen still refuses a stale holder's write.

## Acquire And Release Manually

```ts
const handle = await locks.acquire("order:42");
if (handle) {
  try {
    // ... work ...
  } finally {
    await handle.release(); // resolves true only if we still held it
  }
}
```

`acquire` resolves `null` when the lock is already held. `release()` and `extend()` resolve `true` only when your token still owns the key; both run the atomic Lua, so they are safe under expiry races. `extend()` with no argument re-applies the TTL this acquisition was taken with, not the store default.

**An `acquire`d handle is not renewed in the background.** Nothing watches it on your behalf: if the work can outlive `ttlMs`, you have to call `extend()` yourself. That also means `handle.signal` cannot fire unless you do, because your own `extend()` resolving `false` is the only thing that can abort it. If you want renewal, use `run`.

```ts
const handle = await locks.acquire("report", { ttlMs: 30_000 });
// ... halfway through a long job, keep the lock alive:
const stillOurs = await handle?.extend(30_000);
if (stillOurs === false) {
  // we overran: the lock is gone and handle.signal has aborted
}
```

## Options

| Option | Where | Default | Meaning |
| --- | --- | --- | --- |
| `prefix` | `lock(client, …)` | `"lock"` | Key namespace; locks are `<prefix>:<id>`, fence counters `{<prefix>:<id>}:fence`. |
| `ttlMs` | `lock` / `acquire` / `run` | `30000` | Lock lifetime. It is the crash backstop, and with `run` it is also the renewal window. |
| `retries` | `acquire` / `run` | `0` | Attempts when the lock is held. `0` fails fast; unlimited when only `waitTimeoutMs` is set. |
| `retryDelayMs` | `acquire` / `run` | `100` | Delay between retries, jittered over 0.5 to 1.5 times this. |
| `waitTimeoutMs` | `acquire` / `run` | none | The most time to spend waiting for the lock, across all retries. |
| `heartbeatMs` | `run` | `ttlMs / 4` | Renewal interval while `fn` runs. Must be at most half of `ttlMs` when set explicitly. `false` disables renewal. |
| `onRenewError` | `run` | none | Called when a renewal round trip fails. Not a lost lock. |

## Relationship To `semaphore`

A lock lets one caller through. [`semaphore`](/benni/primitives/semaphore/) lets `N` through, and follows the same lease policy: `run` renews in the background, a lost slot surfaces as a typed error (`SemaphoreLeaseLostError`) rather than a silent success, and its handle carries the same `signal`. The difference is what a lost lease costs you: for a lock it means two writers collided, and for a semaphore it means the pool over-admits. Everything else you know here transfers.

## See Also

- [Semaphore](/benni/primitives/semaphore/) for bounded concurrency rather than one-at-a-time
- [Idempotency](/benni/primitives/idempotency/) when the goal is "run once per request", not "one at a time"
- [Queue](/benni/primitives/queue/) when callers must all run, in order

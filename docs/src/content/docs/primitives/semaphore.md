---
title: "Semaphore"
description: "Bounded concurrency across processes: at most N callers in the critical section at once, with leases that renew while you work and reclaim slots from dead holders."
---

A lock lets one caller through. A semaphore lets `N` through. That number is usually what a provider actually enforces.

Rate and concurrency are different constraints, and model providers impose both: a rate limit protects their billing, a concurrency limit protects their capacity, and exceeding either gets you 429s. `p-limit` solves this inside one process; the moment you run two instances, the limit is per-instance and the provider sees the sum.

```ts
// schema.ts
import { semaphore } from "benni/schema";

export const slots = semaphore("provider", { limit: 20, leaseMs: 60_000 });
```

```ts
// app.ts
const answer = await redis.query.slots.run("openai", async () => callModel(prompt));
```

Declared as a schema value it lands in [`redis.query`](/benni/core-concepts/schema-registry/) and needs no client of its own. `benni/primitives` exports the same semaphore in its client-taking form for code that holds a client but no handle: `semaphore({ client, limit: 20 })`.

At most 20 callers are inside that body at once, across every process pointed at the same Redis. The lease is renewed while the body runs, so a slow call keeps its slot rather than losing it mid-flight.

## Acquiring Fails Fast

`retries` defaults to `0`. A caller that finds every slot taken does not wait: `acquire` returns `null` and `run` throws `SemaphoreNotAcquiredError`, which carries the `.key` and the `.limit`.

Concretely, 25 concurrent callers on a `limit: 20` semaphore means 20 run and **five throw**. That is load shedding, and it is usually what you want in a request handler.

```ts
const held = await slots.acquire("openai");
if (!held) return new Response("Busy, try again", { status: 503 });
try {
  await doWork();
} finally {
  await held.release();
}
```

`acquire` returns `null` rather than throwing when every slot is taken, so "no capacity" is an ordinary branch. `run` throws `SemaphoreNotAcquiredError` instead, since it has nowhere to put a null.

To queue instead of shedding:

```ts
await slots.run("openai", work, { retries: 100, retryDelayMs: 50 });
```

Retries are a bounded spin, not a fair queue: callers do not get slots in arrival order, and a heavily contended semaphore can starve an unlucky one. If strict ordering matters, that is a job for the [queue](/benni/primitives/queue/), which is built for it.

## Leases And Dead Holders

A slot is held by a lease, not by a connection. Holders live in a sorted set scored by expiry, so reclaiming the slots of processes that crashed is just dropping the expired range, on the next acquire, with no sweeper to run.

That is what makes a crashed holder harmless. It is also what makes a *slow* holder dangerous: when your lease lapses while you are still working, the next acquire prunes you and admits somebody else. Nothing failed, nothing was logged, and the semaphore is now over its limit. A `limit: 20` guarding a provider quota quietly runs 21 in flight, which is precisely the 429 it existed to prevent.

This is the one place the semaphore differs from a [lock](/benni/primitives/lock/) in kind rather than in degree. A lost lock means two writers collided on one key. A lost slot means the pool **over-admits**: everyone else is behaving correctly and the ceiling is simply wrong.

## Lease Renewal

So `run` renews the lease while `fn` is in flight, every `heartbeatMs`:

```ts
const slots = semaphore(client, { limit: 20, leaseMs: 60_000 }); // 15s heartbeat

await slots.run("openai", async () => {
  // a streaming completion that runs for minutes keeps its slot throughout
  return callModel(prompt);
});
```

`heartbeatMs` defaults to a quarter of the effective `leaseMs` (`Math.max(1, Math.floor(leaseMs / 4))`, which at the default `leaseMs` is exactly the 15s the [queue](/benni/primitives/queue/) uses for job leases). A quarter means three renewals in a row can fail outright before the slot could lapse, so a blip on the wire is survivable rather than fatal.

Set it yourself for a different margin, and pass `false` to opt out entirely:

```ts
// Renew more often: a tighter margin against a flaky connection.
await slots.run("openai", work, { heartbeatMs: 5_000 });

// Opt out: the slot is reclaimable leaseMs after it was taken, whatever fn is
// doing. This is the pre-renewal behaviour, kept reachable for short bodies
// that genuinely cannot outlive their lease.
await slots.run("openai", work, { heartbeatMs: false });
```

A body that finishes inside the first interval costs no extra round trips, so renewal is free for the short calls that never needed it.

## When The Slot Is Lost

If renewal finds the slot gone, `run` rejects with `SemaphoreLeaseLostError`, carrying the `.key` and the `.limit`.

It rejects **even when `fn` resolved**. A body that finished without a slot did not finish under the bound it was written against, and resolving would hide exactly the over-admission you added the semaphore to prevent:

```ts
import { SemaphoreLeaseLostError } from "benni/primitives";

try {
  return await slots.run("openai", () => callModel(prompt));
} catch (error) {
  if (error instanceof SemaphoreLeaseLostError) {
    // the call may have completed, but the pool was over its limit while it
    // ran: treat it as a capacity incident, and raise leaseMs if it recurs
    logger.error({ key: error.key, limit: error.limit }, "semaphore overran");
  }
  throw error;
}
```

### Detection Is Two-Pronged

A lost lease is not the same thing as a failed round trip, and conflating them would make every network hiccup fatal. `run` declares the slot lost only when:

1. **`extend` reports it is gone.** The renewal Lua checks that our member is present *and* that its score is still in the future, because presence is not ownership: an expired member sits in the set until some acquire prunes it. A `false` result is immediate and definitive.
2. **A full `leaseMs` has passed with no successful renewal.** This catches what the first prong cannot see: renewals that keep rejecting, and a renewal that hangs and never answers at all. Silence is treated as loss, because after `leaseMs` the slot is demonstrably reclaimable.

One failed renewal is not a loss. The next tick simply retries. To see those failures, pass `onRenewError`:

```ts
await slots.run("openai", work, {
  onRenewError: (error) => {
    // A renewal round trip failed: a dropped connection, a timeout. The next
    // tick retries and the slot may well survive, so this is telemetry, not a
    // failure. Without the hook these errors are swallowed.
    logger.warn({ error }, "semaphore renewal round trip failed");
  }
});
```

### `held.signal`

Rejecting after the fact is a correct report, but it is late: the call already went out over the limit. `held.signal` is an `AbortSignal` that aborts with the `SemaphoreLeaseLostError` the moment the slot is known to be gone, so the work can stop instead of running unaccounted for.

It composes with anything that takes a signal, which for a semaphore is usually the very call you are bounding:

```ts
await slots.run("openai", async (held) => {
  const { text } = await generateText({
    model,
    prompt,
    abortSignal: held.signal // stops the moment we are over the limit
  });
  return text;
});
```

```ts
await slots.run("openai", async (held) => {
  const res = await fetch(url, { signal: held.signal });
  return res.json();
});
```

If `fn` rejects with the abort reason itself (as `fetch` does), that error propagates unchanged rather than being replaced.

### Renewing By Hand

**An `acquire`d handle is not renewed in the background.** Nothing watches the lease on your behalf: if the work can outlive `leaseMs`, call `extend()` yourself. That also means `held.signal` cannot fire unless you do, because your own `extend()` resolving `false` is the only thing that can abort it. If you want renewal, use `run`.

```ts
const held = await slots.acquire("openai", { leaseMs: 5_000 });
// ... 6 seconds pass ...
const stillOurs = await held?.extend();
if (stillOurs === false) {
  // our slot is gone and held.signal has aborted: someone else has it now
}
```

## Inspecting

```ts
await slots.count("openai");   // live holders, ignoring lapsed leases
```

## Relationship To `lock`

This is [`lock`](/benni/primitives/lock/) with a number: same handle shape, same `run`, same retry options, same lease renewal, same `signal`. Reach for `lock` when the answer is one and for this when it is a budget. Everything you know about one transfers, apart from what a lost lease costs: for `lock` it is two writers in one critical section, and here it is a pool that admits one caller too many.

## Options

| Option | Where | Default | What it does |
| --- | --- | --- | --- |
| `limit` | `semaphore(client, …)` | required | How many holders at once. |
| `prefix` | `semaphore(client, …)` | `"semaphore"` | Key namespace; keys are `<prefix>:<id>`. |
| `leaseMs` | `semaphore` / `acquire` / `run` | `60000` | How long a slot is held without an `extend`. With `run` it is also the renewal window. |
| `retries` | `acquire` / `run` | `0` | Attempts when every slot is taken. `0` fails fast. |
| `retryDelayMs` | `acquire` / `run` | `100` | Delay between retries. |
| `heartbeatMs` | `run` | `leaseMs / 4` | Renewal interval while `fn` runs. `false` disables renewal. |
| `onRenewError` | `run` | none | Called when a renewal round trip fails. Not a lost slot. |

## When You Don't Need This

- **One caller at a time.** That is [`lock`](/benni/primitives/lock/).
- **Limiting request rate.** That is [`ratelimit`](/benni/primitives/ratelimit/). Concurrency and rate are independent; you often want both.
- **Concurrency inside one process.** `p-limit` is in-memory and free. This costs a round trip per acquire, which only buys you something when the limit spans processes.
- **Work that should be queued, not rejected.** If callers must eventually all run, in order, use the [queue](/benni/primitives/queue/) and set its worker concurrency.

## See Also

- [Distributed Lock](/benni/primitives/lock/)
- [Rate Limiting](/benni/primitives/ratelimit/)
- [Budget](/benni/primitives/budget/) for spend limits

---
title: "Semaphore"
description: "Bounded concurrency across processes: at most N callers in the critical section at once, with leases that reclaim slots from dead holders."
---

A lock lets one caller through. A semaphore lets `N` through. That number is usually what a provider actually enforces.

Rate and concurrency are different constraints, and model providers impose both: a rate limit protects their billing, a concurrency limit protects their capacity, and exceeding either gets you 429s. `p-limit` solves this inside one process; the moment you run two instances, the limit is per-instance and the provider sees the sum.

```ts
import { semaphore } from "beni/primitives";

const slots = semaphore(client, { limit: 20, leaseMs: 60_000 });

const answer = await slots.run("openai", async (held) => {
  await held.extend();      // heartbeat a long call
  return callModel(prompt);
});
```

At most 20 callers are inside that body at once, across every process pointed at the same Redis.

## Acquiring Without `run`

`run` releases in a `finally`, which is what you want almost always. When you need the handle to outlive a block:

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

## Waiting For A Slot

By default acquisition fails immediately. To queue instead:

```ts
await slots.run("openai", work, { retries: 100, retryDelayMs: 50 });
```

Retries are a bounded spin, not a fair queue: callers do not get slots in arrival order, and a heavily contended semaphore can starve an unlucky one. If strict ordering matters, that is a job for the [queue](/beni/primitives/queue/), which is built for it.

## Dead Holders

A slot is held by a lease, not by a connection. Holders live in a sorted set scored by expiry, so reclaiming the slots of processes that crashed is just dropping the expired range, on the next acquire, with no sweeper to run.

The tradeoff is the usual one for leases: pick `leaseMs` longer than your slowest legitimate call, or a slow caller will have its slot handed to someone else while it is still working. `extend()` renews an active lease and returns `false` if the slot was already reclaimed, which is a useful signal that you overran.

```ts
const held = await slots.acquire("openai", { leaseMs: 5_000 });
// ... 6 seconds pass ...
await held?.extend();   // false: our slot is gone
```

## Inspecting

```ts
await slots.count("openai");   // live holders, ignoring lapsed leases
```

## Relationship To `lock`

This is [`lock`](/beni/primitives/lock/) with a number: same handle shape, same `run`, same retry options. Reach for `lock` when the answer is one and for this when it is a budget. Everything you know about one transfers.

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `limit` | required | How many holders at once. |
| `prefix` | `"semaphore"` | Key namespace. |
| `leaseMs` | `60000` | How long a slot is held without an `extend`. |

`acquire` and `run` additionally take `{ leaseMs, retries, retryDelayMs }` per call.

## When You Don't Need This

- **One caller at a time.** That is [`lock`](/beni/primitives/lock/).
- **Limiting request rate.** That is [`ratelimit`](/beni/primitives/ratelimit/). Concurrency and rate are independent; you often want both.
- **Concurrency inside one process.** `p-limit` is in-memory and free. This costs a round trip per acquire, which only buys you something when the limit spans processes.
- **Work that should be queued, not rejected.** If callers must eventually all run, in order, use the [queue](/beni/primitives/queue/) and set its worker concurrency.

## See Also

- [Distributed Lock](/beni/primitives/lock/)
- [Rate Limiting](/beni/primitives/ratelimit/)
- [Budget](/beni/primitives/budget/) for spend limits

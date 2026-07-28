---
title: "Budget"
description: "Cost-weighted spend limits: cap a user at tokens or cents per window, with reservations that hold an estimate while a model call is in flight."
---

Rate limits count requests. Model calls are not priced by the request, so counting them caps nothing you actually care about.

One call with a 200k-token context costs what fifty 4k-token calls cost. "100 requests per minute" lets a single user spend fifty times more than another while both stay inside the limit. `budget` counts the unit you are billed in: tokens, cents, credits.

## The Simple Case

When you know the cost before you spend it:

```ts
import { budget } from "beni/primitives";

const budgets = budget(client, {
  limit: 2_000_000,       // tokens
  windowMs: 86_400_000    // per day
});

const { ok, remaining, retryAfterMs } = await budgets.charge(userId, promptTokens);
if (!ok) {
  return Response.json(
    { error: "Daily token budget exhausted", remaining },
    { status: 429, headers: { "Retry-After": String(Math.ceil(retryAfterMs / 1000)) } }
  );
}
```

One atomic round trip. Nothing is charged when it does not fit.

Amounts must be whole numbers, because the counters underneath are Redis integers. Budget in the smallest unit you meter: tokens are already whole, and money should be cents, or micro-cents when per-token prices need the resolution.

## Reservations

The hard part is that you do not know a call's real cost until it returns. Check-then-spend is a race: ten concurrent requests all see room, all proceed, and the budget is blown by the time any of them reports usage.

Hold an estimate first, then reconcile:

```ts
const hold = await budgets.reserve(userId, 8_000);
if (!hold) return new Response("Budget exhausted", { status: 429 });

try {
  const result = await callModel(prompt);
  await hold.settle(result.usage.totalTokens);   // charge what was really used
} catch {
  await hold.release();                           // charge nothing
}
```

The estimate counts against everyone else from the moment it is taken, so concurrent callers see it. On `settle` the hold is replaced by the real number, which is usually smaller, and the difference goes straight back to the budget.

A hold is a **lease, not a lock**. If the process holding it dies, the hold lapses on its own and stops counting; there is no sweeper to run and nothing to clean up. For calls that outlive `holdTtlMs` (two minutes by default), heartbeat with `hold.extend()`.

### Settle Semantics

Two cases look identical from the outside and need opposite answers, so it is worth being precise:

- **Settling twice charges once**, whether or not the hold was still there. Double-billing one call silently under-serves a paying user, which is the worse failure.
- **Settling after the hold has lapsed still charges.** The money was spent. A budget that forgets real spend is not a budget, even though this can briefly push usage over the limit.

Both hold together because settle-once is enforced on the handle, not in Redis. The reservation token is minted inside `reserve()` and never leaves your process, so a second settle for it is always the same handle: a `finally` running after an explicit call, or retry logic that does not track what it already reconciled. That also keeps the reservation set bounded by concurrency rather than by call volume, which a server-side marker per settle would not.

The second case is why `extend()` exists: keep long calls inside their own hold and it never arises.

## Reading Without Spending

```ts
const { remaining, retryAfterMs } = await budgets.check(userId);
await budgets.reset(userId);   // clear spend and holds outright
```

`check` includes live holds, so it reflects what a caller would actually be allowed right now.

## Accuracy

The window is a two-bucket sliding estimate: the previous window's spend decays out linearly as the current one fills. That means usage can drift slightly over the limit near a bucket boundary.

This is deliberate. The exact alternative is a log with one entry per request, and for a daily token budget that keeps every request of the last 24 hours alive in memory just to add up numbers. A counter is O(1) and never grows. If you need a hard ceiling rather than a spend guardrail, enforce it at the billing layer, not here.

The one place cost is not O(1) is summing live reservations, which walks the reservation set. That is bounded by *concurrent in-flight calls for a single id*, normally single digits.

## Cluster Safety

Each id's two window buckets and its reservation set share a `{<id>}` hash tag, so all three live on one node and the scripts can touch them together. Different ids still spread across the keyspace. See [Redis Cluster](/beni/advanced/cluster/).

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `limit` | required | Units allowed per window. Must be a whole number. |
| `windowMs` | required | Window length in milliseconds. |
| `prefix` | `"budget"` | Key namespace. |
| `holdTtlMs` | `120000` | How long a reservation counts before lapsing. |

## When You Don't Need This

- **You are limiting request rate, not spend.** Use [`ratelimit`](/beni/primitives/ratelimit/); it is exact and cheaper.
- **You are limiting concurrency.** "At most 20 calls in flight" is [`semaphore`](/beni/primitives/semaphore/).
- **You need per-request billing records.** This is a guardrail, not a ledger. Keep the ledger in your database and use this to stop runaway spend before it happens.

## See Also

- [Rate Limiting](/beni/primitives/ratelimit/) for request-count limits
- [Semaphore](/beni/primitives/semaphore/) for concurrency limits
- [AI Apps](/beni/patterns/ai-apps/) for how these compose

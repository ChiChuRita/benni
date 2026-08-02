---
title: "Budget"
description: "Cost-weighted spend limits: cap a user at tokens or cents per window, with reservations that hold an estimate while a model call is in flight."
---

Rate limits count requests. Model calls are not priced by the request, so counting them caps nothing you actually care about.

One call with a 200k-token context costs what fifty 4k-token calls cost. "100 requests per minute" lets a single user spend fifty times more than another while both stay inside the limit. `budget` counts the unit you are billed in: tokens, cents, credits.

## The Simple Case

When you know the cost before you spend it:

```ts
import { budget } from "benni/primitives";

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

Both hold together because settling is deduplicated in Redis, on the reservation token. The first settle for a token claims a small marker key that lives for `holdTtlMs`; any settle that finds the marker already claimed charges nothing. The handle short-circuits a repeat settle before it costs a round trip, but the marker is the actual guarantee.

That distinction matters for one failure in particular. If a settle's reply is lost on the way back, a socket reset, a command timeout, a failover, the call rejects and you cannot tell whether Redis applied it. So retry it:

```ts
try {
  await hold.settle(usedTokens);
} catch {
  await hold.settle(usedTokens);   // safe: the token is charged at most once
}
```

A settle that rejects always leaves the hold usable, and a retry that reaches a server which already charged is a no-op. Handle-local bookkeeping cannot make that call, because the handle never learns what the server did.

The second case above is why `extend()` exists: keep long calls inside their own hold and it never arises.

The marker is its own key rather than an entry in the reservation set, so it never lengthens the scan that summing live holds pays for.

## Reading Without Spending

```ts
const { remaining, retryAfterMs } = await budgets.check(userId);
await budgets.reset(userId);   // clear spend and holds outright
```

`check` includes live holds, so it reflects what a caller would actually be allowed right now.

## Accuracy

The window is a two-bucket sliding estimate: the previous window's spend decays out linearly as the current one fills. That means usage can drift slightly over the limit near a bucket boundary.

This is deliberate. The exact alternative is a log with one entry per request, and for a daily token budget that keeps every request of the last 24 hours alive in memory just to add up numbers. A counter is O(1) and never grows. If you need a hard ceiling rather than a spend guardrail, enforce it at the billing layer, not here.

The one place cost is not O(1) is summing live reservations, which walks the reservation set. That is bounded by *concurrent in-flight calls for a single id*, normally single digits. The limit does most of that bounding on its own, since every hold consumes headroom, but a hold for `0` consumes none, so `maxHolds` (10000 by default) puts a ceiling on the set regardless. Past it `reserve` returns `null` like any other denial.

`retryAfterMs` is the time until enough units decay out of the window for that exact spend, computed server-side. It is not the time to the next bucket boundary, which frees nothing: the two-bucket estimate is continuous across the roll.

Which bucket a call lands in is decided by the server's clock, so a call that crosses a boundary is re-run against the bucket the server named. If this process is stalled for longer than a whole window in between, every attempt misses and the call throws `BudgetWindowRolledError` (exported from `benni/primitives`) rather than inventing an answer. Nothing was applied, so it is safe to retry, and a hold whose `settle` throws it is still usable.

## Cluster Safety

Each id's two window buckets, its reservation set, and its settle markers share a `{<id>}` hash tag, so they live on one node and the scripts can touch them together. Different ids still spread across the keyspace. See [Redis Cluster](/benni/advanced/cluster/).

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `limit` | required | Units allowed per window. Must be a whole number. |
| `windowMs` | required | Window length in milliseconds. |
| `prefix` | `"budget"` | Key namespace. |
| `holdTtlMs` | `120000` | How long a reservation counts before lapsing. |
| `maxHolds` | `10000` | Most reservations one id may hold at once. |

## When You Don't Need This

- **You are limiting request rate, not spend.** Use [`ratelimit`](/benni/primitives/ratelimit/); it is exact and cheaper.
- **You are limiting concurrency.** "At most 20 calls in flight" is [`semaphore`](/benni/primitives/semaphore/).
- **You need per-request billing records.** This is a guardrail, not a ledger. Keep the ledger in your database and use this to stop runaway spend before it happens.

## See Also

- [Rate Limiting](/benni/primitives/ratelimit/) for request-count limits
- [Semaphore](/benni/primitives/semaphore/) for concurrency limits
- [AI Apps](/benni/patterns/ai-apps/) for how these compose

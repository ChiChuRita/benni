---
"beni": minor
---

Add three primitives to `beni/primitives`: `budget`, `semaphore`, and `idempotency`.

These came out of auditing the existing primitives against what people actually install. `queue` and `lock` hold up, `cache` is narrow but sound, and `ratelimit` was a strict subset of @upstash/ratelimit and rate-limiter-flexible. Rather than chase their feature lists and ship a worse clone, we went after gaps nobody fills.

**`budget` — cost-weighted spend limits.** Rate limits count requests, but model calls are not priced by the request: one 200k-token call costs what fifty 4k-token calls cost, so "100 requests/minute" caps nothing you care about. `budget` counts the unit you are billed in.

```ts
const budgets = budget(client, { limit: 2_000_000, windowMs: 86_400_000 });

// Cost known up front.
const { ok, remaining } = await budgets.charge(userId, promptTokens);

// Cost known only after the call: hold an estimate, then reconcile.
const hold = await budgets.reserve(userId, 8_000);
if (!hold) return new Response("Budget exhausted", { status: 429 });
try {
  const res = await callModel();
  await hold.settle(res.usage.totalTokens);
} catch {
  await hold.release();
}
```

Reservations are the part that is genuinely missing elsewhere. You cannot know a call's real cost until it returns, so check-then-spend is a race: ten concurrent requests all see room and collectively blow the budget. A hold counts against everyone else from the moment it is taken and is replaced by the real number on settle. It is a lease, not a lock, so a caller that dies stops counting on its own. Settling twice on a handle charges once (the token never leaves the process, so a duplicate is always the same handle); settling after the hold lapsed still charges, because the money was spent. Every existing answer to this problem (LiteLLM, Agent Gateway, TrueFoundry) is a gateway you deploy rather than a library you import.

**`semaphore` — bounded concurrency.** "At most 20 calls in flight" is a different constraint from "100 calls per minute", and providers enforce both. `p-limit` solves it inside one process; the moment you run two instances the limit is per-instance and the provider sees the sum. Same handle, `run`, and retry options as `lock`, so it is `lock` with a number. Slots are held by leases, so a crashed holder cannot wedge the pool.

**`idempotency` — exactly-once side effects.** Stripe-style `Idempotency-Key` for POST handlers: a retried request must not charge the card twice and must return the original response. A losing concurrent caller waits for the winner's result by default, so a double-click gets the same receipt rather than a 409. Distinct from `cache`, which may freely recompute a pure read.

Amounts in `budget` must be whole numbers, since the counters underneath are Redis integers; budget in the smallest unit you meter.

All three take their time from the Redis server rather than the caller, work over every adapter including `beni/upstash`, and are cluster-safe by construction. Their concurrency guarantees are proved against a live server, not a fake client: 50 concurrent `semaphore` runs never exceed the limit, 20 concurrent `reserve` calls admit exactly the number that fit, and 10 concurrent `idempotency` calls run the handler once.

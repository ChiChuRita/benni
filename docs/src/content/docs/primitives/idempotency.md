---
title: "Idempotency"
description: "Exactly-once side effects keyed by a client-supplied Idempotency-Key, replaying the original response to retries instead of running the effect twice."
---

A retried POST must not charge the card twice, and it must return the *first* response rather than a fresh one. That is the [Stripe `Idempotency-Key`](https://docs.stripe.com/api/idempotent_requests) contract, and clients retry far more often than you would like: double-clicks, mobile reconnects, proxy timeouts, and every SDK with automatic retries.

```ts
import { idempotency } from "benni/primitives";

const once = idempotency<Receipt>(client);

export async function POST(request: Request) {
  const { value, replayed } = await once.run(
    request.headers.get("Idempotency-Key"),
    () => chargeCard(order)
  );
  return Response.json(value, { headers: { "Idempotent-Replay": String(replayed) } });
}
```

The first caller runs the handler and stores its result. Every later caller with that key gets the stored result back, without the handler running again.

## Not A Cache

The two look alike and behave differently in the way that matters. A cache may recompute a pure read whenever it likes; a miss costs latency. Here a "miss" costs a second charge on someone's card, so the effect must run exactly once and the *stored* outcome must be replayed even if recomputing would be cheap.

Which is why [`cache`](/benni/primitives/cache/) is keyed by what you are reading, and this is keyed by the request the client made.

## Concurrent Duplicates

A double-click sends two requests before either finishes. The loser waits for the winner's result and returns the same receipt:

```ts
// Both calls return { id: "rcpt_1" }. chargeCard runs once.
const [a, b] = await Promise.all([
  once.run("key-1", () => chargeCard(order)),
  once.run("key-1", () => chargeCard(order))
]);
```

If you would rather reject than wait, `onConflict: "throw"` raises `IdempotencyConflictError` while another caller holds the key. Waiting gives up after `waitTimeoutMs` with `IdempotencyTimeoutError` rather than hanging forever.

## Optional Keys

Passing `null`, `undefined`, or `""` runs the handler unguarded and reports `replayed: false`, so you can forward an optional header straight through:

```ts
// No branching on whether the client sent a key.
await once.run(request.headers.get("Idempotency-Key"), handler);
```

## Failures Release The Key

**If the handler throws, the record is deleted so the operation can be retried.** That is right for the failures you actually see, a timeout or a 503, where the client should be able to try again with the same key.

It also means a handler that fails *after* a partial side effect will repeat that part. This is an idempotency key, not a transaction. Either make the effect safe to repeat, or record progress inside it:

```ts
await once.run(key, async () => {
  const charge = await stripe.charges.create(
    { amount, currency: "usd" },
    { idempotencyKey: key }   // pass it downstream too
  );
  await db.orders.markPaid(order.id, charge.id);
  return toReceipt(charge);
});
```

Forwarding the same key to the downstream provider is the belt-and-braces version, and worth doing whenever the provider supports it.

## When The Result Cannot Be Stored

If the handler succeeds but storing its result fails, `run` throws
`IdempotencyNotRecordedError` rather than returning normally. That is
deliberate: the side effect happened, but nothing was recorded, so the running
marker will lapse and a later call with the same key will run the handler
again. Reporting plain success would hide exactly the guarantee you came here
for.

Treat it as indeterminate rather than as a failure. The work is done, and the
error carries the result so you can still use it:

```ts
try {
  const { value } = await once.run(key, () => chargeCard(order));
  return Response.json(value);
} catch (error) {
  if (error instanceof IdempotencyNotRecordedError) {
    // The charge went through; only the record of it did not. Return it, and
    // do not let the client retry blind.
    return Response.json(error.value, { status: 200 });
  }
  throw error;
}
```

The usual causes are a codec that cannot encode the result, or a Redis blip
between finishing the work and recording it.

## Inspecting

```ts
await once.peek("key-1");    // the stored result, or null if absent or running
await once.forget("key-1");  // drop it so the next call runs again
```

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `ttlMs` | `86400000` | How long a result stays replayable (24h, matching Stripe). |
| `prefix` | `"idem"` | Key namespace. |
| `codec` | `codecs.json<T>()` | How the result is stored. |
| `runningTtlMs` | `waitTimeoutMs` | How long one caller may hold the key before others assume it died. |
| `onConflict` | `"wait"` | `"wait"` for the holder's result, or `"throw"`. |
| `waitTimeoutMs` | `30000` | How long to wait under `"wait"`. |
| `pollMs` | `50` | Poll interval while waiting. |

Size `runningTtlMs` to your slowest handler. Too short and a second caller assumes the first died and runs the effect again, which is the failure this primitive exists to prevent.

## When You Don't Need This

- **The operation is naturally idempotent.** A `PUT` that sets a value needs no key.
- **You are caching a read.** Use [`cache`](/benni/primitives/cache/); it has stampede protection and no exactly-once bookkeeping to pay for.
- **The work is long-running.** Hand it to the [queue](/benni/primitives/queue/), which takes an `idempotencyKey` of its own and gives you a job to poll.

## See Also

- [Cache](/benni/primitives/cache/)
- [AI Job Queue](/benni/primitives/queue/), which has idempotency built in
- [Next.js integration](/benni/integrations/nextjs/)

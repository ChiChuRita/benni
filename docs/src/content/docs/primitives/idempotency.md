---
title: "Idempotency"
description: "Run a side effect once per client-supplied Idempotency-Key and replay the original response to retries, with the exact guarantee spelled out: what happens on a throw, a crash, and a lost claim."
---

A retried POST must not charge the card twice, and it must return the *first* response rather than a fresh one. That is the [Stripe `Idempotency-Key`](https://docs.stripe.com/api/idempotent_requests) contract, and clients retry far more often than you would like: double-clicks, mobile reconnects, proxy timeouts, and every SDK with automatic retries.

```ts
// schema.ts
import { idempotency } from "benni/schema";

export const charges = idempotency<Receipt>("charge");
```

```ts
// app.ts
export async function POST(request: Request) {
  const body = await request.text();
  const { value, replayed } = await redis.query.charges.run(
    request.headers.get("Idempotency-Key"),
    ({ signal }) => chargeCard(JSON.parse(body), { signal }),
    { fingerprint: await sha256(body) }
  );
  return Response.json(value, { headers: { "Idempotent-Replay": String(replayed) } });
}
```

The first caller runs the handler and stores its result. Every later caller with that key gets the stored result back, without the handler running again.

## What It Guarantees

Precisely: **at most one caller runs the handler for a key at a time, and once one succeeds, its result is replayed for `ttlMs`.** The claim is a Redis key holding a running marker, renewed every quarter of `runningTtlMs` while the handler runs, so a 35-second charge under the 30-second default keeps its claim for as long as it takes.

That is not exactly-once, and it is worth knowing the three ways the handler can run again:

| What happens | What you see | Can the handler run again? |
| --- | --- | --- |
| The handler throws | Its error, and the key is released | Yes: the next call with the key runs it. See [Failures Release The Key](#failures-release-the-key) |
| The process dies mid-handler | Nothing, in that process | Yes: the claim lapses after `runningTtlMs`, and the next call runs it, whatever the first got done |
| The claim is lost while the handler runs: renewal could not reach Redis for `runningTtlMs`, the event loop was blocked that long, or a failover promoted a replica that never saw the claim | The handler's `signal` aborts with `IdempotencyLeaseLostError`; if the handler resolves anyway, `run` throws `IdempotencyNotRecordedError` with that as its `cause` | Yes, possibly *concurrently*: another caller may already have claimed the key |
| The result cannot be stored | `IdempotencyNotRecordedError`, carrying the value | Yes. See [When The Result Cannot Be Stored](#when-the-result-cannot-be-stored) |

Redis replication is asynchronous, so a failover can lose a claim or a stored result that was acknowledged moments earlier. Nothing in a single Redis key can close that gap. What does is making the effect itself idempotent downstream: pass the same key to your payment provider, or check a unique constraint in your database. This primitive then does the part it is good at, turning a burst of retries into one call and one consistent response.

### The Handler's `signal`

The handler receives `{ signal }`. It aborts the moment the claim is known to be lost, so an effect that has not been committed yet can stop instead of racing a second caller:

```ts
await once.run(key, async ({ signal }) => {
  const quote = await pricing.quote(order, { signal });
  signal.throwIfAborted(); // last check before the irreversible step
  return payments.capture(quote);
});
```

A handler that rejects with the abort reason releases the key like any other throw, so the retry is clean.

Declared as a schema value it lands in [`redis.query`](/benni/core-concepts/schema-registry/) and needs no client of its own. Code that holds a client but no schema module reaches the same runner with `benni({ client }).store(idempotency("charge", { codec: json<Receipt>() }))`.

## Not A Cache

The two look alike and behave differently in the way that matters. A cache may recompute a pure read whenever it likes; a miss costs latency. Here a "miss" costs a second charge on someone's card, so the effect must run once and the *stored* outcome must be replayed even if recomputing would be cheap.

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

## Fingerprints: One Key, One Request

An idempotency key names one request. A client that reuses a key with a different body (a bug, usually) should get an error, not the first request's receipt. Pass a `fingerprint` computed from the request, and a mismatch throws `IdempotencyFingerprintMismatchError` before anything runs:

```ts
const body = await request.text();
try {
  const { value } = await once.run(key, () => charge(JSON.parse(body)), {
    fingerprint: await sha256(body)   // any stable digest of what identifies the request
  });
  return Response.json(value);
} catch (error) {
  if (error instanceof IdempotencyFingerprintMismatchError) {
    return new Response("Idempotency-Key reused with a different body", { status: 422 });
  }
  throw error;
}
```

The fingerprint is stored with the running marker and with the result, so a mismatch is caught both while the first request is still running and after it finished. It is compared only when both calls passed one: records written without a fingerprint (by older code, say) are still replayed. Hash the body rather than passing it verbatim; the fingerprint lives in Redis for `ttlMs`.

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
deliberate: the side effect happened, but nothing was recorded, so a later call
with the same key will run the handler again. Reporting plain success would
hide exactly the guarantee you came here for.

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

The usual causes are a codec that cannot encode the result, a Redis blip
between finishing the work and recording it, or a lost claim, in which case
`error.cause` is an `IdempotencyLeaseLostError` and another caller may have run
the handler too.

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
| `runningTtlMs` | `waitTimeoutMs` | How long a caller that died mid-handler blocks the key before another may run it. The claim is renewed every quarter of this while the handler runs, so it does not bound the handler. |
| `onConflict` | `"wait"` | `"wait"` for the holder's result, or `"throw"`. |
| `waitTimeoutMs` | `30000` | How long to wait under `"wait"`. A handler still running after this makes waiters throw `IdempotencyTimeoutError`. |
| `pollMs` | `50` | Poll interval while waiting. |

`run(key, fn, options)` takes one option per call:

| Option | Default | What it does |
| --- | --- | --- |
| `fingerprint` | none | A digest of the request; a mismatch with the one recorded for the key throws `IdempotencyFingerprintMismatchError`. |

`runningTtlMs` is a crash-recovery window, not a timeout. Shorter means a crashed handler holds retries up for less time; longer means a Redis blip or a stalled event loop has to last longer before the claim is lost.

## Three Idempotency Mechanisms

Benni has three things that sound alike and guard different doors:

| | What it dedupes | Keyed by | How long it remembers | On a crash mid-work |
| --- | --- | --- | --- | --- |
| **`idempotency`** (this page) | Running a request handler: one run, one replayed response | The client's `Idempotency-Key` | `ttlMs` (24h) after success; nothing after a throw | The claim lapses after `runningTtlMs` and the next call runs again |
| **The queue's `idempotencyKey`** | *Enqueueing* a job: the same key returns the same job instead of a second one | The key you pass to `enqueue` | While the job runs, then `idempotencyTtlMs` after it completes; a failed or cancelled job releases it | The job is reclaimed and retried under the queue's leases and `maxAttempts`, so its handler can run more than once |
| **`budget`'s settle-once marker** | *Charging* a reservation: a retried `settle` charges nothing extra | The hold's token, internal | `holdTtlMs` after the first settle | Nothing to recover: the hold lapses on its own and stops counting |

Use this one at the HTTP edge, the queue's key when the work is a job, and never think about the budget's: it is what makes `hold.settle()` safe to retry after a lost reply.

## When You Don't Need This

- **The operation is naturally idempotent.** A `PUT` that sets a value needs no key.
- **You are caching a read.** Use [`cache`](/benni/primitives/cache/); it has stampede protection and no run-once bookkeeping to pay for.
- **The work is long-running.** Hand it to the [queue](/benni/primitives/queue/), which takes an `idempotencyKey` of its own and gives you a job to poll.

## See Also

- [Cache](/benni/primitives/cache/)
- [AI Job Queue](/benni/primitives/queue/), which has idempotency built in
- [Next.js integration](/benni/integrations/nextjs/)

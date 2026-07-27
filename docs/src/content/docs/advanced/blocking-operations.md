---
title: "Blocking Operations"
description: "Block on lists, sorted sets, and streams from a session with a required timeout and typed multi-key attribution."
---

Blocking commands park a connection until an entry is available or the timeout elapses. Because they monopolize a connection, they live only on the [session](/beni/advanced/sessions/) accessors (`session.list(x)`, `session.zset(x)`, and `session.stream(x)`), never on the shared client. Calling one on `redis.list(x)` is a compile error.

## The Timeout Is Required

Every blocking method takes a `{ timeoutSeconds }` option. There is no default: forgetting it is a type error, not a silent block-forever.

```ts
await using s = await redis.session();
const job = await s.list(jobs).blpop("pending", { timeoutSeconds: 5 });
```

The unit is in the name, and fractional values are allowed (`{ timeoutSeconds: 0.1 }`). `0`, negatives, `NaN`, and `Infinity` throw a `TypeError`. Redis treats a `0` timeout as block-forever, and arriving there through arithmetic is a shutdown hazard.

To block forever, spell it out with the literal `{ timeoutSeconds: "forever" }`:

```ts
const job = await s.list(jobs).blpop("pending", { timeoutSeconds: "forever" });
//    ^? Job, never null: the call either resolves a value or rejects on close
```

`{ timeoutSeconds: "forever" }` is a visible, greppable literal rather than a stray `0`. As a bonus it narrows the return type: a forever call cannot time out, so `null` drops from the result. `close()` still rejects a forever-blocked call promptly, so `await using` never hangs.

## Lists

Session list stores add the blocking pops and blocking move on top of the [regular list methods](/beni/data-structures/sets-and-lists/):

```ts
await using s = await redis.session();
const queue = s.list(jobs);

const job = await queue.blpop("pending", { timeoutSeconds: 5 });   // BLPOP  -> Job | null
const tail = await queue.brpop("pending", { timeoutSeconds: 5 });  // BRPOP  -> Job | null

// BLMOVE: pop from one end of source, push to one end of destination
const moved = await queue.blmove(
  "pending", "processing", "left", "right", { timeoutSeconds: 5 }
); // -> Job | null
```

A single-key pop returns the decoded value or `null` on timeout.

## Sorted Sets

Session sorted-set stores add blocking pops of the lowest or highest scoring member:

```ts
const s = await redis.session();
const min = await s.zset(priorities).bzpopmin("queue", { timeoutSeconds: 5 });
//    ^? { member: string; score: number } | null   (BZPOPMIN)
const max = await s.zset(priorities).bzpopmax("queue", { timeoutSeconds: 5 }); // BZPOPMAX
await s.close();
```

Each returns a `{ member, score }` entry or `null` on timeout.

## Streams

Session stream stores add a blocking read for entries newer than an id:

```ts
const batch = await s.stream(auditEvents).xread(
  "login", lastSeenId, { timeoutSeconds: 5, count: 100 }
); // XREAD BLOCK -> StreamEntry[]  ([] on timeout)
```

`xread` with a `timeoutSeconds` wraps `XREAD BLOCK` and returns an empty array on timeout, matching non-blocking `xread`'s null-to-`[]` convention. The `afterEntryId` accepts a concrete entry id or `"$"`; track the last seen id across iterations, because `"$"` re-arms "from now" on each call and can miss entries that arrive between calls. For at-least-once delivery across many workers, use [consumer groups](/beni/data-structures/consumer-groups/) instead.

## Typed Id Attribution Across Keys

Passing an **array** of keys blocks across several keys at once and tells you which key answered, with the id typed to exactly the keys you passed:

```ts
const hit = await s.list(jobs).blpop(["urgent", "pending"], { timeoutSeconds: 5 });
if (hit) {
  console.log(hit.id, hit.value);
  //         ^? "urgent" | "pending"    ^? Job
}
```

The answering key from the reply is reverse-mapped back to your typed id, so literal id types survive the round trip. The sorted-set forms mirror this shape:

```ts
const hit = await s.zset(priorities).bzpopmin(["high", "low"], { timeoutSeconds: 5 });
//    ^? { id: "high" | "low"; entry: { member: string; score: number } } | null
// brpop and bzpopmax are the mirror-image variants.
```

## Non-Blocking Multi-Key Pops

`LMPOP` and `ZMPOP` never block, so they land on the **shared** store with the same typed attribution shape, no session needed:

```ts
const hit = await redis.list(jobs).lmpop(["urgent", "pending"], { direction: "left", count: 10 });
//    ^? { id: "urgent" | "pending"; values: Job[] } | null

const scored = await redis.zset(priorities).zmpop(["high", "low"], { min: true, count: 10 });
//    ^? { id: "high" | "low"; entries: Array<{ member: string; score: number }> } | null
// lmpop with { direction: "right" } and zmpop with { max: true } are the mirror-image variants.
```

These check the keys in order and return `null` only when all of them are empty. A session inherits them too, since it reuses the same stores, but reach for the blocking `blpop`/`brpop`/`blmove`/`blmpop` when you want to wait for work rather than poll.

## Blocking Counted Multi-Key Pops

`BLMPOP` and `BZMPOP` are the blocking counterparts of `LMPOP`/`ZMPOP`: they pop up to `count` entries from the first non-empty of several keys, blocking until one has data. Like the other blocking commands they are **session-only**, and they carry the same typed attribution:

```ts
await using s = await redis.session();

const hit = await s.list(jobs).blmpop(["urgent", "pending"], {
  direction: "left",
  timeoutSeconds: 5,
  count: 10
});
//    ^? { id: "urgent" | "pending"; values: Job[] } | null

const scored = await s.zset(priorities).bzmpop(["high", "low"], { min: true, count: 10 }, {
  timeoutSeconds: "forever"
});
//    ^? { id: "high" | "low"; entries: Array<{ member: string; score: number }> }
// blmpop with { direction: "right" } and bzmpop with { max: true } are the mirror-image variants.
```

Mind the distinction from `blpop` on a key array (`BLPOP`) above: `blmpop` (`BLMPOP`) pops a **counted batch** from the first non-empty key and returns `values`/`entries` arrays, while `blpop` on a key array (`BLPOP`) pops a **single** item and returns one `value`/`entry`. As with every blocking call, `{ timeoutSeconds: "forever" }` drops `null` from the return type.

## A Reliable Worker Queue

A `BLPOP` reply served onto a connection that dies before the reply is read is lost. The durable pattern is `blmove` (`BLMOVE`) into a per-worker processing list: the move is atomic server-side, so a crash leaves the job recoverable in the processing list. On startup, drain that list before blocking for new work.

```ts
// schema.ts
import { list, json } from "beni/schema";
type Job = { id: string; kind: "email" | "report" };
export const jobs = list("jobs", json<Job>());

// worker.ts: reliable BLMOVE loop, shutdown- and crash-safe
const processing = `worker-${process.pid}`;
const stop = new AbortController();
process.on("SIGTERM", () => stop.abort());

// startup recovery: drain this worker's processing list from a previous crash
for (
  let job = await redis.list(jobs).lpop(processing);
  job;
  job = await redis.list(jobs).lpop(processing)
) {
  await handle(job);
}

while (!stop.signal.aborted) {
  await using s = await redis.session();          // one extra connection, owned here
  const abort = () => void s.close();          // close() rejects an in-flight block in ~ms
  stop.signal.addEventListener("abort", abort, { once: true });
  try {
    const queue = s.list(jobs);
    while (!stop.signal.aborted) {
      // redis.list(jobs).blmove(...) would be a compile error; session only.
      const job = await queue.blmove(
        "pending", processing, "left", "right", { timeoutSeconds: 5 }
      );
      if (job === null) continue;              // heartbeat tick: re-check the stop signal
      await handle(job);
      await redis.list(jobs).lrem(processing, 1, job); // ack via the shared client
    }
  } catch (error) {
    if (s.closed) break;                       // shutdown or dropped connection
    await sleep(1000);                          // recover: the outer loop opens a fresh session
  } finally {
    stop.signal.removeEventListener("abort", abort);
  }
}
```

The `{ timeoutSeconds: 5 }` block doubles as a heartbeat: it returns `null` every five seconds so the loop can re-check the stop signal even when the queue is idle. Closing the session on `SIGTERM` rejects the in-flight block within milliseconds rather than waiting out the timeout.

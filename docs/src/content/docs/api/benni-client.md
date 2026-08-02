---
title: "Benni Client"
description: "Create a Benni client by passing a Redis adapter to benni(), then reach every schema through typed data-structure accessors."
---

Create a Benni client by passing a Redis adapter to `benni`. It takes either shape, and they are the same call:

```ts
import { benni } from "benni";
import { node } from "benni/node";

// One object, no top-level await: the promise resolves on the first command.
const redis = benni({ client: node({ url }), schema });

// Or a client you already have.
const redis = benni(client, { schema });
```

The `client` accepts a connected `RedisClient`, a promise of one, a factory returning either, or another Benni handle. The cost of either lazy form is that a bad `REDIS_URL` surfaces at the first command instead of at startup.

The two lazy forms differ in one way worth knowing:

| Source | When it connects | `close()` before the first command | After a failed connect |
| --- | --- | --- | --- |
| A promise (`node({ url })`) | Already connecting when you pass it | Closes the client it opened | Every command reports that same failure: a settled promise cannot be retried |
| A factory (`() => node({ url })`) | On the first command | Opens nothing | The next command calls the factory again |

Reach for the factory when a module is loaded in a context that must not connect at all, which is why `benni/next`'s `cacheHandler` documents one: Next.js loads `cache-handler.mjs` at build time.

`BenniOptions` has three fields, all optional:

| Option | Effect |
| --- | --- |
| `schema` | The schema module that backs [`redis.query`](#redisquery). |
| `onPubSubError` | Called when a Pub/Sub handler throws (see [`redis.pubsub`](#redispubsub)). Without it, the error is rethrown asynchronously rather than swallowed. |
| `cluster` | Check, before sending, that every key in a multi-key command hashes to one Redis Cluster slot, throwing `CrossSlotError` when it does not. Off by default. See [Redis Cluster](/benni/advanced/cluster/). |

Everything else a client can do follows from the adapter you pass in.

Every data-structure accessor exposes the store's methods plus `key(id)` for the full Redis key and `del(id)`.

## Registering The Schema Module

Declare the schema module once through the `Register` interface and the bare `Benni` type is the fully typed handle everywhere, so no signature has to repeat `typeof schema`:

```ts
// redis.ts
import * as schema from "./schema";

export const redis = benni({ client: node({ url }), schema });

declare module "benni" {
  interface Register {
    schema: typeof schema;
  }
}
```

```ts
import type { Benni } from "benni";

export function makeHandlers(redis: Benni) {
  // redis.query.users, redis.hash(...), ... all fully typed
}
```

Registration is optional and changes nothing else. Without it, `Benni` stays generic over the open schema type and you name the module explicitly:

```ts
export function makeHandlers(redis: Benni<typeof schema>) { /* ... */ }
```

Pass the generic explicitly for a second handle bound to a different module, too: the registration sets the default, not a ceiling.

## `redis.query`

The schema registry. When a `{ schema }` module is bound, `redis.query.<exportName>` resolves each schema to its typed resource, dispatched by the schema's `kind`:

```ts
await redis.query.users.hset("42", { name: "Ada", score: 10 });

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number } | null

await redis.query.leaderboard.zadd("daily", [{ member: "ada", score: 100 }]);
```

`redis.query.<name>` returns the same resource as the matching `redis.<kind>(schema)` accessor. It covers the twelve data kinds (`kv`, `hash`, `set`, `list`, `zset`, `stream`, `bitmap`, `geo`, `hll`, pub/sub channels and patterns, and scripts) and the seven primitives (`cache`, `ratelimit`, `queue`, `lock`, `semaphore`, `idempotency`, `budget`):

```ts
await redis.query.userEvents.publish({ id: "42", action: "created" });
await redis.query.rateLimit.run({ keys: { counter: "user:42" }, args: { limit: 100 } });

// A primitive declared in the same module, reached the same way.
const profile = await redis.query.profiles.get(userId, () => db.load(userId));
```

Counter and string stores are not separate kinds, so a `kv` schema always maps to the `kv` resource. `redis.query.<name>` on a `kv(prefix, number())` therefore has `get` / `set` / `del` but no `incr`: reach for `redis.counter(schema)` for the counter commands and `redis.string(schema)` for the string ones. Both work on the same keys as the `kv` resource, so mixing them on one schema is fine.

```ts
await redis.query.clicks.set("home", 0);          // kv resource
await redis.counter(clicks).incr("home");         // counter view, not on redis.query
```

Non-schema exports (types, helpers) are dropped, and `redis.query` is `{}` when no schema is bound. See [Schema Registry](/benni/core-concepts/schema-registry/).

## `redis.kv(schema)`

Typed Redis string values:

```ts
await redis.kv(profiles).set("42", profile, { ttlSeconds: 3600 });
const loaded = await redis.kv(profiles).get("42");
await redis.kv(profiles).del("42");
```

`set` returns `Promise<void>` for plain writes. With `{ nx: true }` (only create) or `{ xx: true }` (only update) it returns `Promise<boolean>` indicating whether the write happened:

```ts
const created = await redis.kv(profiles).set("42", profile, { nx: true });
const updated = await redis.kv(profiles).set("42", profile, { xx: true });
```

## `redis.string(schema)`

String operations for `kv` schemas with a `string()` codec:

```ts
const drafts = kv("draft", string());

await redis.string(drafts).append("42", " more text");
const slice = await redis.string(drafts).getrange("42", 0, 4);
const length = await redis.string(drafts).strlen("42");
const value = await redis.string(drafts).getex("42", 3600);

// LCS: longest common subsequence of two keys in the same schema.
const sub = await redis.string(drafts).lcs("42", "43"); // the subsequence string
const len = await redis.string(drafts).lcs("42", "43", { len: true }); // its length
const idx = await redis.string(drafts).lcs("42", "43", {
  idx: true,
  withMatchLen: true
});
//    ^? { matches: { a: [number, number]; b: [number, number]; length?: number }[]; length: number }
```

`getrange`, `setrange`, and `strlen` work in **bytes**, not string indices,
because that is how Redis indexes a string. For ASCII the two are the same. For
anything else they are not: `"café"` is 5 bytes and 4 characters. A range
boundary that falls inside a multi-byte character decodes to the replacement
character, so read the whole value with `getrange(id, 0, -1)`, or split your
chunks on byte boundaries you computed yourself.

## `redis.counter(schema)`

Atomic counters for `kv` schemas with a `number()` codec:

```ts
const hits = kv("hits", number());

const total = await redis.counter(hits).incr("42");
await redis.counter(hits).incrby("42", 10);
await redis.counter(hits).decrby("42", 3);
```

Redis counters are 64-bit. Once a counter passes `Number.MAX_SAFE_INTEGER` its
value can no longer be represented exactly as a JavaScript number, so the
integer commands throw a `ReplyShapeError` rather than resolve a rounded one.
The same applies to `BITFIELD` reads of the wide encodings (`i64`, `u63`).

## `redis.hash(schema)`

Typed Redis hashes:

```ts
await redis.hash(users).hset("42", { name: "Ada", score: 10 });
await redis.hash(users).hset("42", "score", 11);
const user = await redis.hash(users).hget("42");
const field = await redis.hash(users).hrandfield("42");
```

## `redis.set(schema)`

Typed Redis sets:

```ts
await redis.set(teamMembers).sadd("engineering", ["ada"]);
const members = await redis.set(teamMembers).smembers("engineering");
```

## `redis.list(schema)`

Typed Redis lists:

```ts
await redis.list(events).rpush("user:42", [event]);
const recent = await redis.list(events).lrange("user:42", 0, 9);
```

## `redis.zset(schema)`

Typed Redis sorted sets:

```ts
await redis.zset(leaderboards).zadd("weekly", [
  { member: "user:42", score: 100 }
]);

const top = await redis.zset(leaderboards).zrange("weekly", {
  start: 0,
  stop: 9,
  rev: true
});
```

When members share a score, `zrange` with `{ byLex: true }` ranges over them lexically, as do `zlexcount`, `zremrangebylex`, and `zrangestore` with `{ byLex: true }`:

```ts
const names = await redis.zset(nameIndex).zrange("directory", {
  byLex: true,
  min: { value: "ada" },
  max: "+"
});
```

See [Lexicographic Ranges](/benni/data-structures/sorted-sets/#lexicographic-ranges).

## `redis.hll(schema)`

Typed Redis HyperLogLog values:

```ts
await redis.hll(pageViews).pfadd("2026-07-04", ["user:42"]);
const count = await redis.hll(pageViews).pfcount("2026-07-04");
```

## `redis.stream(schema)`

Typed Redis streams:

```ts
await redis.stream(activity).xadd("42", { action: "login", points: 5 });
const entries = await redis.stream(activity).xrange("42", { count: 10 });
```

`.group(name)` opens a consumer group on the stream for at-least-once delivery across workers:

```ts
const group = redis.stream(activity).group("processors");
await group.create("42", { from: "start" });
const batch = await group.consumer("w-1").xreadgroup("42", { count: 10 });
```

See [Consumer Groups](/benni/data-structures/consumer-groups/).

## `redis.bitmap(schema)`

Typed Redis bitmaps:

```ts
await redis.bitmap(dailyActive).setbit("2026-07-04", 42, true);
const total = await redis.bitmap(dailyActive).bitcount("2026-07-04");

// Packed integer fields via BITFIELD; the result tuple is typed to the chain.
const [visits] = await redis.bitmap(dailyActive)
  .bitfield("2026-07-04")
  .incrby("u32", 0, 1)
  .exec();
```

## `redis.geo(schema)`

Typed Redis geospatial indexes:

```ts
await redis.geo(stores).geoadd("berlin", [
  { member: "store:1", longitude: 13.405, latitude: 52.52 }
]);

const nearby = await redis.geo(stores).geosearch("berlin", {
  from: { longitude: 13.4, latitude: 52.52 },
  by: { radius: 5, unit: "km" }
});
```

## `redis.scan`

Async-iterable scans over keys and collection members:

```ts
for await (const key of redis.scan.keys({ match: "user:*" })) {
  console.log(key);
}

for await (const key of redis.scan.kv(profiles)) { /* profile:* keys */ }
for await (const member of redis.scan.set(teamMembers, "engineering")) { /* ... */ }
for await (const entry of redis.scan.hash(users, "42")) { /* { field, value } */ }
for await (const entry of redis.scan.zset(leaderboards, "global")) { /* { member, score } */ }
```

See [Scans](/benni/advanced/scans/) for options and iteration guarantees.

## `redis.pubsub`

Typed publish and subscribe. `PUBLISH` is a stateless command, so publishing rides the bound client and works on every adapter; it returns the number of subscribers Redis delivered to:

```ts
const receivers = await redis.pubsub.channel(userEvents).publish({
  id: "42",
  action: "created"
});
```

`subscribe` takes just a handler and returns a subscription with `unsubscribe()`. The first subscription lazily leases one subscriber connection from the client and every channel and pattern is multiplexed onto it; it closes when the last subscription goes away:

```ts
const subscription = await redis.pubsub.channel(userEvents).subscribe((message) => {
  // message is the channel's decoded output type
});

await subscription.unsubscribe();
```

`redis.pubsub.pattern(...).subscribe(handler)` receives every matching channel, and the handler's second argument is the concrete channel name:

```ts
const patternSubscription = await redis.pubsub
  .pattern(userEventPattern)
  .subscribe((message, channelName) => { /* ... */ });
```

`stream(options?)` is the async-iterator form of the same subscription. A channel stream yields decoded messages; a pattern stream yields `{ message, channel }`. Aborting `options.signal` (or leaving the loop) ends iteration and releases the subscription:

```ts
const controller = new AbortController();

for await (const message of redis.pubsub
  .channel(userEvents)
  .stream({ signal: controller.signal })) {
  // ...
}
```

`redis.pubsub.close()` drops every subscription and closes the leased connection. Publishing keeps working afterwards, and the next `subscribe` leases a fresh connection:

```ts
await redis.pubsub.close();
```

Subscribing requires a client that can hold a connection. An adapter advertises this with the optional `subscriber?()` method on the `RedisClient` contract, the pub/sub counterpart to `session?()`:

```ts
import type { RedisClient, RedisSubscriber } from "benni";

declare const client: RedisClient;
//    ^? { send, pipeline, transaction?, session?, subscriber?, close }

declare function open(): Promise<RedisSubscriber>;
//    ^? { subscribe, unsubscribe, psubscribe?, punsubscribe?, closed, close }
```

Benni leases at most one subscriber per client, so adapters do no bookkeeping. When `subscriber` is undefined (the HTTP adapter), `subscribe` throws `TypeError` at call time, the same style as the session guard. `psubscribe`/`punsubscribe` are optional in turn, which is how the Bun adapter reports patterns as unsupported instead of hanging. Pass `onPubSubError` to `benni()` to route a handler that throws; without it the error is rethrown asynchronously rather than swallowed. See [Pub/Sub](/benni/data-structures/pubsub/).

## `redis.session`

Lease a dedicated connection for blocking commands and `WATCH` transactions. The scoped form closes the session when the callback settles; the bare form returns it and hands you the `close()` obligation (pair with `await using`):

```ts
const job = await redis.session(async (s) => {
  return s.list(jobs).blpop("pending", { timeoutSeconds: 5 });
});

await using session = await redis.session();
```

A session carries the same store accessors as the Benni handle, bound to its private connection, where `list`, `zset`, and `stream` are supersets that add the blocking variants and the blocking consumer-group read. It also adds `session.watch(keys)`, `session.unwatch()`, and `session.multi()`, plus `session.raw`, `session.closed`, and `session.close()`. It throws `TypeError` if the client does not support sessions. See [Sessions](/benni/advanced/sessions/), [Blocking Operations](/benni/advanced/blocking-operations/), and [Consumer Groups](/benni/data-structures/consumer-groups/).

## `redis.watch`

Retrying optimistic transaction (`WATCH`/`MULTI`/`EXEC`), discoverable next to `redis.multi()`:

```ts
const result = await redis.watch(
  views.key("home"),
  async (s) => {
    const current = (await s.kv(views).get("home")) ?? 0;
    return s.multi().add(["SET", views.key("home"), String(current + 1)], okReply);
  },
  { attempts: 5, onAbort: ({ attempt }) => metrics.increment("cas.conflict", { attempt }) }
);
//    ^? [void] | null   (null = the body opted out)
```

Each attempt watches the keys, runs the body, and commits the transaction it returns; a conflict retries, a `null` body opts out, and exhausted attempts throw `WatchRetriesExceededError`. See [Optimistic Transactions](/benni/advanced/optimistic-transactions/).

## `redis.raw`

Direct Redis access:

```ts
await redis.raw.send(["PING"]);
await redis.raw.pipeline([
  ["SET", "a", "1"],
  ["GET", "a"]
]);
```

---
title: "Benni Client"
description: "Create a Benni client by passing a Redis adapter to benni(), then reach every schema through typed data-structure accessors."
---

Create a Benni client by passing a Redis adapter to `benni`. It takes one config object:

```ts
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

export const redis = benni({ client: node({ url }), schema });
```

Adapters return their client synchronously and connect on the first command, so this needs no top-level `await` and importing the module opens no connection. A connect that fails rejects the commands that were waiting on it, and a later command tries again, with a backoff that grows from 100 ms to 5 s, so a process that started while Redis was down recovers once Redis is back. To find a bad `REDIS_URL` at startup rather than at the first request, send one command: `await redis.raw.send(["PING"])`.

`client` is an adapter's client (`node(…)`, `ioredis(…)`, `bun(…)`, `upstash(…)`) or another Benni handle, whose client the new handle shares. A raw node-redis or ioredis instance is refused with a message saying how to wrap it: `ioredis(instance)` adopts an ioredis client, and node-redis clients cannot be adopted, so create one with `node({ url })`, which takes every node-redis option.

| Field | Effect |
| --- | --- |
| `client` | Required. The adapter's client, or a Benni handle. |
| `schema` | The schema module that backs [`redis.query`](#redisquery). Required whenever you pass the schema type explicitly (`benni<typeof schema>(…)`), so the type cannot promise a registry the handle does not have. |
| `onPubSubError` | Called when a Pub/Sub handler throws (see [`redis.pubsub`](#redispubsub)). Without it, the error is rethrown asynchronously rather than swallowed. |
| `cluster` | Check, before sending, that every key in a multi-key command hashes to one Redis Cluster slot, throwing `CrossSlotError` when it does not. Off by default. See [Redis Cluster](/benni/advanced/cluster/). |

Everything else a client can do follows from the adapter you pass in, and the handle's type says so: over `benni/upstash` there is no `redis.session()`, `redis.watch()`, or `subscribe()`, and over `benni/bun` no `redis.pubsub.pattern()`. See [What the client can do](#what-the-client-can-do).

Every data-structure accessor exposes the store's methods plus `key(id)` for the full Redis key and `del(id)`.

## Closing

`redis.close()` shuts the handle down, in order: the Pub/Sub subscriptions, then queue workers started through the handle (each drains its in-flight jobs, as `worker.stop()` does), then sessions still open, then the client. `await using` does the same:

```ts
process.on("SIGTERM", async () => {
  await redis.close();
});

// Or, scoped:
await using redis = benni({ client: node({ url }), schema });
```

It closes only what the handle opened. A handle built over another handle leaves that handle's client open, and an ioredis client you adopted with `ioredis(instance)` is never closed: whoever created it owns it. `close()` is idempotent, and every command or lease issued after it rejects instead of reopening a connection nothing would close. `redis.raw.close()` is the same call.

A worker's own `stop()` accepts a `timeoutMs`; `redis.close()` waits for in-flight jobs without one, so stop long-running workers yourself first if shutdown has a deadline.

## What the client can do

Each adapter returns a client type that says which optional capabilities it has, and `benni()` carries that type into the handle:

| Adapter | Client type | `session()` / `watch()` | `subscribe()` | `pubsub.pattern()` |
| --- | --- | --- | --- | --- |
| `benni/node` | `FullRedisClient` | yes | yes | yes |
| `benni/ioredis` | `FullRedisClient` | yes | yes | yes |
| `benni/bun` | `BunClient` | yes | yes | no |
| `benni/upstash` | `UpstashClient` | no | no (publish only) | no |

A member the client cannot back is not on the handle's type at all, so the mistake is a compile error rather than a runtime `UnsupportedCapabilityError` (which stays as the backstop for untyped code). Blocking reads live on sessions, so they follow `session()`.

`Benni<typeof schema>` assumes the full client, as `benni/node` and `benni/ioredis` return. On another adapter, name its client type: `Benni<typeof schema, UpstashClient>` with `import type { UpstashClient } from "benni/upstash"`. A library that accepts any handle takes `AnyBenni`, which offers the capability-free surface.

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

Register in apps only, once per program, and never in a library. The augmentation is global to the compilation: a second one is a compile error (TS2717, "subsequent property declarations must have the same type"), and a library that registers its own schema retypes the bare `Benni` of every app that installs it. In a monorepo, declare it in each app package and nowhere else; shared packages take `AnyBenni` or an explicit `Benni<typeof schema>`.

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
import type { RedisClient, RedisSubscriber } from "benni/core";

declare const client: RedisClient;
//    ^? { send, pipeline, transaction?, session?, subscriber?, close }

declare function open(): Promise<RedisSubscriber>;
//    ^? { subscribe, unsubscribe, psubscribe?, punsubscribe?, closed, close }
```

Benni leases at most one subscriber per client, so adapters do no bookkeeping. When `subscriber` is undefined (the HTTP adapter), the handle's channel resources have `publish` but no `subscribe` or `stream`; code that forces the call anyway gets `UnsupportedCapabilityError`. `psubscribe`/`punsubscribe` are optional in turn, which is how the Bun adapter leaves out patterns instead of hanging on them. Pass `onPubSubError` to `benni()` to route a handler that throws; without it the error is rethrown asynchronously rather than swallowed. See [Pub/Sub](/benni/data-structures/pubsub/).

## `redis.session`

Lease a dedicated connection for blocking commands and `WATCH` transactions. The scoped form closes the session when the callback settles; the bare form returns it and hands you the `close()` obligation (pair with `await using`):

```ts
const job = await redis.session(async (s) => {
  return s.list(jobs).blpop("pending", { timeoutSeconds: 5 });
});

await using session = await redis.session();
```

A session carries the same store accessors as the Benni handle, bound to its private connection, where `list`, `zset`, and `stream` are supersets that add the blocking variants and the blocking consumer-group read. It also adds `session.watch(keys)`, `session.unwatch()`, and `session.multi()`, plus `session.raw`, `session.closed`, and `session.close()`. A handle over a client without sessions (`benni/upstash`) has no `session()`; forced through, it throws `UnsupportedCapabilityError`. See [Sessions](/benni/advanced/sessions/), [Blocking Operations](/benni/advanced/blocking-operations/), and [Consumer Groups](/benni/data-structures/consumer-groups/).

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

Replies come back in the same shape on every adapter: the RESP2 shape, whatever protocol the adapter speaks underneath. So a decoder you write against `redis.raw` behaves identically on Node, ioredis, Bun, and Upstash:

| Redis reply | What `raw` returns | Example |
| --- | --- | --- |
| Simple or bulk string | `string` (UTF-8) | `GET` -> `"benni"` |
| Integer | `number` | `HLEN` -> `2` |
| Nil | `null` | `GET` on a missing key -> `null` |
| Double | decimal `string` | `ZSCORE` -> `"1.5"`, `"inf"` |
| Map | flat array | `HGETALL` -> `["name", "ada", "score", "2"]` |
| Scores and pairs | flat array | `ZRANGE ... WITHSCORES` -> `["a", "1", "b", "2"]` |
| Stream read | `[stream, entries]` pairs | `XREAD` -> `[["events", [["1-1", ["f", "v"]]]]]` |
| Error | rejects with `RedisServerError` | `WRONGTYPE ...` |

A `pipeline` returns one reply per command, in order, and rejects with the first failing command's error. Bun is the adapter this takes work for: it only speaks RESP3, where `HGETALL` is a map and `ZSCORE` a number, so `benni/bun` reshapes its replies to match. The full contract is documented on the `RedisClient` type.

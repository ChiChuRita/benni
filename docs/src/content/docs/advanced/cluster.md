---
title: "Redis Cluster"
description: "Declare where a schema's hash tag goes so multi-key commands stay in one slot, and catch cross-slot mistakes at compile time and before they are sent."
---

Benni models slot **co-location**, not cluster topology. Declare where a schema puts its hash tag and Benni will keep multi-key commands inside one slot, reject the ones that cannot be, and tell you which layout fixes it.

## Benni Does Not Route

Routing is your driver's job. Benni has no transport of its own: it binds to whatever `RedisClient` an adapter hands it, so cluster routing comes from the client underneath. Today that means adopting a cluster-aware ioredis instance through [`benni/ioredis`](/benni/runtime/ioredis/); `benni/node` builds its own single-node `createClient()` and cannot be handed a `createCluster()` one.

Topology discovery, `MOVED`/`ASK` redirects, per-node pools, and failover all stay in the driver, which has had a decade to get them right. What no driver can do for you is know, before you send, that a command's keys belong together. That is the part Benni owns, because Benni is the only client where keys come from schemas rather than string concatenation.

## The Problem

Redis routes a key by CRC16 of the substring between the first `{` and the first `}` after it, or of the whole key when there is no such pair. Every key in a single command must land on the same slot, or the server answers `CROSSSLOT Keys in request don't hash to the same slot`.

With the default `prefix:id` layout, two ids essentially never share a slot:

```ts
const carts = kv("cart", json<Cart>());

carts.key("u1"); // "cart:u1"  -> slot 13083
carts.key("u2"); // "cart:u2"  -> slot 888
```

So `mget(["u1", "u2"])`, `sunionstore`, `zmpop`, `bitop`, `pfmerge`, `lmove`, and every other multi-key method is unusable on a cluster. This works perfectly on a single node, which is exactly why it is discovered in production.

## The Three Layouts

`hashTag` is an opt-in option on every keyed schema. Omitting it changes nothing.

| Option | Key | What it buys |
| --- | --- | --- |
| omitted | `cart:u1` | Today's layout. One slot per id. |
| `hashTag: "prefix"` | `{cart}:u1` | The whole keyspace shares one slot, so every multi-key method over this schema is legal. |
| `hashTag: "id"` | `cart:{u1}` | Keys stay spread, but the same id co-locates across schemas. |

```ts
import { json, kv, zset } from "benni/schema";

// Bounded keyspace, needs within-schema set algebra: pin the whole thing.
const featureFlags = zset("flags", string(), { hashTag: "prefix" });
await redis.zset(featureFlags).zunionstore("all", "beta", ["internal"]);

// Unbounded keyspace, needs per-user co-location: tag the id.
const carts = kv("cart", json<Cart>(), { hashTag: "id" });
const orders = kv("order", json<Order>(), { hashTag: "id" });
// "cart:{u1}" and "order:{u1}" share a slot; "cart:{u2}" does not.
```

### Choosing Between Them

This is a co-location decision, not a compatibility flag.

`hashTag: "prefix"` makes multi-key commands legal by putting an entire keyspace on one node. That is right for a leaderboard, a feature-flag set, or a per-tenant index. It is a production incident for your main user keyspace, which will then be served by one node no matter how many you run.

`hashTag: "id"` keeps the distribution and co-locates everything about one entity. Reach for it whenever the thing you want in a single command is "all the data for user X" rather than "all the users".

Neither is free to adopt later: both change the key format, so turning one on orphans existing data.

A `hashTag: "id"` prefix may not contain `{`. Redis reads the tag from the first `{` in the whole key, so a prefix like `cart{v2}` would take the tag away from the id and quietly undo the co-location the layout exists for. The schema builder rejects it when you declare the schema.

## Compile-Time Checking

Because the tag is part of the key's template-literal type, Benni can reject cross-slot combinations before you run anything. This covers `script().run()`, `redis.watch()`, and the transaction key declaration:

```ts
await redis.script(moveItem).run({
  keys: { from: carts.key("u1"), to: orders.key("u2") },
  //                                  ^ Type '"order:{u2}"' is not assignable to type
  //                                    'KeysMustShareOneHashSlot<"order:{u2}", "u1">'
  args: { amount: 1 }
});
```

The alias name is the error message: these keys must share one hash slot, and the tag they had to match was `u1`.

**A passing check means "no provable conflict", not "provably co-located."** Benni rejects only pairs whose hash tags are distinct string literals. Three things pass silently:

- Untagged keys, so adopting `hashTag` on one schema never breaks unrelated call sites.
- Keys built from runtime ids. `carts.key(userId)` has type `` `cart:{${string}}` ``, and no type system can tell whether two of those hold the same value.
- Raw `multi().add()` commands and the within-schema multi-key methods, which are checked at runtime instead.

That last group is why the runtime guard exists.

## Runtime Checking

Install the guard and every multi-key command is verified before it is sent:

```ts
import { assertSameSlot } from "benni/cluster";

const redis = benni(client, { cluster: assertSameSlot });

await redis.set(sessions).sunion("a1", ["b7"]);
// CrossSlotError: SUNION spans two Redis Cluster hash slots, which the server
// rejects with CROSSSLOT.
//   "sessions:a1" hashes to slot 9716
//   "sessions:b7" hashes to slot 4193
// Every key in one command must hash to the same slot. Declare the schema with
// hashTag: "prefix" so its keys become "{sessions}:<id>" and the whole keyspace
// shares one slot, or with hashTag: "id" so its keys become "sessions:{<id>}"
// and stay spread while co-locating one id across schemas.
```

`CrossSlotError` extends `ValidationError`, carries `command`, `keys`, and `slots`, and is thrown before anything reaches the socket. It is exported from `benni/cluster` alongside `slotOf` and `hashTagOf`.

The guard is **off by default** and should be: cross-slot multi-key commands are perfectly legal on a single-node Redis, and plenty of code relies on that. Turn it on in development and CI, where you want the mistake to surface.

### Why You Pass The Checker, Not `true`

`benni()` has to reference the guard in order to install it, so a `cluster: true` boolean would mean the root entry names it, and no bundler could then drop it. The CRC16 table and the error's fix-hint prose would ship in every app, including every app that never turns the check on. Taking the function as a value moves all of it into `benni/cluster`, which only an app that imports it ever pays for. That is about 1.4 KB gzipped, roughly 15% of the default root entry.

The cost when it *is* installed is close to nothing. The guard compares hash tags as strings first, so a correctly configured schema never runs a CRC at all. When it is absent, each check is an optional call on an undefined function, and an optional call short-circuits its argument evaluation, so the key arrays are never even built.

### Declaring Transaction Keys

`multi()` queues raw command tuples, so Benni cannot see which keys they touch. Declare them:

```ts
await redis
  .multi()
  .keys([carts.key("u1"), orders.key("u1")])
  .add(["INCR", carts.key("u1")], numberReply)
  .add(["SADD", orders.key("u1"), "x"], numberReply)
  .exec();
```

This is a declaration, not a derivation. A key you queue but do not declare is not checked. Keys accumulate across calls, and each call is checked against the first tag seen, including across an intervening `add()`.

## What Is Not Covered

Be clear-eyed about the boundary. A green build is not a cluster-safe build:

- Benni does not route. `MOVED`, `ASK`, topology, and failover are the driver's.
- Sharded pub/sub (`SSUBSCRIBE`/`SPUBLISH`) is not modeled.
- `redis.raw.send()` and `redis.raw.pipeline()` bypass every check by design.
- Each surface validates itself. The keys you `watch()` are not checked against the keys the body's transaction declares.

## Primitives

The bundled primitives are already slot-safe. `lock` and `ratelimit` touch one key per call. `queue` hash-tags every key into one slot, which is also what lets its Lua derive per-job key names. `cache` tags the id, so an entry and its own fill lock share a node while the cache itself stays spread.

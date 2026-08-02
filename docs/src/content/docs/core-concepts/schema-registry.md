---
title: "Schema Registry"
description: "Declare schemas once, bind the module, and reach every store by name through redis.query."
---

Declare schemas once, bind the module, and reach every store by name through `redis.query`.

```ts
// schema.ts
import { hash, kv, zset, json, number, string } from "benni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});

export const profiles = kv("profile", json<{ tier: string }>());
export const leaderboard = zset("leaderboard", string());
```

Bind the module once when you create the client:

```ts
// redis.ts
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

export const redis = benni(await node(), { schema });
```

Then reach each store by its export name, with full inference:

```ts
// app.ts
import { redis } from "./redis";

await redis.query.users.hset("42", { name: "Ada", score: 10 });

const user = await redis.query.users.hgetall("42");
//    ^? { name: string; score: number } | null

await redis.query.leaderboard.zadd("daily", [{ member: "ada", score: 100 }]);
```

## How It Works

Each schema builder stamps a `kind` discriminant, one of `kv`, `hash`, `set`, `list`, `zset`, `stream`, `bitmap`, `geo`, `hll`, `channel`, `pattern`, or `script`. `redis.query.<name>` dispatches on that `kind` and resolves each schema to exactly the store `redis.<kind>(schema)` would return: same methods, same inference.

- Entries in the bound module that are not schemas (a re-exported type, a helper function, a Zod or Valibot validator you pass to `json()`) are dropped from the registry. Being a schema means carrying the store binding a builder attaches, not merely having a `kind` property of your own.
- `redis.query` is `{}` when no `{ schema }` is bound.
- Counter and string operations are the exception to "prefer `redis.query`", because they are not kinds. See [The Counter And String Exception](#the-counter-and-string-exception) below before you write your first `incr`.

## Kind To Resource

| `kind` | Resource | Access |
| --- | --- | --- |
| `kv` | `redis.kv(schema)` | `redis.query.<name>.get` / `.set` |
| `hash` | `redis.hash(schema)` | `redis.query.<name>.hget` / `.hset` |
| `set` | `redis.set(schema)` | `redis.query.<name>.sadd` / `.smembers` |
| `list` | `redis.list(schema)` | `redis.query.<name>.rpush` / `.lrange` |
| `zset` | `redis.zset(schema)` | `redis.query.<name>.zadd` / `.zrange` |
| `stream` | `redis.stream(schema)` | `redis.query.<name>.xadd` / `.group` |
| `bitmap` | `redis.bitmap(schema)` | `redis.query.<name>.setbit` / `.bitcount` |
| `geo` | `redis.geo(schema)` | `redis.query.<name>.geoadd` / `.geosearch` |
| `hll` | `redis.hll(schema)` | `redis.query.<name>.pfadd` / `.pfcount` |
| `channel` | `redis.pubsub.channel(schema)` | `redis.query.<name>.publish` / `.subscribe` |
| `pattern` | `redis.pubsub.pattern(schema)` | `redis.query.<name>.subscribe` |
| `script` | `redis.script(schema)` | `redis.query.<name>.run({ keys, args })` |

## The Counter And String Exception

`redis.query` covers those twelve kinds and nothing else, and there is one gap worth knowing before you meet it. Counters and strings are not kinds of their own: they are alternate views over a plain `kv` keyspace, so a `kv` schema always resolves to the `kv` resource in the registry, whatever its codec.

That means `redis.query.<name>` gives you `get` / `set` / `del` but **no `incr`**, even when the schema is a `kv(prefix, number())` that exists only to be incremented:

```ts
// schema.ts
export const clicks = kv("clicks", number());
```

```ts
// app.ts
await redis.query.clicks.set("home", 0); // fine: the kv resource
await redis.query.clicks.incr("home");   // does not compile: kv has no incr

const total = await redis.counter(clicks).incr("home"); // reach for the counter view
```

The same holds for the string view: `append`, `getrange`, `strlen`, and friends live on `redis.string(schema)`, not on `redis.query.<name>`.

So the "prefer `redis.query`" rule has exactly two exceptions, and they are both on `kv`:

| Want | Use |
| --- | --- |
| `get`, `set`, `del`, `expire`, … | `redis.query.<name>` (the `kv` resource) |
| `incr`, `incrby`, `incrbyfloat`, `decr`, `decrby` | `redis.counter(schema)` |
| `append`, `getrange`, `setrange`, `strlen` | `redis.string(schema)` |

Both accessors take the schema value, so a counter-heavy module tends to import its schemas directly rather than going through the registry for those calls. They read and write the same keys as the `kv` resource, so mixing them on one schema is normal: `redis.query.clicks.set("home", 0)` to seed and `redis.counter(clicks).incr("home")` to bump.

## Relationship To Explicit Accessors

The registry is sugar over the explicit accessors. `redis.query.users` returns the same resource as `redis.hash(users)`, so you can mix the two styles freely:

```ts
// These are equivalent
await redis.query.users.hset("42", { name: "Ada", score: 10 });
await redis.hash(users).hset("42", { name: "Ada", score: 10 });
```

The explicit `redis.kv(schema)` / `redis.hash(schema)` accessors still exist unchanged. Use them when a schema is not part of a bound module, or when you prefer passing the schema value directly.

## A Multi-Kind Module

A single schema module can mix every kind. Each export becomes a registry entry:

```ts
// schema.ts
import {
  hash, kv, zset, channel, script,
  json, number, string
} from "benni/schema";

export type UserEvent = { id: string; action: string };

export const users = hash("user", { name: string(), score: number() });
export const profiles = kv("profile", json<{ tier: string }>());
export const leaderboard = zset("leaderboard", string());
export const userEvents = channel("events:user", json<UserEvent>());
export const rateLimit = script("rate-limit", {
  keys: ["counter"],
  args: { limit: number() },
  returns: number(),
  lua: `return redis.call("INCR", KEYS[1])`
});
```

```ts
// app.ts
await redis.query.profiles.set("42", { tier: "pro" }, { ttlSeconds: 3600 });
await redis.query.leaderboard.zadd("daily", [{ member: "ada", score: 100 }]);
await redis.query.userEvents.publish({ id: "42", action: "created" });

const allowed = await redis.query.rateLimit.run({
  keys: { counter: "user:42" },
  args: { limit: 100 }
});
//    ^? number
```

The `UserEvent` type export is dropped from the registry; only the schemas resolve to stores.

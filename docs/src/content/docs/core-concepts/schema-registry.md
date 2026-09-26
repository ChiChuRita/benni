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

export const redis = benni({ client: node(), schema });
```

Then reach each store by its export name, with full inference:

```ts
// app.ts
import { redis } from "./redis";

await redis.query.users.hset("42", { name: "Ada", score: 10 });

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number } | null

await redis.query.leaderboard.zadd("daily", [{ member: "ada", score: 100 }]);
```

## How It Works

Each schema builder stamps a `kind` discriminant: one of the twelve data kinds (`kv`, `hash`, `set`, `list`, `zset`, `stream`, `bitmap`, `geo`, `hll`, `channel`, `pattern`, `script`) or one of the seven primitives (`cache`, `ratelimit`, `queue`, `lock`, `semaphore`, `idempotency`, `budget`). `redis.query.<name>` dispatches on that `kind` and resolves each schema to its typed store.

- Entries in the bound module that are not schemas (a re-exported type, a helper function, a Zod or Valibot validator you pass to `json()`) are dropped from the registry. Being a schema means carrying the store binding a builder attaches, not merely having a `kind` property of your own.
- `redis.query` is `{}` when no `{ schema }` is bound.
- A `kv` store carries the commands its codec supports: a `kv(prefix, number())` has `incr` and the other counter commands next to `get` and `set`, and a `kv(prefix, string())` has `append` and the other string commands. See [Key-Value](/benni/data-structures/key-values/).

## Kind To Resource

| `kind` | Access |
| --- | --- |
| `kv` | `redis.query.<name>.get` / `.set` / `.getex`; `.incr` for `number()`, `.append` for `string()` |
| `hash` | `redis.query.<name>.hget` / `.hset` |
| `set` | `redis.query.<name>.sadd` / `.smembers` |
| `list` | `redis.query.<name>.rpush` / `.lrange` |
| `zset` | `redis.query.<name>.zadd` / `.zrange` |
| `stream` | `redis.query.<name>.xadd` / `.group` |
| `bitmap` | `redis.query.<name>.setbit` / `.bitcount` |
| `geo` | `redis.query.<name>.geoadd` / `.geosearch` |
| `hll` | `redis.query.<name>.pfadd` / `.pfcount` |
| `channel` | `redis.query.<name>.publish` / `.subscribe` / `.at(id)` |
| `pattern` | `redis.query.<name>.subscribe` |
| `script` | `redis.query.<name>.run({ keys, args })` |
| `cache` | `redis.query.<name>.get` / `.peek` / `.del` |
| `ratelimit` | `redis.query.<name>.check` |
| `queue` | `redis.query.<name>.enqueue` / `.worker` / `.watch` |
| `lock` | `redis.query.<name>.run` / `.acquire` |
| `semaphore` | `redis.query.<name>.run` / `.acquire` |
| `idempotency` | `redis.query.<name>.run` |
| `budget` | `redis.query.<name>.reserve` / `.charge` |

## Primitives In The Registry

The [primitives](/benni/primitives/cache/) declare themselves the same way the data structures do, so a cache or a queue is a schema value that lands in `redis.query` and carries its own configuration. Import them from `benni/schema` and they sit next to the stores they belong beside:

```ts
// schema.ts
import {
  budget, cache, hash, json, number, queue, ratelimit, string
} from "benni/schema";
import { z } from "zod";

export const users = hash("user", { name: string(), score: number() });

const profile = z.object({ name: z.string(), score: z.number() });
export const profiles = cache("profile", { ttlMs: 60_000, codec: json(profile) });
export const apiLimit = ratelimit("api", { limit: 10, windowMs: 60_000 });
export const generate = queue<{ prompt: string }, string>("generate");
export const tokens = budget("tokens", { limit: 1_000_000, windowMs: 86_400_000 });
```

```ts
// app.ts
const profile = await redis.query.profiles.get(userId, () => db.load(userId));
const { success } = await redis.query.apiLimit.check(userId);
const { id } = await redis.query.generate.enqueue({ prompt });
```

The first argument is the key prefix, exactly as it is for `hash` or `kv`; the second is the primitive's options. Nothing is imported that you do not declare: each schema carries its own store binding, so a bundle only pulls in the primitives that appear in the module.

## Schemas Outside The Module: `redis.store()`

A schema that is not in the bound module (one a library declares for itself, or any schema on a handle built without `schema`) goes through `redis.store(schema)`, which returns the same resource `redis.query` would. For a schema that is in the module, it is the very object `redis.query` holds, so there is never a second copy of a primitive's in-process state.

```ts
import { benni } from "benni";
import { kv, lock, number } from "benni/schema";

// A store the bound module does not declare.
const flags = redis.store(kv("flag", number()));

// A middleware factory that holds a client, not a handle.
const locks = benni({ client }).store(lock("order", { ttlMs: 10_000 }));
```

## Inside Sessions

`redis.session()` and `redis.watch()` bodies get the same registry on the session: `s.query.<name>` for the bound module's data stores and `s.store(schema)` for others, bound to the session's own connection. Lists, sorted sets, and streams add their blocking commands there. Primitives, channels, and scripts are reached from the handle, since a dedicated connection gives them nothing.

```ts
await redis.watch(users.key("42"), async (s) => {
  const user = await s.query.users.hget("42");
  if (!user) return null;
  return s.multi().add(["HSET", users.key("42"), "score", user.score + 1], numberReply);
});
```

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

---
"benni": minor
---

**Breaking:** the handle's type says what its client can do, and `Benni` is a named type.

Each adapter now returns a client type with its capabilities made explicit (`FullRedisClient` from `benni/node` and `benni/ioredis`, `BunClient`, `UpstashClient`), and `benni()` carries it into the handle. A member the client cannot back is not on the type: no `redis.session()` or `redis.watch()` over Upstash (and so no blocking reads, which live on sessions), channels that only `publish` over Upstash, and no pattern entries in `redis.query` over Bun. What used to type-check and then throw at runtime is now a compile error. The runtime backstop for untyped code is a new `UnsupportedCapabilityError` (exported from `benni` and `benni/core`), whose `capability` field says which of `"transaction"`, `"session"`, or `"subscriber"` the client lacked. It extends `TypeError` and keeps the old messages verbatim, so existing `catch` blocks keep working. `redis.multi()` on a client without MULTI still throws it rather than quietly becoming a pipeline; only `hset(id, value, { ttlSeconds })`, which is correct without the atomicity, falls back.

`Benni` is now `Benni<TSchema, TClient>`, a named type over named interfaces, so hovers and errors print `Benni<typeof schema, UpstashClient>` instead of the whole structure. `TClient` defaults to a client that can do everything, which is what node and ioredis return, so existing `Benni<typeof schema>` signatures keep working there; on another adapter, name its client. `AnyBenni` is exported for libraries that accept any handle.

```ts
// Before: compiled, then threw UnsupportedCapabilityError at runtime
const redis = benni({ client: upstash({ url, token }), schema });
await redis.session(async (s) => s.query.jobs.blpop("q", { timeoutSeconds: 5 }));

// After: a compile error
// Property 'session' does not exist on type 'Benni<typeof schema, UpstashClient>'.

// Typing a handle by hand on Upstash or Bun
import type { UpstashClient } from "benni/upstash";
export function handlers(redis: Benni<typeof schema, UpstashClient>) {}

// A library that takes any handle
import type { AnyBenni } from "benni";
export function instrument(redis: AnyBenni) {}
```

A hand-written client typed as plain `RedisClient` gets the capability-free surface, since its optional members are not guaranteed; return an interface that makes the members you implement required (as `FullRedisClient` does) to get the rest.

`Register` is unchanged, and now documents that it is for apps only, once per program: a second augmentation is a compile error (TS2717), and a library that registers a schema retypes the bare `Benni` of every app that installs it.

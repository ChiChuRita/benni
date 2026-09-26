---
"benni": minor
---

**Breaking:** a kv store carries the commands its codec supports, so `redis.counter()` and `redis.string()` are gone.

A kv declared with `number()` has `incr`, `incrby`, `decr`, `decrby`, and `incrbyfloat`; one declared with `string()` has `append`, `getrange`, `setrange`, `strlen`, and `lcs`. They sit on `redis.query.<name>` next to `get` and `set`, and every kv, whatever its codec, gains `getex`, which decodes through the codec. The gate is the codec, not the value type: `json<number>()` holds numbers too, but only `number()` promises the plain decimal INCR works on, and `APPEND` on a `json<string>()` or `enumOf()` value would corrupt it. Type and runtime read the same codec `format`, so the object has exactly the members its type lists.

```ts
// schema.ts
export const views = kv("views", number());
export const logs = kv("log", string());

// Before
await redis.counter(views).incr("post-1");
await redis.string(logs).append("today", "line\n");

// After
await redis.query.views.incr("post-1");
await redis.query.logs.append("today", "line\n");
```

The integer commands keep their 0.1 behaviour: `number()` also stores fractions, and Redis refuses `incr` on one of those with a `RedisServerError`; `incrbyfloat` works on both. A result past `Number.MAX_SAFE_INTEGER` still throws `ReplyShapeError` rather than rounding.

`incr` takes `{ ttlMs }`, which increments and gives the key that expiry in one atomic step, replacing the two separate calls where a crash in between left a counter that never expired. The expiry only lands on a key that has none, so the increment that creates the counter starts the window and later increments leave it running (a fixed window). It runs as a small Lua script rather than MULTI, so it works on every adapter and inside `redis.watch()` bodies without ending the WATCH.

```ts
// Before: two round trips, not atomic
const attempts = await redis.counter(logins).incr(ip);
if (attempts === 1) await redis.counter(logins).expire(ip, 60);

// After
const attempts = await redis.query.logins.incr(ip, { ttlMs: 60_000 });
```

The type-only `incr`/`append` hint members on a kv store are removed with the accessors they pointed to: calling `incr` on a `json` kv is now the plain "property does not exist" error, which is the truth, since there is no other accessor to name.

For adapter authors, `benni/core` replaces `createCounterStore` and `createStringStore` with `createCounterCommands` and `createStringCommands` (the command sets alone, which `createKvResource` mixes in by format), and `StringGetExOptions` is now `KeyValueGetExOptions`. `number()` and `string()` return the named codec types `NumberCodec` and `StringCodec`, exported from `benni/schema`.

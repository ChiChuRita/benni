---
title: "TTL And Expiration"
description: "Use ttl when a value should expire automatically."
---

Use `ttl` when a value should expire automatically.

```ts
await redis.query.sessions.set(
  sessionId,
  {
    userId: "42",
    createdAt: new Date().toISOString()
  },
  { ttlSeconds: 60 * 60 * 24 * 7 }
);
```

`ttl` is measured in seconds and maps to Redis expiration commands.

For hashes, Benni writes the fields and then applies expiration to the Redis key:

```ts
await redis.query.users.hset(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 60 * 60 }
);
```

Every keyed store (kv, hash, set, list, sorted set, stream, geo, bitmap, HyperLogLog, string, and counter) exposes the same key-level lifecycle helpers: `exists`, `ttl`, `expire`, and `persist`:

```ts
const ttl = await redis.query.sessions.ttl(sessionId);

await redis.query.sessions.expire(sessionId, 60 * 15);
await redis.query.sessions.persist(sessionId);
```

So a hash can read back the TTL it set via `hset`:

```ts
await redis.query.users.hset(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 60 * 60 }
);

const remaining = await redis.query.users.ttl("42"); // > 0
await redis.query.users.expire("42", 60 * 60 * 24); // extend
await redis.query.users.persist("42"); // clear the TTL
await redis.query.users.exists("42"); // true
```

`ttl` returns the remaining seconds, `-1` for a key with no expiry, and `-2` for a missing key.

Use `nx` when setting a key only if it does not already exist:

```ts
await redis.query.sessions.set(sessionId, nextSession, {
  nx: true,
  ttlSeconds: 60 * 60
});
```

Use `xx` when replacing a key only if it already exists:

```ts
await redis.query.sessions.set(sessionId, nextSession, {
  xx: true,
  ttlSeconds: 60 * 60
});
```

`nx` and `xx` cannot be combined, and neither can `ttlSeconds` with `keepTtl`; both invalid pairs are compile errors, not runtime throws. See [Type Safety](/benni/core-concepts/type-safety/).

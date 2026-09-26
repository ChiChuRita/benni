---
"benni": minor
---

`hmset(id, fields)` updates any subset of a hash's declared fields in one `HSET`, leaving the rest alone.

```ts
// before: one round trip per field, or Redis 8's hsetex
await redis.query.users.hset("42", "score", 11);
await redis.query.users.hset("42", "bio", "Mathematician");

// after
await redis.query.users.hmset("42", { score: 11, bio: "Mathematician" });
```

It resolves to the number of fields that were new, checks each value against its codec, and rejects an empty object or an `undefined` value before sending anything. It sends `HSET`, not the deprecated `HMSET` command, exactly as `hget(id)` sends `HMGET`; the name pairs it with `hmget`.

It is a separate method rather than a looser `hset` on purpose. If `hset(id, record)` also took a partial object, a record with a forgotten required field would compile as a partial update, and "missing required field" would no longer be caught at compile time. `hsetex` now takes the same partial-input type as `hmset`, so the two agree on which fields and values they accept.

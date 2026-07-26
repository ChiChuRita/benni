---
title: "Raw Redis Access"
description: "Beni does not try to hide Redis or cover every command with a typed abstraction."
---

Beni does not try to hide Redis or cover every command with a typed abstraction.

For commands that are not typed yet, advanced Redis usage, debugging, or one-off operations, use the underlying Redis client directly:

```ts
await redis.raw.send(["PING"]);
await redis.raw.send(["SET", "custom:key", "value"]);
await redis.raw.send(["ZADD", "custom:leaderboard", 10, "user:42"]);
```

Use typed schemas where they reduce repeated app code:

```ts
await redis.kv(profiles).set("42", profile, {
  ttlSeconds: 3600
});
```

Use raw Redis where Redis itself is the clearest API:

```ts
await redis.raw.send(["CLIENT", "INFO"]);
```

Typed Beni keys are useful even when you drop down to raw Redis:

```ts
const key = redis.hash(users).key("42");
await redis.raw.send(["EXISTS", key]);
```

The raw client accepts Redis command arguments as strings, numbers, bigints, and byte arrays. Replies are returned in the adapter's Redis reply shape.

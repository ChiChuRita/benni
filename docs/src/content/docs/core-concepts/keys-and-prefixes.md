---
title: "Keys And Prefixes"
description: "Every Beni schema has a prefix. Beni combines the prefix with an id to produce a Redis key."
---

Every Beni schema has a prefix. Beni combines the prefix with an id to produce a Redis key.

```ts
export const users = hash("user", {
  name: string(),
  score: number()
});

const key = redis.hash(users).key("42");
// "user:42"
```

Use prefixes as stable names for Redis key families:

```ts
kv("session", json<Session>());
hash("user", { name: string(), score: number() });
zset("leaderboard", string());
```

If an id comes from a route, database row, token, or Redis itself, pass it as a normal `string`, `number`, or `bigint`.

```ts
await redis.hash(users).hset(userId, {
  name: "Ada",
  score: 10
});
```

When ids are known at compile time, pass them to the schema for editor autocomplete:

```ts
export const demo = kv("demo", string(), {
  ids: ["test1", "test2"]
});

demo.key("test1");
// "demo:test1"
```

Prefixes should describe the data family, not the Redis command used to store it. Prefer `user`, `session`, and `feature-flag` over names like `hash-user`.

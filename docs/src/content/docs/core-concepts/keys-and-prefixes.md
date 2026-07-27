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

## Hash Tags

That same options bag takes `hashTag`, which moves braces into the key so Redis Cluster routes it deliberately rather than by accident:

```ts
kv("profile", string()); //                         "profile:42"
kv("profile", string(), { hashTag: "prefix" }); //  "{profile}:42"
kv("cart", string(), { hashTag: "id" }); //         "cart:{42}"
```

A cluster hashes only the text between the first `{` and the first `}`, so `"prefix"` pins a whole keyspace to one slot and `"id"` co-locates the same id across every schema tagged that way. On a single-node Redis it changes nothing but the key text. See [Redis Cluster](/beni/advanced/cluster/) for how to choose, and for the compile-time and runtime checks that come with it.

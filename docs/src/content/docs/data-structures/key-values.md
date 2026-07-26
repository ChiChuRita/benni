---
title: "Key Values"
description: "Use key-value schemas when one Redis key stores one scalar or serialized value."
---

Use key-value schemas when one Redis key stores one scalar or serialized value.

## Define A Key-Value Schema

```ts
import { json, kv } from "beni/schema";

type UserProfile = {
  name: string;
  score: number;
};

export const profiles = kv("profile", json<UserProfile>());
```

## Write

```ts
await redis.kv(profiles).set("42", {
  name: "Ada",
  score: 10
});
```

## Read

```ts
const profile = await redis.kv(profiles).get("42");
//    ^? UserProfile | null
```

## With TTL

```ts
await redis.kv(profiles).set(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 3600 }
);
```

## Conditional Writes

```ts
const created = await redis.kv(profiles).set("42", profile, {
  nx: true,
  ttlSeconds: 3600
});

const updated = await redis.kv(profiles).set("42", profile, {
  xx: true
});
```

## Raw Redis Equivalent

```ts
await nodeRedis.set("profile:42", JSON.stringify(profile), {
  EX: 3600
});
```

Use key-value schemas for sessions, feature flags, cached API responses, and values that are usually read or written as a whole.

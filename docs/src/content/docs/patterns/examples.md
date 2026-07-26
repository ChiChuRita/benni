---
title: "Worked Examples"
description: "These examples show Beni as application Redis code, not isolated method calls."
---

These examples show Beni as application Redis code, not isolated method calls.

## User Profiles

```ts
import { hash, json, kv, number, string } from "beni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});

export const profiles = kv(
  "profile",
  json<{
    bio: string;
    links: string[];
  }>()
);

await redis.hash(users).hset("42", {
  name: "Ada",
  score: 10
});

await redis.kv(profiles).set("42", {
  bio: "First programmer",
  links: ["https://example.com"]
});
```

## Feature Flags

```ts
import { boolean, kv } from "beni/schema";

export const flags = kv("feature-flag", boolean());

await redis.kv(flags).set("new-dashboard", true);

if (await redis.kv(flags).get("new-dashboard")) {
  // enable the feature
}
```

## Raw Escape Hatch

```ts
const key = redis.hash(users).key("42");
const exists = await redis.raw.send(["EXISTS", key]);
```

For lower-level examples that use the core store factories directly, see [Examples Reference](/beni/examples).

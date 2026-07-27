---
title: "Defining Schemas"
description: "A Beni schema describes one Redis key family."
---

A Beni schema describes one Redis key family.

```ts
import { hash, json, kv, number, string } from "beni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});

export const profiles = kv(
  "profile",
  json<{ name: string; score: number }>()
);
```

The `users` schema describes keys like:

```txt
user:42
user:123
user:ada
```

It also gives you typed access to those keys:

```ts
await redis.hash(users).hset("42", { name: "Ada", score: 10 });
const user = await redis.hash(users).hgetall("42");
```

Schemas are not database schemas in the migration sense. They are plain TypeScript values.

- They do not create Redis keys.
- They do not require migrations.
- They do not block raw Redis access.
- They can live next to the application code that owns the data.

## Builders

Use schema builders from `beni/schema`:

```ts
import {
  boolean,
  channel,
  hash,
  json,
  kv,
  list,
  number,
  pattern,
  set,
  zset,
  string
} from "beni/schema";
```

When your app has a codec Beni does not ship, pass a plain `Codec` object, anything with `encode`/`decode`:

```ts
import type { Codec } from "beni";

const dateString: Codec<Date, Date> = {
  encode(value) {
    return value.toISOString();
  },
  decode(stored) {
    return new Date(stored);
  }
};
```

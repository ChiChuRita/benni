---
title: "Defining Schemas"
description: "A Benni schema describes one Redis key family."
---

A Benni schema describes one Redis key family.

```ts
import { z } from "zod";
import { hash, json, kv, number, string } from "benni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});

export const profiles = kv(
  "profile",
  json(z.object({ name: z.string(), score: z.number() }))
);
```

`json(validator)` takes any [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType, …), infers the value type from it, and validates every read. Prefer it. The bare-type form, `json<{ name: string; score: number }>()`, is a cast with no runtime check: a stored value missing a required field still reads back typed as if it were complete. Reach for it only when you own every writer of that key and accept that. See [JSON values](/benni/data-structures/json-values/).

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

Use schema builders from `benni/schema`:

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
} from "benni/schema";
```

When your app has a codec Benni does not ship, pass a plain `Codec` object, anything with `encode`/`decode`:

```ts
import type { Codec } from "benni";

const dateString: Codec<Date, Date> = {
  encode(value) {
    return value.toISOString();
  },
  decode(stored) {
    return new Date(stored);
  }
};
```

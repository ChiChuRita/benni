---
title: "Schema Builders"
description: "Schema builders are exported from benni/schema."
---

Schema builders are exported from `benni/schema`.

## Codecs

```ts
string();
number();
boolean();
json<T>();
json(validator);
bytes();
enumOf(["pending", "active", "done"]);
```

A codec controls how Benni writes values to Redis and decodes values returned by Redis. `bytes()` stores `Uint8Array` values as base64-encoded strings in Redis. `enumOf([...])` constrains a field to a fixed set of string literals, stored as the plain string (no JSON overhead) and validated on decode, inferring the union of the values (`"pending" | "active" | "done"`).

`json` has two forms, and the validating one is the default to reach for. `json(validator)` accepts any [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType, …): every read is validated at runtime, and the value type is inferred from the validator, with no type parameter needed. Benni stays zero-dependency; the Standard Schema interface is inlined.

`json<T>()` is the escape hatch: a pure cast, with no runtime validation at all. `JSON.parse` runs and its result is asserted to be `T`. A stored value missing required fields, or carrying a field of the wrong type, is handed back typed as a complete `T` and nothing throws. Use it only where you own every writer of the key.

```ts
import { z } from "zod";

const users = kv("user", json(z.object({ name: z.string() })));     // validated
//    reads infer { name: string } | null from the Zod schema
const profiles = kv("profile", json<Profile>());                    // cast, unchecked
```

Invalid stored data throws a `ReplyShapeError` naming the validation issues. Async validators (schemas with async refinements) throw a clear error; `json(validator)` requires a synchronous validator.

Standard Schema defines only the read direction, so `json(validator)` cannot validate writes. The optional [`benni/zod`](/benni/integrations/zod/) subpath runs [Zod codecs](https://zod.dev/codecs) in both directions: `zodCodec(schema)` for string-stored fields (rich types like `Date` that round-trip) and `zodJson(schema)` as a write-validating `json(validator)`.

`number()` rejects non-finite input (`NaN`/`Infinity`) at write time, so a bad value fails at the `set` rather than poisoning a later `get`. Decode failures (a malformed stored value, an out-of-set enum, a wrong reply shape) throw a `ReplyShapeError` (which carries the offending `.reply`); invalid caller input throws a `ValidationError`. Both extend `TypeError`, so existing `catch` blocks keep working while you can now discriminate the two.

When your app needs a codec Benni does not ship, pass a plain `Codec` object, anything with `encode`/`decode`:

```ts
import type { Codec } from "benni";

const uppercase: Codec<string, string> = {
  encode(value) {
    return value.toUpperCase();
  },
  decode(stored) {
    return stored;
  }
};
```

## Key-Value

```ts
const profiles = kv("profile", json<Profile>());
```

Use with:

```ts
redis.kv(profiles);
```

## Hash

```ts
const users = hash("user", {
  name: string(),
  score: number()
});
```

Use with:

```ts
redis.hash(users);
```

## Collections

```ts
const tags = set("tags", string());
const events = list("events", json<Event>());
const leaderboard = zset("leaderboard", string());
const pageViews = hll("page-views", string());
```

Use with:

```ts
redis.set(tags);
redis.list(events);
redis.zset(leaderboard);
redis.hll(pageViews);
```

## Stream

```ts
const activity = stream("activity", {
  action: string(),
  points: number()
});
```

Use with:

```ts
redis.stream(activity);
```

## Bitmap

```ts
const dailyActive = bitmap("daily-active");
```

Bitmaps take no codec; bits are addressed by offset and exposed as booleans. Use with:

```ts
redis.bitmap(dailyActive);
```

## Geo

```ts
const stores = geo("stores", string());
```

Use with:

```ts
redis.geo(stores);
```

## Script

```ts
const rateLimit = script("rate-limit", {
  keys: ["counter"],
  args: { limit: number(), windowSeconds: number() },
  returns: number(),
  lua: `return redis.call("INCR", KEYS[1])`
});
```

Use with:

```ts
redis.script(rateLimit);
```

## Pub/Sub

```ts
const userEvents = channel("events:user", json<UserEvent>());
const userEventPattern = pattern("events:user:*", json<UserEvent>());
```

Use with:

```ts
redis.pubsub.channel(userEvents);
redis.pubsub.pattern(userEventPattern);
```

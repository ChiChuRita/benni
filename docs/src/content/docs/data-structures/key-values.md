---
title: "Key Values"
description: "Use key-value schemas when one Redis key stores one scalar or serialized value."
---

Use key-value schemas when one Redis key stores one scalar or serialized value.

## Define A Key-Value Schema

```ts
import { json, kv, number, string } from "benni/schema";

type UserProfile = {
  name: string;
  score: number;
};

export const profiles = kv("profile", json<UserProfile>());
```

## Write

```ts
await redis.query.profiles.set("42", {
  name: "Ada",
  score: 10
});
```

## Read

```ts
const profile = await redis.query.profiles.get("42");
//    ^? UserProfile | null
```

## With TTL

```ts
await redis.query.profiles.set(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 3600 }
);
```

## Conditional Writes

```ts
const created = await redis.query.profiles.set("42", profile, {
  nx: true,
  ttlSeconds: 3600
});

const updated = await redis.query.profiles.set("42", profile, {
  xx: true
});
```

`created` and `updated` are booleans: whether the write happened. A computed flag (`nx: isNew`) resolves and is typed the same way; a plain write resolves `void`.

## Read And Refresh The TTL

`getex` reads the value, decoded through the codec like `get`, while setting a new expiry, so a session read can also keep it alive:

```ts
const profile = await redis.query.profiles.getex("42", 3600);
//    ^? UserProfile | null
```

## Counters

A `kv` declared with `number()` carries Redis's counter commands on the same store:

```ts
export const views = kv("views", number());
```

```ts
const total = await redis.query.views.incr("post-1");
await redis.query.views.incrby("post-1", 10);
await redis.query.views.decr("post-1");
await redis.query.views.incrbyfloat("post-1", 0.5);
```

`incr` with `ttlMs` also gives the key that expiry, in the same atomic step, when the key has none. The increment that creates the counter starts the window; later increments do not extend it:

```ts
const attempts = await redis.query.loginAttempts.incr(ip, { ttlMs: 60_000 });
```

`number()` also stores fractions, and Redis refuses `incr`, `incrby`, `decr`, and `decrby` on one of those; `incrbyfloat` works on both.

## Strings

A `kv` declared with `string()` carries the commands that edit a string in place:

```ts
export const logs = kv("log", string());
```

```ts
await redis.query.logs.append("today", "line\n");
const length = await redis.query.logs.strlen("today"); // in bytes
const head = await redis.query.logs.getrange("today", 0, 99);
```

Only `number()` and `string()` bring extra commands. A `json<number>()` value is a number too, but its stored form is only promised to be JSON, and `APPEND` on a `json` or `enumOf` value would corrupt it, so those stores keep the plain commands.

## Raw Redis Equivalent

```ts
await nodeRedis.set("profile:42", JSON.stringify(profile), {
  EX: 3600
});
```

Use key-value schemas for sessions, feature flags, cached API responses, and values that are usually read or written as a whole.

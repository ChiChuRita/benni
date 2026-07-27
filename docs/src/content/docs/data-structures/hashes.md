---
title: "Hashes"
description: "Use hashes when you want to store object-like data under a Redis key."
---

Use hashes when you want to store object-like data under a Redis key.

## Define A Hash

```ts
import { hash, number, string } from "beni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});
```

## Write A Hash

```ts
await redis.hash(users).hset("42", {
  name: "Ada",
  score: 10
});
```

## Read A Hash

```ts
const user = await redis.hash(users).hgetall("42");
//    ^? { name: string; score: number } | null
```

## Update Fields

```ts
await redis.hash(users).hset("42", "score", 11);
await redis.hash(users).hincrby("42", "score", 1);
```

## Read Fields

```ts
const score = await redis.hash(users).hget("42", "score");
const fields = await redis.hash(users).hmget("42", ["name", "score"]);
```

## Random Fields

Pick field names at random with `HRANDFIELD`. `hrandfield` with no count returns a single field name, or `null` when the key is missing:

```ts
const field = await redis.hash(users).hrandfield("42");
//    ^? string | null
```

Pass a nonzero `count`. A positive count returns that many **distinct** field names (capped at the hash's size); a negative count allows repeats and always returns `|count|` names:

```ts
const distinct = await redis.hash(users).hrandfield("42", { count: 2 });
//    ^? string[]   (up to 2 distinct field names)

const withRepeats = await redis.hash(users).hrandfield("42", { count: -5 });
//    ^? string[]   (exactly 5 names, repeats allowed)
```

Both forms return raw field names; like `hkeys`, the result may include fields not declared in the schema. The value-bearing form (`HRANDFIELD ... WITHVALUES`) is intentionally not provided: a random field's value cannot be soundly decoded without knowing which codec it belongs to, the same reason there is no bare `HVALS` accessor.

## Field Expiration

Redis 7.4+ can expire individual hash fields, and Redis 8 adds get/set variants that touch field TTLs atomically.

Set a per-field TTL with `hexpire`. Pass a number for a relative TTL in seconds, or an options object to choose the unit and whether the value is a relative duration or an absolute Unix time:

```ts
await redis.hash(users).hexpire("42", ["score"], 3600); // HEXPIRE (seconds)
await redis.hash(users).hexpire("42", ["score"], { ttlMilliseconds: 500 }); // HPEXPIRE
await redis.hash(users).hexpire("42", ["score"], { expireAtSeconds: 1893456000 }); // HEXPIREAT
```

Read the remaining TTL or the absolute expiry time (each in seconds by default, or milliseconds with `{ milliseconds: true }`), and clear TTLs with `hpersist`:

```ts
await redis.hash(users).httl("42", "score"); // HTTL (seconds)
await redis.hash(users).httl("42", "score", { milliseconds: true }); // HPTTL
await redis.hash(users).hexpiretime("42", "score"); // HEXPIRETIME
await redis.hash(users).hpersist("42", ["score"]); // HPERSIST
```

Get, set, and delete fields while touching their TTL in a single round trip:

```ts
// HGETEX: read fields and (optionally) reset their TTL.
const seen = await redis.hash(users).hgetex("42", ["name"], { ttlSeconds: 60 });

// HSETEX: set fields with a TTL atomically; fnx writes only if no field exists,
// fxx only if all do (the Redis FNX/FXX tokens); combining them is a compile error.
const wrote = await redis.hash(users).hsetex(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 3600 }
);

// HGETDEL: read fields and delete them (the key is removed once its last field goes).
const removed = await redis.hash(users).hgetdel("42", ["name", "score"]);
```

## Delete Fields Or The Hash

`hdel` takes one field or an array and returns the count removed:

```ts
await redis.hash(users).hdel("42", "score");
await redis.hash(users).hdel("42", ["name", "score"]);
await redis.hash(users).del("42");
```

## With TTL

```ts
await redis.hash(users).hset(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 3600 }
);
```

## Raw Redis Equivalent

```ts
await nodeRedis.hSet("user:42", {
  name: "Ada",
  score: "10"
});
```

Use hashes for users, profiles, counters, session metadata, and object-like data where fields may be read or updated independently. Prefer a JSON key-value schema when the whole object is usually stored and read as one blob.

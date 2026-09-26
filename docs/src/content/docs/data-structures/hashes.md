---
title: "Hashes"
description: "Use hashes when you want to store object-like data under a Redis key."
---

Use hashes when you want to store object-like data under a Redis key.

## Define A Hash

```ts
import { hash, number, string } from "benni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});
```

## Optional Fields

Wrap a field's codec in `optional()` when a stored record may lack it. That is how a schema grows: add the new field as `optional(...)` and every record written before it keeps reading, with nothing to backfill first.

```ts
import { hash, number, optional, string } from "benni/schema";

export const users = hash("user", {
  name: string(),
  score: number(),
  bio: optional(string())
});

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number; bio?: string } | null
```

A missing optional field is **absent** from the record: `"bio" in user` is `false`, never a `bio: undefined` or `bio: null` key. A missing *required* field still throws [`PartialRecordError`](#missing-required-fields-throw). The write side matches: `hset` lets you leave an optional field out, and `InferInput<typeof users>` types it `bio?: string`.

`optional()` only means something on a hash field. On a `kv` value, a stream field, or a script arg it is the wrapped codec unchanged, and the value stays required.

There is no default-value wrapper: read `user.bio ?? ""` where you need one. A default would have to decide what `hgetall`, `hmget`, and `hgetdel` report for a field that was never stored, and each answer hides the fact that it was missing.

## Write A Whole Record

```ts
await redis.query.users.hset("42", {
  name: "Ada",
  score: 10
});
```

The record form requires every required field, so forgetting one is a compile error rather than a partial record in Redis. It writes the whole record: an optional field you leave out (or pass as `undefined`) is deleted, in the same `MULTI`/`EXEC` as the `HSET`, so the next `hget` returns exactly what you wrote and not a `bio` left over from an earlier write. When every optional field is present it is one plain `HSET`.

## Update Some Fields

`hmset` writes any subset of the declared fields in one `HSET` and leaves the rest alone. It is the partial update, and it resolves to the number of fields that were new:

```ts
await redis.query.users.hmset("42", { score: 11, bio: "Mathematician" });
```

Each value is checked against its field's codec, and an unknown field name is a compile error. An empty object and a value of `undefined` are rejected before anything is sent: omit a key to leave that field unchanged, or `hdel` it to remove it. (Redis deprecated its `HMSET` command in favour of variadic `HSET`, which is what this sends; the name pairs it with `hmget`.)

The partial update is its own method rather than a looser `hset` on purpose: if `hset(id, record)` also accepted a partial object, a record with a forgotten required field would compile as a partial update and the mistake would reach Redis.

For one field, or a counter:

```ts
await redis.query.users.hset("42", "score", 11);
await redis.query.users.hincrby("42", "score", 1);
```

`hincrby` throws a `ReplyShapeError` once the stored value passes `Number.MAX_SAFE_INTEGER`, like `incr`, instead of resolving a rounded number.

## Read Fields

One `hget`, two jobs: pass a field name to read that one field, or nothing to read the whole record. There is no `hgetField` or `hgetOne`.

```ts
const score = await redis.query.users.hget("42", "score");
//    ^? number | null      (one field)

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number; bio?: string } | null   (the whole record)

const fields = await redis.query.users.hmget("42", ["name", "bio"]);
//    ^? { name?: string; bio?: string }
```

The single-field form returns the field's decoded type, so `hget("42", "score")` is a `number | null` and not a string you have to parse.

Every read that returns an object says "not stored" the same way: the key is absent. That holds for an optional field on `hget(id)`, for every field on `hgetall`, and for `hmget`, `hgetex`, and `hgetdel`, whose results only carry the fields Redis had a value for. Only a read that returns a bare value, `hget(id, field)`, uses `null`.

## Missing Required Fields Throw

A hash under `hash("user", …)` is a record your schema owns, so the whole-record read insists on it. `hget("42")` needs every required field and throws a `PartialRecordError` naming the missing ones on `.missing` when one is gone (deleted with `hdel`, or expired by a per-field TTL). Optional fields never trigger it. `hgetall` is the tolerant read for exactly that case, and types every field as optional:

```ts
const strict = await redis.query.users.hget("42");
//    ^? { name: string; score: number; bio?: string } | null   (throws PartialRecordError if name or score is missing)

const tolerant = await redis.query.users.hgetall("42");
//    ^? { name?: string; score?: number; bio?: string } | null
```

A key that holds none of the declared fields reads as `null` from both.

## Undeclared Fields Are Left Out

A field in Redis that the schema does not declare (written by another service, by `redis-cli`, or by an older schema that has since dropped it) is not part of the record: `hgetall` leaves it out, the way `hget(id)` never asks for it, and `redis.scan.hash` skips it. That is what makes removing a field from a schema safe; throwing on it would turn every old record into an error. The fields are still there and still counted by `hlen`. List them with `hkeys`, which returns raw names, or read them with `redis.raw.send(["HGET", key, field])`.

This is the opposite of how [stream](/benni/data-structures/streams/) entry values behave, which are always `Partial` and never throw. The difference is who writes the key: a hash is a record you own, while a stream is an append log any producer can write to. See [Entry Values Are Partial](/benni/data-structures/streams/#entry-values-are-partial).

## Random Fields

Pick field names at random with `HRANDFIELD`. `hrandfield` with no count returns a single field name, or `null` when the key is missing:

```ts
const field = await redis.query.users.hrandfield("42");
//    ^? string | null
```

Pass a nonzero `count`. A positive count returns that many **distinct** field names (capped at the hash's size); a negative count allows repeats and always returns `|count|` names:

```ts
const distinct = await redis.query.users.hrandfield("42", { count: 2 });
//    ^? string[]   (up to 2 distinct field names)

const withRepeats = await redis.query.users.hrandfield("42", { count: -5 });
//    ^? string[]   (exactly 5 names, repeats allowed)
```

Both forms return raw field names; like `hkeys`, the result may include fields not declared in the schema. The value-bearing form (`HRANDFIELD ... WITHVALUES`) is intentionally not provided: a random field's value cannot be soundly decoded without knowing which codec it belongs to, the same reason there is no bare `HVALS` accessor.

## Field Expiration

Redis 7.4+ can expire individual hash fields, and Redis 8 adds get/set variants that touch field TTLs atomically.

Set a per-field TTL with `hexpire`. Pass a number for a relative TTL in seconds, or an options object to choose the unit and whether the value is a relative duration or an absolute Unix time:

```ts
await redis.query.users.hexpire("42", ["score"], 3600); // HEXPIRE (seconds)
await redis.query.users.hexpire("42", ["score"], { ttlMilliseconds: 500 }); // HPEXPIRE
await redis.query.users.hexpire("42", ["score"], { expireAtSeconds: 1893456000 }); // HEXPIREAT
```

Read the remaining TTL or the absolute expiry time (each in seconds by default, or milliseconds with `{ milliseconds: true }`), and clear TTLs with `hpersist`:

```ts
await redis.query.users.httl("42", "score"); // HTTL (seconds)
await redis.query.users.httl("42", "score", { milliseconds: true }); // HPTTL
await redis.query.users.hexpiretime("42", "score"); // HEXPIRETIME
await redis.query.users.hpersist("42", ["score"]); // HPERSIST
```

Get, set, and delete fields while touching their TTL in a single round trip:

```ts
// HGETEX: read fields and (optionally) reset their TTL.
const seen = await redis.query.users.hgetex("42", ["name"], { ttlSeconds: 60 });

// HSETEX: set fields with a TTL atomically; fnx writes only if no field exists,
// fxx only if all do (the Redis FNX/FXX tokens); combining them is a compile error.
const wrote = await redis.query.users.hsetex(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 3600 }
);

// HGETDEL: read fields and delete them (the key is removed once its last field goes).
const removed = await redis.query.users.hgetdel("42", ["name", "score"]);
```

`hsetex` takes the same input as [`hmset`](#update-some-fields): any subset of the declared fields, the rest left alone. Like `hmset`, it rejects a field whose value is `undefined` rather than storing the string `"undefined"`: omit the key to leave that field alone. `hgetex` with an empty field list rejects too when you pass an expiry, because there is no field to apply it to.

A lapsed field TTL leaves the hash partially populated, and so does `hdel` or `hgetdel` on a declared field. That is precisely the case [`hgetall` exists for](#missing-required-fields-throw): a record with per-field TTLs should be read with `hgetall`, or have those fields declared `optional()`, since `hget("42")` throws a `PartialRecordError` the moment one required field has gone.

## Delete Fields Or The Hash

`hdel` takes one field or an array and returns the count removed:

```ts
await redis.query.users.hdel("42", "score");
await redis.query.users.hdel("42", ["name", "score"]);
await redis.query.users.del("42");
```

## With TTL

```ts
await redis.query.users.hset(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 3600 }
);
```

The `HSET` (plus any `HDEL` of omitted optional fields) and the `EXPIRE` run in one `MULTI`/`EXEC`, so no reader sees the record without its TTL.

## Raw Redis Equivalent

```ts
await nodeRedis.hSet("user:42", {
  name: "Ada",
  score: "10"
});
```

Use hashes for users, profiles, counters, session metadata, and object-like data where fields may be read or updated independently. Prefer a JSON key-value schema when the whole object is usually stored and read as one blob.

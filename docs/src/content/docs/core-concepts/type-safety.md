---
title: "Type Safety"
description: "Beni types come from codecs and schemas."
---

Beni types come from codecs and schemas.

```ts
export const users = hash("user", {
  name: string(),
  score: number(),
  active: boolean()
});
```

Writes must match the schema:

```ts
await redis.hash(users).hset("42", {
  name: "Ada",
  score: 10,
  active: true
});
```

Reads return decoded values:

```ts
const user = await redis.hash(users).hgetall("42");
//    ^? { name: string; score: number; active: boolean } | null
```

Hash field methods are typed by field name:

```ts
await redis.hash(users).hset("42", "score", 11);
const score = await redis.hash(users).hget("42", "score");
//    ^? number | null
```

For JSON values, the TypeScript type is supplied by the app:

```ts
type Session = {
  userId: string;
  createdAt: string;
};

export const sessions = kv("session", json<Session>());
```

## Inferring Types From Schemas

Every schema carries type-only `$inferInput` / `$inferOutput` anchors, plus the `InferInput<T>` / `InferOutput<T>` utility types exported from `beni/schema` — the equivalent of Drizzle's `$inferSelect`. Name a schema's value types anywhere without redeclaring them:

```ts
import { hash, json, kv, number, string } from "beni/schema";
import type { InferInput, InferOutput } from "beni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});
export const profiles = kv("profile", json<Profile>());

type NewUser = InferInput<typeof users>;
//   ^? { name: string; score: number }

type StoredProfile = typeof profiles.$inferOutput;
//   ^? Profile
```

`InferInput` is the write-side type (what `hset`/`set` accept) and `InferOutput` the read-side type (what `hgetall`/`get` return, before the `| null`). They differ when a codec transforms values on the way through. The `$infer*` properties are type-only phantoms — they never exist at runtime, so only use them in type positions (`typeof users.$inferInput`).

## Runtime Validation With Standard Schema

`json(validator)` accepts any [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType, …). Reads are validated at runtime and the value type is inferred from the validator — no explicit type parameter needed. See [schema builders](/beni/api/schema-builders/) for details.

```ts
import { z } from "zod";

const Profile = z.object({ name: z.string(), score: z.number() });
export const profiles = kv("profile", json(Profile));

const profile = await redis.kv(profiles).get("42");
//    ^? { name: string; score: number } | null — validated at runtime
```

With the plain `json<T>()` form, `T` is trusted, not validated: Beni validates command reply shapes and decodes stored values, but does not check arbitrary JSON against your type. If untrusted code writes to the same Redis keys, pass a validator or validate at your application boundary.

Standard Schema validates reads only — it has no encode direction. To validate writes too, and to store rich types like `Date` or `bigint` that round-trip, use [Zod codecs via `beni/zod`](/beni/integrations/zod/).

## Typed Keys

Keys keep their literal types. `redis.query.users.key("42")` (and `redis.hash(users).key("42")`) has the type `"user:42"`, not `string` — template-literal key types survive the accessors and the query registry, so key-shaped APIs like `redis.watch([...])` stay precise.

## Illegal Option Combinations Don't Compile

Mutually exclusive command options are modeled in the types, so an invalid combination is a compile error rather than a runtime throw:

```ts
await redis.kv(profiles).set("42", value, { nx: true, xx: true });        // compile error
await redis.kv(profiles).set("42", value, { ttlSeconds: 60, keepTtl: true }); // compile error
await redis.zset(board).zadd("global", entry, { nx: true, gt: true });    // compile error
await redis.hash(users).hsetex("42", fields, { fnx: true, fxx: true });   // compile error
```

The same applies to `hsetex`'s expiry modes (at most one of `ttlSeconds` / `ttlMilliseconds` / `expireAtSeconds` / `expireAtMilliseconds` / `keepTtl`) and `geoadd`'s `nx`/`xx`.

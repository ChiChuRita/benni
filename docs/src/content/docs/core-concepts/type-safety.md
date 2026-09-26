---
title: "Type Safety"
description: "Benni types come from codecs and schemas."
---

Benni types come from codecs and schemas.

```ts
export const users = hash("user", {
  name: string(),
  score: number(),
  active: boolean()
});
```

Writes must match the schema:

```ts
await redis.query.users.hset("42", {
  name: "Ada",
  score: 10,
  active: true
});
```

Reads return decoded values:

```ts
const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number; active: boolean } | null
```

That comment is also what your editor shows. Inferred value types are flattened, so hovering `user` prints the object type itself, not the `InferHashOutput<{ name: Codec<string, string>; … }>` that computes it. The same goes for `hgetall`, `hmget`, stream entries, and `redis.scan.hash` entries.

A field a stored record may lack is declared `optional()`, and the types say so on both sides:

```ts
import { optional } from "benni/schema";

export const accounts = hash("account", {
  email: string(),
  nickname: optional(string())
});

await redis.query.accounts.hset("7", { email: "ada@example.com" }); // nickname may be left out
await redis.query.accounts.hset("7", { nickname: "ada" });           // compile error: email is required

const account = await redis.query.accounts.hget("7");
//    ^? { email: string; nickname?: string } | null
```

A missing optional field is absent from the object; `nickname` is never present as `undefined` or `null`. See [Optional Fields](/benni/data-structures/hashes/#optional-fields).

Hash field methods are typed by field name:

```ts
await redis.query.users.hset("42", "score", 11);
const score = await redis.query.users.hget("42", "score");
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

`InferInput<T>` and `InferOutput<T>`, exported from `benni/schema` (and `benni`), name a schema's value types anywhere without redeclaring them. They work on every schema that encodes or decodes values, and on a bare codec:

```ts
import { hash, json, kv, number, string } from "benni/schema";
import type { InferInput, InferOutput } from "benni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});
export const profiles = kv("profile", json<Profile>());

type NewUser = InferInput<typeof users>;
//   ^? { name: string; score: number }

type StoredProfile = InferOutput<typeof profiles>;
//   ^? Profile
```

`InferInput` is the write-side type (what `hset`/`set` accept) and `InferOutput` the read-side type (what `hget`/`get` return, before the `| null`). They differ when a codec transforms values on the way through. The schema carries nothing at runtime for them to read: the type lives on a phantom key that only the type system can see, so there is no property to access by mistake. (0.1's `typeof users.$inferInput` is `InferInput<typeof users>` now.)

## Runtime Validation With Standard Schema

`json(validator)` accepts any [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType, …). Reads are validated at runtime and the value type is inferred from the validator, with no explicit type parameter needed. See [schema builders](/benni/api/schema-builders/) for details.

```ts
import { z } from "zod";

const Profile = z.object({ name: z.string(), score: z.number() });
export const profiles = kv("profile", json(Profile));

const profile = await redis.query.profiles.get("42");
//    ^? { name: string; score: number } | null (validated at runtime)
```

With the plain `json<T>()` form, `T` is trusted, not validated: Benni validates command reply shapes and decodes stored values, but does not check arbitrary JSON against your type. If untrusted code writes to the same Redis keys, pass a validator or validate at your application boundary.

Standard Schema validates reads only; it has no encode direction. To validate writes too, and to store rich types like `Date` or `bigint` that round-trip, use [Zod codecs via `benni/zod`](/benni/integrations/zod/).

## Typed Keys

Keys keep their literal types. `redis.query.users.key("42")` (and `users.key("42")` on the schema itself) has the type `"user:42"`, not `string`; template-literal key types survive the query registry, so key-shaped APIs like `redis.watch([...])` stay precise.

## Illegal Option Combinations Don't Compile

Mutually exclusive command options are modeled in the types, so an invalid combination is a compile error rather than a runtime throw:

```ts
await redis.query.profiles.set("42", value, { nx: true, xx: true });        // compile error
await redis.query.profiles.set("42", value, { ttlSeconds: 60, keepTtl: true }); // compile error
await redis.query.board.zadd("global", entry, { nx: true, gt: true });    // compile error
await redis.query.users.hsetex("42", fields, { fnx: true, fxx: true });   // compile error
```

The same applies to `hsetex`'s expiry modes (at most one of `ttlSeconds` / `ttlMilliseconds` / `expireAtSeconds` / `expireAtMilliseconds` / `keepTtl`) and `geoadd`'s `nx`/`xx`.

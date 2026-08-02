---
title: "Quick Start"
description: "Declare two Redis key families, bind a client, and read your own types back: the five-minute path from install to first typed round trip."
---

This example defines two Redis key families: one hash for user metadata and one JSON key-value store for full profiles.

First install Benni and the Node client (see [Installation](/benni/getting-started/installation/) for other runtimes):

```sh
pnpm add benni redis zod
```

[`zod`](https://zod.dev) is here because the JSON store below validates its reads. Any [Standard Schema](https://standardschema.dev) validator works (Zod, Valibot, ArkType) and Benni depends on none of them; the validator you already use is the one it will use.

## Define Schemas

```ts
// schema.ts
import { hash, json, kv, number, string } from "benni/schema";
import { z } from "zod";

export const users = hash("user", {
  name: string(),
  score: number()
});

// json(validator) infers the value type from the validator and checks every
// read against it at runtime. This is the form to reach for.
const profile = z.object({
  name: z.string(),
  score: z.number()
});

export type UserProfile = z.infer<typeof profile>;
export const profiles = kv("profile", json(profile));
```

Schemas are plain TypeScript values. They do not create Redis keys or require migrations.

One thing to know about that JSON store before you build on it. `json(profile)` validates every read: if the stored JSON does not match, the read throws [`ReplyShapeError`](/benni/api/errors/#replyshapeerror) with the offending value attached rather than handing back something that lies about its type. There is a second form, `json<UserProfile>()`, which is the **unchecked escape hatch**:

```ts
// no validator, no runtime check: JSON.parse plus an assertion of the type
export type UserProfile = { name: string; score: number };
export const profiles = kv("profile", json<UserProfile>());
```

That is a pure cast. A value written by an older deploy, by another service, or by hand in `redis-cli` still types as a complete `UserProfile` even when fields are missing, and nothing throws. Reach for it only when you own every writer and the value has no shape worth checking. Prefer `json(validator)` everywhere else, and especially anywhere the data outlives the code that wrote it, which in Redis is most data. See [JSON values](/benni/data-structures/json-values/) for the full comparison.

## Create A Client

```ts
// redis.ts
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

export const redis = benni({
  client: node({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" }),
  schema
});
```

`node()` returns a promise, and `benni()` takes it unawaited: the connection opens on the first command instead of at module scope, so this file needs no top-level `await` and drops into a Next.js route or an edge bundle unchanged. The trade is that a connection failure surfaces at the first command rather than at construction. Pass a client you already awaited when you would rather find out at startup:

```ts
const client = await node({ url: process.env.REDIS_URL });
export const redis = benni(client, { schema });
```

Both forms take the same options. `benni(client, options)` and `benni({ client, ...options })` are the same call.

To pass the bound handle around, register the schema module once and the exported `Benni` type is already the fully typed handle:

```ts
// redis.ts, next to the code above
declare module "benni" {
  interface Register {
    schema: typeof schema;
  }
}
```

```ts
import type { Benni } from "benni";

export function makeHandlers(redis: Benni) { /* ... */ }
```

Without the registration nothing breaks: `Benni` stays generic and `Benni<typeof schema>` still names the handle. Every accessor it exposes is listed in the [Benni Client reference](/benni/api/benni-client/).

The client owns a connection, so close it when your process or test finishes, otherwise Node never exits:

```ts
await redis.raw.close();
```

## Read And Write

Because the schema module is bound to the client, reach each store by its export name through `redis.query`:

```ts
// app.ts
import { redis } from "./redis";

await redis.query.users.hset("42", {
  name: "Ada",
  score: 10
});

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number } | null

await redis.query.profiles.set(
  "42",
  { name: "Ada", score: 10 },
  { ttlSeconds: 60 * 60 }
);
```

The explicit `redis.hash(schema)` accessors remain available and return the same store; see the [Schema Registry](/benni/core-concepts/schema-registry/).

## Drop To Redis

```ts
const userKey = redis.query.users.key("42");
// "user:42"

const pong = await redis.raw.send(["PING"]);
```

The typed API handles repeated app patterns. The raw client stays available for commands that are not typed yet or when direct Redis is simpler.

---
title: "Quick Start"
description: "Declare two Redis key families, bind a client, and read your own types back — the five-minute path from install to first typed round trip."
---

This example defines two Redis key families: one hash for user metadata and one JSON key-value store for full profiles.

First install Beni and the Node client (see [Installation](/beni/getting-started/installation/) for other runtimes):

```sh
pnpm add beni redis
```

## Define Schemas

```ts
// schema.ts
import { hash, json, kv, number, string } from "beni/schema";

type UserProfile = {
  name: string;
  score: number;
};

export const users = hash("user", {
  name: string(),
  score: number()
});

export const profiles = kv("profile", json<UserProfile>());
```

Schemas are plain TypeScript values. They do not create Redis keys or require migrations.

## Create A Client

```ts
// redis.ts
import { beni } from "beni";
import { node } from "beni/node";
import * as schema from "./schema";

const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});

export const redis = beni(client, { schema });
```

To pass the bound handle around, type it with the exported `Beni<TSchema>` — the way you would Drizzle's `NodePgDatabase`:

```ts
import type { Beni } from "beni";

export function makeHandlers(redis: Beni<typeof schema>) { /* ... */ }
```

Every accessor the handle exposes is listed in the [Beni Client reference](/beni/api/beni-client/). The client owns a connection, so close it when your process or test finishes — otherwise Node never exits:

```ts
await client.close();
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

The explicit `redis.hash(schema)` accessors remain available and return the same store — see the [Schema Registry](/beni/core-concepts/schema-registry/).

## Drop To Redis

```ts
const userKey = redis.query.users.key("42");
// "user:42"

const pong = await redis.raw.send(["PING"]);
```

The typed API handles repeated app patterns. The raw client stays available for commands that are not typed yet or when direct Redis is simpler.

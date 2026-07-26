---
title: "Introduction"
description: "The end-to-end typed Redis client for TypeScript — one API across Node, Bun, Deno, and the edge."
---

Beni is the **end-to-end typed Redis client for TypeScript** — one typed API across Node, Bun, Deno, and the edge (Upstash/HTTP).

`node-redis` and `ioredis` are already typed, but only at the *command surface*: a reply comes back as Redis's generic wire shape (`string | null`, `Record<string, string>`), so your type is gone the moment data crosses the Redis edge and you re-parse it by hand. Beni declares your data model once with typed codecs and carries those types from write to read — **they type the commands; Beni types your data.** (Same relationship `pg` has to Drizzle.)

```ts
import { hash, number, string } from "beni/schema";

export const users = hash("user", {
  name: string(),
  score: number()
});

await redis.hash(users).hset("42", {
  name: "Ada",
  score: 10
});

const user = await redis.hash(users).hget("42");
//    ^? { name: string; score: number } | null
```

Beni is a typed **client**, not an ORM. It does not create tables, run migrations, or hide Redis. A Beni schema is a plain TypeScript description of a Redis key family, and raw access is always one call away.

The mental model is small:

```txt
Schema = how a Redis key family is shaped
Client = typed Redis access bound to your schemas
Raw = direct Redis access when that is clearer
```

Use Beni where schemas help. Use raw Redis where Redis itself is the clearest API.

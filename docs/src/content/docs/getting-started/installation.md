---
title: "Installation"
description: "Install Beni and the Redis client used by the Node.js adapter:"
---

Install Beni and the Redis client used by the Node.js adapter:

```sh
pnpm add beni redis
```

Beni is an ESM package. The primary imports are:

```ts
import { beni } from "beni";
import { node } from "beni/node";
import { hash, json, kv, number, string } from "beni/schema";
```

The Node.js adapter uses the [`redis`](https://www.npmjs.com/package/redis) package (node-redis, the officially recommended Node client). Beni declares it as an **optional peer dependency**, so you install it alongside Beni only when you use `beni/node` — that keeps the install tiny for other runtimes. Bun is supported through Bun's built-in Redis client and needs no extra package:

```sh
bun add beni
```

Deno needs no separate adapter: it runs node-redis directly through npm compatibility, so Deno users import the Node adapter (`npm:beni/node`) and `npm:redis`. There is no `beni/deno` entrypoint — Beni ships one runtime-agnostic core plus thin client adapters, not per-runtime builds.

## Server Compatibility

The integration suite runs against every row of this table:

| Server | Coverage |
| --- | --- |
| Redis 8 | Full surface. |
| Redis 7.4 | Everything except `hsetex`/`hgetex`/`hgetdel` (Redis 8 commands). |
| Redis 7.2 | Additionally no hash field TTLs (`hexpire`/`httl`/…, introduced in 7.4). |
| Valkey 8 | Same profile as Redis 7.2 (Valkey forked pre-7.4). |
| Dragonfly | The common surface works (kv, hashes, sets, lists, sorted sets, streams, bitmaps, Pub/Sub, transactions, scripts); `LCS`, `GEOSEARCHSTORE`, and hash field TTLs are not implemented by Dragonfly. |

Everything else — streams, sorted sets, `lmpop`, `sintercard`, geo, bitfields — works from Redis 7.2 up, and Upstash is covered through the [HTTP adapter](/runtime/edge/).

For local development, run Redis with Docker:

```sh
pnpm redis:build
pnpm redis:run
```

Then point Beni at Redis:

```ts
const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});
```

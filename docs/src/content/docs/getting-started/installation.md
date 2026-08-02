---
title: "Installation"
description: "Install Benni, pick the peer dependency your runtime needs, and check which Redis servers are supported."
---

Install Benni and the Redis client used by the Node.js adapter:

```sh
pnpm add benni redis
```

Benni is an ESM package. The primary imports are:

```ts
import { benni } from "benni";
import { node } from "benni/node";
import { hash, json, kv, number, string } from "benni/schema";
```

The Node.js adapter uses the [`redis`](https://www.npmjs.com/package/redis) package (node-redis, the officially recommended Node client). Benni declares it as an **optional peer dependency**, so you install it alongside Benni only when you use `benni/node`. That keeps the install tiny for other runtimes. Bun is supported through Bun's built-in Redis client and needs no extra package:

```sh
bun add benni
```

Benni has four optional peer dependencies, all opt-in; install one only when
you import the subpath that needs it:

| Peer | Needed by | Version |
| --- | --- | --- |
| [`redis`](https://www.npmjs.com/package/redis) | `benni/node` | `^6.1.0` |
| [`ioredis`](https://www.npmjs.com/package/ioredis) | `benni/ioredis` | `^5.0.0` |
| [`hono`](https://hono.dev) | `benni/hono` | `>=4.0.0` |
| [`zod`](https://zod.dev) | `benni/zod` | `^4.1.0` |

Already running ioredis? [`benni/ioredis`](/benni/runtime/ioredis/) gives the same
typed API and can adopt the client you already have, so adopting Benni is not a
client migration:

```sh
pnpm add benni ioredis
```

Deno needs no separate adapter: it runs node-redis directly through npm compatibility, so Deno users import the Node adapter (`npm:benni/node`) and `npm:redis`. There is no `benni/deno` entrypoint: Benni ships one runtime-agnostic core plus thin client adapters, not per-runtime builds.

## Server Compatibility

CI runs the integration suite against Redis 8 and an Upstash-REST-compatible
endpoint. The other rows are verified manually against the same suite:

| Server | Coverage |
| --- | --- |
| Redis 8 | Full surface. |
| Redis 7.4 | Everything except `hsetex`/`hgetex`/`hgetdel` (Redis 8 commands). |
| Redis 7.2 | Additionally no hash field TTLs (`hexpire`/`httl`/…, introduced in 7.4). |
| Valkey 8 | Same profile as Redis 7.2 (Valkey forked pre-7.4). |
| Dragonfly | The common surface works (kv, hashes, sets, lists, sorted sets, streams, geo, HyperLogLog, bitmaps, Pub/Sub, transactions, scripts); `LCS`, `GEOSEARCHSTORE`, and hash field TTLs are not implemented by Dragonfly. |

Everything else (streams, sorted sets, `lmpop`, `sintercard`, geo, bitfields) works from Redis 7.2 up.

Upstash and other serverless endpoints are covered through the [HTTP adapter](/benni/runtime/edge/).

For local development, run Redis with Docker:

```sh
pnpm redis:build
pnpm redis:run
```

Then point Benni at Redis:

```ts
const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});
```

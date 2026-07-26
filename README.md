# Beni

**The end-to-end typed Redis client for TypeScript.** One typed API across
Node, Bun, Deno, and the edge.

Declare your Redis data model once with typed codecs, bind a client, and call
methods named after the Redis commands they run — with full input **and output**
type inference and typed key prefixing. Your declared types travel from write to
read, so replies come back as *your* types, not `string | null`.

## They type the commands; Beni types your data

`node-redis` and `ioredis` are already typed — but only at the *command surface*.
A reply comes back as Redis's generic wire shape, and the type is gone the moment
your data crosses the Redis edge; you re-parse and cast it by hand, and that cast
is where the bugs live:

```ts
// raw node-redis — typed… as a string
const raw = await client.get("profile:42"); // string | null
const user = JSON.parse(raw!) as UserProfile; // hand-cast; the compiler never checked it

// Beni — your declared type, decoded for you
const user = await redis.kv(profiles).get("42");
//    ^? UserProfile | null
```

It's the same relationship `pg` has to Drizzle: `pg` types the driver; Drizzle
types your rows. Beni is a typed **client**, not an ORM or object mapper —
schemas are plain TypeScript values that don't create keys, run migrations, or
block raw Redis access (`redis.raw.send([...])` is always there).

Works with your validator — pass any [Standard Schema](https://standardschema.dev)
validator (Zod, Valibot, ArkType) and `json(zodSchema)` gives runtime-validated,
fully inferred reads, with zero added dependencies.

## Docs

The docs site is built with [Astro Starlight](https://starlight.astro.build/) and lives in
[docs/](docs/). Content pages start at
[docs/src/content/docs/getting-started/introduction.md](docs/src/content/docs/getting-started/introduction.md).

Run the docs site locally:

```sh
pnpm docs:dev
```

Build and preview the static site:

```sh
pnpm docs:build
pnpm docs:preview
```

## Quick Start

Install Beni. The Node adapter uses [`redis`](https://www.npmjs.com/package/redis) (node-redis), declared as an optional peer dependency, so install it alongside Beni when you use `beni/node`:

```sh
pnpm add beni redis
```

On Bun, install just `beni` — the Bun adapter uses Bun's built-in Redis client.

Define Redis schemas as plain TypeScript values:

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

Bind a Redis client once:

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

Methods are named after the Redis commands they run:

```ts
await redis.hash(users).hset("42", { name: "Ada", score: 10 });
const user = await redis.hash(users).hget("42");
//    ^? { name: string; score: number } | null

await redis.kv(profiles).set("42", { name: "Ada", score: 10 }, { ttlSeconds: 3600 });

const rawKey = redis.hash(users).key("42"); // "user:42"
const pong = await redis.raw.send(["PING"]);
```

## Runtime Support

One runtime-agnostic core, thin client adapters — the same typed API everywhere.

| Runtime | Adapter | Client |
|---|---|---|
| Node.js | `beni/node` | [`redis`](https://www.npmjs.com/package/redis) (node-redis), optional peer dependency |
| Bun | `beni/bun` | Bun's built-in Redis client — no extra package |
| Deno | `beni/node` | `npm:redis` via Deno's npm compatibility |
| Edge / serverless | `beni/upstash` | HTTP adapter over the Upstash REST protocol (or any compatible server) — zero deps |

`redis` is an optional peer dependency, so non-Node runtimes stay dependency-free.
Blocking commands, sessions, and `WATCH` transactions need a persistent connection
(Node/Bun/Deno); the HTTP/edge adapter (`beni/upstash`) covers the stateless command
surface — the whole typed API minus those connection-bound features.

**Server compatibility** (the integration suite runs against every row):

| Server | Result |
|---|---|
| Redis 8 | Full surface. |
| Redis 7.4 | All but `hsetex`/`hgetex`/`hgetdel` (Redis 8 commands). |
| Redis 7.2 | Additionally no hash field TTLs (`hexpire`/`httl`/…, introduced in 7.4). |
| Valkey 8 | Same profile as Redis 7.2 (Valkey forked pre-7.4) — 65/67 integration tests. |
| Dragonfly | Common surface works (kv/hash/set/list/zset/stream/bitmap/pub-sub/tx/scripts); no `LCS`, `GEOSEARCHSTORE`, or hash field TTLs, plus minor semantic edges. |

Everything else — streams, sorted sets, `lmpop`, `sintercard`, geo, bitfields —
works from Redis 7.2 up.

## Primitives

`beni/primitives` ships the batteries you'd otherwise hand-roll (and get subtly
wrong), built on any adapter — including the edge:

```ts
import { cache, lock, ratelimit } from "beni/primitives";

// A correct distributed lock — never frees a lock that expired and was re-acquired.
await lock(client).run("order:42", async () => { /* critical section */ });

// A sliding-window rate limiter — one atomic round trip per check.
const { success } = await ratelimit(client, { limit: 10, windowMs: 60_000 }).check(userId);

// A read-through cache with stampede protection — one loader call per miss.
const profile = await cache<Profile>(client, { ttlMs: 60_000 })
  .get(userId, () => db.loadProfile(userId));
```

## More

This repository contains ESM package exports, runtime entrypoints, `tsdown`
builds with declaration files, Vitest tests, typed Redis data-structure stores,
scans, Pub/Sub, transactions, scripts, TTL options, and the runtime adapters.

See [docs/src/content/docs/getting-started/quick-start.md](docs/src/content/docs/getting-started/quick-start.md)
for the main guide and
[docs/src/content/docs/examples.md](docs/src/content/docs/examples.md) for
lower-level examples.

Run the Node example against Redis:

```sh
REDIS_URL=redis://127.0.0.1:6379 pnpm example:node
```

The walkthrough is in [examples/node-basic.md](examples/node-basic.md), and
[examples/beni-use-cases.md](examples/beni-use-cases.md) works through
common application workloads (sessions, queues, leaderboards, rate limiting)
end to end.

## Redis benchmark

Start Redis in Docker:

```sh
pnpm redis:build
pnpm redis:run
```

Run the benchmark in another shell:

```sh
pnpm bench
```

Options:

```sh
REDIS_URL=redis://127.0.0.1:6379 BENCH_ITERATIONS=10000 BENCH_PIPELINE=64 pnpm bench
```

The benchmark compares a tiny dependency-free RESP client baseline against the
Beni Node adapter.

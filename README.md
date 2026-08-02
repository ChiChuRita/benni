# Benni

**The end-to-end typed Redis client for TypeScript.** One typed API across
Node, Bun, Deno, and the edge.

Declare your Redis data model once with typed codecs, bind a client, and call
methods named after the Redis commands they run, with full input **and output**
type inference and typed key prefixing. Your declared types travel from write to
read, so replies come back as *your* types, not `string | null`.

**[Documentation](https://chichurita.github.io/benni/)** ·
**[llms.txt](llms.txt)** (condensed reference for coding agents)

## Install

```sh
pnpm add benni redis
```

[`redis`](https://www.npmjs.com/package/redis) (node-redis) is an optional peer
dependency used only by the Node adapter. On Bun, install just `benni`; the Bun
adapter uses Bun's built-in Redis client. On the edge, install just `benni`; the
HTTP adapter needs nothing but `fetch`.

## Quick Start

Three files: declare schemas, bind a client, use it.

```ts
// schema.ts: plain TypeScript values; they create no keys and run no migrations
import { hash, json, kv, number, string } from "benni/schema";

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

```ts
// redis.ts: bind once, export the handle
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});

export const redis = benni(client, { schema });
```

```ts
// app.ts: methods are named after the Redis commands they run
import { redis } from "./redis";

await redis.query.users.hset("42", { name: "Ada", score: 10 });

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number } | null

await redis.query.profiles.set("42", { name: "Ada", score: 10 }, {
  ttlSeconds: 3600
});

const rawKey = redis.query.users.key("42"); // "user:42"
const pong = await redis.raw.send(["PING"]);
```

Because the schema module is bound to the client, each store is reachable by its
export name through `redis.query`. The explicit `redis.hash(users)` /
`redis.kv(profiles)` accessors return the same store; see the
[Schema Registry](https://chichurita.github.io/benni/core-concepts/schema-registry/).

## They type the commands; Benni types your data

`node-redis` and `ioredis` are already typed, but only at the *command surface*.
A reply comes back as Redis's generic wire shape, and the type is gone the moment
your data crosses the Redis edge; you re-parse and cast it by hand, and that cast
is where the bugs live:

```ts
// raw node-redis: typed… as a string
const raw = await nodeRedis.get("profile:42"); // string | null
const cast = JSON.parse(raw!) as UserProfile; // hand-cast; the compiler never checked it

// Benni: your declared type, decoded for you
const user = await redis.query.profiles.get("42");
//    ^? UserProfile | null
```

Your Redis client types the commands; Benni types your data. Benni is a typed
**client**, not an ORM or object mapper:
schemas are plain TypeScript values that don't create keys, run migrations, or
block raw Redis access (`redis.raw.send([...])` is always there).

Works with your validator: pass any [Standard Schema](https://standardschema.dev)
validator (Zod, Valibot, ArkType) and `json(zodSchema)` gives runtime-validated,
fully inferred reads, with zero added dependencies.

## Philosophy

- **Command names stay.** `hgetall` runs `HGETALL`. Benni adds a type layer, not
  a query language, so everything you know about Redis still applies.
- **Schemas are values, not migrations.** No CLI, no codegen, no generated
  files. Declaring a schema creates no keys and runs nothing.
- **One round trip whenever Redis allows one.** `hget` is a single `HMGET`, not
  a pipeline of `HGET`s; `ratelimit.check()` is one atomic Lua call. On the edge
  every command is an HTTP request, so Benni never quietly turns one call into
  four.
- **Nothing is hidden.** No lazy loading, no identity map. The key is always
  yours (`.key(id)`), and `redis.raw` is always there.
- **Nothing is silent.** An unexpected reply throws `ReplyShapeError` with the
  raw value attached; it never casts and moves on. Adapters that can't do
  something say so instead of emulating it.
- **Batteries only for what's easy to get wrong.** A correct lock, an accurate
  sliding window, a stampede-proof cache. Not a search engine or an index
  manager.

[Read the full philosophy →](https://chichurita.github.io/benni/getting-started/philosophy/)

## Runtime Support

One runtime-agnostic core, thin client adapters, the same typed API everywhere.

| Runtime | Adapter | Client |
|---|---|---|
| Node.js | `benni/node` | [`redis`](https://www.npmjs.com/package/redis) (node-redis), optional peer dependency |
| Node.js | `benni/ioredis` | [`ioredis`](https://www.npmjs.com/package/ioredis), optional peer dependency. **Can adopt a client you already have** |
| Bun | `benni/bun` | Bun's built-in Redis client, no extra package |
| Deno | `benni/node` | `npm:redis` via Deno's npm compatibility |
| Edge / serverless | `benni/upstash` | HTTP adapter over the Upstash REST protocol (or any compatible server), zero deps |

Already running ioredis? You don't have to switch clients to use Benni: hand your
existing instance to `benni/ioredis` and it shares that connection. Benni closes only
what it leased from it; the client stays yours.

```ts
import Redis from "ioredis";
import { ioredis } from "benni/ioredis";

const client = await ioredis(myExistingRedis); // or a URL, or options
```

`redis` and `ioredis` are optional peer dependencies, so non-Node runtimes stay dependency-free.
Blocking commands, sessions, `WATCH` transactions, and Pub/Sub *subscribing* need a
persistent connection (Node/Bun/Deno); the HTTP/edge adapter (`benni/upstash`) covers
the stateless command surface, the whole typed API minus those connection-bound
features. Pub/Sub *publishing* is one stateless `PUBLISH`, so it works everywhere,
edge included.

Subscribing takes no setup: the first subscription leases one subscriber connection
from the client you already bound, multiplexes every channel and pattern onto it, and
closes it when the last subscription goes away.

```ts
import { channel, json } from "benni/schema";

const userEvents = channel(
  "events:user",
  json<{ id: string; action: "created" | "deleted" }>()
);

const subscription = await redis.pubsub.channel(userEvents).subscribe((message) => {
  console.log(message.action);
  //          ^? "created" | "deleted"
});

await redis.pubsub.channel(userEvents).publish({ id: "42", action: "created" });
await subscription.unsubscribe();
```

Pattern subscriptions are Node-only for now; Bun 1.3.14's `psubscribe` hangs
upstream, so the Bun adapter reports patterns as unsupported instead of deadlocking.

**Server compatibility** (CI runs the integration suite against Redis 8 and an
Upstash-REST-compatible endpoint; the other rows are verified manually):

| Server | Result |
|---|---|
| Redis 8 | Full surface. |
| Redis 7.4 | All but `hsetex`/`hgetex`/`hgetdel` (Redis 8 commands). |
| Redis 7.2 | Additionally no hash field TTLs (`hexpire`/`httl`/…, introduced in 7.4). |
| Valkey 8 | Same profile as Redis 7.2 (Valkey forked pre-7.4). |
| Dragonfly | Common surface works (kv/hash/set/list/zset/stream/geo/hll/bitmap/pub-sub/tx/scripts); no `LCS`, `GEOSEARCHSTORE`, or hash field TTLs. |

Everything else (streams, sorted sets, `lmpop`, `sintercard`, geo, bitfields)
works from Redis 7.2 up.

## Primitives

`benni/primitives` ships the batteries you'd otherwise hand-roll (and get subtly
wrong), built on any adapter, including the edge:

```ts
import { cache, lock, queue, ratelimit } from "benni/primitives";

// A correct distributed lock: never frees a lock that expired and was re-acquired.
await lock(client).run("order:42", async () => { /* critical section */ });

// A sliding-window rate limiter: one atomic round trip per check.
const { success } = await ratelimit(client, { limit: 10, windowMs: 60_000 }).check(userId);

// A read-through cache with stampede protection: one loader call per miss.
const profile = await cache<Profile>(client, { ttlMs: 60_000 })
  .get(userId, () => db.loadProfile(userId));

// A job queue built for model calls: heartbeat leases so a ten-minute
// generation is ordinary, a resumable output stream per job, and cancellation
// that aborts the provider call instead of just marking a row.
const jobs = queue<{ prompt: string }, string>(client, { prefix: "generate" });
const { id } = await jobs.enqueue({ prompt }, { idempotencyKey: requestId });

jobs.worker(async (job) => {
  const { textStream } = streamText({ model, prompt: job.payload.prompt, abortSignal: job.signal });
  let text = "";
  for await (const delta of textStream) { text += delta; await job.emit(delta); }
  return text;
}, { concurrency: 8 });

// Reconnecting client replays from where it left off, no second generation.
for await (const event of jobs.watch(id, { after: lastSeenEventId })) {
  if (event.type === "chunk") write(event.data);
}
```

## Docs

The full documentation is at
**[chichurita.github.io/benni](https://chichurita.github.io/benni/)**. For LLMs and
coding agents, the same content is served flattened at
[`/llms.txt`](https://chichurita.github.io/benni/llms.txt) and
[`/llms-full.txt`](https://chichurita.github.io/benni/llms-full.txt); the
[`llms.txt`](llms.txt) shipped inside the package is the condensed version.

The site is built with [Astro Starlight](https://starlight.astro.build/) and
lives in [docs/](docs/). Content pages start at
[docs/src/content/docs/getting-started/introduction.md](docs/src/content/docs/getting-started/introduction.md).
Run it locally with `pnpm docs:dev`, or build and preview the static site with
`pnpm docs:build` and `pnpm docs:preview`.

## More

This repository contains ESM package exports, runtime entrypoints, `tsdown`
builds with declaration files, Vitest tests, typed Redis data-structure stores,
scans, Pub/Sub, transactions, scripts, TTL options, and the runtime adapters.

See [docs/src/content/docs/getting-started/quick-start.md](docs/src/content/docs/getting-started/quick-start.md)
for the main guide and
[docs/src/content/docs/examples.md](docs/src/content/docs/examples.md) for a
copy-pasteable example of every data structure.

Run the Node example against Redis:

```sh
REDIS_URL=redis://127.0.0.1:6379 pnpm example:node
```

The walkthrough is in [examples/node-basic.md](examples/node-basic.md), and
[examples/benni-use-cases.md](examples/benni-use-cases.md) works through
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
Benni Node adapter.

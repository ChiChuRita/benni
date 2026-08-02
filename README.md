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

The quick start below stores a JSON value through a validator, so it also uses
[`zod`](https://zod.dev). Any [Standard Schema](https://standardschema.dev)
validator works (Zod, Valibot, ArkType) and Benni depends on none of them; the
validator you already use is the one it will use.

## Quick Start

Three files: declare schemas, bind a client, use it.

```ts
// schema.ts: plain TypeScript values; they create no keys and run no migrations
import { hash, json, kv, number, string } from "benni/schema";
import { z } from "zod";

export const users = hash("user", {
  name: string(),
  score: number()
});

// json(validator) infers the type from the validator and checks every read
// against it at runtime. Prefer this form for JSON values.
const profile = z.object({
  name: z.string(),
  score: z.number()
});

export type Profile = z.infer<typeof profile>;
export const profiles = kv("profile", json(profile));
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

One exception worth knowing up front: `redis.query.x` gives you the store for the
schema's kind, and a `kv(prefix, number())` is a string keyspace, so it has `get`
and `set` but no `incr`. Counters are an alternate view over the same keys, so
reach for `redis.counter(x)` (`incr`, `incrby`, `decr`, …) and `redis.string(x)`
when you want those.

```ts
export const views = kv("views", number()); // schema.ts

await redis.counter(views).incr("post-1"); // 1, typed as number
const total = await redis.query.views.get("post-1"); // number | null, no incr here
```

### `json(validator)` checks; `json<T>()` does not

`json(validator)` validates every read: if the stored JSON does not match, the
read throws `ReplyShapeError` with the offending value attached instead of
handing back a value that lies about its type.

`json<T>()` is the escape hatch for "I wrote this, I trust it, skip the check".
It is a pure cast: `JSON.parse` plus an assertion that the result is `T`, with no
runtime validation at all. A record written by an older version of your code, by
another service, or by hand in `redis-cli` comes back typed as a complete `T`
even when fields are missing, and nothing throws. Use it when the value has no
shape worth checking or when you own every writer; use `json(validator)`
everywhere else, and especially anywhere the data outlives the code that wrote
it.

## They type the commands; Benni types your data

`node-redis` and `ioredis` are already typed, but only at the *command surface*.
A reply comes back as Redis's generic wire shape, and the type is gone the moment
your data crosses the Redis edge; you re-parse and cast it by hand, and that cast
is where the bugs live:

```ts
// raw node-redis: typed… as a string
const raw = await nodeRedis.get("profile:42"); // string | null
// hand-cast; the compiler never checked it, and neither did anything else
const cast = JSON.parse(raw!) as { name: string; score: number };

// Benni: your declared type, decoded (and validated) for you
const user = await redis.query.profiles.get("42");
//    ^? { name: string; score: number } | null
```

Your Redis client types the commands; Benni types your data. Benni is a typed
**client**, not an ORM or object mapper:
schemas are plain TypeScript values that don't create keys, run migrations, or
block raw Redis access (`redis.raw.send([...])` is always there).

### The same amount of code, checked by the compiler

Benni is not a way to write less Redis code, and we would rather say so than have
you find out. We built the same URL shortener twice against Redis 8, once through
Benni and once on raw `node-redis`: **164 lines with Benni, 177 lines raw.** Call
that a rounding error.

What changes is what the compiler catches. We then planted the same nine ordinary
Redis bugs in both versions:

1. a typo in a hash field name
2. a wrong value type for a declared field
3. a missing required field
4. a read of a field that is not on the schema
5. a nullable read treated as non-null
6. a counter reply assumed to be a string
7. the wrong shape pushed into a typed list
8. the wrong store kind for the schema
9. two schemas' key spaces mixed up

**Benni caught 9 of 9 at compile time** (11 type errors; nothing ran).
**Raw `node-redis` caught 0 of 9.** All nine compiled clean and reached the
server.

A raw failure is also rarely a crash. It is silent corruption:

```text
link:a  as stored: { ur: 'https://x.example', owner: 'ada',
                     createdAt: '2026-08-02', url: 'https://x.example' }
link:a2 as stored: { url: 'https://x.example' }
Number(createdAt) => NaN
```

The typo added a second field instead of replacing one; the date string in a
number slot reads back as `NaN`; the incomplete write left a partial record.
Nothing threw, so nothing pages you. That is the trade Benni is offering: not
fewer lines, the same lines with the compiler reading them.

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
  raw value attached; it never casts and moves on. A Redis error reply throws
  `RedisServerError` on every adapter, with the error code parsed onto `.code`,
  so a `WRONGTYPE` handler written on Node still matches on the edge. Adapters
  that can't do something say so instead of emulating it.
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

// A distributed lock that never frees a lock that expired and was re-acquired.
// Acquisition is fail-fast by default (retries: 0): a caller that finds the lock
// held throws LockNotAcquiredError immediately. Pass retries when callers
// legitimately contend for the same id and should queue up behind it instead.
await lock(client).run("order:42", async () => { /* critical section */ }, {
  retries: 20,
  retryDelayMs: 100
});

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

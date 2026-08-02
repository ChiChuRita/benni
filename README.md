# Benni

**The end-to-end typed Redis client for TypeScript.** Declare your data model
once, and replies come back as *your* types, not `string | null`. One API across
Node, Bun, Deno, and the edge.

[![npm](https://img.shields.io/npm/v/benni?color=%23c14444)](https://www.npmjs.com/package/benni)
[![CI](https://github.com/ChiChuRita/benni/actions/workflows/ci.yml/badge.svg)](https://github.com/ChiChuRita/benni/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/benni)](LICENSE)

**[Documentation](https://chichurita.github.io/benni/)** ·
**[llms.txt](llms.txt)** (condensed reference for coding agents)

```sh
pnpm add benni redis
```

`redis` (node-redis) is an optional peer used only by the Node adapter. Bun uses
its built-in client and the edge adapter needs nothing but `fetch`, so both
install just `benni`.

## Quick Start

Three files: declare schemas, bind a client, use it.

```ts
// schema.ts: plain TypeScript values. They create no keys and run no migrations.
import { hash, json, kv, number, string } from "benni/schema";
import { z } from "zod";

export const users = hash("user", { name: string(), score: number() });

// json(validator) infers the type from the validator and checks every read
// against it at runtime. Any Standard Schema validator works (Zod, Valibot,
// ArkType); Benni depends on none of them.
const profile = z.object({ name: z.string(), score: z.number() });

export type Profile = z.infer<typeof profile>;
export const profiles = kv("profile", json(profile));
```

```ts
// redis.ts: bind once, export the handle
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

export const redis = benni({
  client: node({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" }),
  schema
});

// Optional, once per app: now the bare `Benni` type is this handle, so no
// signature has to repeat `typeof schema`.
declare module "benni" {
  interface Register {
    schema: typeof schema;
  }
}
```

`benni()` takes the adapter's promise unawaited and connects on the first
command, so this file needs no top-level `await`. Pass a client you already
awaited (`benni(client, { schema })`) when you would rather find out about a bad
`REDIS_URL` at startup than at first use.

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

Binding the schema module makes every store reachable by its export name through
`redis.query`. The explicit `redis.hash(users)` / `redis.kv(profiles)` accessors
return the same store, and a few operations only exist there: a `kv` schema is a
string keyspace, so counters live on `redis.counter(x)` (`incr`, `incrby`, …).

Prefer `json(validator)` over `json<T>()`. The first validates every read and
throws `ReplyShapeError` with the offending value attached. The second is a pure
cast with no runtime check, so a record written by older code, another service,
or `redis-cli` comes back typed as a complete `T` even when fields are missing.

## They type the commands; Benni types your data

`node-redis` and `ioredis` are already typed, but only at the *command surface*.
Your type is gone the moment data crosses the Redis edge, and the cast you write
to get it back is where the bugs live:

```ts
// raw node-redis: typed… as a string
const raw = await nodeRedis.get("profile:42"); // string | null
// hand-cast; the compiler never checked it, and neither did anything else
const cast = JSON.parse(raw!) as { name: string; score: number };

// Benni: your declared type, decoded and validated for you
const user = await redis.query.profiles.get("42");
//    ^? { name: string; score: number } | null
```

Benni is a typed **client**, not an ORM: schemas are plain TypeScript values that
don't create keys, run migrations, or block raw access (`redis.raw.send([...])`
is always there).

### Not less code. The same code, checked.

We built the same URL shortener twice against Redis 8: **164 lines with Benni,
177 lines raw.** Call that a rounding error, and we would rather say so than have
you find out.

What changes is what the compiler catches. We planted the same nine ordinary
Redis bugs in both versions: a typo'd hash field, a wrong value type, a missing
required field, a read of a field that isn't on the schema, a nullable read
treated as non-null, a counter reply assumed to be a string, the wrong shape
pushed into a typed list, the wrong store kind, and two schemas' key spaces mixed
up.

**Benni caught 9 of 9 at compile time** (11 type errors; nothing ran).
**Raw `node-redis` caught 0 of 9.** All nine compiled clean and reached the
server, where the failure is not a crash but silent corruption:

```text
link:a  as stored: { ur: 'https://x.example', owner: 'ada', url: '…' }
link:a2 as stored: { url: 'https://x.example' }
Number(createdAt) => NaN
```

The typo added a second field instead of replacing one; the date string in a
number slot reads back as `NaN`; the incomplete write left a partial record.
Nothing threw, so nothing pages you.

## Philosophy

- **Command names stay.** `hgetall` runs `HGETALL`. A type layer, not a query
  language, so everything you know about Redis still applies.
- **Schemas are values, not migrations.** No CLI, no codegen, no generated files.
- **One round trip whenever Redis allows one.** `hget` is a single `HMGET`;
  `ratelimit.check()` is one atomic Lua call. Benni never quietly turns one call
  into four, which matters most on the edge.
- **Nothing is hidden.** No lazy loading, no identity map. The key is always
  yours (`.key(id)`), and `redis.raw` is always there.
- **Nothing is silent.** Unexpected replies throw `ReplyShapeError` with the raw
  value attached; Redis error replies throw `RedisServerError` with `.code`
  parsed, so a `WRONGTYPE` handler written on Node still matches on the edge.
- **Batteries only for what's easy to get wrong.** A correct lock, an accurate
  sliding window, a stampede-proof cache. Not a search engine.

[Read the full philosophy →](https://chichurita.github.io/benni/getting-started/philosophy/)

## Runtime Support

One runtime-agnostic core, thin client adapters, the same typed API everywhere.

| Runtime | Adapter | Client |
|---|---|---|
| Node.js | `benni/node` | [`redis`](https://www.npmjs.com/package/redis) (node-redis), optional peer |
| Node.js | `benni/ioredis` | [`ioredis`](https://www.npmjs.com/package/ioredis), optional peer. **Can adopt a client you already have** |
| Bun | `benni/bun` | Bun's built-in Redis client, no extra package |
| Deno | `benni/node` | `npm:redis` via Deno's npm compatibility |
| Edge / serverless | `benni/upstash` | HTTP over the Upstash REST protocol (or any compatible server), zero deps |

Already on ioredis? You don't have to switch clients. Hand your instance to
`benni/ioredis` and it shares that connection; Benni closes only what it leased.

```ts
import { ioredis } from "benni/ioredis";

const client = await ioredis(myExistingRedis); // or a URL, or options
```

Blocking commands, sessions, `WATCH`, and Pub/Sub *subscribing* need a persistent
connection (Node/Bun/Deno). The edge adapter covers the stateless surface, which
is the whole typed API minus those. Publishing is one stateless `PUBLISH`, so it
works everywhere.

Subscribing takes no setup: the first subscription leases one subscriber
connection from the client you already bound, multiplexes every channel and
pattern onto it, and closes it when the last subscription goes away.

```ts
import { channel, json } from "benni/schema";

const userEvents = channel("events:user", json<{ action: "created" | "deleted" }>());

await redis.pubsub.channel(userEvents).subscribe((message) => {
  console.log(message.action);
  //          ^? "created" | "deleted"
});
```

Pattern subscriptions are Node-only for now; Bun 1.3.14's `psubscribe` hangs
upstream, so the Bun adapter reports patterns as unsupported instead of
deadlocking.

**Server compatibility.** CI runs the integration suite against Redis 8 and an
Upstash-REST-compatible endpoint; the other rows are verified manually.

| Server | Result |
|---|---|
| Redis 8 | Full surface. |
| Redis 7.4 | All but `hsetex`/`hgetex`/`hgetdel` (Redis 8 commands). |
| Redis 7.2 | Additionally no hash field TTLs (`hexpire`/`httl`/…, added in 7.4). |
| Valkey 8 | Same profile as Redis 7.2 (Valkey forked pre-7.4). |
| Dragonfly | Common surface works; no `LCS`, `GEOSEARCHSTORE`, or hash field TTLs. |

## Primitives

The batteries you'd otherwise hand-roll and get subtly wrong, on any adapter
including the edge. They declare themselves the way the data structures do, so
they live in the same schema module and land in the same `redis.query`:

```ts
// schema.ts
import { cache, json, lock, queue, ratelimit } from "benni/schema";

export const orderLocks = lock("order", { ttlMs: 10_000 });
export const apiLimit = ratelimit("api", { limit: 10, windowMs: 60_000 });
export const profiles = cache("profile", { ttlMs: 60_000, codec: json(Profile) });
export const generate = queue<{ prompt: string }, string>("generate");
```

```ts
// app.ts
// Never frees a lock that expired and was re-acquired. Fail-fast by default;
// pass retries when callers legitimately contend for the same id.
await redis.query.orderLocks.run("42", async () => { /* critical section */ }, {
  retries: 20,
  retryDelayMs: 100
});

// A sliding window: one atomic round trip per check.
const { success } = await redis.query.apiLimit.check(userId);

// Read-through with stampede protection: one loader call per miss.
const profile = await redis.query.profiles.get(userId, () => db.loadProfile(userId));
```

`benni/primitives` keeps the client-taking form for code that holds a client but
no handle, such as a middleware factory: `ratelimit({ client, limit, windowMs })`.

The queue is built for model calls: heartbeat leases so a ten-minute generation
is ordinary, a resumable output stream per job, and cancellation that aborts the
provider call instead of just marking a row.

```ts
const jobs = redis.query.generate;
const { id } = await jobs.enqueue({ prompt }, { idempotencyKey: requestId });

jobs.worker(async (job) => {
  const { textStream } = streamText({ model, prompt: job.payload.prompt, abortSignal: job.signal });
  let text = "";
  for await (const delta of textStream) { text += delta; await job.emit(delta); }
  return text;
}, { concurrency: 8 });

// A reconnecting client replays from where it left off, no second generation.
for await (const event of jobs.watch(id, { after: lastSeenEventId })) {
  if (event.type === "chunk") write(event.data);
}
```

## Docs And Development

Full documentation lives at
**[chichurita.github.io/benni](https://chichurita.github.io/benni/)**, built with
[Astro Starlight](https://starlight.astro.build/) from [docs/](docs/). For coding
agents, the same content is served flattened at
[`/llms.txt`](https://chichurita.github.io/benni/llms.txt) and
[`/llms-full.txt`](https://chichurita.github.io/benni/llms-full.txt).

```sh
pnpm docs:dev     # docs site with live reload
pnpm check        # lint, typecheck, coverage, build, publint, attw
```

Worked examples: [examples/node-basic.md](examples/node-basic.md) for a
walkthrough, [examples/benni-use-cases.md](examples/benni-use-cases.md) for
sessions, queues, leaderboards, and rate limiting end to end, and
[docs/src/content/docs/examples.md](docs/src/content/docs/examples.md) for a
copy-pasteable example of every data structure.

```sh
pnpm redis:build && pnpm redis:run          # Redis in Docker
REDIS_URL=redis://127.0.0.1:6379 pnpm example:node
```

The benchmark compares a tiny dependency-free RESP client baseline against the
Benni Node adapter:

```sh
pnpm bench
BENCH_ITERATIONS=10000 BENCH_PIPELINE=64 pnpm bench   # options
```

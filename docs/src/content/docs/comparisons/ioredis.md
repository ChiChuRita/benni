---
title: "Beni vs ioredis"
description: "An honest comparison of Beni and ioredis for TypeScript projects: what each one types, which features only ioredis has, and how to choose."
---

Short version: **ioredis is a Redis client. Beni is a typed layer over one.** They
are not competing for the same job, and in fact
[`beni/ioredis`](/beni/runtime/ioredis/) runs the whole typed API *on* ioredis,
including an instance you already have. So the real question is not "which one"
but "do I want my data typed on top of the client I already run".

## Choose by what you need

**Reach for plain ioredis when** you want nothing between you and the commands,
or you need an ecosystem package built specifically on it (BullMQ being the big
one), or you rely on transport features Beni does not model, such as Sentinel
failover and sharded Pub/Sub.

**Reach for Beni when** your application stores values with a shape you care
about, and you want the declared type to survive the round trip, plus one API
that runs unchanged on Node, Bun, Deno, and edge/serverless runtimes.

**You do not have to choose the transport.** Beni's ioredis adapter means
adopting Beni is not a client migration: keep ioredis, keep your connection, and
add types on top.

## The core difference: command types vs data types

ioredis is fully typed, but only at the *command surface*. The type describes
Redis's wire shape, not your value:

```ts
// plain ioredis: typed, as a string
const raw = await ioredisClient.get("profile:42"); // string | null
const profile = JSON.parse(raw!) as Profile; // hand-cast; the compiler never checked it

const h = await ioredisClient.hgetall("user:42"); // Record<string, string>
const score = Number(h.score); // coerce by hand, every time
```

The type is gone the moment your data crosses the Redis edge, and the cast you
write to get it back is unchecked. Beni moves the declaration up front:

```ts
const users = hash("user", { name: string(), score: number() });

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number } | null
```

`score` is a `number` because the schema says so, with no coercion and no cast.
Keys are derived from the schema too (`redis.query.users.key("42")` is
`"user:42"`), so prefix strings stop being scattered across the codebase.

## What ioredis has that Beni does not

Stated plainly, because these are real reasons to keep reaching for ioredis
directly. Note that the first two are about *transport*, so
[`beni/ioredis`](/beni/runtime/ioredis/) lets you keep them and still get typed
data on top.

- **Redis Cluster routing.** ioredis has `Redis.Cluster` with slot awareness,
  `MOVED`/`ASK` handling, and multi-node routing. Beni has no transport of its
  own and does not route. What it does add is slot *co-location*: schemas
  declare where their hash tag goes, and cross-slot commands are caught at
  compile time and (opt-in) before they are sent. See
  [Redis Cluster](/beni/advanced/cluster/).
- **Sentinel.** High-availability failover via Sentinel is ioredis's, not
  Beni's, though an adopted ioredis client keeps it.
- **Sharded Pub/Sub.** `SSUBSCRIBE`/`SPUBLISH` are not modelled by Beni's typed
  Pub/Sub.
- **Ecosystem lock-in, in the good sense.** BullMQ, `ioredis-mock`, rate-limiter
  libraries, and many framework session stores expect an ioredis instance.
- **Maturity.** ioredis has been the default for years, with the battle-testing
  that implies.

## What Beni has that ioredis does not

- **End-to-end typed data**, as above.
- **One API across runtimes.** The same typed calls run on Node and Bun over TCP,
  and on Cloudflare Workers / Vercel Edge over HTTP. Only the adapter import
  changes. Practically, this means local development against real Redis over TCP
  and production over HTTP from the same code.
- **Schema-derived keys and TTLs** instead of hand-built strings.
- **Correct primitives included.** A distributed lock that never frees a lock
  that expired and was re-acquired, a sliding-window rate limiter in one atomic
  round trip, a stampede-proof read-through cache, and an AI-shaped job queue
  with resumable output streams.
- **Validator integration.** `json(zodSchema)` gives runtime-validated, inferred
  reads through any [Standard Schema](https://standardschema.dev) validator (Zod,
  Valibot, ArkType) with no added dependency.

## Side by side

| | ioredis | Beni |
| --- | --- | --- |
| Raw command access | Yes | Yes, via `redis.raw.send([...])` |
| Command-level types | Yes | Yes |
| Your data's types survive a read | No, you cast | Yes |
| Schema-derived keys | Manual | Yes |
| Runtimes | Node (and Bun/Deno via compat) | Node, Bun, Deno, edge/serverless |
| Redis Cluster routing | Yes | No, but slot co-location is typed |
| Sentinel | Yes | Via an adopted ioredis client |
| Sharded Pub/Sub | Yes | No |
| Distributed lock / rate limit / cache / queue | Via ecosystem packages | Built in |
| Transport | Its own | ioredis, node-redis, Bun's client, or HTTP |

## Migration cost, honestly

There is no client migration. `beni/ioredis` takes a URL, ioredis options, or an
ioredis instance you already built and tuned:

```ts
import Redis from "ioredis";
import { beni } from "beni";
import { ioredis } from "beni/ioredis";
import * as schema from "./schema";

const existing = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");

const client = await ioredis(existing);
export const redis = beni(client, { schema });
```

An adopted client is borrowed, not taken over: `client.close()` reaps only the
sessions and subscriber connections Beni leased from it, and leaves your client
open. See [the ioredis adapter](/beni/runtime/ioredis/) for the details.

That also means ioredis-dependent packages in the same process (BullMQ, for
instance) keep working on the very same connection. Nothing about Beni's schemas
prevents another client from reading or writing the same keys: schemas describe
keys, they do not own them.

## Adopting incrementally

Beni does not require a migration. Schemas are plain values that create no keys
and run no migrations, so you can declare one key family, use it, and leave the
rest of your Redis access exactly as it is. `redis.raw.send([...])` is always
available for commands you have not modelled.

---
title: "Philosophy"
description: "The seven principles behind Benni's API: command names stay, schemas are plain values, one round trip whenever Redis allows one, and nothing is hidden or silent."
---

Benni is built on a small set of opinions. They explain most of the API, including the parts that look like omissions.

## Command Names Stay

Benni adds a type layer, not a query language. Methods are named after the Redis commands they run, so `hgetall` runs `HGETALL` and `zincrby` runs `ZINCRBY`. Every Redis doc page, every StackOverflow answer, and every `MONITOR` line still applies.

```ts
await redis.hash(users).hincrby("42", "score", 1); // HINCRBY user:42 score 1
```

Where Redis itself has folded old commands into a newer one, Benni follows Redis rather than the history: `zrange(id, { byScore: true, min, max })` mirrors modern `ZRANGE ... BYSCORE` instead of reviving the deprecated `ZRANGEBYSCORE`.

The argument list follows from the same rule, which is worth knowing before you reach for the docs on a method you have not called yet. A command with one fixed form takes its arguments positionally, in the command's own order:

```ts
await redis.zset(board).zremrangebyscore("daily", "-inf", cutoff); // ZREMRANGEBYSCORE board:daily -inf <cutoff>
await redis.zset(board).zincrby("daily", 5, "alice");              // ZINCRBY board:daily 5 alice
```

A command with modifiers, or with several forms that give the same slot different meanings, takes one options object instead, so the call site reads like the modifiers it is choosing:

```ts
await redis.zset(board).zrange("daily", { start: 0, stop: 9, rev: true, withScores: true });
```

That is why `zrange` puts even its bounds in the object while `zremrangebyscore` does not: `ZRANGE`'s bounds are indexes, scores, or lex bounds depending on the modifier chosen alongside them, and `ZREMRANGEBYSCORE` only ever takes a score range.

This is deliberate altitude. An entity/document API would be further from Redis and would have to re-teach you everything you already know. Staying at command level means the only new thing to learn is the schema.

## Schemas Are Values, Not Migrations

A schema is a plain TypeScript value describing the shape of a key family. Declaring one creates no keys, opens no connection, and runs nothing at import time.

```ts
export const users = hash("user", { name: string(), score: number() });
```

That is why Benni has no CLI, no codegen step, and no migration story: there is nothing to generate and nothing to migrate. Delete a schema and Redis is untouched. Adopt Benni for one key family and leave the other twelve on your raw client.

## One Round Trip Whenever Redis Allows One

Where a single command can do the job, Benni sends a single command:

- `hget(id)` with no field list is one `HMGET`, not a pipeline of `HGET`s. A pipeline can interleave with another client's write and hand you a torn record.
- `hgetex` reads fields and slides the TTL in one command.
- `ratelimit.check()` is one atomic Lua evaluation: expire, count, admit.
- Calls with empty input resolve without touching the network at all.

This matters most on [the edge](/benni/runtime/edge/), where every command is an HTTP request and a saved round trip is tens of milliseconds, not microseconds. Benni will not quietly turn one of your calls into four.

## Nothing Is Hidden

There is no lazy loading, no identity map, no background fetching. A call sends the commands its name says it sends, and nothing else. When Benni cannot express something, it hands you the key instead of inventing an abstraction:

```ts
const key = redis.query.users.key("42"); // "user:42"
await redis.raw.send(["OBJECT", "ENCODING", key]);
```

## Nothing Is Silent

A layer that guesses is worse than no layer. When reality does not match the declared type, Benni throws where it happened:

- A reply that does not match the expected shape throws `ReplyShapeError`, carrying the raw reply on `.reply`. It never casts and moves on.
- Bad caller input throws `ValidationError` before anything is sent to Redis.
- An adapter that cannot do something says so. Calling `redis.session()` on the HTTP adapter throws, because the edge has no long-lived connection and pretending otherwise would only move the failure somewhere harder to debug.

`ReplyShapeError` and `ValidationError` both extend `TypeError`, so existing error handling keeps working.

## One API, Every Runtime, Honest About The Differences

The same typed API runs on Node, Bun, Deno, and the edge. Benni does not polyfill away real platform differences: sessions, blocking commands, `WATCH`, and Pub/Sub subscribing all need a persistent connection, so the HTTP adapter omits them rather than emulating them by polling behind your back. Pipelines and `MULTI`/`EXEC`, which HTTP *can* do, are there. See [what works and what doesn't](/benni/runtime/edge/) on the edge.

## Batteries Only For What Is Easy To Get Wrong

[Primitives](/benni/primitives/lock/) exist for the handful of patterns that are subtly hard: a correct distributed lock, an accurate sliding window, a stampede-proof cache, a job queue with leases and resumable output. These are worth shipping because most hand-rolled versions have a race in them.

Benni deliberately does not ship a secondary-index manager, a full-text search layer, or a general job framework. Those belong to Redis Search, or to BullMQ, or to your application, and shipping a mediocre version of each is how a client turns into an ORM.

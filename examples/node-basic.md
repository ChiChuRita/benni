# Node Basic Example

This example shows how an app uses Benni from Node:

- declare Redis schemas as plain TypeScript values
- bind a client once with `benni(client, { schema })`
- store JSON objects, counters, hashes, sets, lists, and sorted sets, with
  every read decoded back to your declared type
- clean up keys after the demo

The runnable file is [node-basic.mjs](node-basic.mjs).

## Run It

Start Redis:

```sh
pnpm redis:build
pnpm redis:run
```

Run the example in another shell:

```sh
REDIS_URL=redis://127.0.0.1:6379 pnpm example:node
```

Expected output shape:

```js
{
  profile: { name: "Ada", score: 10 },
  visits: 3,
  user: { name: "Ada", score: 15 },
  userRoles: ["admin", "editor"],
  nextJob: { id: "job-1", kind: "email" },
  topScores: [
    { member: "grace", score: 12 },
    { member: "ada", score: 15 }
  ],
  fullKey: "example:user:demo:…",
  pong: "PONG"
}
```

## Declare Schemas Once

Schemas are plain values: a key prefix bound to a Redis data structure and a
codec. They don't create keys or run migrations; they just carry your types.

```js
import { benni } from "benni";
import { node } from "benni/node";
import { hash, json, kv, list, number, set, string, zset } from "benni/schema";

const schema = {
  profiles: kv("example:profile", json()),
  counters: kv("example:counter", number()),
  users: hash("example:user", {
    name: string(),
    score: number()
  }),
  roles: set("example:roles", string()),
  jobs: list("example:jobs", json()),
  leaderboard: zset("example:leaderboard", string())
};
```

In TypeScript, `json<UserProfile>()` pins the value type so every later read
comes back as `UserProfile | null`, with no casts.

## Bind A Client

```js
const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});

const redis = benni(client, { schema });
```

## Use Typed Operations

Methods are named after the Redis commands they run:

```js
await redis.kv(schema.profiles).set("demo:1", { name: "Ada", score: 10 });
const profile = await redis.kv(schema.profiles).get("demo:1");

const visits = await redis.counter(schema.counters).incrby("demo:1", 3);

await redis.hash(schema.users).hset("demo:1", { name: "Ada", score: 10 });
await redis.hash(schema.users).hincrby("demo:1", "score", 5);
const user = await redis.hash(schema.users).hget("demo:1");

await redis.set(schema.roles).sadd("demo:1", ["admin", "editor"]);

await redis.list(schema.jobs).rpush("demo:1", [
  { id: "job-1", kind: "email" },
  { id: "job-2", kind: "report" }
]);
const nextJob = await redis.list(schema.jobs).lpop("demo:1");

await redis.zset(schema.leaderboard).zadd("daily", [
  { member: "ada", score: 15 },
  { member: "grace", score: 12 }
]);
const topScores = await redis.zset(schema.leaderboard).zrange("daily", {
  start: 0,
  stop: -1,
  withScores: true
});
```

## Why This Shape

The app owns the domain types. Benni owns key formatting, encoding/decoding,
and Redis reply checks.

Every resource exposes `key(id)` when you need the full Redis key, and the raw
escape hatch is always there for commands without a typed helper yet:

```js
const fullKey = redis.hash(schema.users).key("demo:1"); // "example:user:demo:1"

await redis.raw.send(["SET", "raw:key", "value"]);
const value = await redis.raw.send(["GET", "raw:key"]);
```

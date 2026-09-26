---
title: "Examples"
description: "Copy-pasteable examples for the schema-first Benni API, one data structure at a time."
---

Every example on this page uses the same shape: declare schemas once, bind a
client once, then reach each data structure through the bound handle.

```ts
// schema.ts
import {
  bitmap,
  channel,
  geo,
  hash,
  hll,
  json,
  kv,
  list,
  number,
  pattern,
  script,
  set,
  stream,
  string,
  zset
} from "benni/schema";

type UserProfile = {
  name: string;
  score: number;
};

export const profiles = kv("profile", json<UserProfile>());
export const counters = kv("counter", number());
export const texts = kv("text", string());

export const users = hash("user", {
  name: string(),
  score: number()
});

export const roles = set("roles", string());
export const jobs = list("jobs", json<{ id: string; kind: "email" | "report" }>());
export const leaderboard = zset("leaderboard", string());

export const events = stream("events", {
  type: string(),
  userId: string()
});

export const activity = bitmap("activity");
export const cities = geo("cities", string());
export const visitors = hll("visitors", string());

export const userEvents = channel(
  "events:user",
  json<{ id: string; action: "created" | "deleted" }>()
);
export const userEventPattern = pattern(
  "events:user:*",
  json<{ id: string; action: string }>()
);

export const incrementBy = script("increment-by", {
  keys: ["counter"],
  args: { amount: number() },
  returns: number(),
  lua: "return redis.call('INCRBY', KEYS[1], ARGV[1])"
});
```

```ts
// redis.ts
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

export const redis = benni({
  client: node({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" }),
  schema
});
```

The sections below assume these two files. The lower-level building blocks the
client is made of (`createKeyValueStore`, `createHashStore`, …) live under
`benni/core` for adapter authors and advanced integrations; see the
[API overview](/benni/api/overview/).

## Typed JSON Key-Value

```ts
await redis.query.profiles.set("42", { name: "Ada", score: 10 }, { ttlSeconds: 60 });

const profile = await redis.query.profiles.get("42");
// profile is UserProfile | null

await redis.query.profiles.mset([
  ["43", { name: "Grace", score: 12 }],
  ["44", { name: "Linus", score: 8 }]
]);

const many = await redis.query.profiles.mget(["42", "43", "missing"]);
// many is Array<UserProfile | null>

await redis.query.profiles.del("42");
```

## Known IDs For Autocomplete

When IDs are known at compile time, pass them into the schema. Editors then
autocomplete IDs such as `"test1"` and full key strings such as `"demo:test1"`.

```ts
import { type RedisKey } from "benni";
import { kv, string } from "benni/schema";

// schema.ts
export const demos = kv("demo", string(), {
  ids: ["test1", "test2"]
});

// app.ts
await redis.query.demos.set("test1", "value");

const key = redis.query.demos.key("test1");
// key is "demo:test1"

type DemoKey = RedisKey<"demo", "test1" | "test2">;
// DemoKey is "demo:test1" | "demo:test2"
```

If IDs come from users, databases, or Redis itself, leave `ids` out and Benni
accepts normal `string | number | bigint` IDs.

## Integer Counter

A `kv` declared with `number()` carries the counter commands on the same
store as `get` and `set`.

```ts
await redis.query.counters.incr("page-views");
await redis.query.counters.incrby("page-views", 5);
await redis.query.counters.decr("page-views");

// Increment and start a 60-second window in one atomic step.
await redis.query.counters.incr("login:1.2.3.4", { ttlMs: 60_000 });
```

## String Commands

A `kv` declared with `string()` carries the Redis string commands that only
make sense for plain string values.

```ts
await redis.query.texts.append("welcome", "hello");
await redis.query.texts.append("welcome", " world");

const firstWord = await redis.query.texts.getrange("welcome", 0, 4);
const length = await redis.query.texts.strlen("welcome");
const value = await redis.query.texts.getex("welcome", 60);
```

## Typed Hash

```ts
await redis.query.users.hset("42", { name: "Ada", score: 10 }, { ttlSeconds: 300 });

const user = await redis.query.users.hget("42");
// user is { name: string; score: number } | null

await redis.query.users.hset("42", "name", "Grace");
const score = await redis.query.users.hincrby("42", "score", 1);
const hasName = await redis.query.users.hexists("42", "name");

await redis.query.users.del("42");
```

## Typed Set

```ts
await redis.query.roles.sadd("user:42", ["admin", "editor"]);

const isAdmin = await redis.query.roles.sismember("user:42", "admin");
const allRoles = await redis.query.roles.smembers("user:42");

await redis.query.roles.srem("user:42", ["editor"]);
await redis.query.roles.del("user:42");
```

## Typed List

```ts
await redis.query.jobs.rpush("pending", [
  { id: "job-1", kind: "email" },
  { id: "job-2", kind: "report" }
]);

const nextJob = await redis.query.jobs.lpop("pending");
// nextJob is { id: string; kind: "email" | "report" } | null

const remaining = await redis.query.jobs.lrange("pending", 0, -1);
// remaining is Array<{ id: string; kind: "email" | "report" }>

await redis.query.jobs.del("pending");
```

## Typed Sorted Set

```ts
await redis.query.leaderboard.zadd("daily", [
  { member: "alice", score: 10 },
  { member: "bob", score: 20 }
]);

const top = await redis.query.leaderboard.zrange("daily", {
  start: 0,
  stop: -1,
  withScores: true
});
// top is Array<{ readonly member: string; readonly score: number }>

await redis.query.leaderboard.zincrby("daily", 5, "alice");
const aliceScore = await redis.query.leaderboard.zscore("daily", "alice");

await redis.query.leaderboard.del("daily");
```

## Typed Stream

```ts
const entryId = await redis.query.events.xadd("audit", {
  type: "login",
  userId: "42"
});

const latest = await redis.query.events.xread("audit", "0-0", { count: 10 });
const history = await redis.query.events.xrange("audit", { count: 10 });

await redis.query.events.del("audit");
```

An entry is `{ id, value }`, and `value` is a `Partial` of the declared fields,
because a stream entry can legally carry any subset of them. Read fields off
`value`, with a fallback for the ones you require:

```ts
for (const entry of history) {
  const type = entry.value.type ?? "(unknown)";
  console.log(entry.id, type, entry.value.userId);
  //                          ^? string | undefined
}
```

## Typed Bitmap

```ts
await redis.query.activity.setbit("2026-07-04", 42, true);

const active = await redis.query.activity.getbit("2026-07-04", 42);
const activeCount = await redis.query.activity.bitcount("2026-07-04");

await redis.query.activity.del("2026-07-04");
```

## Typed Geo

```ts
await redis.query.cities.geoadd("europe", [
  { member: "Berlin", longitude: 13.405, latitude: 52.52 },
  { member: "Paris", longitude: 2.3522, latitude: 48.8566 }
]);

const nearby = await redis.query.cities.geosearch("europe", {
  from: { longitude: 13.405, latitude: 52.52 },
  by: { radius: 1000, unit: "km" },
  withDistance: true,
  withCoordinates: true
});

await redis.query.cities.del("europe");
```

## Typed HyperLogLog

```ts
await redis.query.visitors.pfadd("today", ["user:1", "user:2", "user:1"]);

const approximateVisitors = await redis.query.visitors.pfcount("today");

await redis.query.visitors.del("today");
```

## Cursor Scans

```ts
import { leaderboard, profiles } from "./schema";

for await (const key of redis.scan.kv(profiles, { count: 100 })) {
  // key is a Redis key matching profile:*
}

for await (const entry of redis.scan.zset(leaderboard, "daily")) {
  // entry is { member: string; score: number }
}
```

## Pub/Sub

Subscribing needs no extra setup: the first subscription leases one subscriber
connection from the bound client and closes it again when the last subscription
goes away.

```ts
const subscription = await redis.query.userEvents.subscribe(
  (message) => {
    // message is { id: string; action: "created" | "deleted" }
    console.log(message);
  }
);

await redis.query.userEvents.publish({
  id: "42",
  action: "created"
});

await subscription.unsubscribe();
```

Use a typed pattern when one handler should receive several matching channels:

```ts
const patternSubscription = await redis.query.userEventPattern
  .subscribe((message, channel) => {
    // message is decoded; channel is the concrete channel name
  });

await patternSubscription.unsubscribe();
```

Or consume a subscription as an async iterator, which releases it when the loop
ends:

```ts
const controller = new AbortController();

for await (const message of redis.query.userEvents
  .stream({ signal: controller.signal })) {
  console.log(message.action);
}
```

Publishing is one stateless `PUBLISH` on the bound client, so it works on every
adapter including [`benni/upstash`](/benni/runtime/edge/). Subscribing needs a
connection the adapter can hold, and `redis.pubsub.close()` drops every
subscription at once. See [Pub/Sub](/benni/data-structures/pubsub/).

## Typed Transaction

`redis.multi()` builds a `MULTI`/`EXEC` transaction whose result is a
position-typed tuple:

```ts
import { booleanNumberReply, okReply, stringOrNullReply } from "benni";

const [setResult, stored, exists] = await redis
  .multi()
  .add(["SET", "tx:key", "value"], okReply)
  .add(["GET", "tx:key"], stringOrNullReply)
  .add(["EXISTS", "tx:key"], booleanNumberReply)
  .exec();
```

For `WATCH`-based optimistic transactions, see
[Optimistic Transactions](/benni/advanced/optimistic-transactions/).

## Typed Lua Script

The `script()` schema names its keys and types its args; the first run loads
the script and later runs send cached `EVALSHA`:

```ts
const value = await redis.query.incrementBy.run({
  keys: { counter: "script:counter" },
  args: { amount: 5 }
});
// value is number
```

## Raw Command Fallback

Use raw commands when a typed helper does not exist yet.

```ts
const reply = await redis.raw.send(["SET", "raw:key", "value"]);
if (reply !== "OK") {
  throw new TypeError("SET failed");
}

const value = await redis.raw.send(["GET", "raw:key"]);
```

## Test With A Fake Client

The `RedisClient` contract is three required methods: `send`, `pipeline`, and
`close` (`transaction` and `session` are optional), so unit tests can drive the whole
typed API with a scripted fake:

```ts
import { benni } from "benni";
import type { RedisClient, RedisCommand, RedisReply } from "benni/core";
import { json, kv } from "benni/schema";

function fakeClient(commands: RedisCommand[], replies: RedisReply[]): RedisClient {
  return {
    async send(command) {
      commands.push(command);
      const reply = replies.shift();
      if (reply === undefined) throw new Error("No fake Redis reply queued");
      return reply;
    },
    async pipeline(pipelineCommands) {
      commands.push(...pipelineCommands);
      return replies.splice(0, pipelineCommands.length);
    },
    async close() {}
  };
}

const commands: RedisCommand[] = [];
export const profiles = kv("user", json<{ name: string }>());
const redis = benni({
  client: fakeClient(commands, ["OK", "{\"name\":\"Ada\"}"]),
  schema: { profiles }
});

await redis.query.profiles.set("42", { name: "Ada" });
const user = await redis.query.profiles.get("42");

console.log(commands);
console.log(user);
```

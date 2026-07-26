---
title: "Beni Examples"
description: "Copy-pasteable examples for the schema-first Beni API, one data structure at a time."
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
} from "beni/schema";

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
import { beni } from "beni";
import { node } from "beni/node";
import * as schema from "./schema";

const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});

export const redis = beni(client, { schema });
```

The sections below assume these two files. The lower-level building blocks the
client is made of (`defineKeyspace`, `createHashStore`, …) live under
`beni/core` for adapter authors and advanced integrations — see the
[API overview](/beni/api/overview/).

## Typed JSON Key-Value

```ts
import { profiles } from "./schema";

await redis.kv(profiles).set("42", { name: "Ada", score: 10 }, { ttlSeconds: 60 });

const profile = await redis.kv(profiles).get("42");
// profile is UserProfile | null

await redis.kv(profiles).mset([
  ["43", { name: "Grace", score: 12 }],
  ["44", { name: "Linus", score: 8 }]
]);

const many = await redis.kv(profiles).mget(["42", "43", "missing"]);
// many is Array<UserProfile | null>

await redis.kv(profiles).del("42");
```

## Known IDs For Autocomplete

When IDs are known at compile time, pass them into the schema. Editors then
autocomplete IDs such as `"test1"` and full key strings such as `"demo:test1"`.

```ts
import { type RedisKey } from "beni";
import { kv, string } from "beni/schema";

const demos = kv("demo", string(), {
  ids: ["test1", "test2"]
});

await redis.kv(demos).set("test1", "value");

const key = redis.kv(demos).key("test1");
// key is "demo:test1"

type DemoKey = RedisKey<"demo", "test1" | "test2">;
// DemoKey is "demo:test1" | "demo:test2"
```

If IDs come from users, databases, or Redis itself, leave `ids` out and Beni
accepts normal `string | number | bigint` IDs.

## Integer Counter

```ts
import { counters } from "./schema";

await redis.counter(counters).incr("page-views");
await redis.counter(counters).incrby("page-views", 5);
await redis.counter(counters).decr("page-views");
```

## String Commands

`redis.string()` exposes the Redis string commands that only make sense for
plain string values.

```ts
import { texts } from "./schema";

await redis.string(texts).append("welcome", "hello");
await redis.string(texts).append("welcome", " world");

const firstWord = await redis.string(texts).getrange("welcome", 0, 4);
const length = await redis.string(texts).strlen("welcome");
const value = await redis.string(texts).getex("welcome", 60);
```

## Typed Hash

```ts
import { users } from "./schema";

await redis.hash(users).hset("42", { name: "Ada", score: 10 }, { ttlSeconds: 300 });

const user = await redis.hash(users).hget("42");
// user is { name: string; score: number } | null

await redis.hash(users).hset("42", "name", "Grace");
const score = await redis.hash(users).hincrby("42", "score", 1);
const hasName = await redis.hash(users).hexists("42", "name");

await redis.hash(users).del("42");
```

## Typed Set

```ts
import { roles } from "./schema";

await redis.set(roles).sadd("user:42", ["admin", "editor"]);

const isAdmin = await redis.set(roles).sismember("user:42", "admin");
const allRoles = await redis.set(roles).smembers("user:42");

await redis.set(roles).srem("user:42", ["editor"]);
await redis.set(roles).del("user:42");
```

## Typed List

```ts
import { jobs } from "./schema";

await redis.list(jobs).rpush("pending", [
  { id: "job-1", kind: "email" },
  { id: "job-2", kind: "report" }
]);

const nextJob = await redis.list(jobs).lpop("pending");
// nextJob is { id: string; kind: "email" | "report" } | null

const remaining = await redis.list(jobs).lrange("pending", 0, -1);
// remaining is Array<{ id: string; kind: "email" | "report" }>

await redis.list(jobs).del("pending");
```

## Typed Sorted Set

```ts
import { leaderboard } from "./schema";

await redis.zset(leaderboard).zadd("daily", [
  { member: "alice", score: 10 },
  { member: "bob", score: 20 }
]);

const top = await redis.zset(leaderboard).zrange("daily", {
  start: 0,
  stop: -1,
  withScores: true
});
// top is Array<{ readonly member: string; readonly score: number }>

await redis.zset(leaderboard).zincrby("daily", 5, "alice");
const aliceScore = await redis.zset(leaderboard).zscore("daily", "alice");

await redis.zset(leaderboard).del("daily");
```

## Typed Stream

```ts
import { events } from "./schema";

const entryId = await redis.stream(events).xadd("audit", {
  type: "login",
  userId: "42"
});

const latest = await redis.stream(events).xread("audit", "0-0", { count: 10 });
const history = await redis.stream(events).xrange("audit", { count: 10 });

await redis.stream(events).del("audit");
```

## Typed Bitmap

```ts
import { activity } from "./schema";

await redis.bitmap(activity).setbit("2026-07-04", 42, true);

const active = await redis.bitmap(activity).getbit("2026-07-04", 42);
const activeCount = await redis.bitmap(activity).bitcount("2026-07-04");

await redis.bitmap(activity).del("2026-07-04");
```

## Typed Geo

```ts
import { cities } from "./schema";

await redis.geo(cities).geoadd("europe", [
  { member: "Berlin", longitude: 13.405, latitude: 52.52 },
  { member: "Paris", longitude: 2.3522, latitude: 48.8566 }
]);

const nearby = await redis.geo(cities).geosearch("europe", {
  from: { longitude: 13.405, latitude: 52.52 },
  by: { radius: 1000, unit: "km" },
  withDistance: true,
  withCoordinates: true
});

await redis.geo(cities).del("europe");
```

## Typed HyperLogLog

```ts
import { visitors } from "./schema";

await redis.hll(visitors).pfadd("today", ["user:1", "user:2", "user:1"]);

const approximateVisitors = await redis.hll(visitors).pfcount("today");

await redis.hll(visitors).del("today");
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

Subscribing needs a dedicated subscriber connection, so pass a pub/sub adapter
when binding:

```ts
import { beni } from "beni";
import { node, pubsub } from "beni/node";
import * as schema from "./schema";

const client = await node();
const redis = beni(client, { schema, pubsub: await pubsub() });

const subscription = await redis.pubsub.channel(schema.userEvents).subscribe(
  (message) => {
    // message is { id: string; action: "created" | "deleted" }
    console.log(message);
  }
);

await redis.pubsub.channel(schema.userEvents).publish({
  id: "42",
  action: "created"
});

await subscription.unsubscribe();
```

Use a typed pattern when one handler should receive several matching channels:

```ts
const patternSubscription = await redis.pubsub
  .pattern(schema.userEventPattern)
  .subscribe((message, channel) => {
    // message is decoded; channel is the concrete channel name
  });

await patternSubscription.unsubscribe();
```

Publishing alone works without an adapter — `redis.pubsub.channel(x).publish()`
falls back to the bound client.

## Typed Transaction

`redis.multi()` builds a `MULTI`/`EXEC` transaction whose result is a
position-typed tuple:

```ts
import { booleanNumberReply, okReply, stringOrNullReply } from "beni";

const [setResult, stored, exists] = await redis
  .multi()
  .add(["SET", "tx:key", "value"], okReply)
  .add(["GET", "tx:key"], stringOrNullReply)
  .add(["EXISTS", "tx:key"], booleanNumberReply)
  .exec();
```

For `WATCH`-based optimistic transactions, see
[Optimistic Transactions](/beni/advanced/optimistic-transactions/).

## Typed Lua Script

The `script()` schema names its keys and types its args; the first run loads
the script and later runs send cached `EVALSHA`:

```ts
import { incrementBy } from "./schema";

const value = await redis.script(incrementBy).run({
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

The `RedisClient` contract is three methods, so unit tests can drive the whole
typed API with a scripted fake:

```ts
import {
  beni,
  type RedisClient,
  type RedisCommand,
  type RedisReply
} from "beni";
import { json, kv } from "beni/schema";

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
const profiles = kv("user", json<{ name: string }>());
const redis = beni(fakeClient(commands, ["OK", "{\"name\":\"Ada\"}"]), {
  schema: { profiles }
});

await redis.kv(profiles).set("42", { name: "Ada" });
const user = await redis.kv(profiles).get("42");

console.log(commands);
console.log(user);
```

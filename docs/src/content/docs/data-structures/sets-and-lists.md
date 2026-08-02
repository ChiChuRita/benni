---
title: "Sets And Lists"
description: "Sets and lists model collections under Redis keys while preserving member types."
---

Sets and lists model collections under Redis keys while preserving member types.

## Sets

Use sets for unique membership.

```ts
import { set, string } from "benni/schema";

export const teamMembers = set("team-members", string());

await redis.set(teamMembers).sadd("engineering", ["ada", "grace"]);

const hasAda = await redis.set(teamMembers).sismember("engineering", "ada");
const members = await redis.set(teamMembers).smembers("engineering");
```

Raw Redis equivalent:

```ts
await nodeRedis.sAdd("team-members:engineering", ["ada", "grace"]);
const members = await nodeRedis.sMembers("team-members:engineering");
```

## Lists

Use lists for ordered queues, recent items, and bounded histories.

```ts
import { json, list } from "benni/schema";

type Event = {
  type: string;
  at: string;
};

export const events = list("events", json<Event>());

await redis.list(events).rpush("user:42", [
  { type: "login", at: new Date().toISOString() }
]);

const recent = await redis.list(events).lrange("user:42", 0, 9);
```

Raw Redis equivalent:

```ts
await nodeRedis.rPush("events:user:42", JSON.stringify(event));
const recent = await nodeRedis.lRange("events:user:42", 0, 9);
```

## Members Are Always Passed As An Array

The variadic writers (`sadd`, `srem`, `lpush`, `rpush`, and `pfadd` on a [HyperLogLog](/benni/data-structures/hyperloglog/)) take an array, even for a single member. There is no single-value overload, so `lpush(id, value)` does not compile:

```ts
await redis.list(events).lpush("user:42", [event]);  // one member
await redis.list(events).lpush("user:42", [a, b]);   // many
```

One shape means a loop that pushes one item and a batch that pushes a hundred are the same call, and an empty array is a no-op rather than a command that would create the key. The readers that return a slice are positional, matching Redis: `lrange(id, start, stop)`.

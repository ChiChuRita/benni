---
title: "Sets And Lists"
description: "Sets and lists model collections under Redis keys while preserving member types."
---

Sets and lists model collections under Redis keys while preserving member types.

## Sets

Use sets for unique membership.

```ts
import { set, string } from "beni/schema";

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
import { json, list } from "beni/schema";

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

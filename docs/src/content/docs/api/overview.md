---
title: "API Overview"
description: "Most applications use three imports:"
---

Most applications use three imports:

```ts
import { beni } from "beni";
import { node } from "beni/node";
import { hash, hll, json, kv, number, string, zset } from "beni/schema";
```

## Schema API

Use `beni/schema` to define Redis key families:

```ts
kv("profile", json<Profile>());
hash("user", { name: string(), score: number() });
set("team-members", string());
list("events", json<Event>());
zset("leaderboard", string());
hll("page-views", string());
channel("events:user", json<UserEvent>());
```

## Client API

Bind a Redis client:

```ts
const redis = beni(client, { schema });
```

Use data-structure resources:

```ts
redis.kv(profiles);
redis.hash(users);
redis.set(teamMembers);
redis.list(events);
redis.zset(leaderboards);
redis.hll(pageViews);
redis.pubsub.channel(userEvents);
```

Use `redis.raw` for direct Redis commands:

```ts
await redis.raw.send(["PING"]);
```

## Lower-Level Core API

The `beni/core` entrypoint exposes the building blocks the client is made of — `defineKeyspace`, `createKeyValueStore`, `createHashStore`, and the other store builders — for adapter authors and advanced integrations.

Application code should prefer the schema-first API shown in the guide.

---
title: "API Overview"
description: "The three imports most applications need, plus where the lower-level core builders live."
---

Most applications use three imports:

```ts
import { benni } from "benni";
import { node } from "benni/node";
import { hash, hll, json, kv, number, string, zset } from "benni/schema";
```

## Schema API

Use `benni/schema` to define Redis key families:

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

Bind a Redis client, and close the handle at shutdown:

```ts
const redis = benni({ client: node({ url }), schema });

await redis.close();
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

The `benni/core` entrypoint is the adapter-author surface: the `RedisClient` contract and its capability types (`FullRedisClient`, `RedisSession`, `RedisSubscriber`, …), `resolveClient`, the server-error normalizer `redisServerError`, the script runner (`createScriptRunner`, `defineScript`), and the store builders the client is made of (`createKeyValueStore`, `createHashStore`, …). Schemas are declared from `benni/schema` only; `benni/core` does not repeat the builders under `define*` names.

Application code should prefer the schema-first API shown in the guide; every accessor is documented in the [Benni Client reference](/benni/api/benni-client/).

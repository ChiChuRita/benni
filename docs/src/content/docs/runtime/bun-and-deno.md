---
title: "Bun And Deno"
description: "Bun uses Bun's built-in Redis client. Deno uses the Node adapter through npm compatibility."
---

Bun is supported through Bun's built-in Redis client. Deno uses the Node adapter through npm compatibility.

## Bun

```ts
import { beni } from "beni";
import { bun } from "beni/bun";
import * as schema from "./schema";

const client = await bun({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});

export const redis = beni(client, { schema });
```

For Pub/Sub, create the Pub/Sub adapter and pass it to `beni`:

```ts
import { bun } from "beni/bun";

const client = await bun();
const pubsub = await bun.pubsub();

const redis = beni(client, { schema, pubsub });
```

The Bun Pub/Sub adapter supports channel subscriptions only. Pattern subscriptions are not implemented because `psubscribe` is broken in Bun 1.3.14, so `redis.pubsub.pattern(...).subscribe(...)` throws until Bun ships a fix. Use channel subscriptions on Bun.

The Bun adapter supports [sessions](/beni/advanced/sessions/), so `redis.session()` and `redis.watch()` work: each session is a fresh Bun Redis client with reconnection and offline queueing disabled, and closing it rejects an in-flight blocking read promptly.

The Bun adapter runs the same Redis contract suite as the Node adapter against a real server:

```sh
BENI_REDIS_URL=redis://127.0.0.1:6379 pnpm test:bun
```

## Deno

Deno needs no dedicated adapter — it runs node-redis directly through npm compatibility, which gives full Redis 8 command support and reuses the same adapter Node uses. Use the **Node adapter** with `npm:` specifiers:

```ts
// deno.json import map, or inline npm: specifiers
import { beni } from "npm:beni";
import { node } from "npm:beni/node";

const client = await node({ url: "redis://127.0.0.1:6379" });
export const redis = beni(client, { schema });
```

Deno resolves `redis` through its own `npm:` specifiers, so Beni's optional `redis` peer dependency (an npm concern) does not apply. A Deno-native adapter over a JSR client such as `@redis/redis` is a possible future addition, but it would only be a different *engine* behind the same core.

If you prefer a different client entirely, the portable seam is the core `RedisClient` interface:

```ts
type RedisClient = {
  send(command: RedisCommand): Promise<RedisReply>;
  pipeline(commands: readonly RedisCommand[]): Promise<RedisReply[]>;
  transaction?(commands: readonly RedisCommand[]): Promise<RedisReply[]>;
  session?(): Promise<RedisSession>;
  close?(): Promise<void>;
};
```

If you already have a Deno Redis client, an adapter can implement that interface and then pass it to `beni(client)`. `transaction` and `session` are optional: an adapter that omits `session` still works, but `redis.session()` and `redis.watch()` throw `TypeError: Redis client does not support sessions` until it implements one. See [Sessions](/beni/advanced/sessions/) for the connection role a session fills.

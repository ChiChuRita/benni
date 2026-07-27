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

[Pub/Sub](/beni/data-structures/pubsub/) needs no setup: the Bun adapter can lease a subscriber connection, so `redis.pubsub.channel(...).subscribe(...)` works on the bound client:

```ts
const subscription = await redis.pubsub
  .channel(schema.userEvents)
  .subscribe((message) => { /* ... */ });

await subscription.unsubscribe();
```

Channel subscriptions only, though. The Bun subscriber deliberately omits `psubscribe` because it is broken in Bun 1.3.14 (it hangs rather than resolving), so `redis.pubsub.pattern(...).subscribe(...)` throws `TypeError` on Bun instead of deadlocking. Subscribe to the individual channels until Bun ships a fix, or run pattern subscriptions on the [Node adapter](/beni/runtime/node/). Publishing is unaffected: it is one stateless `PUBLISH` on the bound client.

The Bun adapter supports [sessions](/beni/advanced/sessions/), so `redis.session()` and `redis.watch()` work: each session is a fresh Bun Redis client with reconnection and offline queueing disabled, and closing it rejects an in-flight blocking read promptly.

The Bun adapter runs the same Redis contract suite as the Node adapter against a real server:

```sh
BENI_REDIS_URL=redis://127.0.0.1:6379 pnpm test:bun
```

## Deno

Deno needs no dedicated adapter: it runs node-redis directly through npm compatibility, which gives full Redis 8 command support and reuses the same adapter Node uses. Use the **Node adapter** with `npm:` specifiers:

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
  subscriber?(): Promise<RedisSubscriber>;
  close(): Promise<void>;
};
```

If you already have a Deno Redis client, an adapter can implement that interface and then pass it to `beni(client)`. `transaction`, `session`, and `subscriber` are optional, and each one gates a feature rather than the whole client: an adapter that omits `session` still works, but `redis.session()` and `redis.watch()` throw `TypeError: Redis client does not support sessions` until it implements one, and an adapter that omits `subscriber` can still publish while `redis.pubsub.channel(...).subscribe(...)` throws. See [Sessions](/beni/advanced/sessions/) for the connection role a session fills, and [Pub/Sub](/beni/data-structures/pubsub/) for the subscriber one.

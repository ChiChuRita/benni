---
title: "Bun And Deno"
description: "Bun uses Bun's built-in Redis client. Deno uses the Node adapter through npm compatibility."
---

Bun is supported through Bun's built-in Redis client. Deno uses the Node adapter through npm compatibility.

## Bun

```ts
import { benni } from "benni";
import { bun } from "benni/bun";
import * as schema from "./schema";

export const redis = benni({
  client: bun({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" }),
  schema
});
```

`bun()` returns its client synchronously and connects on the first command. A connect that fails rejects the commands waiting on it with `benni/bun could not connect to Redis: <cause>`, and a later command dials again, backing off from 100 ms to 5 s. Once connected, Bun's `autoReconnect` handles drops; if it gives up (after `maxRetries`), the next command connects afresh. `onError` and `onReconnect` work as on [`benni/node`](/benni/runtime/node/#connection-events); Bun has no error event of its own, so `onError` sees failed connects and a connection that is gone for good. Close the handle with `await redis.close()`.

[Pub/Sub](/benni/data-structures/pubsub/) needs no setup: the Bun adapter can lease a subscriber connection, so `redis.query.<channel>.subscribe(...)` works on the bound client:

```ts
const subscription = await redis.query.userEvents
  .subscribe((message) => { /* ... */ });

await subscription.unsubscribe();
```

Channel subscriptions only, though. The Bun subscriber deliberately omits `psubscribe` because it is broken in Bun 1.3.14 (it hangs rather than resolving), so a handle over `bun()` has no pattern entries in `redis.query` (and a pattern subscribe forced through throws `UnsupportedCapabilityError` instead of deadlocking). Subscribe to the individual channels until Bun ships a fix, or run pattern subscriptions on the [Node adapter](/benni/runtime/node/). Publishing is unaffected: it is one stateless `PUBLISH` on the bound client.

If the subscriber connection drops, the adapter resubscribes every channel on the reconnect. Bun's own client reconnects a subscriber but comes back with no subscriptions, so without this the handlers would go silent while the connection looked healthy. Messages published during the outage are still lost, as on every adapter: see [Reconnects](/benni/data-structures/pubsub/#reconnects). If Bun gives up reconnecting (after `maxRetries`), the lease reports itself closed and the next subscribe opens a fresh connection.

Bun's client only speaks RESP3, where `HGETALL` is a map, `ZSCORE` a number, and `ZRANGE ... WITHSCORES` a list of pairs. The adapter reshapes every reply to the RESP2 shape the other adapters return, so [`redis.raw`](/benni/api/benni-client/#redisraw) and your own decoders see the same values on Bun as everywhere else.

The Bun adapter supports [sessions](/benni/advanced/sessions/), so `redis.session()` and `redis.watch()` work: each session is a fresh Bun Redis client with reconnection and offline queueing disabled, and closing it rejects an in-flight blocking read promptly.

The Bun adapter runs the same Redis contract suite as the Node adapter against a real server:

```sh
BENNI_REDIS_URL=redis://127.0.0.1:6379 pnpm test:bun
```

## Deno

Deno needs no dedicated adapter: it runs node-redis directly through npm compatibility, which gives full Redis 8 command support and reuses the same adapter Node uses. Use the **Node adapter** with `npm:` specifiers:

```ts
// deno.json import map, or inline npm: specifiers
import { benni } from "npm:benni";
import { node } from "npm:benni/node";

export const redis = benni({
  client: node({ url: "redis://127.0.0.1:6379" }),
  schema
});
```

Deno resolves `redis` through its own `npm:` specifiers, so Benni's optional `redis` peer dependency (an npm concern) does not apply. A Deno-native adapter over a JSR client such as `@redis/redis` is a possible future addition, but it would only be a different *engine* behind the same core.

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

If you already have a Deno Redis client, an adapter can implement that interface (import it from `benni/core`) and then pass it as `benni({ client })`. `transaction`, `session`, and `subscriber` are optional, and each one gates a feature rather than the whole client: an adapter that omits `session` still works, but the handle has no `redis.session()` or `redis.watch()` until it implements one, and an adapter that omits `subscriber` can still publish while channels have no `subscribe()`. Return an interface that makes the members you implement required (as `FullRedisClient` does) and the handle's type follows; forced through anyway, a missing capability throws `UnsupportedCapabilityError`. See [Sessions](/benni/advanced/sessions/) for the connection role a session fills, and [Pub/Sub](/benni/data-structures/pubsub/) for the subscriber one.

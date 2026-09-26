---
title: "ioredis"
description: "Use Benni with the ioredis client you already run, including adopting an existing instance, so adopting Benni is not a client migration."
---

`benni/ioredis` runs the whole typed API on [ioredis](https://www.npmjs.com/package/ioredis), the most widely deployed Redis client for Node. If your app already uses ioredis, this is the adapter to pick: **you do not have to swap Redis clients to use Benni.**

```sh
pnpm add benni ioredis
```

```ts
import { benni } from "benni";
import { ioredis } from "benni/ioredis";
import * as schema from "./schema";

export const redis = benni({
  client: ioredis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379"),
  schema
});
```

All three forms below return their client synchronously. A client Benni creates connects on the first command: a connect that fails rejects the commands waiting on it with `benni/ioredis could not connect to Redis: <cause>`, and a later command dials again, backing off from 100 ms to 5 s. Once connected, ioredis's own `retryStrategy` handles drops. An adopted client is used as it is, connected (or not) however you configured it.

## Three ways in

A URL:

```ts
const client = ioredis("redis://127.0.0.1:6379");
```

Any ioredis options (`host`, `port`, `password`, `tls`, `sentinels`, …):

```ts
const client = ioredis({
  host: process.env.REDIS_HOST,
  port: 6379,
  password: process.env.REDIS_PASSWORD
});
```

Or an ioredis instance you already have, which is the important one:

```ts
import Redis from "ioredis";

const existing = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379", {
  protocol: 2 // needed on ioredis 6, which defaults to RESP3
}); // yours, already configured
const client = ioredis(existing);
```

Adopting means Benni shares the connection you already tuned, monitor, and pool. There is no second client, no second connection budget, and no migration: you can start typing one keyspace and leave the rest of your app calling `existing` directly.

## Who owns the connection

An adopted client is **borrowed**. `redis.close()` shuts down the Pub/Sub, workers, sessions, and subscriber connections Benni opened, and leaves your client open, because you still own its lifetime:

```ts
const redis = benni({ client: ioredis(existing), schema });

await redis.close();   // Benni's connections are gone
await existing.quit(); // you close yours, when you're ready
```

A client Benni created from a URL or options is **owned**, and `redis.close()` quits it for you.

One consequence worth knowing: Benni attaches an `"error"` listener to an adopted client only if you ask for one. An adopted client keeps whatever error handling you gave it, and Benni will not silently swallow errors on a client it does not own. Make sure yours has a listener, or an idle network blip will crash the process (that is ioredis behaviour, not Benni's).

### Connection events

`onError` and `onReconnect` work as on [`benni/node`](/benni/runtime/node/#connection-events). With a URL or options they go in the options; for an adopted client, as the second argument, where they also cover the subscriber connection Benni duplicates from it:

```ts
ioredis({ url, onError: (error) => logger.warn({ error }) });
ioredis(existing, { onReconnect: (connection) => refetch(connection) });
```

### RESP2 only

Benni's typed stores decode RESP2 reply shapes. ioredis 6 switched its default to RESP3, which reshapes some of them even in its RESP2-compatible mapping: `XREAD` answers with a map, so every stream read would fail with `ReplyShapeError`. So:

- A client Benni creates from a URL or options is pinned to `protocol: 2`. You don't need to do anything, and ioredis 5 (RESP2 only) is unaffected.
- Adopting a RESP3 client throws a `TypeError` up front, telling you to pass `protocol: 2`. On ioredis 6, create the client you hand over with `new Redis(url, { protocol: 2 })`, or `new Redis.Cluster(nodes, { redisOptions: { protocol: 2 } })` for a cluster. Passing `protocol: 3` to `ioredis({ ... })` throws the same way.

Both ioredis 5 and 6 are supported and tested.

### `keyPrefix` is not supported

`ioredis({ keyPrefix })`, and adopting a client that sets it, both throw. ioredis rewrites key *arguments* but leaves `SCAN`/`MATCH` patterns alone, so a prefixed client stores at `<prefix><key>` while `schema.key()` and every scan still say `<key>`. Scans would return nothing at all, without an error.

Benni's schemas already own key naming, so put the prefix there instead:

```ts
const users = hash(prefix + "user", { name: string() });
```

## What's supported

Everything. Over RESP2 the flat reply shapes are exactly what the typed stores decode, so replies pass through with no normalization:

| Feature | Supported |
|---|---|
| Typed stores, transactions, scripts | Yes |
| [Sessions](/benni/advanced/sessions/): blocking commands, `WATCH` | Yes |
| [Pub/Sub](/benni/data-structures/pubsub/) subscribe | Yes |
| Pattern subscriptions (`psubscribe`) | Yes |
| [Primitives](/benni/primitives/queue/): queue, cache, lock, ratelimit | Yes |

Sessions duplicate the connection with reconnection disabled and the offline queue off, so a drop rejects in-flight and subsequent commands instead of silently reconnecting, which would lose `WATCH` state and blocked reads. Closing a session calls `disconnect()` rather than `quit()`, so an in-flight blocking read is rejected at once instead of waiting out its server-side timeout. The parent client tracks live sessions and subscribers and force-closes any survivors.

Pub/Sub delivers every subscription through one connection-level event, so the adapter routes by channel and pattern name internally. You just subscribe:

```ts
const subscription = await redis.pubsub.channel(userEvents).subscribe((message) => {
  console.log(message.action);
});
```

If the subscriber connection drops, ioredis reconnects it and resubscribes every channel and pattern on its own. Only when ioredis gives up (it reaches its final `"end"` state, for example because a `retryStrategy` you passed returned `null`) does the lease report itself closed, and the next subscribe then opens a fresh connection. Messages published while the connection was down are lost, as on every adapter: see [Reconnects](/benni/data-structures/pubsub/#reconnects).

## ioredis or node-redis?

Both adapters expose the identical typed API and pass the same client-contract suite, so this is a question about your app, not about Benni:

- **Already on ioredis.** Use `benni/ioredis`, and adopt your existing instance. Zero migration.
- **Already on node-redis.** Use [`benni/node`](/benni/runtime/node/).
- **Greenfield.** Either works. `redis` (node-redis) is the officially maintained client and tracks new Redis 8 commands soonest; ioredis has the larger install base and richer cluster/sentinel configuration.

You can switch adapters later by changing one import; the schemas, stores, and primitives above it do not move.

## Cluster and Sentinel

Sentinel configuration works, since it is just ioredis options:

```ts
const client = ioredis({
  sentinels: [{ host: "localhost", port: 26379 }],
  name: "mymaster"
});
```

Cluster splits the responsibility. Adopt an `ioredis.Cluster` instance and ioredis does the routing (topology, `MOVED`/`ASK`, failover); Benni never had a transport of its own and does not try to. What Benni adds on top is slot **co-location**: schemas declare where their hash tag goes, the compiler rejects multi-key calls whose tags provably disagree, and `benni({ client, cluster: assertSameSlot })` catches the rest before they are sent. See [Redis Cluster](/benni/advanced/cluster/) for the layouts and the guard.

Scripts need no setup on a cluster. `SCRIPT LOAD` carries no key, so ioredis sends it to whichever node it likes; when the node that owns a script's keys answers `NOSCRIPT`, Benni runs the script once with `EVAL`, which carries the keys, routes to that node, and caches the script there for the next `EVALSHA`.

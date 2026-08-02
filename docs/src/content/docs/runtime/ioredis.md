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

const client = await ioredis(process.env.REDIS_URL);

export const redis = benni(client, { schema });
```

## Three ways in

A URL:

```ts
const client = await ioredis("redis://127.0.0.1:6379");
```

Any ioredis options (`host`, `port`, `password`, `tls`, `sentinels`, …):

```ts
const client = await ioredis({
  host: process.env.REDIS_HOST,
  port: 6379,
  password: process.env.REDIS_PASSWORD
});
```

Or an ioredis instance you already have, which is the important one:

```ts
import Redis from "ioredis";

const existing = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379"); // yours, already configured
const client = await ioredis(existing);
```

Adopting means Benni shares the connection you already tuned, monitor, and pool. There is no second client, no second connection budget, and no migration: you can start typing one keyspace and leave the rest of your app calling `existing` directly.

## Who owns the connection

An adopted client is **borrowed**. `client.close()` shuts down the sessions and subscriber connections Benni leased, and leaves your client open, because you still own its lifetime:

```ts
await client.close();  // Benni's leases are gone
await existing.quit(); // you close yours, when you're ready
```

A client Benni created from a URL or options is **owned**, and `close()` quits it for you.

One consequence worth knowing: Benni attaches an `"error"` listener only to clients it created. An adopted client keeps whatever error handling you gave it, and Benni will not silently swallow errors on a client it does not own. Make sure yours has a listener, or an idle network blip will crash the process (that is ioredis behaviour, not Benni's).

### `keyPrefix` is not supported

`ioredis({ keyPrefix })`, and adopting a client that sets it, both throw. ioredis rewrites key *arguments* but leaves `SCAN`/`MATCH` patterns alone, so a prefixed client stores at `<prefix><key>` while `schema.key()` and every scan still say `<key>`. Scans would return nothing at all, without an error.

Benni's schemas already own key naming, so put the prefix there instead:

```ts
const users = hash(prefix + "user", { name: string() });
```

## What's supported

Everything. ioredis speaks RESP2, whose flat reply shapes are exactly what the typed stores decode, so replies pass through with no normalization:

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

## ioredis or node-redis?

Both adapters expose the identical typed API and pass the same client-contract suite, so this is a question about your app, not about Benni:

- **Already on ioredis.** Use `benni/ioredis`, and adopt your existing instance. Zero migration.
- **Already on node-redis.** Use [`benni/node`](/benni/runtime/node/).
- **Greenfield.** Either works. `redis` (node-redis) is the officially maintained client and tracks new Redis 8 commands soonest; ioredis has the larger install base and richer cluster/sentinel configuration.

You can switch adapters later by changing one import; the schemas, stores, and primitives above it do not move.

## Cluster and Sentinel

Sentinel configuration works, since it is just ioredis options:

```ts
const client = await ioredis({
  sentinels: [{ host: "localhost", port: 26379 }],
  name: "mymaster"
});
```

Cluster splits the responsibility. Adopt an `ioredis.Cluster` instance and ioredis does the routing (topology, `MOVED`/`ASK`, failover); Benni never had a transport of its own and does not try to. What Benni adds on top is slot **co-location**: schemas declare where their hash tag goes, the compiler rejects multi-key calls whose tags provably disagree, and `benni(client, { cluster: true })` catches the rest before they are sent. See [Redis Cluster](/benni/advanced/cluster/) for the layouts and the guard.

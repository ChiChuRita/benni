---
title: "Node.js Setup"
description: "Node.js is supported through the redis package."
---

Node.js is supported through the `redis` package.

```ts
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

export const redis = benni({
  client: node({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" }),
  schema
});
```

`node()` returns its client synchronously and connects on the first command, so importing this module opens no socket. If that connect fails (Redis restarting while your process boots, a bad URL), the commands waiting on it reject with `benni/node could not connect to Redis: <cause>`, and a later command dials again, backing off from 100 ms to 5 s between attempts. Once connected, a dropped connection reconnects in the background under node-redis's own `reconnectStrategy`, and commands issued meanwhile wait in its offline queue. To fail fast at startup instead, send one command when the app boots: `await redis.raw.send(["PING"])`.

Close the handle when your process or test is done:

```ts
await redis.close();
```

`close()` stops Pub/Sub, queue workers, and sessions opened through the handle, then the client. It is safe to call twice, and it is final: once it has run, commands, `redis.session()`, and a fresh Pub/Sub subscribe reject rather than quietly opening a connection nothing will ever close. The same holds for `benni/ioredis` and `benni/bun`.

### Connection events

node-redis reports socket errors as `"error"` events, and a client without a listener crashes the process on the first idle blip, so the adapter always listens. Pass `onError` to see those errors, and `onReconnect` to hear when a dropped connection is back:

```ts
node({
  url,
  onError: (error) => logger.warn({ error }, "redis connection error"),
  // "client" for the command connection, "subscriber" for Pub/Sub. Messages
  // published while the subscriber was down are lost: refetch on "subscriber".
  onReconnect: (connection) => logger.info({ connection }, "redis reconnected")
});
```

Both only observe: a command a failure affects still rejects. The same two options exist on `benni/ioredis` and `benni/bun`, and `onError` on `benni/upstash`.

The Node adapter defaults to RESP2 replies because the typed stores validate Redis reply shapes such as arrays, maps, numbers, strings, and nulls. You can pass normal `redis` client options to `node`.

The Node adapter supports [sessions](/benni/advanced/sessions/), so `redis.session()` and `redis.watch()` work: each session duplicates the connection with reconnection disabled and closes by destroying the socket, which rejects an in-flight blocking read promptly rather than waiting out its timeout. Session connections never reconnect on their own: a drop fails the session, because a silent reconnect would lose its WATCH state or its blocked read. `redis.close()` closes any session still open.

[Pub/Sub](/benni/data-structures/pubsub/) needs no setup either: the adapter can also lease a subscriber connection, so `redis.query.<channel>.subscribe(...)` and the pattern form both work out of the box. Benni leases that connection on the first subscribe and closes it when the last subscription goes away; `redis.close()` closes it too. Pattern subscriptions work here and on [`benni/ioredis`](/benni/runtime/ioredis/); Bun is the one adapter without them. A dropped subscriber connection is reconnected and resubscribed by node-redis itself; messages published in between are lost (see [Reconnects](/benni/data-structures/pubsub/#reconnects)).

Already using ioredis instead? [`benni/ioredis`](/benni/runtime/ioredis/) gives the identical typed API and can adopt your existing client, so you do not have to switch Redis clients to use Benni.

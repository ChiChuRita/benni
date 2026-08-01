---
title: "Node.js Setup"
description: "Node.js is supported through the redis package."
---

Node.js is supported through the `redis` package.

```ts
import { beni } from "beni";
import { node } from "beni/node";
import * as schema from "./schema";

const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});

export const redis = beni(client, { schema });
```

Close the client when your process or test is done:

```ts
await client.close();
```

`close()` is safe to call twice, and it is final: once it has run, `redis.session()` and a fresh Pub/Sub subscribe reject with "client is closed" rather than quietly opening a connection nothing will ever close. The same holds for `beni/ioredis` and `beni/bun`.

The Node adapter defaults to RESP2 replies because the typed stores validate Redis reply shapes such as arrays, maps, numbers, strings, and nulls. You can pass normal `redis` client options to `node`.

The Node adapter supports [sessions](/beni/advanced/sessions/), so `redis.session()` and `redis.watch()` work: each session duplicates the connection with reconnection disabled and closes by destroying the socket, which rejects an in-flight blocking read promptly rather than waiting out its timeout. The parent client tracks live sessions and force-closes any survivors when you close it.

[Pub/Sub](/beni/data-structures/pubsub/) needs no setup either: the adapter can also lease a subscriber connection, so `redis.pubsub.channel(...).subscribe(...)` and the pattern form both work out of the box. Beni leases that connection on the first subscribe and closes it when the last subscription goes away; the parent `client.close()` force-closes it too if you skip `redis.pubsub.close()`. Pattern subscriptions work here and on [`beni/ioredis`](/beni/runtime/ioredis/); Bun is the one adapter without them.

Already using ioredis instead? [`beni/ioredis`](/beni/runtime/ioredis/) gives the identical typed API and can adopt your existing client, so you do not have to switch Redis clients to use Beni.

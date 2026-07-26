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

For Pub/Sub, create the Pub/Sub adapter and pass it to `beni`:

```ts
import { node, pubsub } from "beni/node";

const client = await node();
const pubsubAdapter = await pubsub();

const redis = beni(client, { schema, pubsub: pubsubAdapter });
```

The Node adapter defaults to RESP2 replies because the typed stores validate Redis reply shapes such as arrays, maps, numbers, strings, and nulls. You can pass normal `redis` client options to `node`.

The Node adapter supports [sessions](/beni/advanced/sessions/), so `redis.session()` and `redis.watch()` work: each session duplicates the connection with reconnection disabled and closes by destroying the socket, which rejects an in-flight blocking read promptly rather than waiting out its timeout. The parent client tracks live sessions and force-closes any survivors when you close it.

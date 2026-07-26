---
title: "Pub/Sub"
description: "Use typed channels when publishers and subscribers should share a message shape."
---

Use typed channels when publishers and subscribers should share a message shape.

```ts
import { channel, json } from "beni/schema";

export const userEvents = channel(
  "events:user",
  json<{ id: string; action: "created" | "deleted" }>()
);
```

Publish through the Beni client:

```ts
await redis.pubsub.channel(userEvents).publish({
  id: "42",
  action: "created"
});
```

Subscribe with the Node Pub/Sub adapter:

```ts
import { beni } from "beni";
import { node, pubsub } from "beni/node";

const client = await node();
const pubsubAdapter = await pubsub();
const redis = beni(client, { pubsub: pubsubAdapter });

const subscription = await redis.pubsub.channel(userEvents).subscribe((message) => {
  // message is { id: string; action: "created" | "deleted" }
});

await subscription.unsubscribe();
await pubsubAdapter.close();
```

Use patterns when one handler receives several channels:

```ts
import { pattern } from "beni/schema";

export const userEventPattern = pattern(
  "events:user:*",
  json<{ id: string; action: string }>()
);

await redis.pubsub.pattern(userEventPattern).subscribe((message, channel) => {
  console.log(channel, message);
});
```

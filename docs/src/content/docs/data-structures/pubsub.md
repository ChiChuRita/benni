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

There is nothing to configure. Bind a client the usual way and reach the channel through `redis.pubsub`:

```ts
import { beni } from "beni";
import { node } from "beni/node";
import * as schema from "./schema";

const client = await node();
export const redis = beni(client, { schema });
```

## Publishing

`PUBLISH` is one stateless command, so publishing rides the bound client and works on every adapter, including [`beni/upstash`](/beni/runtime/edge/) on the edge:

```ts
const receivers = await redis.pubsub.channel(userEvents).publish({
  id: "42",
  action: "created"
});
//    ^? number (how many subscribers Redis delivered to)
```

## Subscribing

`subscribe` takes only a handler. The first subscription lazily leases one subscriber connection from the bound client; every later channel and pattern is multiplexed onto that same connection, and it closes again when the last subscription goes away. So there are no idle connections to manage, and no second object to pass around:

```ts
const subscription = await redis.pubsub.channel(userEvents).subscribe((message) => {
  // message is { id: string; action: "created" | "deleted" }
  console.log(message.action);
});

await subscription.unsubscribe();
```

Subscribing to the same channel twice costs one Redis subscription, not two: Beni registers a single listener per channel name and fans out to your handlers. Each `subscribe` call gets its own `unsubscribe`, and the channel is dropped from Redis when the last handler for it leaves.

To tear everything down at once (on shutdown, or between tests), use `close`:

```ts
await redis.pubsub.close();
```

That drops every subscription and closes the leased connection. It also ends any `stream()` loop that is still running, including one you started without a signal, so a shutdown path can await its consumers. Publishing keeps working afterwards, and a later `subscribe` simply leases a fresh connection.

## Patterns

Use a pattern when one handler should receive several channels. The handler also gets the concrete channel the message arrived on:

```ts
import { json, pattern } from "beni/schema";

export const userEventPattern = pattern(
  "events:user:*",
  json<{ id: string; action: string }>()
);

const subscription = await redis.pubsub
  .pattern(userEventPattern)
  .subscribe((message, channelName) => {
    console.log(channelName, message.action);
  });

await subscription.unsubscribe();
```

Patterns share the same leased connection and the same ref-counting as channels.

## Consuming as an async iterator

Callbacks are awkward when the consumer is a loop: an SSE response, a worker that processes one message at a time. `stream()` gives you the same subscription as an async iterable, and releases it when iteration ends:

```ts
const controller = new AbortController();

for await (const message of redis.pubsub
  .channel(userEvents)
  .stream({ signal: controller.signal })) {
  console.log(message.action);
}
```

Aborting the signal ends the loop; so does `break`ing out of it or `return`ing from the enclosing function. Either way the subscription is released on the way out, which means the leased connection closes too if nothing else is subscribed.

The pattern form yields the channel alongside the message, because with a pattern you usually need to know which channel matched:

```ts
for await (const { message, channel: channelName } of redis.pubsub
  .pattern(userEventPattern)
  .stream({ signal: controller.signal })) {
  console.log(channelName, message.action);
}
```

Messages that arrive while your loop body is busy are buffered in memory, so a slow consumer does not drop messages, but it also does not apply backpressure to Redis, which has none for Pub/Sub. If your consumer can fall behind indefinitely, use a [stream](/beni/data-structures/streams/) instead: Pub/Sub is fire-and-forget and has no replay.

## When a handler throws

Delivery continues to the other handlers no matter what one of them does. By default a handler that throws or rejects is rethrown asynchronously, so the failure surfaces as an unhandled error instead of being swallowed. Pass `onPubSubError` when you would rather route it somewhere:

```ts
const redis = beni(client, {
  schema,
  onPubSubError: (error) => logger.error({ error }, "pubsub handler failed")
});
```

## Adapter support

Subscribing needs a connection the adapter can hold open, which is the one thing HTTP cannot do:

| Adapter | Publish | Channel subscribe | Pattern subscribe |
| --- | --- | --- | --- |
| [`beni/node`](/beni/runtime/node/) | Yes | Yes | Yes |
| [`beni/bun`](/beni/runtime/bun-and-deno/) | Yes | Yes | No (`psubscribe` is broken upstream in Bun 1.3.14) |
| [`beni/upstash`](/beni/runtime/edge/) | Yes | No (HTTP is stateless) | No |

Both gaps fail loudly rather than hanging. Subscribing on an adapter that cannot lease a connection throws `TypeError`, and so does `pattern(...).subscribe(...)` on Bun. An adapter advertises the capability by implementing the optional `subscriber?()` method on the [client contract](/beni/api/beni-client/#redispubsub), the same way `session?()` advertises sessions.

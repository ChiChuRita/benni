---
title: "Pub/Sub"
description: "Use typed channels when publishers and subscribers should share a message shape."
---

Use typed channels when publishers and subscribers should share a message shape.

```ts
import { channel, json } from "benni/schema";

export const userEvents = channel(
  "events:user",
  json<{ id: string; action: "created" | "deleted" }>()
);
```

There is nothing to configure. Bind a client the usual way and reach the channel through `redis.pubsub`:

```ts
import { benni } from "benni";
import { node } from "benni/node";
import * as schema from "./schema";

const client = await node();
export const redis = benni(client, { schema });
```

## Publishing

`PUBLISH` is one stateless command, so publishing rides the bound client and works on every adapter, including [`benni/upstash`](/benni/runtime/edge/) on the edge:

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

Subscribing to the same channel twice costs one Redis subscription, not two: Benni registers a single listener per channel name and fans out to your handlers. Each `subscribe` call gets its own `unsubscribe`, and the channel is dropped from Redis when the last handler for it leaves.

To tear everything down at once (on shutdown, or between tests), use `close`:

```ts
await redis.pubsub.close();
```

That drops every subscription and closes the leased connection. It also ends any `stream()` loop that is still running, including one you started without a signal, so a shutdown path can await its consumers. Publishing keeps working afterwards, and a later `subscribe` simply leases a fresh connection.

## Patterns

Use a pattern when one handler should receive several channels. The handler also gets the concrete channel the message arrived on:

```ts
import { json, pattern } from "benni/schema";

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

## One channel per entity

Most Pub/Sub is per something: one channel per chat room, per user, per job. Pass an id to `channel()` and Benni derives `name:<id>` the same way a keyspace derives `prefix:<id>`:

```ts
import { channel, json } from "benni/schema";

export const roomEvents = channel(
  "chat:room",
  json<{ from: string; text: string }>()
);
```

```ts
// PUBLISH chat:room:42
await redis.pubsub
  .channel(roomEvents, "42")
  .publish({ from: "ada", text: "hi" });

// SUBSCRIBE chat:room:42
const subscription = await redis.pubsub
  .channel(roomEvents, "42")
  .subscribe((message) => {
    console.log(message.text);
  });
```

The id is optional, and leaving it off is unchanged: `redis.pubsub.channel(roomEvents)` still addresses `chat:room` itself and nothing else. So one schema can carry both a per-room feed and a channel for everyone.

Ids are typed like keyspace ids (a string, a number, or a bigint), and `ids` narrows them to a known set for autocomplete and a compile-time check:

```ts
export const jobEvents = channel(
  "jobs",
  json<{ state: "queued" | "running" | "done" }>(),
  { ids: ["import", "export"] }
);

await redis.pubsub.channel(jobEvents, "import").publish({ state: "done" });
// redis.pubsub.channel(jobEvents, "nope") does not compile
```

You never have to build the channel string yourself. `channelName` resolves it, on the schema and on the resource, the way `key` does for a keyspace:

```ts
roomEvents.channelName("42"); // "chat:room:42"
roomEvents.channelName(); // "chat:room"
redis.pubsub.channel(roomEvents, "42").channelName(); // "chat:room:42"
```

A schema reached through the [registry](/benni/core-concepts/schema-registry/) scopes with `at(id)`, which is what `redis.pubsub.channel(schema, id)` calls underneath:

```ts
await redis.query.roomEvents.at("42").publish({ from: "ada", text: "hi" });
```

There is no `hashTag` option on a channel, because a channel is not a key: plain Pub/Sub is broadcast across a cluster rather than routed by slot, so there is no co-location to arrange.

### Pairing with a pattern

Per-entity channels and patterns are two halves of the same shape: publish to one room, subscribe to all of them. The id is derived by exactly the rule a keyspace uses, so a pattern over the prefix matches every channel the schema can produce:

```ts
import { json, pattern } from "benni/schema";

export const anyRoom = pattern(
  "chat:room:*",
  json<{ from: string; text: string }>()
);

await redis.pubsub.pattern(anyRoom).subscribe((message, channelName) => {
  console.log(channelName, message.text); // "chat:room:42 hi"
});

await redis.pubsub
  .channel(roomEvents, "42")
  .publish({ from: "ada", text: "hi" });
```

Ids are joined on verbatim, exactly as a keyspace joins them, so nothing about an id is interpreted: a channel subscribe is a literal name, never a glob, and only `pattern()` reads `*`, `?`, and `[...]` as wildcards. An id containing a colon simply nests one level further, and `chat:room:*` still matches it, because a Redis glob `*` spans colons too.

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

Messages that arrive while your loop body is busy are buffered in memory, so a slow consumer does not drop messages, but it also does not apply backpressure to Redis, which has none for Pub/Sub. If your consumer can fall behind indefinitely, use a [stream](/benni/data-structures/streams/) instead: Pub/Sub is fire-and-forget and has no replay.

## When a handler throws

Delivery continues to the other handlers no matter what one of them does. By default a handler that throws or rejects is rethrown asynchronously, so the failure surfaces as an unhandled error instead of being swallowed. Pass `onPubSubError` when you would rather route it somewhere:

```ts
const redis = benni(client, {
  schema,
  onPubSubError: (error) => logger.error({ error }, "pubsub handler failed")
});
```

## Adapter support

Subscribing needs a connection the adapter can hold open, which is the one thing HTTP cannot do:

| Adapter | Publish | Channel subscribe | Pattern subscribe |
| --- | --- | --- | --- |
| [`benni/node`](/benni/runtime/node/) | Yes | Yes | Yes |
| [`benni/bun`](/benni/runtime/bun-and-deno/) | Yes | Yes | No (`psubscribe` is broken upstream in Bun 1.3.14) |
| [`benni/upstash`](/benni/runtime/edge/) | Yes | No (HTTP is stateless) | No |

Both gaps fail loudly rather than hanging. Subscribing on an adapter that cannot lease a connection throws `TypeError`, and so does `pattern(...).subscribe(...)` on Bun. An adapter advertises the capability by implementing the optional `subscriber?()` method on the [client contract](/benni/api/benni-client/#redispubsub), the same way `session?()` advertises sessions.

---
title: "Streams"
description: "Use streams for append-only event logs with ordered, ID-addressable entries."
---

Use streams for append-only event logs with ordered, ID-addressable entries.

## Define A Stream

```ts
import { number, stream, string } from "beni/schema";

export const activity = stream("activity", {
  action: string(),
  points: number()
});
```

## Add Entries

```ts
const entryId = await redis.stream(activity).xadd("42", {
  action: "login",
  points: 5
});
// "1720094400000-0"
```

`xadd` accepts options for the entry ID, stream creation, and trimming on write:

```ts
await redis.stream(activity).xadd(
  "42",
  { action: "login", points: 5 },
  {
    nomkstream: true,
    maxLen: { count: 1000, approximate: true }
  }
);
```

With `nomkstream: true`, Redis skips missing streams (`NOMKSTREAM`) and `xadd` returns `null` instead of an entry ID — only that form types as `Promise<string | null>`; the plain form is `Promise<string>`. `maxLen` trims while adding, with the same shape as `xtrim`'s `maxLen`. Pass `entryId` to set an explicit ID instead of the default `*`.

## Read Ranges

```ts
const entries = await redis.stream(activity).xrange("42", { count: 10 });
//    ^? Array<{ id: string; value: Partial<{ action: string; points: number }> }>

const newest = await redis.stream(activity).xrevrange("42", { count: 10 });
```

`start` and `end` default to the full stream (`-` to `+`). Entry values are `Partial` because Redis does not enforce stream entry shapes: older entries may predate fields you added to the schema. Fields not declared in the schema are skipped.

## Read After An Entry ID

```ts
const next = await redis.stream(activity).xread("42", "1720094400000-0", {
  count: 100
});
```

`xread` returns entries newer than the given entry ID, or an empty array when there is nothing new. Use `"0"` to read from the beginning.

## Trim

```ts
await redis.stream(activity).xtrim("42", { maxLen: { count: 1000, approximate: true } });
await redis.stream(activity).xtrim("42", { minId: { value: "1720094400000-0" } });
```

Both return the number of removed entries. `approximate: true` lets Redis trim in whole macro nodes, which is faster.

## Remove, Count, Delete

```ts
await redis.stream(activity).xdel("42", ["1720094400000-0"]);
const size = await redis.stream(activity).xlen("42");
await redis.stream(activity).del("42");
```

## Raw Redis Equivalent

```ts
await redis.xAdd("activity:42", "*", {
  action: "login",
  points: "5"
});
```

For at-least-once delivery across many workers, use [consumer groups](/beni/data-structures/consumer-groups/) (`XGROUP`, `XREADGROUP`, `XACK`) via `redis.stream(activity).group(name)`. Use `xread` for single-consumer polling. To block a worker until an entry arrives, [`xread` with a `timeoutSeconds`](/beni/advanced/blocking-operations/) and the blocking group read run on a [session](/beni/advanced/sessions/).

Use streams for activity feeds, audit logs, and event pipelines where entries need stable IDs and time ordering.

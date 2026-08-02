---
title: "Streams"
description: "Use streams for append-only event logs with ordered, ID-addressable entries."
---

Use streams for append-only event logs with ordered, ID-addressable entries.

## Define A Stream

```ts
import { number, stream, string } from "benni/schema";

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

With `nomkstream: true`, Redis skips missing streams (`NOMKSTREAM`) and `xadd` returns `null` instead of an entry ID. Any call that spells the flag out, including a computed `nomkstream: someBoolean`, types as `Promise<string | null>`; a call that leaves it off is `Promise<string>`. Passing an options object typed as `StreamAddOptions`, where the flag is optional, does not compile: the reply shape depends on the flag, so it has to be visible at the call site. `maxLen` trims while adding, with the same shape as `xtrim`'s `maxLen`. Pass `entryId` to set an explicit ID instead of the default `*`.

## Read Ranges

```ts
const entries = await redis.stream(activity).xrange("42", { count: 10 });
//    ^? Array<{ id: string; value: Partial<{ action: string; points: number }> }>

const newest = await redis.stream(activity).xrevrange("42", { count: 10 });
```

`start` and `end` default to the full stream (`-` to `+`). Fields not declared in the schema are skipped.

## Entry Values Are Partial

Every read shape that carries a stream entry value (`xrange`, `xrevrange`, `xread`, and the consumer-group reads) types it as `Partial<...>`, so a field declared as `action: string()` reads back as `string | undefined` and needs a fallback:

```ts
for (const entry of await redis.stream(activity).xrange("42")) {
  const action = entry.value.action ?? "(unknown)";
  const points = entry.value.points ?? 0;
}
```

This is deliberate, and it is worth knowing that it is the **opposite** policy from hashes, because the two use the same declared-fields concept:

| | Missing declared field |
| --- | --- |
| Stream entry (`xrange`, `xread`, group reads) | Reads as `undefined`; you supply the fallback |
| Hash whole-record read (`hget(id)`) | Throws `PartialRecordError` |
| Hash tolerant read (`hgetall(id)`) | Reads as `undefined` (also `Partial`) |

The difference follows from who writes the key. A hash under `hash("user", …)` is a record your schema owns, so a declared field that has gone missing is a bug worth a loud `PartialRecordError` from `hget`, with `hgetall` as the explicit tolerant read for records that use per-field TTLs. See [Hashes](/benni/data-structures/hashes/).

A stream is an append-only log, and any producer can append to it: an older service, a `redis-cli XADD`, a version of your code that predates the field you just added to the schema. Entries already written are immutable, so a schema can never be retrofitted onto them. Typing entry values as complete records would be a claim about every past and future writer that Benni cannot check, so it stays a `Partial` and the fallback stays visible at the read.

The write side has no such doubt. `xadd` requires every declared field, so entries your own code appends are always complete.

A [consumer group](/benni/data-structures/consumer-groups/) re-reading its pending list goes one step further: there the whole `value` can be `null`, meaning the entry was deleted upstream and there is nothing left to decode.

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

Both return the number of removed entries. `approximate: true` lets Redis trim in whole macro nodes, which is faster. `{ maxLen: { count: 0 } }` empties the stream but keeps the key, so its consumer groups and their pending lists survive; `del` deletes the groups along with the stream.

## Remove, Count, Delete

```ts
await redis.stream(activity).xdel("42", ["1720094400000-0"]);
const size = await redis.stream(activity).xlen("42");
await redis.stream(activity).del("42");
```

## Raw Redis Equivalent

```ts
await nodeRedis.xAdd("activity:42", "*", {
  action: "login",
  points: "5"
});
```

For at-least-once delivery across many workers, use [consumer groups](/benni/data-structures/consumer-groups/) (`XGROUP`, `XREADGROUP`, `XACK`) via `redis.stream(activity).group(name)`. Use `xread` for single-consumer polling. To block a worker until an entry arrives, [`xread` with a `timeoutSeconds`](/benni/advanced/blocking-operations/) and the blocking group read run on a [session](/benni/advanced/sessions/).

Use streams for activity feeds, audit logs, and event pipelines where entries need stable IDs and time ordering.

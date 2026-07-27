---
title: "Bitmaps"
description: "Use bitmaps for dense boolean flags addressed by integer offsets."
---

Use bitmaps for dense boolean flags addressed by integer offsets.

## Define A Bitmap

```ts
import { bitmap } from "beni/schema";

export const dailyActive = bitmap("daily-active");
```

Bitmaps take no value codec. Each bit is addressed by a non-negative integer offset and exposed as a `boolean`.

## Set And Get Bits

```ts
const previous = await redis.bitmap(dailyActive).setbit("2026-07-04", 42, true);
//    ^? boolean (the bit's previous value)

const active = await redis.bitmap(dailyActive).getbit("2026-07-04", 42);
```

## Count Set Bits

```ts
const total = await redis.bitmap(dailyActive).bitcount("2026-07-04");

const inFirstKilobyte = await redis.bitmap(dailyActive).bitcount("2026-07-04", {
  start: 0,
  end: 1023,
  unit: "BYTE"
});
```

The optional range takes `start`, `end`, and a `unit` of `"BYTE"` (the Redis default) or `"BIT"`.

## Find The First Bit

```ts
const first = await redis.bitmap(dailyActive).bitpos("2026-07-04", true);
//    ^? number | null
```

`bitpos` wraps `BITPOS` and returns `null` when no matching bit exists. Pass `start`, `end`, and `unit` to limit the search; `end` requires `start`, and `unit` requires both.

## Combine Bitmaps

```ts
const sizeInBytes = await redis.bitmap(dailyActive).bitop("2026-week-27", "OR", [
  "2026-07-01",
  "2026-07-02",
  "2026-07-03"
]);
```

`bitop(destination, operation, sources)` runs `BITOP` with `"AND"`, `"OR"`, `"XOR"`, or `"NOT"` and stores the result under the destination ID. `"NOT"` requires exactly one source ID.

## Packed Integer Fields

`bitfield` treats a key as a row of arbitrary-width integers packed at bit offsets, ideal for compact per-entity counters. Chain operations and call `exec`; the result is a **tuple typed to match the chain**:

```ts
const [views, previous, level] = await redis.bitmap(dailyActive)
  .bitfield("2026-07-04")
  .get("u32", 0) //           number       (read a 32-bit unsigned field)
  .set("u32", 0, 100) //      number | null (returns the previous value)
  .overflow("sat") //         mode for the ops that follow it
  .incrby("u8", "#8", 1) //   number | null (the new value)
  .exec();
```

- **Encoding**: `u1`–`u63` (unsigned) or `i1`–`i64` (signed).
- **Offset**: an absolute bit offset (`0`), or `#n` to address the _nth_ field of that width (`"#8"` = the 9th `u8`, i.e. bit offset 64).
- **`get`** yields a `number`; **`set`** and **`incrby`** yield `number | null`.
- **`overflow`** sets the mode for the operations _after_ it: `"wrap"` (the default, modular), `"sat"` (clamp to the type's min/max), or `"fail"` (leave the field unchanged and return `null`). That `null` is why `set`/`incrby` are nullable.

```ts
// Atomic per-user counters clamped to a byte, no read-modify-write race.
const [clamped] = await redis.bitmap(quotas)
  .bitfield(userId)
  .overflow("sat")
  .incrby("u8", 0, 1)
  .exec();
```

## Delete

```ts
await redis.bitmap(dailyActive).del("2026-07-04");
```

## Raw Redis Equivalent

```ts
await nodeRedis.setBit("daily-active:2026-07-04", 42, 1);
const total = await nodeRedis.bitCount("daily-active:2026-07-04");
```

Use bitmaps for daily-active tracking, feature rollouts keyed by numeric user ID, and any dense set of boolean flags where offsets map to entities. Use sets when members are sparse strings rather than dense integers.

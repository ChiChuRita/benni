---
title: "Sorted Sets"
description: "Use sorted sets for ranked values: leaderboards, priorities, timestamps, and scored indexes."
---

Use sorted sets for ranked values: leaderboards, priorities, timestamps, and scored indexes.

```ts
import { zset, string } from "beni/schema";

export const leaderboards = zset("leaderboard", string());
```

Add members with scores — `zadd` takes a single entry or an array:

```ts
await redis.zset(leaderboards).zadd("global", { member: "user:42", score: 100 });

await redis.zset(leaderboards).zadd("global", [
  { member: "user:42", score: 100 },
  { member: "user:7", score: 80 }
]);
```

Conditions mirror the Redis tokens: `nx` (only add new members), `xx` (only update existing), `gt`/`lt` (only move a score up/down), and `ch` (count changed members instead of only added ones). Illegal combinations — `nx` with `xx`, `gt`, or `lt`, and `gt` with `lt` — are compile errors:

```ts
await redis.zset(leaderboards).zadd("global", entries, { gt: true, ch: true });
```

Read the top members:

```ts
const top = await redis.zset(leaderboards).zrange("global", { start: 0, stop: 9, rev: true });
```

Read members with scores:

```ts
const entries = await redis.zset(leaderboards).zrange("global", { start: 0, stop: 9, withScores: true });
//    ^? Array<{ member: string; score: number }>
```

Increment a score:

```ts
await redis.zset(leaderboards).zincrby("global", 5, "user:42");
```

Raw Redis equivalent:

```ts
await nodeRedis.zAdd("leaderboard:global", [
  { value: "user:42", score: 100 },
  { value: "user:7", score: 80 }
]);
```

Sorted sets are a good fit for leaderboards, ranking search candidates, rate-limit windows, delayed jobs, and anything where score ordering matters.

## Lexicographic Ranges

When every member in a sorted set shares the same score, Redis orders them lexically by member value. That turns a sorted set into a sorted index — handy for autocomplete, prefix search, or any alphabetized listing. Calling `zrange` with `byLex: true` exposes Redis's `BYLEX` family over that ordering.

```ts
import { zset, string } from "beni/schema";

export const names = zset("name-index", string());
```

Add every member with the **same score** so ordering is purely lexical:

```ts
await redis.zset(names).zadd("directory", [
  { member: "adam", score: 0 },
  { member: "ada", score: 0 },
  { member: "ben", score: 0 },
  { member: "bella", score: 0 },
  { member: "cara", score: 0 }
]);
```

Range over members between two bounds. A bound is either the `"-"` / `"+"` sentinel (lowest / highest possible member) or `{ value }`, which is inclusive by default:

```ts
const aToB = await redis.zset(names).zrange("directory", {
  byLex: true,
  min: { value: "ada" },
  max: { value: "ben" }
});
//    ^? string[]   → ["ada", "adam", "ben"]
```

Set `inclusive: false` on a bound to make it exclusive:

```ts
const openEnded = await redis.zset(names).zrange("directory", {
  byLex: true,
  min: { value: "ada", inclusive: false },
  max: { value: "ben", inclusive: false }
});
//    → ["adam"]
```

Use the `"-"` and `"+"` sentinels for open ranges — this reads every member, in order. `offset` and `count` apply a `LIMIT` and must be provided together:

```ts
const firstThree = await redis.zset(names).zrange("directory", {
  byLex: true,
  min: "-",
  max: "+",
  offset: 0,
  count: 3
});
//    → ["ada", "adam", "bella"]
```

Set `rev: true` to walk the range high-to-low. The `min`/`max` bounds still describe the low and high ends of the range; only the result order flips:

```ts
const reversed = await redis.zset(names).zrange("directory", {
  byLex: true,
  min: "-",
  max: "+",
  rev: true
});
//    → ["cara", "bella", "ben", "adam", "ada"]
```

Count the members in a lex range without materializing them:

```ts
const inRange = await redis.zset(names).zlexcount(
  "directory",
  { value: "ada" },
  { value: "ben" }
);
//    ^? number   → 3
```

Remove every member in a lex range:

```ts
const removed = await redis.zset(names).zremrangebylex(
  "directory",
  { value: "ada" },
  { value: "adam" }
);
//    ^? number   (members deleted)
```

Store a lex slice into another key with `zrangestore` and `byLex: true`. It accepts the same `min`, `max`, `rev`, `offset`, and `count` options as a `byLex` `zrange` and returns the number of members written:

```ts
const stored = await redis.zset(names).zrangestore("b-names", "directory", {
  byLex: true,
  min: { value: "b" },
  max: { value: "c", inclusive: false }
});
//    ^? number   → 2   ("bella", "ben" written to the "b-names" key)
```

The member `value` in a bound is encoded through the schema's codec, exactly like a member passed to `zadd`. Lex ranges assume **all scores are equal** — this is standard Redis `BYLEX` behavior, and results are undefined when scores differ. Because Redis rejects `WITHSCORES` alongside `BYLEX`, you cannot combine `byLex: true` with `withScores: true`; use `zrange` with `{ byScore: true, withScores: true }` when you need scores back.

Raw Redis equivalent:

```ts
await nodeRedis.sendCommand([
  "ZRANGE",
  "name-index:directory",
  "[ada",
  "[ben",
  "BYLEX"
]);
```

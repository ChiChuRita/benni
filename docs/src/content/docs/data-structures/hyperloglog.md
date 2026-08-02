---
title: "HyperLogLog"
description: "Use HyperLogLog when you need approximate cardinality counts with low memory usage."
---

Use HyperLogLog when you need approximate cardinality counts with low memory usage.

```ts
import { hll, string } from "benni/schema";

export const pageViews = hll("page-views", string());
```

Add values:

```ts
await redis.hll(pageViews).pfadd("2026-07-04", [
  "user:42",
  "user:7"
]);
```

`pfadd` always takes an array, so a single value is `pfadd(id, [value])`; there is no single-value overload. An empty array is a no-op rather than a command that would create the key.

Count unique values:

```ts
const uniqueVisitors = await redis.hll(pageViews).pfcount("2026-07-04");
```

Count across multiple keys:

```ts
const weeklyVisitors = await redis.hll(pageViews).pfcount([
  "2026-07-01",
  "2026-07-02",
  "2026-07-03"
]);
```

Merge keys:

```ts
await redis.hll(pageViews).pfmerge("2026-week-27", [
  "2026-07-01",
  "2026-07-02",
  "2026-07-03"
]);
```

Raw Redis equivalent:

```ts
await nodeRedis.pfAdd("page-views:2026-07-04", ["user:42", "user:7"]);
const uniqueVisitors = await nodeRedis.pfCount("page-views:2026-07-04");
```

Use HyperLogLog for approximate unique counts such as daily visitors, active users, unique IPs, and event reach. Use sets when you need exact membership checks or exact members back.

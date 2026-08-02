---
title: "Leaderboards"
description: "Use a sorted set when Redis should rank members by score."
---

Use a sorted set when Redis should rank members by score.

```ts
import { zset, string } from "benni/schema";

export const leaderboards = zset("leaderboard", string());
```

Record scores:

```ts
await redis.zset(leaderboards).zadd("weekly", [
  { member: "user:42", score: 1200 },
  { member: "user:7", score: 950 }
]);
```

Increment a score:

```ts
await redis.zset(leaderboards).zincrby("weekly", 25, "user:42");
```

Read the top 10:

```ts
const top = await redis
  .zset(leaderboards)
  .zrange("weekly", { start: 0, stop: 9, rev: true });
```

Read scores with members:

```ts
const ranked = await redis
  .zset(leaderboards)
  .zrange("weekly", { start: 0, stop: 9, withScores: true });
```

Raw Redis equivalent:

```ts
await nodeRedis.zIncrBy("leaderboard:weekly", 25, "user:42");
const top = await nodeRedis.zRange("leaderboard:weekly", 0, 9, {
  REV: true
});
```

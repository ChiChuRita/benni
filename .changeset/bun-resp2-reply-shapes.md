---
"benni": minor
---

**Breaking:** `benni/bun` now returns the same RESP2-shaped replies as every other adapter, so `redis.raw` and your own reply decoders behave identically on Bun, Node, ioredis, and Upstash.

Bun's client only speaks RESP3 and has no switch to RESP2. The typed stores already coped with both shapes, but anything reading raw replies saw different values on Bun: `HGETALL` came back as a `Map`, `ZSCORE` as a number, and `ZRANGE ... WITHSCORES` as nested `[member, score]` pairs. The adapter now reshapes each reply the way RESP2 would have sent it: maps become flat `[field, value, ...]` arrays (`XREAD`/`XREADGROUP` become RESP2's `[stream, entries]` pairs), doubles become decimal strings (`"inf"`/`"-inf"` for the infinities), and scored or paired replies are flattened. The adapter contract on `RedisClient` now spells out these shapes, and the shared contract suite asserts them for every adapter, including inside pipelines and transactions.

Only raw access on Bun changes. Typed store results are identical.

```ts
// Before, on benni/bun
await redis.raw.send(["HGETALL", "user:1"]); // Map { "name" => "ada" }
await redis.raw.send(["ZSCORE", "board", "ada"]); // 1.5
await redis.raw.send(["ZRANGE", "board", "0", "-1", "WITHSCORES"]); // [["ada", 1.5]]

// After, on every adapter
await redis.raw.send(["HGETALL", "user:1"]); // ["name", "ada"]
await redis.raw.send(["ZSCORE", "board", "ada"]); // "1.5"
await redis.raw.send(["ZRANGE", "board", "0", "-1", "WITHSCORES"]); // ["ada", "1.5"]
```

The per-command table covers the commands whose RESP3 reply carries doubles or pairs (every sorted-set score reply, `HRANDFIELD ... WITHVALUES`, `GEOPOS`, `GEOSEARCH ... WITHCOORD`), each checked against what Bun 1.4.2 decodes from redis 8 and what node-redis receives over RESP2. A command outside that table keeps its RESP3 doubles as numbers.

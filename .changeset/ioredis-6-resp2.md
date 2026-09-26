---
"benni": minor
---

**Breaking:** `benni/ioredis` now supports ioredis 6 as well as 5, and refuses an adopted client that speaks RESP3 instead of failing on the first stream read.

ioredis 6 (npm `latest` since 2026-07-31) switched its default protocol to RESP3. Even in its RESP2-compatible reply mapping, RESP3 reshapes replies the typed stores decode: `XREAD` and `XREADGROUP` answer with a map, so every stream read, the job queue included, failed with `ReplyShapeError: Expected Redis XREAD to return key/entries pairs`.

- Clients the adapter creates from a URL or options are pinned to `protocol: 2`, the way `benni/node` pins node-redis. Nothing to change on your side.
- Adopting a RESP3 client now throws a `TypeError` naming the fix. On ioredis 6, create the client you hand over with `protocol: 2`:

  ```ts
  // Before: adopted fine, then stream reads threw ReplyShapeError
  const client = await ioredis(new Redis(url));

  // After
  const client = await ioredis(new Redis(url, { protocol: 2 }));
  const cluster = await ioredis(
    new Redis.Cluster(nodes, { redisOptions: { protocol: 2 } })
  );
  ```

- Passing `protocol: 3` to `ioredis({ ... })`, or `?protocol=3` in its URL, throws the same way rather than being silently overridden.

ioredis 5 speaks only RESP2, so nothing changes there. The `ioredis` peer range widens to `^5.0.0 || ^6.0.0`, and CI runs the ioredis suites against both majors.

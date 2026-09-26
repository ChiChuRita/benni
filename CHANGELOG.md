# Changelog

## 0.1.0

First release of Benni, the end-to-end typed Redis client for TypeScript:
declare schemas once as plain values, bind a client, and every read decodes back
to the type you declared, with one API across Node, Bun, Deno, and the edge.

- **Schemas and typed stores.** `benni/schema` declares keys and their codecs;
  `benni(client, { schema })` binds them. Stores cover strings and KV, counters,
  hashes (including hash field TTLs and Redis 8 `hsetex`/`hgetex`/`hgetdel`),
  lists, sets, sorted sets, streams and consumer groups, geo, bitmaps with a
  typed `BITFIELD` builder, and HyperLogLog. Replies that do not match the
  declared shape throw `ReplyShapeError` instead of passing through.
- **Transactions, sessions, scripts.** Typed `MULTI`/`EXEC` tuples, sessions
  that hold a connection for blocking commands, `redis.watch()` for retried
  optimistic transactions, and typed Lua scripts with named keys and cached
  `EVALSHA`.
- **Pub/Sub and scans.** Typed channels and patterns, subscribed straight off
  the bound client (one leased, ref-counted subscriber connection) or consumed
  as async iterators, and cursor scans as async iterators.
- **Adapters.** `benni/node` (node-redis), `benni/ioredis` (including adopting
  an ioredis instance or `Cluster` you already run), `benni/bun` (Bun's built-in
  client), Deno through `benni/node` and `npm:redis`, and `benni/upstash`, a
  zero-dependency HTTP adapter for edge and serverless.
- **Redis Cluster.** Schemas declare where their hash tag goes, multi-key calls
  whose slots provably disagree fail to compile, and an optional guard from
  `benni/cluster` checks the rest before sending. Routing stays with the
  cluster-aware client underneath.
- **Primitives** (`benni/primitives`): a distributed `lock`, a sliding-window
  `ratelimit`, a stampede-proof read-through `cache`, `budget`, `semaphore`,
  `idempotency`, and `queue`, a job queue built for AI work (heartbeat leases,
  resumable per-job output streams, cancellation, provider-shaped retries).
- **Integrations.** `benni/next` (ISR `cacheHandler` and rate limiting),
  `benni/hono` (rate-limit, cache, and session middleware), and `benni/zod`
  (bidirectional Zod codecs); any Standard Schema validator works with
  `json(schema)`.
- **Tree-shakable.** A bundle keeps only the store kinds an app declares.

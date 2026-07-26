# Changelog

Notable user-facing changes to Beni are documented here. This project uses
[Changesets](https://github.com/changesets/changesets) to prepare releases.

## Unreleased

- Schema-first typed Redis client: declare schemas as plain TypeScript values
  (`beni/schema`), bind a client once with `beni(client, { schema })`,
  and every read decodes back to your declared type.
- Typed data structures: strings/KV, counters, hashes (including Redis 8
  `hsetex`/`hgetex`/`hgetdel` and hash-field TTLs), lists, sets, sorted sets,
  streams and consumer groups, geo, bitmaps (with a typed `BITFIELD` builder),
  and HyperLogLog.
- Runtime adapters: `beni/node` (node-redis, optional peer dependency),
  `beni/bun` (Bun's built-in client), Deno via `npm:redis`, and
  `beni/upstash` — a zero-dependency HTTP adapter for edge and serverless.
- Transactions and sessions: typed `MULTI`/`EXEC` tuples via `redis.multi()`,
  connection-holding sessions with blocking commands, and `redis.watch()` for
  retrying optimistic transactions.
- Typed Lua scripts with named keys, typed args, and cached `EVALSHA`.
- Typed Pub/Sub channels and patterns, plus cursor scans as async iterators.
- `beni/primitives`: a correct distributed lock, a sliding-window rate
  limiter, and a stampede-proof read-through cache.
- Integrations: `beni/next` (ISR `cacheHandler` and rate-limit helper),
  `beni/hono` (rate-limit, cache, and session middleware), and
  `beni/zod` (bidirectional Zod codecs); any Standard Schema validator
  works with `json(schema)`.
- Server compatibility: Redis 7.2 through 8, Valkey 8, and Dragonfly, with the
  per-version surface documented in the README.
- The Hono cache middleware reports hits on the `X-Beni-Cache` response header.
  If you tracked this during pre-release development it was previously
  `X-Redtype-Cache`.

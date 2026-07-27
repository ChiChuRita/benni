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
  `beni/upstash`, a zero-dependency HTTP adapter for edge and serverless.
- Transactions and sessions: typed `MULTI`/`EXEC` tuples via `redis.multi()`,
  connection-holding sessions with blocking commands, and `redis.watch()` for
  retrying optimistic transactions.
- Typed Lua scripts with named keys, typed args, and cached `EVALSHA`.
- Typed Pub/Sub channels and patterns, plus cursor scans as async iterators.
  Subscribing needs no second object: `redis.pubsub.channel(userEvents).subscribe(handler)`
  leases one subscriber connection from the bound client on the first subscribe,
  multiplexes every channel and pattern onto it (ref-counted: one Redis subscription
  per name however many handlers you attach), and closes it when the last
  subscription is unsubscribed. `redis.pubsub.close()` drops everything at once.
  Publishing always rides the bound client, so it works on every adapter including
  `beni/upstash` on the edge.
- Pub/Sub subscriptions can also be consumed as async iterators:
  `redis.pubsub.channel(userEvents).stream({ signal })` yields decoded messages and
  `redis.pubsub.pattern(userEventPattern).stream({ signal })` yields `{ message, channel }`.
  Aborting the signal (or leaving the `for await` loop) ends iteration and releases
  the subscription.
- Adapters advertise Pub/Sub support with a new optional `subscriber?()` method on the
  `RedisClient` contract, the counterpart to `session?()`; `RedisSubscriber` is
  exported from the root entrypoint alongside it. An adapter that cannot hold a
  connection (`beni/upstash`) omits it, and subscribing throws `TypeError` at call
  time. `psubscribe`/`punsubscribe` are optional in turn: the Bun subscriber omits
  them because Bun 1.3.14's `psubscribe` hangs upstream, so pattern subscribes throw
  `TypeError` on Bun instead of deadlocking.
- `BeniOptions` gained `onPubSubError(error)`, called when a Pub/Sub handler throws or
  rejects; delivery to the other handlers continues either way, and without the
  callback the error is rethrown asynchronously rather than swallowed.
- **Breaking (pre-release):** the `pubsub` option on `beni(client, { ... })` is gone,
  as are the standalone `pubsub()` factory from `beni/node` and `bun.pubsub` from
  `beni/bun`. `bun` is now just the client function. Delete the adapter and its
  option; subscribing works off the bound client.
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

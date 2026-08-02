# Changelog

## 0.1.0

### Minor Changes

- 3c07e16: Add `queue` to `benni/primitives` — a job queue built for AI work.

  Model calls run for minutes, stream their output, cost money per attempt, and
  get cancelled mid-flight. `queue` treats those as the design rather than as
  configuration:

  - **Heartbeat leases, not idle timers.** A reserved job is owned for `leaseMs`
    and renewed while the handler runs, so a ten-minute generation is ordinary.
    A crashed worker's lease lapses and the job is reclaimed or dead-lettered.
  - **A resumable output stream per job.** `job.emit(token)` appends to a capped
    per-job stream _and_ renews the lease in one round trip, so
    `queue.watch(id, { after })` is a resumable feed — a client that drops
    mid-generation replays from its last entry id instead of paying twice. A
    retried attempt emits `restarted` so watchers discard the failed generation.
  - **First-class cancellation.** `queue.cancel(id)` aborts the handler's
    `AbortSignal`, stopping the in-flight provider call, and settles the job
    `cancelled` rather than `failed`.
  - **Retries that match provider failures.** Exponential backoff with full
    jitter by default; `RetryJobError` carries a provider `Retry-After`, and
    `TerminalJobError` dead-letters without burning attempts.
  - **Idempotency keys** that collapse duplicate requests onto one job and keep
    serving its result after it completes.

  Job lifecycle lives in sorted sets (delays, priority, backoff, dead-lettering);
  streams carry output. Every key shares one hash tag, so a queue occupies a
  single Redis Cluster slot. `enqueue`/`get`/`cancel`/`wait`/`watch`/`stats` need
  only `EVALSHA` and stream reads and run on `benni/upstash` at the edge;
  `worker()` needs a persistent process and blocks on a doorbell list where the
  adapter provides a dedicated connection, falling back to polling where it does
  not.

- 4887dfc: Add three primitives to `benni/primitives`: `budget`, `semaphore`, and `idempotency`.

  These came out of auditing the existing primitives against what people actually install. `queue` and `lock` hold up, `cache` is narrow but sound, and `ratelimit` was a strict subset of @upstash/ratelimit and rate-limiter-flexible. Rather than chase their feature lists and ship a worse clone, we went after gaps nobody fills.

  **`budget` — cost-weighted spend limits.** Rate limits count requests, but model calls are not priced by the request: one 200k-token call costs what fifty 4k-token calls cost, so "100 requests/minute" caps nothing you care about. `budget` counts the unit you are billed in.

  ```ts
  const budgets = budget(client, { limit: 2_000_000, windowMs: 86_400_000 });

  // Cost known up front.
  const { ok, remaining } = await budgets.charge(userId, promptTokens);

  // Cost known only after the call: hold an estimate, then reconcile.
  const hold = await budgets.reserve(userId, 8_000);
  if (!hold) return new Response("Budget exhausted", { status: 429 });
  try {
    const res = await callModel();
    await hold.settle(res.usage.totalTokens);
  } catch {
    await hold.release();
  }
  ```

  Reservations are the part that is genuinely missing elsewhere. You cannot know a call's real cost until it returns, so check-then-spend is a race: ten concurrent requests all see room and collectively blow the budget. A hold counts against everyone else from the moment it is taken and is replaced by the real number on settle. It is a lease, not a lock, so a caller that dies stops counting on its own. Settling twice on a handle charges once (the token never leaves the process, so a duplicate is always the same handle); settling after the hold lapsed still charges, because the money was spent. Every existing answer to this problem (LiteLLM, Agent Gateway, TrueFoundry) is a gateway you deploy rather than a library you import.

  **`semaphore` — bounded concurrency.** "At most 20 calls in flight" is a different constraint from "100 calls per minute", and providers enforce both. `p-limit` solves it inside one process; the moment you run two instances the limit is per-instance and the provider sees the sum. Same handle, `run`, and retry options as `lock`, so it is `lock` with a number. Slots are held by leases, so a crashed holder cannot wedge the pool.

  **`idempotency` — exactly-once side effects.** Stripe-style `Idempotency-Key` for POST handlers: a retried request must not charge the card twice and must return the original response. A losing concurrent caller waits for the winner's result by default, so a double-click gets the same receipt rather than a 409. Distinct from `cache`, which may freely recompute a pure read.

  Amounts in `budget` must be whole numbers, since the counters underneath are Redis integers; budget in the smallest unit you meter.

  All three take their time from the Redis server rather than the caller, work over every adapter including `benni/upstash`, and are cluster-safe by construction. Their concurrency guarantees are proved against a live server, not a fake client: 50 concurrent `semaphore` runs never exceed the limit, 20 concurrent `reserve` calls admit exactly the number that fit, and 10 concurrent `idempotency` calls run the handler once.

- f40bdac: Add cluster-safe keys: declare where a schema puts its Redis Cluster hash tag, and catch cross-slot mistakes at compile time and before they are sent.

  Benni models slot co-location, not cluster topology. Routing stays your driver's job (pass `createCluster()` or an ioredis `Cluster`). What no driver can do is know, before you send, that a command's keys belong together, and Benni is the only TypeScript client positioned to: keys come from schemas rather than string concatenation.

  Every keyed schema factory now takes an opt-in `hashTag` option. Omitting it leaves today's `prefix:id` layout and behaviour byte-for-byte unchanged.

  - `hashTag: "prefix"` builds `{prefix}:id`, pinning a keyspace to one slot so every within-schema multi-key method (`mget`, `sunionstore`, `zmpop`, `bitop`, `pfmerge`, `lmove`, and about twenty more) becomes legal on a cluster.
  - `hashTag: "id"` builds `prefix:{id}`, keeping keys spread while co-locating the same id across schemas, so `cart:{u1}` and `order:{u1}` can appear in one command.

  Because the tag lives in the key's template-literal type, cross-slot combinations are a compile error on `script().run()`, `redis.watch()`, and a new `multi().keys([...])` declaration:

  ```ts
  await redis.script(moveItem).run({
    keys: { from: carts.key("u1"), to: orders.key("u2") },
    //                                  ^ Type '"order:{u2}"' is not assignable to type
    //                                    'KeysMustShareOneHashSlot<"order:{u2}", "u1">'
    args: { amount: 1 },
  });
  ```

  A passing check means "no provable conflict", not "provably co-located": untagged keys and keys built from runtime ids pass silently. For those, install the runtime guard from the new `benni/cluster` entry:

  ```ts
  import { assertSameSlot } from "benni/cluster";

  const redis = benni(client, { cluster: assertSameSlot });
  ```

  It verifies every multi-key command before it is sent and throws `CrossSlotError` naming both keys, both slots, and the layout that fixes it. Off by default, because cross-slot commands are legal on a single-node Redis; turn it on in development and CI.

  You pass the checker rather than `true` for a reason worth stating: `benni()` has to reference the guard to install it, so a boolean would mean the root entry names it and no bundler could drop it, putting the CRC16 table and the error's fix-hint prose in every app that never turns the check on. Taking it as a value keeps all of that in `benni/cluster`, which is about 1.4 KB gzipped, roughly 15% of the default root entry. When the guard is absent each check is an optional call on an undefined function, which short-circuits argument evaluation, so the key arrays are never even built.

  `benni/cluster` also exports `slotOf` and `hashTagOf`, verified against a live cluster-enabled Redis for every generated key.

- f9f76f1: Fix four correctness bugs in `budget`, and bound the reservation set.

  Settling is now deduplicated in Redis on the reservation token instead of only
  on the handle. A settle whose reply was lost on the way back, a socket reset, a
  command timeout, a failover, used to charge the budget twice when the caller
  retried it, metering the user at double their real spend. The retry is now a
  no-op on the server, which is what the primitive always documented.

  `retryAfterMs` now reports when the window actually frees enough units for the
  spend that was denied. It used to report the time to the next bucket boundary,
  which frees nothing, so a client that obeyed it retried at a moment guaranteed
  to fail, or waited far longer than it had to.

  A process stalled for longer than a whole window no longer gets a bogus answer.
  The internal stale-bucket sentinel used to escape as a real reply: `charge`
  returned a spurious denial, `reserve` a spurious exhaustion, and `settle`
  resolved successfully having charged nothing. Those calls now throw
  `BudgetWindowRolledError`, and a hold whose `settle` throws it stays usable.

  `extend()` works for estimates of 1e14 and above. The hold was stored under a
  member Lua had formatted to 14 significant digits, so the heartbeat could never
  find it and the hold lapsed mid-call.

  New `maxHolds` option, default 10000, caps how many reservations one id may
  hold at once. Summing live holds walks the whole set, and an estimate of `0`
  consumes no headroom, so nothing else bounded it.

- eb65011: Nine small correctness fixes across the core. Three of them can break a build
  or a call that used to succeed silently: the `lpos` and `script` overload
  changes below, and the new schema-definition and reply checks.

  `benni()` no longer refuses to bind a schema module that co-exports a validator.
  Any object carrying a `kind` property used to be claimed as a benni schema and
  crash at bind time, which hit every module declaring a Valibot schema or an
  ArkType type next to its benni schemas, the layout `json(validator)` invites.
  The store binding decides now, and a copied benni schema still fails loudly and
  names the export.

  A `hashTag: "id"` prefix containing `{` is rejected when the schema is defined.
  Redis takes the tag from the first brace in the whole key, so such a prefix
  silently voided the co-location the layout exists for and only showed up as
  CROSSSLOT on a real cluster.

  Counter reads and BITFIELD reads now throw `ReplyShapeError` instead of
  returning a silently rounded value past `Number.MAX_SAFE_INTEGER`. The write
  side already refused unsafe integers; the read side now matches.

  A script that returns its own `NOSCRIPT`-coded error is no longer mistaken for
  a server-side cache miss and re-run. Benni confirms with `SCRIPT EXISTS` before
  reloading a cached SHA, so a script's side effects are not applied twice.

  `script()` no longer accepts a forwarded or computed `nullable`, and `lpos()`
  no longer accepts an options bag whose `count` is `number | undefined`. Both
  used to select an overload whose declared result type could not hold what the
  call actually resolved. Passing either through a helper is now a compile error.

  `bytes()` throws `ReplyShapeError` with the offending value on `.reply`, like
  every other codec, instead of a bare `TypeError`.

  `getrange`, `setrange`, and `strlen` are documented as working in bytes, which
  is what Redis does. Chunked reads of non-ASCII values must split on byte
  boundaries.

- b15f448: Close a set of correctness and safety holes in the `benni/hono` middleware, and add session id rotation.

  `cache()` no longer stores a response that was derived from the session identity. The touched-tracking that keeps a per-user response out of a shared cache only fired on `get`/`set`/`delete`/`clear`, so a handler that returned something built from `session.id` or `session.isNew` slipped past it. A live session id was then stored under a session-independent key and replayed to every later visitor, who could send it back as their own cookie. Reading the bag at all now counts, whether you reach it through `getSession(c)` or `c.get("session")`.

  `Session` gains `regenerate()`. It mints a fresh id, carries the current data over, deletes the record under the old id, and issues a new `Set-Cookie`. There was previously no way to rotate a session id and no code path that could re-issue the cookie for an existing session, so the standard fixation defence, renew the identifier on login and on privilege change, was unavailable. Call it on login.

  Sessions loaded from an existing record are now written back with `SET ... XX`. A request already in flight when a concurrent `clear()` deleted the record used to re-store its stale snapshot with a fresh full lifetime, re-authenticating the session the user had just logged out of.

  `cache()` is stricter about what it will store. Only a plain `200` is storable, so a `206` built for someone else's `Range` header can no longer be replayed, with `Content-Range` stripped, to clients that sent no `Range` at all; ranged requests pass straight through. A response saying `no-store`, `no-cache`, or `private` in `Cache-Control` is respected, as is a `Vary` naming a header the key does not fold in (`Vary: *` is never stored). Stored entries now keep `cache-control`, `vary`, `etag`, and `last-modified` alongside `content-type`, so a replay stays honest to the browser and to any CDN in front of you.

  The default cache key includes the request origin, so one app bound to several hostnames no longer shares a single entry per path across them. This changes the default key format, which means existing entries are missed once and rebuilt. Callers who want cross-host sharing can restore the old behaviour with `key: (c) => { const url = new URL(c.req.url); return c.req.method + ":" + url.pathname + url.search; }`.

  `ratelimit()` sets `X-RateLimit-Limit`, `-Remaining`, and `-Reset` after the handler runs rather than before. Set before, they lived in Hono's prepared-header bag and were dropped whenever anything downstream returned a fresh `Response`, including `cache()` on every hit, so the documented headers vanished on exactly the cheapest requests.

- 64ea020: Fix five queue defects around cancellation, job-id reuse, and retries.

  Cancelling an active job now wins atomically: if the handler completes, or throws a retryable error, after `cancel()` returned `true`, the job settles `cancelled` instead of recording a result or scheduling another paid attempt. Previously that only happened once the worker noticed the flag on its next heartbeat, so anything finishing inside the heartbeat window slipped through.

  Re-enqueuing an explicit `id` now starts a genuinely clean generation: the old event stream, dead-letter entry, lifecycle memberships, and idempotency mapping are all cleared first, so `watch()` no longer replays the previous generation's terminal event. Reusing an id that is still waiting, scheduled, or active throws a `ValidationError` rather than putting one id in two indexes and running the job twice. This is the breaking part of the release.

  A retried attempt now trims its output stream instead of deleting it. Deleting reset the stream's id counter, so the `restarted` marker could be recreated at or below a cursor a watcher already held and the whole second generation went unseen.

  An idempotency key is now held for the job's entire run and only starts its `idempotencyTtlMs` retention once the job completes, capped at the record's own TTL. A slow job no longer loses its key mid-flight and lets a duplicate request pay for a second generation.

  `RetryJobError` now rejects a non-finite `retryAfterMs`, such as an unparsable `Retry-After` header. Redis refused it as a score only after the retry script had already released the lease, which left the job outside every lifecycle index with nothing able to reserve it.

- b15f448: The rate-limit subject is now a required option: `key` on `ratelimit` from `benni/hono`, and
  `identify` on `rateLimit` from `benni/next`. Both previously defaulted to the first
  `x-forwarded-for` hop (with `cf-connecting-ip` and `"anonymous"` behind it on Hono).

  That default was unsafe. There is no request property a limiter can trust without knowing the
  deployment: on a self-hosted app the header is set by the client, and many proxies append to it
  rather than replacing it, so a caller could pick its own identity, bypass the limit by varying one
  header, and mint a separate Redis key on every request. Taking the subject from the caller matches
  what `@upstash/ratelimit` does, and it makes the trust boundary explicit instead of implied.

  To keep the old behaviour where your platform genuinely overwrites the header, pass it yourself:

  ```ts
  // benni/next
  identify: (request) =>
    request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ??
    "anonymous";

  // benni/hono
  key: (c) =>
    c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous";
  ```

- 72324a3: First Benni release: the end-to-end typed Redis client for TypeScript.
  Declare schemas once, bind a client, and replies come back as your types —
  across Node, Bun, Deno, and the edge. Ships typed data structures (KV, hashes,
  lists, sets, sorted sets, streams, geo, bitmaps, HyperLogLog), transactions,
  sessions, typed Lua scripts, Pub/Sub, scans, the `lock`/`ratelimit`/`cache`
  primitives, and Next.js/Hono/Zod integrations.
- fa2a28c: Add `benni/ioredis` — a full adapter for [ioredis](https://www.npmjs.com/package/ioredis), the most widely deployed Redis client for Node.

  Until now Benni's Node story required node-redis, so trying it meant migrating your
  data layer _and_ your Redis client. This removes the second migration. It accepts
  a URL, ioredis options, or — the point — an ioredis instance you already have:

  ```ts
  import Redis from "ioredis";
  import { ioredis } from "benni/ioredis";

  const existing = new Redis(process.env.REDIS_URL); // yours, already tuned
  const client = await ioredis(existing);
  ```

  An adopted client is borrowed: `close()` reaps the sessions and subscriber
  connections Benni leased from it and leaves the client itself open, because the
  caller still owns its lifetime. Benni also attaches an `"error"` listener only to
  clients it creates, so it never swallows errors on a client it does not own.

  The adapter passes the shared client-contract suite in full — sessions with
  blocking reads, `WATCH` transactions (including the abort-to-`null` signal and
  per-command errors inside a committed EXEC), prompt close during a blocked read,
  Pub/Sub with pattern subscriptions, and the parent-close leak backstops. Sessions
  disable reconnection and the offline queue so a drop fails fast instead of
  silently losing `WATCH` state, and close via `disconnect()` so an in-flight
  blocking read is rejected at once rather than waiting out its timeout. ioredis
  speaks RESP2, whose flat reply shapes are what the typed stores already decode, so
  replies need no normalization.

  `ioredis` is an optional peer dependency, so nothing changes for existing users.

- f40bdac: Make the root entry tree-shakable — a kv-only app drops from 13.9 kB to 4.2 kB gzip.

  `benni()` used to dispatch with a `switch (schema.kind)` that named all twelve
  store factories, and `createStoreAccessors` / the session facade named them
  again. Every one of those is a static reference, so a bundler had to retain
  sorted-set, stream, geo, bitmap and the rest even for an app that declares a
  single hash. The cost was flat: 13.9 kB gzip no matter what you used.

  Each schema now carries its own store factory on a non-enumerable symbol,
  stamped by the `define*` builder in that kind's own module. `benni()` dispatches
  through the schema and names no store at all, so the only store code a bundle
  retains is the kinds the app actually declares. The pub/sub hub and the script
  runner became lazy for the same reason — an app with no channel never pulls in
  pub/sub.

  Measured with rolldown, minified + gzipped:

  | app                              | before  | after   |
  | -------------------------------- | ------- | ------- |
  | `benni` + kv only                 | 13.9 kB | 4.2 kB  |
  | `benni/upstash` + one hash schema | 15.2 kB | 7.0 kB  |
  | three kinds (hash + zset + list) | 15.2 kB | 10.2 kB |

  **The public API is unchanged.** `redis.query.<name>`, `redis.hash(schema)`,
  the session accessors, `QueryResource`, `Benni<typeof schema>` — same
  signatures, same inferred types, verified by the existing type-level tests.

  **One behavior change.** Schemas are no longer plain data: a copy that drops
  the symbol (object spread, `structuredClone`, a JSON round-trip) is no longer
  usable. Passing one now throws a `TypeError` naming the offending export, at
  `benni()` bind time rather than at first call. Pass the schema object the
  builder returned.

### Patch Changes

- f40bdac: Fix two key layouts that were broken or wasteful on a Redis Cluster.

  **`benni/next`** — `revalidateTag` deleted cache entries and their tag sets in one `DEL`, but the keys carried no hash tag, so the command was `CROSSSLOT` and the handler simply did not work on a cluster. Every key is now tagged into one slot (`{next-cache}:entry:…`, `{next-cache}:tag:…`). That `DEL` was also unbounded: a popular tag naming tens of thousands of entries produced a multi-megabyte command that blocks the server, so it is now chunked at 500 keys, entries before tag sets (a crash midway then leaves a tag pointing at deleted entries, which is self-healing, rather than entries with no tag, which can never be revalidated).

  **`benni/primitives`** — a cache entry and its own fill lock were `cache:<id>` and `cache:lock:<id>`, which hash to different slots: two nodes per miss, with the single-flight guarantee spread across them. The id now carries the tag (`cache:{<id>}` and `cache:lock:{<id>}`), so the pair is always co-located while the cache itself still spreads across the keyspace. Tagging the prefix instead would have pinned every entry to one node, which defeats the point of a cache.

  Both are key renames, so existing entries are orphaned on upgrade. Both are TTL'd, so the impact is one cold window.

- 442ea9f: Fix five connection-lifetime bugs in the TCP adapters.

  `benni/bun` no longer strands a reconnect loop when the very first connect fails: an unreachable server now rejects in milliseconds instead of after half a minute, and the process exits instead of being pinned forever by a client Bun gives you no way to cancel.

  All three TCP adapters now treat `close()` as final. A session or subscriber whose connect was still in flight when `close()` ran, or one leased after it, used to come back live and untracked, leaking a socket that in Node keeps the event loop alive through a "graceful" shutdown. Both cases now reject with "client is closed".

  `benni/node`'s `close()` is idempotent, matching every other adapter, so a SIGTERM and a SIGINT handler that both close the client no longer produce an unhandled rejection mid-shutdown. It also force-releases the socket, which a graceful close during a reconnect could otherwise leave behind.

  A `benni/node` subscriber now reports `closed` once its connection is terminally gone, so core drops the dead lease instead of reusing it for the next subscribe.

  A session leased from an adopted ioredis `Cluster` is finally fail-fast. Cluster takes different retry options than a standalone client, so the fail-fast settings were silently ignored and a dropped session reconnected with its `WATCH` state gone while still reporting itself open.

  `benni/ioredis`'s `send()` now returns the same reply shape as the other adapters when you write a command name in lowercase. `send(["hgetall", key])` hit ioredis's reply transformers and came back as a plain object instead of the flat array everywhere else.

- f9f76f1: Fix `cache` losing an invalidation to a load that was already running. A loader now publishes only while it still holds the fill lock, and `del()` drops the entry and that lock together, so the usual write-through order (update the row, then invalidate) can no longer be undone by a loader republishing its pre-invalidation snapshot for a full TTL. The same fence stops a loader whose lock has expired from overwriting a newer entry.

  Waiters also watch the fill lock instead of only the value: when a lease is handed to a new loader they wait for that loader rather than all giving up on the previous holder's clock and hitting the backend at once. The total wait stays bounded at three lock lifetimes.

  One behavior change to note: a load that runs longer than `lockTtlMs` still returns its value to the caller, but no longer caches it, because by then it may be older than whatever replaced it. Set `lockTtlMs` above your slowest load.

- eb65011: Follow-ups from the 2026-08-01 hunt that spanned more than one area:

  - `hmget`, `hgetex`, and `hgetdel` now declare their result as optional keys
    (`{ name?: string | null }`). They only fill the field names present at
    runtime, so declaring every member of the requested union as a present key
    promised data the reply need not contain.
  - A whole-record `hget` that finds some but not all declared fields now throws
    `PartialRecordError`, a new subclass of `ReplyShapeError` carrying the absent
    names on `.missing`. The reply in that case is well formed and the record is
    merely incomplete, which per-field TTLs make an ordinary outcome, so a caller
    watching for protocol or adapter faults no longer has to treat it as one.
  - The `string`, `enumOf`, and `boolean` codecs reject input they cannot
    represent instead of coercing it. `encode: String` turned an undefined field
    into the literal `"undefined"`, and `input ? "1" : "0"` turned it into a real
    `false`. `number` already refused non-finite input; the rest now match.
  - A `NOSCRIPT` reply is confirmed with `SCRIPT EXISTS` before the script is
    reloaded and re-run, on the freshly loaded path as well as the cached one. A
    script that returns its own `NOSCRIPT`-shaped error is byte-identical to the
    server's, and re-running one that had already applied its side effects
    applied them twice.
  - The Pub/Sub hub releases its subscriber lease even when the adapter's
    `unsubscribe` rejects. On a connection that had already died the detach
    always rejects, which left the dead subscriber cached as the hub's lease.

- b3732e8: Fix WATCH-safety on sessions and three ways a hash write could go wrong.

  A `hset(id, value, { ttlSeconds })` issued on a session ran a real MULTI/EXEC, which cleared the connection's watch set: an optimistic transaction around it committed over a concurrent write instead of aborting, or failed with a reply-shape error and wrote nothing. A session that holds a WATCH now batches such a write as a pipeline and leaves the watch armed.

  Two `redis.watch` calls sharing one borrowed session no longer interleave their WATCH sets. Each call now holds the session from WATCH to EXEC, so one can no longer abort on a key it never watched while the other commits over a concurrent write. Watches on separate sessions are unaffected.

  `hsetex` now rejects a field whose value is `undefined` instead of storing `"undefined"` or `false`, and `hgetex` now rejects an expiry passed with an empty field list instead of dropping it silently.

- b15f448: Fix `benni/next` cache-handler tag sets never getting an expiry. The handler
  extended a tag set's TTL with `EXPIRE ... GT`, which Redis refuses to apply to
  a key that has no expiry yet, so the set the preceding `SADD` had just created
  stayed permanent: it accumulated every key ever written under that tag, kept
  naming entries that had long since expired, and made every `revalidateTag`
  walk the whole accumulation. Adding a key to a tag set is now one atomic step
  that installs the TTL when the set is new and only extends it afterwards, so a
  tag set is reclaimed once its last entry expires while an entry with
  `revalidate: false` still keeps its tag set permanent.
- d91f677: Fix pub/sub subscriptions that could be silently killed by a concurrent unsubscribe or close.

  A `subscribe` whose SUBSCRIBE was still on the wire was invisible to the teardown path, so any overlapping `unsubscribe` (on that channel or any other) could close the leased connection out from under it. The caller was handed a subscription that looked healthy, received nothing, and poisoned every later subscribe to the same name. Subscribes and unsubscribes for the same channel or pattern are now serialised, and the lease is held for as long as a subscribe is in progress.

  Also fixed: `pubsub.close()` racing a first subscribe no longer wedges that channel name for the life of the process, the subscribe rejects instead; `close()` now ends a `stream()` loop that was started without an abort signal, rather than leaving it parked forever; a channel and a pattern that spell the same string no longer share one in-flight subscribe, which left the pattern with no PSUBSCRIBE and no working unsubscribe; and `stream()` no longer leaks an abort listener on the supplied signal when opening the subscription fails.

- 246acc1: Fix five sorted-set defects: three overloads that described the wrong reply
  shape, and two validators that disagreed with their neighbours.

  `zrange`, `zdiff`, `zunion`, `zinter`, and `zrandmember` all typed a
  `WITHSCORES` reply as bare members whenever `withScores` was a plain `boolean`
  rather than the literal `true`. Excess-property checking does not catch that,
  because the flag is a declared member of every one of those option types, so
  the members-only overload won and the caller got `SortedSetEntry` objects typed
  as members. String handling downstream either threw or silently produced
  `undefined`. The members-only overloads now exclude a `boolean` flag, so the
  ambiguous call is a compile error the caller has to branch on. `zrandmember`
  also had the `zpopmin` problem: a value typed `SortedSetRandomMemberOptions`,
  whose `count` is optional, always landed on the single-member overload while
  the server answered with an array. That overload now excludes `count`.

  Score bounds accept `Infinity` and `-Infinity`. `zadd` and `zincrby` already
  took them and `zscore` handed them back, but `zrange { byScore }`,
  `zrangestore { byScore }`, and `zremrangebyscore` rejected the value the
  library itself produced, and `zcount` validated nothing at all. All four now
  translate the infinities to Redis's `+inf`/`-inf` and reject only `NaN`.

  `zrandmember` accepts `count: 0` and returns `[]` without a round trip, the way
  `zpopmin` already did and the way Redis behaves. A count that comes out of
  `Math.min(wanted, remaining)` no longer throws when it reaches zero.

- 246acc1: Fix four stream bugs.

  `xadd` typed the reply as a plain `string` for any `nomkstream` the compiler
  could not see was `true`, so a computed flag, or an options object typed
  `StreamAddOptions`, resolved `null` under a non-nullable type. A spelled-out
  `nomkstream: someBoolean` now types as `Promise<string | null>`; an options
  value whose `nomkstream` is merely optional no longer compiles, because the
  reply shape is not knowable from its type.

  A stream field named `__proto__` was written to Redis but silently dropped on
  read, and it replaced the decoded entry's prototype. Decoding now defines the
  property instead of assigning it, so no field name can reach a setter on
  `Object.prototype`.

  `xtrim` rejected `{ maxLen: { count: 0 } }`, which is the only way to empty a
  stream without deleting the key and its consumer groups along with it. Zero is
  now accepted; negative and fractional counts still throw.

  `xreadgroup({ after: undefined })` is typed as a read of new deliveries but
  performed a history read, so tombstones arrived with a value declared
  non-nullable. The read now dispatches on the `after` value rather than the
  presence of the key: `undefined` reads `>`, an entry id reads history.

- eb65011: Harden the `benni/zod` codecs so they fail at the write instead of storing something unrecoverable.

  `zodJson` now shares the same stringify guard as the plain `json()` codec. Previously a `NaN` or `Infinity` anywhere in the value was written as JSON `null`, which reads back as the exact sentinel a missing key returns, so a written key became indistinguishable from an absent one. Non-finite numbers, `BigInt` fields, and circular structures now throw `ValidationError` before anything is sent.

  Encode failures inside the zod bridge are also `ValidationError` again. A schema containing a one-way `.transform()` used to surface zod's own `$ZodEncodeError`, which does not extend `TypeError`, breaking the documented promise that every pre-send failure is a `ValidationError` and both error classes extend `TypeError`.

  `zodCodec` now checks that the schema actually encoded to a string. `z.any()` satisfies the "encoded side is a string" type constraint without checking anything, so writes through it landed in Redis as `[object Object]` with no error anywhere.

  The async-schema docs are corrected too: an async refinement that rejects leaves an unhandled rejection zod discards internally, which Benni has no way to claim.

- 68574b7: Fix a clock-skew bug in `ratelimit`, and add `retryAfterMs` to its result.

  The sliding-window script took `now` from the calling process. Two app servers whose clocks disagree therefore disagreed about where the window starts, and the same user got a different limit depending on which server answered: a server running fast expires entries early and admits too many, one running slow rejects requests that should pass. The script now reads `TIME` from Redis, so every caller shares one clock. Nothing about the API changes.

  `RatelimitResult` gains `retryAfterMs`, a duration derived server-side from the same clock as `resetMs`. `resetMs` is an absolute server timestamp, so turning it into a `Retry-After` header meant differencing it against the local clock, reintroducing exactly the skew that was just removed. `benni/next` and `benni/hono` now use the new field for their `Retry-After` headers.

Notable user-facing changes to Benni are documented here. This project uses
[Changesets](https://github.com/changesets/changesets) to prepare releases.

## Unreleased

- Schema-first typed Redis client: declare schemas as plain TypeScript values
  (`benni/schema`), bind a client once with `benni(client, { schema })`,
  and every read decodes back to your declared type.
- Typed data structures: strings/KV, counters, hashes (including Redis 8
  `hsetex`/`hgetex`/`hgetdel` and hash-field TTLs), lists, sets, sorted sets,
  streams and consumer groups, geo, bitmaps (with a typed `BITFIELD` builder),
  and HyperLogLog.
- Runtime adapters: `benni/node` (node-redis, optional peer dependency),
  `benni/bun` (Bun's built-in client), Deno via `npm:redis`, and
  `benni/upstash`, a zero-dependency HTTP adapter for edge and serverless.
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
  `benni/upstash` on the edge.
- Pub/Sub subscriptions can also be consumed as async iterators:
  `redis.pubsub.channel(userEvents).stream({ signal })` yields decoded messages and
  `redis.pubsub.pattern(userEventPattern).stream({ signal })` yields `{ message, channel }`.
  Aborting the signal (or leaving the `for await` loop) ends iteration and releases
  the subscription.
- Adapters advertise Pub/Sub support with a new optional `subscriber?()` method on the
  `RedisClient` contract, the counterpart to `session?()`; `RedisSubscriber` is
  exported from the root entrypoint alongside it. An adapter that cannot hold a
  connection (`benni/upstash`) omits it, and subscribing throws `TypeError` at call
  time. `psubscribe`/`punsubscribe` are optional in turn: the Bun subscriber omits
  them because Bun 1.3.14's `psubscribe` hangs upstream, so pattern subscribes throw
  `TypeError` on Bun instead of deadlocking.
- `BenniOptions` gained `onPubSubError(error)`, called when a Pub/Sub handler throws or
  rejects; delivery to the other handlers continues either way, and without the
  callback the error is rethrown asynchronously rather than swallowed.
- **Breaking (pre-release):** the `pubsub` option on `benni(client, { ... })` is gone,
  as are the standalone `pubsub()` factory from `benni/node` and `bun.pubsub` from
  `benni/bun`. `bun` is now just the client function. Delete the adapter and its
  option; subscribing works off the bound client.
- `benni/primitives`: a correct distributed lock, a sliding-window rate
  limiter, and a stampede-proof read-through cache.
- Integrations: `benni/next` (ISR `cacheHandler` and rate-limit helper),
  `benni/hono` (rate-limit, cache, and session middleware), and
  `benni/zod` (bidirectional Zod codecs); any Standard Schema validator
  works with `json(schema)`.
- Server compatibility: Redis 7.2 through 8, Valkey 8, and Dragonfly, with the
  per-version surface documented in the README.
- The Hono cache middleware reports hits on the `X-Benni-Cache` response header.
  If you tracked this during pre-release development it was previously
  `X-Redtype-Cache`.

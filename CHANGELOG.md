# Changelog

## 0.2.0

### Minor Changes

- 7d488c0: `lock().run()` now renews the lock while the critical section runs, and reports a lost lease instead of hiding it.

  Before, `run()` acquired with `ttlMs` and never renewed, so a body that outlived the TTL silently lost mutual exclusion: the key expired, another caller acquired it, and the original body kept running as if it still held the lock. It now renews on an interval, following the same policy the `queue` primitive already used for job leases.

  - `run()` renews the lock every `heartbeatMs`, which defaults to a quarter of the effective `ttlMs` (matching the queue's `leaseMs` 60000 / `heartbeatMs` 15000 ratio). Pass `heartbeatMs: false` to opt out and keep the previous behaviour.
  - New `LockLeaseLostError`. If renewal finds the lock gone, `run()` rejects with it even when the body resolved, because a body that finished without the lock did not finish under mutual exclusion.
  - `LockHandle` gained a `signal` (an `AbortSignal`) that aborts with that error, so a body can pass it to `fetch` or the AI SDK and stop as soon as the work stops being exclusive. A manual `extend()` that resolves `false` aborts it too.
  - New `onRenewError` hook on `run()`, called when a renewal round trip fails. A failed round trip is not treated as a lost lock: the next tick retries, and the lease is only declared lost once Redis reports another token owns the key or the TTL window has passed with no successful renewal.
  - The renewal timer is unref'd and always cleared, so it can never keep a process alive.

  The fail-fast default is unchanged: `retries` still defaults to `0`, so a contended `acquire()` resolves `null` and a contended `run()` throws `LockNotAcquiredError` rather than waiting. That contract is now spelled out on `lock()`, `acquire()`, and `run()`, along with the `retries` / `retryDelayMs` pair to pass when the intent is to serialize concurrent callers instead.

  `acquire()`, `run()`, `LockHandle.release()`, `LockHandle.extend()`, and `LockNotAcquiredError` are unchanged.

- 8f513d9: Server errors are now normalized across every adapter: Redis error replies arrive as one `RedisServerError`, with the error code parsed out.

  Before, an error the Redis _server_ returned reached the caller in whatever shape the underlying client used. On `benni/node` a `ZADD` against a key holding a hash threw node-redis's own `SimpleError`; `benni/ioredis` threw ioredis's `ReplyError`; `benni/bun` threw Bun's `RedisError`; `benni/upstash` threw a bare `Error` built from the REST payload. So `catch (error) { if (error instanceof ...) }` could not be written portably, and WRONGTYPE or NOSCRIPT handling written against one adapter silently stopped matching after a move to another one, which is the opposite of what one typed API across runtimes is supposed to buy.

  - New `RedisServerError`, exported from `benni` and `benni/core`. It means the command reached Redis and Redis refused it, as opposed to `ValidationError` (benni refused the input before sending) and `ReplyShapeError` (a successful reply did not match the shape a decoder expected).
  - `code` carries the reply's leading error code (`"WRONGTYPE"`, `"NOSCRIPT"`, `"NOAUTH"`, `"OOM"`, `"READONLY"`, `"BUSYGROUP"`, ...), so callers branch on a field instead of matching a substring of the message. It is `undefined` when the text carries no code, which in practice means a Lua script returned a bare `redis.error_reply(...)`.
  - `command` names the command that drew the error, uppercased, wherever the throw site can attribute it: a single `send`, or a pipeline entry the adapter reports per command.
  - `cause` holds the adapter-native error (or, for the HTTP adapter, the raw payload string), so nothing the underlying client attached is lost.
  - `message` stays the server's text verbatim, code included, so message matching that predates this class keeps working.
  - Also exported: `redisErrorCode(message)` for classifying a raw message, and `redisServerError(source, command?)`, the normalizer the adapters use, which passes an already normalized error through unchanged.

  All four adapters agree, including their pipeline, `MULTI`, and `WATCH` paths. Client-side failures are deliberately left alone: a closed client, a dropped socket, an ioredis `MaxRetriesPerRequestError`, or an Upstash HTTP transport failure never came from Redis, so none of them is reported as a server error. The `MULTI` rejection unwrap on `benni/node` and `WATCH` abort detection on every adapter behave exactly as before, and cluster redirections are still followed by the cluster-aware client underneath.

- 223d2cf: One call shape everywhere, a client source that never needs a top-level `await`, and primitives that declare themselves like schemas.

  - **`benni()` takes a config object.** `benni({ client, schema })` is now the same call as `benni(client, { schema })`, and `client` accepts a connected client, a promise of one, a factory, or another Benni handle, so `benni({ client: node({ url }), schema })` binds without a top-level `await`. The trade is that a bad URL surfaces at the first command rather than at construction, which is the trade `benni/hono` and `benni/next` already made. A promise is adopted at bind time, because it is already connecting: its rejection is observed straight away rather than left to become an `unhandledRejection`, `close()` closes the client it opened even if no command was ever sent, and every command reports that same connect failure since a settled promise cannot be retried. A factory is not called until the first command, so nothing is opened, `close()` on an unused client opens nothing, and a failed connect really is retried on the next command.
  - **`Register` types the handle once.** Declare `interface Register { schema: typeof schema }` on the `benni` module and the bare `Benni` is the fully typed handle, so a helper signature reads `function handlers(redis: Benni)` instead of repeating `Benni<typeof schema>`. Without the augmentation nothing changes.
  - **Primitives are schema values.** `cache`, `ratelimit`, `queue`, `lock`, `semaphore`, `idempotency`, and `budget` are exported from `benni/schema` as builders that take a prefix and their options, so they sit in the schema module next to the data stores and are reached through `redis.query.<name>` with the same inference. Each carries its own store binding, so a bundle only pulls in the primitives the module declares.
  - **The primitive constructors take one options object.** `cache({ client, ttlMs })` alongside the existing `cache(client, { ttlMs })`, with `client` accepting the same sources as `benni()`, including the handle itself. This removes the "primitives take `client`, not `redis`" papercut.
  - **The primitive store types are nameable.** `CacheStore<T>`, `QueueStore<TPayload, TResult>`, `RatelimitStore`, `LockStore`, `SemaphoreStore`, `IdempotencyStore<T>`, and `BudgetStore` are exported, so a helper can be typed against a primitive without `ReturnType<typeof ...>`.

  Everything above is additive: every existing call shape still compiles and behaves identically. The one behavior change is that `benni()` now rejects a client source that is neither a client, a promise, a factory, nor a handle, where it previously accepted it and failed at the first command.

- ec45c41: Add per-entity Pub/Sub channels. `redis.pubsub.channel(schema, id)` now addresses `name:<id>`, derived by the same key builder every keyspace uses, so publishing and subscribing to one channel per room, per user, or per job no longer means minting a schema per call or dropping to `redis.raw`. Without an id the resource still addresses the schema's own name, exactly as before.

  - `channel(name, codec, { ids })` narrows the id type the way a keyspace's `ids` option does.
  - `schema.channelName(id)` and `resource.channelName(id)` resolve the concrete channel, so the string is never hand-built.
  - `resource.at(id)` scopes a resource reached through `redis.query`, and is what the second argument calls underneath.
  - Id-scoped publishes are matched by a `pattern()` subscription over the prefix, because both sides derive the name the same way.

- 7d488c0: `semaphore().run()` now renews its lease while the critical section runs, and reports a lost slot instead of hiding it.

  Before, `run()` acquired with `leaseMs` and never renewed, so a body that outlived the lease silently lost its slot: the lease lapsed, the next acquire pruned it and admitted another caller, and the original body kept running as if it were still inside the limit. That is over-admission, the one thing a semaphore exists to prevent, and a `limit: 20` guarding a provider quota would quietly run 21 in flight. It now renews on an interval, matching `lock().run()` and the `queue` primitive's job leases.

  - `run()` renews the lease every `heartbeatMs`, which defaults to a quarter of the effective `leaseMs` (15s at the default `leaseMs` of 60000, the same ratio the queue uses). Pass `heartbeatMs: false` to opt out and keep the previous behaviour.
  - New `SemaphoreLeaseLostError`, carrying `key` and `limit`. If renewal finds the slot gone, `run()` rejects with it even when the body resolved, because a body that finished without a slot did not finish under the bound it was written against.
  - `SemaphoreHandle` gained a `signal` (an `AbortSignal`) that aborts with that error, so a body can pass it to `fetch` or the AI SDK and stop as soon as the pool stops accounting for it. A manual `extend()` that resolves `false` aborts it too.
  - New `onRenewError` hook on `run()`, called when a renewal round trip fails. A failed round trip is not treated as a lost slot: the next tick retries, and the lease is only declared lost once Redis reports the slot is no longer ours, or a full `leaseMs` has passed with no successful renewal.
  - New `SemaphoreRunOptions` type, the third argument to `run()`, widening `SemaphoreAcquireOptions`.
  - Renewal options are validated before a slot is taken, so a bad `heartbeatMs` cannot hold a slot until its lease lapses. The renewal timer is unref'd and always cleared, so it can never keep a process alive.

  The fail-fast default is unchanged: `retries` still defaults to `0`, so a full pool makes `acquire()` resolve `null` and `run()` throw `SemaphoreNotAcquiredError` rather than waiting. That contract is now spelled out on `semaphore()`, `acquire()`, `run()`, and `retries`, along with the `retries` / `retryDelayMs` pair to pass when the intent is to queue callers instead.

  `acquire()`, `count()`, `SemaphoreHandle.release()`, `SemaphoreHandle.extend()`, and `SemaphoreNotAcquiredError` are otherwise unchanged.

  The lock and semaphore documentation pages are rewritten around the shared lease model, and both now state that an `acquire()`d handle is never renewed in the background.

### Patch Changes

- 8ee4705: Docs: a reference page for the whole error surface, and one recommended `json` form across every entry point.

  - New [Errors](https://chichurita.github.io/benni/api/errors/) page under API. It documents every public error class (`ValidationError`, `ReplyShapeError`, `PartialRecordError`, `RedisServerError`, `SessionClosedError`, `WatchRetriesExceededError`, `CrossSlotError`, and the lock, semaphore, queue, idempotency, and budget errors), what throws each one, the structured properties it carries, and how to tell it apart from its siblings. It opens with a "which error should I catch" table and shows a `catch` branching on `RedisServerError.code`. Also covered: `redisErrorCode`, `redisServerError`, and the deliberate exclusion of connection and transport failures from `RedisServerError`.
  - The docs quick start now leads with `json(validator)`, matching the README and `llms.txt`. `json<T>()` is shown right after it, labelled as the unchecked escape hatch. Before, the quick start led with the cast and the README led with the validator, so the two disagreed about the recommended default.
  - The README philosophy section and `llms.txt` both mention `RedisServerError`, including a rule telling coding agents to branch on `.code` rather than match against message text.

- 0e9747c: Document that a failed `MULTI`/`EXEC` over REST may not carry a Redis error, and stop the shared client contract from asserting otherwise.

  The integration job had been red since the 5xx boundary landed, and the failure was a real finding rather than a flake. Over REST a service sits in front of Redis and decides what a failed transaction looks like on the wire. Reproduced against `hiett/serverless-redis-http:latest` in front of `redis:8`, the endpoint CI runs:

  ```text
  POST /pipeline    [["PING"],["ZADD","str","1","member"]]
    -> 200  [{"result":"PONG"},{"error":"WRONGTYPE Operation against a key..."}]

  POST /multi-exec  [["PING"],["ZADD","str","1","member"]]
    -> 500  (no body)
  ```

  There is no reply to normalize, so `redis.multi().exec()` rejects with a transport `Error`. That is `benni/upstash` behaving as intended: inventing a `.code` from a gateway's status line would hand the caller a `RedisServerError` for what might equally be an upstream outage. The shared contract was asserting a guarantee the transport cannot make.

  - `expectRedisClientContract` takes `transactionErrorsCarryNoReply`, and the Upstash integration test sets it with the captured evidence. The assertion narrows rather than disappearing: a failed transaction must still reject on every adapter, and the TCP adapters keep the full `RedisServerError` plus `.code` assertion.
  - [Edge runtime](https://chichurita.github.io/benni/runtime/edge/) gains "A failed transaction may not carry a Redis error", with the wire traffic, what does and does not change (single commands and pipelines are unaffected), and the `catch` that is correct against both kinds of endpoint.
  - `llms.txt` no longer says error handling is uniform without qualification. A failed transaction always rejects; branch on `.code` only after confirming the error is a `RedisServerError`, which rule 8 already required.

- 668cdce: Reaching for a counter or string command on a kv store now gets an error that names the fix.

  `counter` and `string` are alternate views over a kv keyspace rather than kinds of their own, so `INCR` lives on `redis.counter(schema)` and `APPEND` on `redis.string(schema)`. Guessing `redis.query.views.incr("post-1")` first is common, and the old error answered it by printing every method the store does have, naming no fix:

  ```text
  Property 'incr' does not exist on type '{ set: { (id: RedisKeyPart, value: number,
  options: ConditionalSetOptions): Promise<boolean>; ... 10 more ...;
  persist(id: RedisKeyPart): Promise<...>; } & Pick<...>'.
  ```

  The store now carries a type-only member per absent command whose parameter type is the fix, so the fix is the error text:

  ```text
  Argument of type '"post-1"' is not assignable to parameter of type
  '"INCR is a counter command: use redis.counter(schema).incr(id)"'.
  ```

  Covered: `incr`, `incrby`, `incrbyfloat`, `decr`, `decrby`, `append`, `getrange`, `setrange`, `strlen`. Nothing is added at runtime, so calling one from untyped JavaScript fails the way an absent method already failed, and `Object.keys` on a kv store is unchanged.

  Also documented, all three found in the same DX pass:

  - `ReplyShapeError` carries the value Redis returned on **`.reply`**, not `.value`. The API reference already said so; the README philosophy bullet and `llms.txt` did not, which is where someone looks mid-incident.
  - `examples.md` now shows reading a field off an `xrange` entry. An entry is `{ id, value }` and `value` is a `Partial` of the declared fields, because a stream entry can legally carry any subset of them.
  - [Philosophy](https://chichurita.github.io/benni/getting-started/philosophy/) and `llms.txt` now state how arguments map to commands, so the shape of a method you have not called yet is predictable: one fixed form takes positional arguments in the command's own order (`zremrangebyscore(id, min, max)`), while modifiers or several forms take a single options object (`zrange(id, { start, stop, rev })`). `zrange` keeps its bounds in the object because they are indexes, scores, or lex bounds depending on the modifier beside them.

- 9b13a00: A client passed as a promise or a factory now reports the same capabilities as the same client passed connected, and `close()` on one is terminal.

  Both defects were in the lazy facade `resolveClient` builds when the client source is not a client yet, and both reduce to the same rule: the facade has to be indistinguishable from the client it will resolve to.

  - **The facade claimed optional capabilities it might not have.** `RedisClient` has required `send`/`pipeline`/`close` and optional `transaction`/`session`/`subscriber`, and callers feature-detect the optional ones by presence. The facade cannot know at bind time what it will resolve to, so it defines all three, which meant a presence check passed for a client that could not actually do the thing. A caller with a legitimate fallback then took the wrong branch: `hset(id, value, { ttlSeconds })` wants `HSET` plus `EXPIRE` atomic and settles for a pipeline when the client has no MULTI, but `client.transaction?.(...) ?? client.pipeline(...)` found the facade's method and the call threw `Redis client does not support transactions` instead. The same custom client behaved differently depending on whether it was handed to `benni()` connected or as a promise.

    The unsupported case is now distinguishable rather than merely thrown: a new **`UnsupportedCapabilityError`** carries `capability` (`"transaction"`, `"session"`, or `"subscriber"`), and `hset` recognizes exactly that error to take its pipeline fallback. It extends `TypeError` and keeps the connected-client guards' message strings verbatim, so `instanceof TypeError`, existing `catch` blocks, and message matching all keep working unchanged.

    The fix deliberately does **not** make the facade fall back to a pipeline on its own. `redis.multi()` exists for MULTI/EXEC atomicity, so on a client without `transaction` it still throws rather than silently degrading; only a call site that is correct without the atomicity opts into the fallback. Exported from `benni` and `benni/core`, and documented on the [Errors](https://chichurita.github.io/benni/api/errors/) page.

  - **`close()` was not terminal for an unused factory.** It peeks rather than resolves, so closing a client that was never used opens nothing, which is correct and stays. But it recorded nothing, so a command landing after `close()` still called the factory and opened a connection after shutdown. That differs from the adapters, whose `close()` is final, and in Node a live socket pins the event loop, so a request racing shutdown could turn a graceful exit into a hang. `close()` is now idempotent and terminal: later operations reject with `Redis client is closed`, an unused factory is still never invoked, the underlying client is closed exactly once, a second `close()` awaits the first one's teardown rather than resolving early, and a source whose resolution already failed still does not make `close()` throw.

  Found by an independent review, with both defects reproduced before the fix and kept as regression tests.

- 549d19b: `lock` and `semaphore` no longer report a successful `run()` when the lease was lost, and a throwing `onRenewError` can no longer take the process down.

  Lease renewal shipped with its completion check reading one flag, `lease.lost`, which is set only from inside the renewal interval. That left the interval as the single witness to a lost lease, and an interval is not guaranteed to run. A body that blocks the event loop past its TTL (synchronous CPU work, a blocking native call) starves it completely, and because a timer is a macrotask while resuming from `await fn(handle)` is a microtask, the completion check won that race and resolved as though the critical section had been exclusive throughout. A 600ms synchronous body under `ttlMs: 200` returned its value with the lock key already gone from Redis. For the semaphore the same path is real over-admission: the member is pruned by the next `acquire` and another caller is let in on top of a body still running.

  Completion now consults the deadline in the same turn the body finished, and the result of the final `release` (which runs the same token check `extend` does, so a `false` reply is Redis saying the lease had already moved on), alongside the flag. A lease given up deliberately inside the body is still not a loss, and `heartbeatMs: false` still means the documented opt-out rather than a failure. A healthy body that outlives many TTLs while renewals keep succeeding still resolves normally.

  Three smaller fixes in the same area:

  - **A throwing `onRenewError` is contained.** The hook ran inside the rejection handler of a promise the tick discards, so a throw from it became an unobserved rejection, which is fatal in default Node. A telemetry callback must not be able to kill the process. The hook is also no longer called for a renewal that settles after `run()` has already returned.
  - **The interval is torn down when the lease is lost**, instead of staying armed and early-returning on every future tick. It was `unref()`ed so it never held Node open, but a body that ignores the abort signal and never settles used to leave it spinning.
  - **An explicitly passed `heartbeatMs` must be at most half the lease.** Above that the first tick can land at or after expiry, so the lease lapses before renewal is ever attempted, on an uncontended lock. The old behavior was silent and load dependent: it passed for a body that finished before the first tick and failed for a slower one. The derived default is unchanged and still applies to a lease too small for any ratio to hold.

  Found by an independent review of the renewal work, with each case reproduced before the fix and kept as a regression test.

- 294920b: Docs: replace the positioning numbers with measured ones, advertise the compile-time cost, and state the `redis.watch()` write-side gap plainly.

  A second DX evaluation built three apps twice, once through Benni and once through raw `node-redis` v6, and two claims in the public copy did not survive it.

  - **"Expect the same amount of code, not less" was only true for CRUD.** The app behind that line had no primitive in it. Measured across three apps: a URL shortener 97 lines against 171, an AI generation service on `queue` 45 against 437, a realtime presence and payout service 103 against 197. README, `llms.txt`, and [Why Benni?](https://chichurita.github.io/benni/getting-started/why-benni/) now claim the same code for typed CRUD and much less once a primitive replaces hand-written Lua, with the caveat that the raw column hand-rolls what a team would otherwise install.
  - **"Raw `node-redis` caught 0 of 9 planted bugs" was measured against a straw man.** A raw version with an ordinary hand-written typed edge catches 4 of 9. The public figure is now 9 against 4, and it names the five misses, all of which are silent: a typo adds a second field instead of replacing one, a date string in a `number()` slot reads back `NaN`, a partial write leaves a partial record, and an undeclared field reads `undefined`. Only the wrong store kind throws.
  - **The compile-time cost profile is now published**, because "a schema layer will slow my editor down" is the first objection raised. The same three apps cost 13,774 type instantiations through Benni against 198,061 through raw `node-redis`, whose own command generics are the expensive part. Runtime overhead measured 3 to 6 percent on sequential ops, inside the run-to-run spread.
  - **[Optimistic Transactions](https://chichurita.github.io/benni/advanced/optimistic-transactions/) gains "The Write Side Is Not Typed The Way Stores Are"**, and `llms.txt` gains a matching rule 10. Reads inside a watched transaction go through typed stores, writes are hand-built command arrays, and nothing checks the command against the schema's kind, the decoder against the command, or arity and option order. The page now says so where balance-changing code will read it, names `schema.key(id)` and `schema.encode(value)` as the discipline that keeps it honest, and points at a typed `script()` for check-and-set logic that deserves a real guarantee.

- eabe013: `benni/upstash` no longer reports an HTTP 5xx as a Redis error reply when the response carries an `{ "error": ... }` body.

  Upstash uses that envelope for two different things: a genuine Redis error (200 for a pipeline element, 4xx for a single command), and a plain service failure from whatever sits in front of Redis. The adapter only checked whether the envelope was present, not the status, so a `502` with `{ "error": "upstream unavailable" }` came back as a `RedisServerError` attributed to the command, with a `code` parsed out of the gateway's own prose. That contradicted the boundary the error reference documents: `RedisServerError` means the command reached Redis and Redis refused it.

  A 5xx is now a plain `Error` again, the way a non-JSON response and a dropped socket already were, and its message keeps the body's text so the failure stays debuggable (`Upstash HTTP 502: upstream unavailable`). A 4xx carrying the same envelope is still a real server reply and still normalizes to `RedisServerError` with its code, so `NOSCRIPT` handling and the script reload path are unaffected.

  Found by an independent review of the normalization work. The misclassification predates that change; what was new was documenting a boundary the code did not hold to.

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

  | app                               | before  | after   |
  | --------------------------------- | ------- | ------- |
  | `benni` + kv only                 | 13.9 kB | 4.2 kB  |
  | `benni/upstash` + one hash schema | 15.2 kB | 7.0 kB  |
  | three kinds (hash + zset + list)  | 15.2 kB | 10.2 kB |

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

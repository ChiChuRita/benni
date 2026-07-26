---
title: "Connection Sessions"
description: "Use redis.session() to lease a dedicated connection for blocking commands and WATCH transactions."
---

Use `redis.session()` to lease a dedicated connection from the client. A session is shaped like the Beni handle — the same store accessors, bound to a private connection — plus the operations that are only safe when one caller owns the connection: blocking pops, blocking stream reads, and `WATCH`.

## Why A Session Is A Dedicated Connection

Two Redis workloads monopolize a connection. A blocking command (`BLPOP`, `BLMOVE`, `BZPOPMIN`, `XREAD`/`XREADGROUP` with `BLOCK`) parks the connection until an entry arrives or the timeout elapses; nothing else can use it meanwhile. A `WATCH`/`MULTI`/`EXEC` transaction arms optimistic-locking state that belongs to one connection and must not be interleaved with unrelated traffic.

Running either on the shared client would stall every other query. So Beni puts them behind a session: one session is one connection is one logical worker. There is no pooling — an app that needs N workers blocked at once opens N sessions, and the connection cost is explicit (the shared client is one connection; each live session is exactly one more).

Because the blocking and `WATCH` methods live only on the session-flavored accessors, calling them on the shared client is a compile error, not a runtime surprise:

```ts
await using session = await redis.session();
session.list(jobs).blpop("pending", { timeoutSeconds: 5 }); // exists
redis.list(jobs).blpop("pending", { timeoutSeconds: 5 });       // compile error
```

## Open A Session

`redis.session()` has two forms. The scoped form takes a callback and closes the session for you when the callback settles — the recommended shape for a bounded unit of work:

```ts
const job = await redis.session(async (s) => {
  return s.list(jobs).blpop("pending", { timeoutSeconds: 5 });
});
```

The bare form returns the session and hands you the `close()` obligation. Pair it with `await using` (TypeScript 5.2 explicit resource management) so it closes at the end of the scope even on an early return or a throw:

```ts
await using session = await redis.session();
const job = await session.list(jobs).blpop("pending", { timeoutSeconds: 5 });
// session.close() runs automatically when the block exits
```

Without `await using`, you own `close()` and must call it in a `finally`:

```ts
const session = await redis.session();
try {
  const job = await session.list(jobs).blpop("pending", { timeoutSeconds: 5 });
} finally {
  await session.close();
}
```

## What A Session Exposes

A session carries every data-store accessor from the Beni handle (`kv`, `hash`, `list`, `set`, `zset`, `stream`, `counter`, `string`, `bitmap`, `geo`, `hll`), each bound to the private connection. The `list`, `zset`, and `stream` accessors are supersets that also expose their blocking variants (see [Blocking Operations](/beni/advanced/blocking-operations/)) and the blocking consumer-group read (see [Consumer Groups](/beni/data-structures/consumer-groups/)).

On top of the stores, a session adds the `WATCH` primitives:

```ts
session.watch(keys);   // WATCH k1 k2…; throws on an empty list
session.unwatch();     // UNWATCH
session.multi();        // abort-aware transaction builder — exec() resolves the tuple or null
```

See [Optimistic Transactions](/beni/advanced/optimistic-transactions/) for the retrying `redis.watch(...)` helper built on these.

`scan`, `pubsub`, and `script` are intentionally absent from a session — they have no session-specific semantics, and the smaller surface keeps a session's purpose legible: block, or watch-then-commit. For raw commands there is `session.raw`, the underlying adapter session.

## Closed And Close Semantics

`close()` tears the connection down immediately. It rejects any in-flight command — it does **not** wait out a server-side blocking timeout — so a session parked on `{ timeoutSeconds: "forever" }` still exits promptly when you close it. `close()` is idempotent, and `[Symbol.asyncDispose]` is an alias of it, which is what makes `await using` safe.

There is no reconnection. A session that you close, or one whose connection drops, is dead: every in-flight and subsequent call rejects. Recovery is a new session, never a reconnect — a silent reconnect would drop `WATCH` state and blocked reads and turn visible failures into correctness bugs.

Check `session.closed` to tell a shutdown or dropped connection apart from an application error inside a worker loop:

```ts
try {
  const job = await session.list(jobs).blmove(
    "pending", "processing", "left", "right", { timeoutSeconds: 5 }
  );
} catch (error) {
  if (session.closed) return; // shutdown or dropped connection — stop the loop
  throw error;                 // a real error — surface it
}
```

A boolean check is robust where cross-adapter error-class mapping is fragile; a use-after-close otherwise rejects with `SessionClosedError`.

## Requirements

The bound client must implement the optional `session` method of the `RedisClient` interface (the Node and Bun adapters do). Otherwise `redis.session()` throws `TypeError: Redis client does not support sessions`, the same style as the `transaction` guard. Deno uses the Node adapter through npm compatibility, so session support follows the Node adapter there. The edge HTTP adapter omits sessions because it has no persistent connection.

Sessions pin real connections. Prefer the scoped callback form or `await using` so a session cannot outlive its work; as a backstop the parent client tracks live sessions and force-closes any survivors when you close the client.

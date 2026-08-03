---
"benni": patch
---

A client passed as a promise or a factory now reports the same capabilities as the same client passed connected, and `close()` on one is terminal.

Both defects were in the lazy facade `resolveClient` builds when the client source is not a client yet, and both reduce to the same rule: the facade has to be indistinguishable from the client it will resolve to.

- **The facade claimed optional capabilities it might not have.** `RedisClient` has required `send`/`pipeline`/`close` and optional `transaction`/`session`/`subscriber`, and callers feature-detect the optional ones by presence. The facade cannot know at bind time what it will resolve to, so it defines all three, which meant a presence check passed for a client that could not actually do the thing. A caller with a legitimate fallback then took the wrong branch: `hset(id, value, { ttlSeconds })` wants `HSET` plus `EXPIRE` atomic and settles for a pipeline when the client has no MULTI, but `client.transaction?.(...) ?? client.pipeline(...)` found the facade's method and the call threw `Redis client does not support transactions` instead. The same custom client behaved differently depending on whether it was handed to `benni()` connected or as a promise.

  The unsupported case is now distinguishable rather than merely thrown: a new **`UnsupportedCapabilityError`** carries `capability` (`"transaction"`, `"session"`, or `"subscriber"`), and `hset` recognizes exactly that error to take its pipeline fallback. It extends `TypeError` and keeps the connected-client guards' message strings verbatim, so `instanceof TypeError`, existing `catch` blocks, and message matching all keep working unchanged.

  The fix deliberately does **not** make the facade fall back to a pipeline on its own. `redis.multi()` exists for MULTI/EXEC atomicity, so on a client without `transaction` it still throws rather than silently degrading; only a call site that is correct without the atomicity opts into the fallback. Exported from `benni` and `benni/core`, and documented on the [Errors](https://chichurita.github.io/benni/api/errors/) page.

- **`close()` was not terminal for an unused factory.** It peeks rather than resolves, so closing a client that was never used opens nothing, which is correct and stays. But it recorded nothing, so a command landing after `close()` still called the factory and opened a connection after shutdown. That differs from the adapters, whose `close()` is final, and in Node a live socket pins the event loop, so a request racing shutdown could turn a graceful exit into a hang. `close()` is now idempotent and terminal: later operations reject with `Redis client is closed`, an unused factory is still never invoked, the underlying client is closed exactly once, a second `close()` awaits the first one's teardown rather than resolving early, and a source whose resolution already failed still does not make `close()` throw.

Found by an independent review, with both defects reproduced before the fix and kept as regression tests.

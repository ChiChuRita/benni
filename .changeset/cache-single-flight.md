---
"benni": minor
---

**Breaking:** `cache().get()` throws `CacheWaitTimeoutError` when another caller's load outlasts its wait, instead of loading for itself; a slow load keeps its fill lock; and `set()` or `del()` during a load can no longer be overwritten by it.

Before, a waiter that outlasted three lock lifetimes loaded for itself and wrote with `SET NX`. Every waiter hit that deadline at once, so a slow backend got one extra load per waiter, and the unfenced write lost any `del()` issued meanwhile:

```ts
// Before: after 3 × lockTtlMs, every waiter ran the loader.
const profile = await profiles.get(id, () => db.loadProfile(id));

// After: waiters wait up to waitTimeoutMs (default 3 × lockTtlMs), then throw.
try {
  return await profiles.get(id, () => db.loadProfile(id));
} catch (error) {
  if (error instanceof CacheWaitTimeoutError) return serviceUnavailable();
  throw error;
}
```

- **The fill lock is renewed** every quarter of `lockTtlMs` while the loader runs. A 40-second load under the 10-second default used to lose its lock at 10 seconds and hand the load to a new caller every 10 seconds; it is now one load. `lockTtlMs` is how long a *dead* loader holds waiters up, no longer a bound on the load.
- **`set()` breaks an in-flight fill**, as `del()` already did. `set()` wrote without touching the fill lock, so a slower loader holding an older value published over it and served it for `ttlMs`. The fill token now works as the entry's generation: a loader publishes, atomically in Lua, only if no `set()`, `del()`, or lease lapse replaced it since the load began.
- **Polling costs one round trip**, not two GETs, and backs off from `pollMs` to eight times it, with jitter. The poll is a script that returns the entry if present and takes the lock if free, so a waiter still takes over a dead or failed loader's lock the moment it frees. The first miss also drops a round trip: the double-check and the lock are one script.
- **New option `waitTimeoutMs`** sets the wait budget (default three times `lockTtlMs`, the old hard deadline).
- **Ids are validated.** An empty id, one starting with `}`, or a prefix with a lone `{` split the entry and its fill lock across Cluster slots and failed every script for that id with `CROSSSLOT`, on a cluster only. They now throw `ValidationError` up front. The docs also show the real key layout, `<prefix>:{<id>}` and `<prefix>:lock:{<id>}`; they had dropped the braces.

The cache now needs only `GET` and `EVALSHA`.

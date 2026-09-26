---
title: "Cache"
description: "A read-through cache with stampede protection: one loader call per miss, no matter how many concurrent readers."
---

`cache` is a read-through cache with **stampede protection**: on a miss, exactly one caller runs the loader, under a fill lock it renews for as long as the load takes; every other concurrent reader waits for the filled value instead of hammering your backend.

```ts
// schema.ts
import { cache, json } from "benni/schema";
import { z } from "zod";

const profile = z.object({ name: z.string(), score: z.number() });

export const profiles = cache("profile", { ttlMs: 60_000, codec: json(profile) });
```

```ts
// app.ts
const profile = await redis.query.profiles.get(userId, () => db.loadProfile(userId));
```

The classic failure this prevents: a hot key expires, 500 requests miss at once, and all 500 hit the database together. With `cache`, one of them loads; the other 499 poll Redis for the filled entry, backing off as they wait.

Declared as a schema value it lands in [`redis.query`](/benni/core-concepts/schema-registry/) and needs no client of its own. Where you hold a client but no handle, `benni/primitives` exports the same cache in its client-taking form, over the same keys:

```ts
import { cache } from "benni/primitives";

const profiles = cache<Profile>({ client, ttlMs: 60_000 });
const profile = await profiles.get(userId, () => db.loadProfile(userId));
```

`client` accepts a `RedisClient`, a promise of one, a factory, or a Benni handle, so it works over every adapter, including [`benni/upstash`](/benni/runtime/edge/) on the edge: it needs only `GET` and `EVALSHA`.

## API

```ts
const store = cache<T>({ client, ...options });

await store.get(id, loader); // read; run loader once on a miss
await store.peek(id);        // read without loading (T | null)
await store.set(id, value);  // write directly (with the configured TTL); breaks any fill in flight
await store.del(id);         // drop; returns the deleted count; breaks any fill in flight
```

Values are encoded with `codecs.json<T>()` by default; pass `codec` to store anything else.

## Writes and invalidations beat an in-flight load

A loader publishes its result **only if nothing wrote or deleted the entry since it started**. The fill lock's token is the entry's generation: `set()` and `del()` both delete it in the same atomic step as their own write, and the loader's publish is a Lua script that writes only while its token is still the current one. So the canonical write-through orders are safe:

```ts
await db.updateProfile(userId, patch);
await profiles.del(userId);          // invalidate: any load in flight is dropped

await profiles.set(userId, profile); // or write through: same, the fresher value wins
```

A loader that read its value before the `set` or `del` finds its token gone, so it returns that value to its own caller but does not cache it. The same fence stops a loader whose lock lapsed (see below) from overwriting a fresher entry published after it.

## Slow loads, dead loaders, and the wait deadline

**A slow load keeps its lock.** The fill lock is renewed every quarter of `lockTtlMs` while the loader runs, so a 40-second query under the 10-second default is still one query: waiters keep waiting for it, and nobody else starts a load.

**A loader that throws frees the lock at once**, and **a loader whose process dies** stops renewing, so its lock lapses within `lockTtlMs`. Either way the next waiter to poll takes the lock over in the same round trip and loads in its place, while the others keep waiting on the new holder. One failure costs one reload, not a stampede.

**A waiter gives up after `waitTimeoutMs`** (three lock lifetimes by default) with `CacheWaitTimeoutError`, rather than loading for itself. A live loader still running at that point means the backend is slower than the budget you gave it; every waiter adding a load of its own is precisely the stampede this exists to prevent. Serve a 503 or a fallback, or raise `waitTimeoutMs` if loads legitimately take that long:

```ts
import { CacheWaitTimeoutError } from "benni/primitives";

try {
  return await profiles.get(userId, () => db.loadProfile(userId));
} catch (error) {
  if (error instanceof CacheWaitTimeoutError) {
    return new Response("Profile service is slow, try again", { status: 503 });
  }
  throw error;
}
```

**Polling is one round trip per poll**, starting at `pollMs` and doubling, with jitter, up to eight times it. Each poll is a small script that returns the entry if it is there and takes the lock if it is free, so a waiter needs no second read to notice a dead holder. (Pub/Sub would save the polls, but not over the REST adapter, and the backoff already makes a long wait cheap.)

What single-flight does not cover: a loader whose lock lapses because its process stalled or lost Redis for a whole `lockTtlMs` keeps running while a waiter starts a second load. The fence keeps the stale result out of the cache, but the backend sees both loads.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `ttlMs` | - | Entry lifetime. |
| `prefix` | `"cache"` | Key namespace; entries live at `<prefix>:{<id>}`, fill locks at `<prefix>:lock:{<id>}`. |
| `codec` | `codecs.json<T>()` | Value codec. |
| `lockTtlMs` | `10000` | The fill lock's lease. Renewed while the loader runs, so it does not bound the load; it is how long a dead loader holds waiters up before one takes over. |
| `waitTimeoutMs` | `lockTtlMs * 3` | How long a `get` waits on another caller's load before throwing `CacheWaitTimeoutError`. |
| `pollMs` | `50` | First poll interval while waiting; doubles, jittered, up to eight times this. |

## Keys and Redis Cluster

The braces are a Redis Cluster hash tag: an entry and its own fill lock share the id's slot, so the scripts that touch both are legal on a cluster, while different ids still spread across the keyspace. An id that would break that is rejected with a `ValidationError` before anything is sent: an empty id, or one starting with `}`, yields an empty tag that Redis ignores, which would put the entry and its lock in different slots and fail every script for that id with `CROSSSLOT`. So does a `prefix` containing a lone `{`. See [Redis Cluster](/benni/advanced/cluster/).

See [Caching patterns](/benni/patterns/caching/) for the underlying Redis approach if you want to roll your own.

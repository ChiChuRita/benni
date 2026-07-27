---
title: "Cache"
description: "A read-through cache with stampede protection: one loader call per miss, no matter how many concurrent readers."
---

`cache` is a read-through cache with **stampede protection**: on a miss, exactly one caller runs the loader (single-flight via the [distributed lock](/beni/primitives/lock/)); every other concurrent reader waits for the filled value instead of hammering your backend.

```ts
import { cache } from "beni/primitives";

const profiles = cache<Profile>(client, { ttlMs: 60_000 });

const profile = await profiles.get(userId, () => db.loadProfile(userId));
```

The classic failure this prevents: a hot key expires, 500 requests miss at once, and all 500 hit the database together. With `cache`, one of them loads; the other 499 poll Redis for the filled entry.

`cache` takes any `RedisClient`, so it works over every adapter, including [`beni/upstash`](/beni/runtime/edge/) on the edge.

## API

```ts
const store = cache<T>(client, options);

await store.get(id, loader); // read; run loader once on a miss
await store.peek(id);        // read without loading (T | null)
await store.set(id, value);  // write directly (with the configured TTL)
await store.del(id);         // drop; returns the deleted count; the next get reloads
```

Values are encoded with `codecs.json<T>()` by default; pass `codec` to store anything else.

## Failure behavior (fail open, never deadlock)

If the caller holding the fill lock dies mid-load, its lock expires after `lockTtlMs` and waiting readers **load for themselves**. The worst case under failure is a brief duplicate load, never an error, never a deadlock.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `ttlMs` | - | Entry lifetime. |
| `prefix` | `"cache"` | Key namespace; entries live at `<prefix>:<id>`, fill locks at `<prefix>:lock:<id>`. |
| `codec` | `codecs.json<T>()` | Value codec. |
| `lockTtlMs` | `10000` | How long one loader may hold the fill lock before waiters fail open. Set above your slowest load. |
| `pollMs` | `50` | Poll interval while waiting on another caller's load. |

See [Caching patterns](/beni/patterns/caching/) for the underlying Redis approach if you want to roll your own.

---
title: "Distributed Lock"
description: "A correct distributed lock over Redis — acquire with SET NX PX, release atomically so you never free someone else's lock."
---

`lock` is a distributed lock built the correct way: acquire with `SET key token NX PX ttl`, and release with an atomic check-and-delete Lua so a caller can **never** delete a lock that already expired and was re-acquired by someone else — the classic footgun of a naive `DEL`.

```ts
import { lock } from "beni/primitives";

const locks = lock(client, { ttlMs: 10_000 });

await locks.run("order:42", async () => {
  // critical section — the lock is released automatically, even if this throws
});
```

When the lock cannot be acquired, `run()` throws a typed `LockNotAcquiredError` (exported from `beni/primitives`), which carries the contested `.key`:

```ts
import { LockNotAcquiredError } from "beni/primitives";

try {
  await locks.run("order:42", processOrder);
} catch (error) {
  if (error instanceof LockNotAcquiredError) {
    // someone else holds error.key — back off or reschedule
  } else {
    throw error;
  }
}
```

`lock` takes any `RedisClient`, so it works over every adapter — including [`beni/upstash`](/beni/runtime/edge/) on the edge (it needs only `SET` and `EVALSHA`, no persistent connection).

## Acquire and release manually

```ts
const handle = await locks.acquire("order:42");
if (handle) {
  try {
    // ... work ...
  } finally {
    await handle.release(); // resolves true only if we still held it
  }
}
```

`acquire` resolves `null` when the lock is already held. `release()` and `extend()` resolve `true` only when your token still owns the key — both run the atomic Lua, so they are safe under expiry races.

## Retry a contended lock

```ts
const handle = await locks.acquire("order:42", {
  retries: 5,
  retryDelayMs: 100
});
```

By default `acquire` fails fast (`retries: 0`). Pass `retries` / `retryDelayMs` to wait for a busy lock.

## Extend a long task

```ts
const handle = await locks.acquire("report", { ttlMs: 30_000 });
// ... halfway through a long job, keep the lock alive:
await handle?.extend(30_000);
```

## Options

| Option | Where | Default | Meaning |
| --- | --- | --- | --- |
| `prefix` | `lock(client, …)` | `"lock"` | Key namespace; keys are `<prefix>:<id>`. |
| `ttlMs` | `lock` / `acquire` | `30000` | Lock lifetime. Set it above your worst-case critical section, and `extend()` if you might exceed it. |
| `retries` | `acquire` | `0` | Attempts when the lock is held. |
| `retryDelayMs` | `acquire` | `100` | Delay between retries. |

The TTL is the safety net: if your process dies mid-section, the lock expires instead of deadlocking. There is no lock renewal watchdog — pick a TTL comfortably larger than the work, or `extend()` explicitly.

---
"benni": minor
---

**Breaking:** primitives are declared in `benni/schema` and reached through the handle; `benni/primitives` is gone, and every error class is exported from `benni`.

The client-taking forms `lock(client, …)`, `cache({ client, … })`, `ratelimit`, `queue`, `semaphore`, `idempotency`, and `budget` are removed, and with them the `benni/primitives` subpath. Their names collided with the `benni/schema` builders of the same name, and they were a second way to build what `redis.query` already returns. Declare the primitive in the schema module; code that holds a client but no schema module wraps it once with `benni({ client })` and uses `store()`.

```ts
// Before
import { lock } from "benni/primitives";
const locks = lock(client, { prefix: "order", ttlMs: 10_000 });

// After: in the schema module
export const orderLocks = lock("order", { ttlMs: 10_000 });
await redis.query.orderLocks.run("42", async () => { … });

// After: with a client and no schema module
import { benni } from "benni";
import { lock } from "benni/schema";
const locks = benni({ client }).store(lock("order", { ttlMs: 10_000 }));
```

The key prefix is the builder's first argument, so the schema builders no longer accept a `prefix` option (it used to be silently overwritten).

Every error class is now exported from the root `benni` entry: `LockNotAcquiredError`, `LockLeaseLostError`, `SemaphoreNotAcquiredError`, `SemaphoreLeaseLostError`, `CacheWaitTimeoutError`, the five `Idempotency*Error`s, `BudgetWindowRolledError`, and the queue's `JobNotFoundError`, `JobLeaseLostError`, `JobFailedError`, `JobCancelledError`, `WorkerStoppedError`, `TerminalJobError`, and `RetryJobError`. They live in a module that holds only the classes, so `import { LockNotAcquiredError } from "benni"` bundles to 146 bytes minified: the class, none of the lock's Lua or lease logic (it was 593 bytes from `benni/primitives`, and 32 kB for a bundler that ignores `sideEffects`). The types an app names in its own signatures (`LockStore`, `QueueStore`, `Job`, `JobContext`, `RatelimitResult`, `LockHandle`, …) moved to `benni` too, as type-only exports.

```ts
// Before
import { LockNotAcquiredError, type QueueStore } from "benni/primitives";

// After
import { LockNotAcquiredError, type QueueStore } from "benni";
```

The Hono and Next.js rate-limit middlewares take the limiter itself instead of a client and its options, so the limit lives in one place, the schema declaration, and the middleware no longer builds a second limiter behind the handle's back. The 0.1 options throw at startup with the new shape in the message.

```ts
// schema.ts
export const apiLimit = ratelimit("api", { limit: 100, windowMs: 60_000 });

// Before
app.use("*", rateLimitMiddleware({ client, limit: 100, windowMs: 60_000, key }));
const limiter = rateLimitMiddleware({ client, limit: 20, windowMs: 10_000, identify }); // benni/next

// After
app.use("*", rateLimitMiddleware({ limiter: redis.query.apiLimit, key }));
const limiter = rateLimitMiddleware({ limiter: redis.query.apiLimit, identify }); // benni/next
```

`cacheMiddleware`, `sessionMiddleware`, and `cacheHandler` are unchanged: they are integrations with their own storage layout, not primitives, and still take `client`.

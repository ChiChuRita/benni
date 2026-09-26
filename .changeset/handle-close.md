---
"benni": minor
---

The handle owns its lifecycle: `redis.close()` and `await using`.

`redis.close()` shuts down, in order, the Pub/Sub subscriptions, the queue workers started through the handle (each drains its in-flight jobs, as `worker.stop()` does), the sessions still open, and then the client. It closes only what the handle opened: a handle built over another handle (`benni({ client: otherHandle })`) leaves that handle's client open, and an ioredis client adopted with `ioredis(instance)` is never closed. It is idempotent, runs every stage even if one fails (then rejects with the first failure), and afterwards commands and leases reject instead of reopening a connection. `[Symbol.asyncDispose]` is the same call.

```ts
// Before: three calls, in an order you had to know, and a worker you had to track
await worker.stop();
await redis.pubsub.close();
await redis.raw.close();

// After
await redis.close();

// or, scoped
await using redis = benni({ client: node({ url }), schema });
```

`redis.raw` is now the handle's view of the client rather than the adapter object itself: same methods and capabilities, but it refuses commands once the handle is closed, and `redis.raw.close()` is `redis.close()`. Code that compared `redis.raw` to the client it passed in by identity no longer matches.

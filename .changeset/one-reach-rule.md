---
"benni": minor
---

**Breaking:** one way to reach a store. `redis.query.<name>` for the bound schema module, `redis.store(schema)` for anything else, and sessions get the same `query`.

The per-kind accessors are removed from the handle and from sessions: `redis.kv()`, `redis.hash()`, `redis.set()`, `redis.list()`, `redis.zset()`, `redis.stream()`, `redis.geo()`, `redis.hll()`, `redis.bitmap()`, and `redis.script()`, along with `redis.pubsub.channel()` and `redis.pubsub.pattern()`. Each duplicated what `redis.query` already returned, and `redis.set()` (the Redis Set) read as the SET command to everyone. For a schema declared outside the bound module, a library's own schema or a handle built without `schema`, `redis.store(schema)` returns the same resource `redis.query` would, primitives included. For a schema that is in the module it is the very object `redis.query` holds, so a primitive's per-instance state (the cache's single-flight, a queue's workers) is not split between two copies.

```ts
// Before
await redis.hash(users).hset("42", { name: "Ada" });
await redis.pubsub.channel(roomEvents, "42").publish({ text: "hi" });
await redis.script(rateLimit).run({ keys: { counter: "k" }, args: {} });

// After
await redis.query.users.hset("42", { name: "Ada" });
await redis.query.roomEvents.at("42").publish({ text: "hi" });
await redis.query.rateLimit.run({ keys: { counter: "k" }, args: {} });

// A schema that is not in the bound module
const flags = redis.store(kv("flag", boolean()));
```

Sessions have `s.query` and `s.store(schema)`, typed against the bound module, so the headline idiom works inside `redis.session()` and `redis.watch()` bodies. They hold the data stores only, bound to the session's connection, with the blocking commands on lists, sorted sets, and streams as before; primitives, channels, and scripts stay on the handle.

```ts
// Before
await redis.watch(users.key("42"), async (s) => {
  const user = await s.hash(users).hget("42");
  return s.multi().add(["HSET", users.key("42"), "score", 1], numberReply);
});
await redis.session((s) => s.list(jobs).blpop("q", { timeoutSeconds: 5 }));

// After
await redis.watch(users.key("42"), async (s) => {
  const user = await s.query.users.hget("42");
  return s.multi().add(["HSET", users.key("42"), "score", 1], numberReply);
});
await redis.session((s) => s.query.jobs.blpop("q", { timeoutSeconds: 5 }));
```

`redis.pubsub` keeps only `close()`. `BenniSession`, `BenniSessions`, and `BenniWatchOptions` take the schema type as a parameter (defaulting to the registered one), and `BenniPubSub` no longer takes the client type; `BenniPatterns` and `BenniNoPatterns` are gone, since pattern availability now shows in `redis.query` and `redis.store()` alone. `redis.scan.*` is unchanged.

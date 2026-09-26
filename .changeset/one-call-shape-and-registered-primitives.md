---
"benni": minor
---

Primitives that declare themselves like schemas, a handle type you register once, and one options object for the primitive constructors.

- **`Register` types the handle once.** Declare `interface Register { schema: typeof schema }` on the `benni` module and the bare `Benni` is the fully typed handle, so a helper signature reads `function handlers(redis: Benni)` instead of repeating `Benni<typeof schema>`. Without the augmentation nothing changes.
- **Primitives are schema values.** `cache`, `ratelimit`, `queue`, `lock`, `semaphore`, `idempotency`, and `budget` are exported from `benni/schema` as builders that take a prefix and their options, so they sit in the schema module next to the data stores and are reached through `redis.query.<name>` with the same inference. Each carries its own store binding, so a bundle only pulls in the primitives the module declares.
- **The primitive constructors take one options object.** `cache({ client, ttlMs })` alongside the existing `cache(client, { ttlMs })`, with `client` accepting an adapter's client or the `benni()` handle itself. This removes the "primitives take `client`, not `redis`" papercut.
- **The primitive store types are nameable.** `CacheStore<T>`, `QueueStore<TPayload, TResult>`, `RatelimitStore`, `LockStore`, `SemaphoreStore`, `IdempotencyStore<T>`, and `BudgetStore` are exported, so a helper can be typed against a primitive without `ReturnType<typeof ...>`.

Everything above is additive. The constructor itself changed shape separately; see the `benni({ client, schema })` entry.

---
"benni": minor
---

**Breaking:** `node()`, `ioredis()`, and `bun()` return their client synchronously and connect on the first command, and a connect that fails no longer poisons the client.

In 0.1 the TCP adapters returned a promise. Handing that promise to `benni()` unawaited meant the connection opened at import, and after one failed connect every command re-reported that same rejection for the life of the process, so a pod that booted while Redis was restarting stayed dead. Now nothing touches the network until a command needs it. A connect that fails rejects the commands waiting on it with `benni/<adapter> could not connect to Redis: <cause>` (the client's own error is the `cause`), and a later command dials again, backing off from 100 ms to 5 s. Once connected, a dropped connection reconnects under the client's own strategy exactly as before; sessions still fail fast and never reconnect. `upstash()` was already synchronous.

```ts
// Before
const client = await node({ url: process.env.REDIS_URL });
export const redis = benni(client, { schema });

// or, unawaited, which connected at import and could not recover
export const redis = benni({ client: node({ url }), schema });

// After: no await anywhere, nothing opened at import
export const redis = benni({ client: node({ url }), schema });

// To fail fast at startup, send one command:
await redis.raw.send(["PING"]);
```

Promise and factory client sources are gone from every entry point (`benni()`, the primitives, `benni/hono`, `benni/next`): pass the adapter's client or a Benni handle. A promise, a factory, or a raw node-redis or ioredis instance is refused with a message saying what to write instead (`ioredis(instance)` adopts an ioredis client; node-redis clients cannot be adopted, so let `node({ url })` create one). Adopting an ioredis instance stays synchronous and never closes it.

`ioredis()`'s up-front checks (`keyPrefix`, RESP3) now throw synchronously instead of rejecting:

```ts
// Before
await expect(ioredis({ url, keyPrefix: "app:" })).rejects.toThrow(/keyPrefix/);

// After
expect(() => ioredis({ url, keyPrefix: "app:" })).toThrow(/keyPrefix/);
```

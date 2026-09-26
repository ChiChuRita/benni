---
"benni": minor
---

Every adapter takes `onError`, and the TCP adapters take `onReconnect`, so connection failures are no longer invisible.

`benni/node` used to swallow every `error` event from node-redis, so a flapping connection left no trace. `onError` now receives every connection-level failure the adapter sees (a failed connect, a socket error, a drop; on `benni/upstash`, a request that failed in transit), while error replies from Redis still reject the command that drew them. `onReconnect` is called with `"client"` or `"subscriber"` when a connection that was ready comes back; since messages published while the subscriber was down are lost, it is the moment to refetch what a subscription keeps current. Both only observe, and a callback that throws is rethrown asynchronously rather than into the client.

```ts
const client = node({
  url,
  onError: (error) => logger.warn({ error }, "redis connection error"),
  onReconnect: (connection) => {
    if (connection === "subscriber") void refreshDashboard();
  }
});

// ioredis: in the options, or as the second argument when adopting
ioredis(existing, { onReconnect });
```

An adopted ioredis client gets an `error` listener only when you pass `onError`, as before: Benni does not absorb errors on a client it does not own.

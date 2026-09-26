---
"benni": minor
---

`benni/upstash` takes a `timeoutMs` and an `AbortSignal`.

A REST request that never answered used to hang until the runtime gave up. `timeoutMs` aborts any request, response body included, that has not finished in time, rejecting with a `DOMException` named `"TimeoutError"`; `signal` aborts every in-flight and later request on the client with the signal's reason, for example to tie a per-request client to the incoming request. The timeout also holds against a custom `fetch` that ignores its signal. Neither is on by default.

```ts
const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string,
  timeoutMs: 2_000,
  signal: request.signal
});
```

A timeout does not undo a command the server already received, so a write that times out may still have been applied.

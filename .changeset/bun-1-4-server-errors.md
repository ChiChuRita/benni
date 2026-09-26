---
"benni": patch
---

`benni/bun` normalizes server error replies to `RedisServerError` on Bun 1.4 again.

Bun 1.4.0 moved a Redis error reply onto its own code, `ERR_REDIS_SERVER_ERROR`; Bun 1.3 had reported it as `ERR_REDIS_INVALID_RESPONSE`. The adapter only knew the old code, so on Bun 1.4 a `WRONGTYPE` or `NOSCRIPT` reached the caller as Bun's own `RedisError`, `instanceof RedisServerError` was false, and code that branched on `error.code === "NOSCRIPT"` read Bun's `"ERR_REDIS_SERVER_ERROR"` instead.

The adapter now recognizes the code each Bun version uses for a server reply. On 1.4 and later, `ERR_REDIS_INVALID_RESPONSE` keeps its new meaning, a malformed reply, and passes through untouched as the protocol failure it is. Verified against Bun 1.3.14 and 1.4.2, which CI now runs as a matrix.

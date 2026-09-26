---
"benni": patch
---

Docs: the rate-limit examples compute `Retry-After` from `retryAfterMs`, not from `resetMs - Date.now()`.

`resetMs` is a timestamp on the Redis server's clock, so subtracting the local clock from it baked the skew between the two into every header, which is exactly the bug the `retryAfterMs` field exists to avoid. The `ratelimit` page and the AI-apps pattern now use `retryAfterMs`, and the documented `RatelimitResult` type includes it.

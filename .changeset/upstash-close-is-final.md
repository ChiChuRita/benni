---
"benni": patch
---

`close()` on a `benni/upstash` client is now final, like every other adapter's.

It was a no-op, so commands issued after `close()` kept succeeding, while the client contract and every TCP adapter treat a command after shutdown as the caller's bug and reject it. Requests already in flight still finish; anything issued afterwards rejects with `benni/upstash client is closed` without a request being sent, and a repeated `close()` is harmless.

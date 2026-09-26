---
"benni": patch
---

`hincrby` now throws a `ReplyShapeError` once the field passes `Number.MAX_SAFE_INTEGER`, the way `incr` and `incrby` already did, instead of resolving a rounded number the caller cannot tell apart from the real one. Redis hash counters are 64-bit, so the reply can be exact on the server and wrong by the time it is a JavaScript number.

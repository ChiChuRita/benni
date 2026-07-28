---
"beni": patch
---

Fix a clock-skew bug in `ratelimit`, and add `retryAfterMs` to its result.

The sliding-window script took `now` from the calling process. Two app servers whose clocks disagree therefore disagreed about where the window starts, and the same user got a different limit depending on which server answered: a server running fast expires entries early and admits too many, one running slow rejects requests that should pass. The script now reads `TIME` from Redis, so every caller shares one clock. Nothing about the API changes.

`RatelimitResult` gains `retryAfterMs`, a duration derived server-side from the same clock as `resetMs`. `resetMs` is an absolute server timestamp, so turning it into a `Retry-After` header meant differencing it against the local clock, reintroducing exactly the skew that was just removed. `beni/next` and `beni/hono` now use the new field for their `Retry-After` headers.

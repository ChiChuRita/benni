---
"benni": patch
---

The `benni/ioredis` subscriber now reports itself closed once ioredis has given up on its connection.

Its `closed` flag only ever flipped when Benni itself called `close()`. When ioredis reached its terminal `"end"` state on its own, because a `retryStrategy` you configured stopped retrying, the lease kept claiming to be open, so the Pub/Sub hub handed the next subscribe a connection that would never deliver again. `closed` now also reads ioredis's own status, the way `benni/node` reads node-redis's `isOpen`. Only `"end"` counts: `"close"` is the passing state between a drop and a reconnect, and ioredis resubscribes on its own when it reconnects.

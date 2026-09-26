---
"benni": patch
---

`benni/bun` subscriptions now survive a reconnect, and closing a Bun client with subscriptions still live no longer keeps the process alive.

- **Subscriptions went silent after a reconnect.** Bun's Redis client reconnects a dropped subscriber connection on its own but does not resubscribe, so after a network blip, a Redis restart, or a `CLIENT KILL`, the socket came back with no subscriptions: `PUBLISH` reported 0 receivers, every handler stopped firing, and the lease still claimed to be open, so the hub kept handing it to new subscribes too. `benni/node` and `benni/ioredis` both received the messages published after the same kill. The adapter now resubscribes every channel on Bun's own reconnect, with one `SUBSCRIBE` issued in the same turn it reads the channel list, so an unsubscribe racing the reconnect cannot be undone by it. This was chosen over reporting the lease closed and letting core re-lease: that would only rescue the *next* subscribe, and the handlers already registered would have stayed dead.
- **The lease now reports when Bun gives up.** Once Bun stops reconnecting (after `maxRetries`), `closed` turns true, so the next subscribe leases a fresh connection instead of reusing the dead one.
- **`close()` with live subscriptions pinned the process.** Bun keeps the process alive when a client is closed while it still holds subscriptions (verified on 1.4.2), and the parent `client.close()` force-closes a subscriber in exactly that state. The subscriber now drops its subscriptions before closing, so the process exits.

Messages published while the connection is down are still lost. That is how Redis Pub/Sub works on every adapter, and the [Pub/Sub page](https://chichurita.github.io/benni/data-structures/pubsub/#reconnects) now says so plainly and points to streams for delivery that has to survive an outage.

Verified against redis 8 with a new shared test that kills the subscriber connection server-side and asserts delivery on every channel afterwards; it runs for `benni/node`, `benni/ioredis` (channels and patterns), and `benni/bun`, and failed on Bun before the fix.

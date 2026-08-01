---
"beni": patch
---

Fix five connection-lifetime bugs in the TCP adapters.

`beni/bun` no longer strands a reconnect loop when the very first connect fails: an unreachable server now rejects in milliseconds instead of after half a minute, and the process exits instead of being pinned forever by a client Bun gives you no way to cancel.

All three TCP adapters now treat `close()` as final. A session or subscriber whose connect was still in flight when `close()` ran, or one leased after it, used to come back live and untracked, leaking a socket that in Node keeps the event loop alive through a "graceful" shutdown. Both cases now reject with "client is closed".

`beni/node`'s `close()` is idempotent, matching every other adapter, so a SIGTERM and a SIGINT handler that both close the client no longer produce an unhandled rejection mid-shutdown. It also force-releases the socket, which a graceful close during a reconnect could otherwise leave behind.

A `beni/node` subscriber now reports `closed` once its connection is terminally gone, so core drops the dead lease instead of reusing it for the next subscribe.

A session leased from an adopted ioredis `Cluster` is finally fail-fast. Cluster takes different retry options than a standalone client, so the fail-fast settings were silently ignored and a dropped session reconnected with its `WATCH` state gone while still reporting itself open.

`beni/ioredis`'s `send()` now returns the same reply shape as the other adapters when you write a command name in lowercase. `send(["hgetall", key])` hit ioredis's reply transformers and came back as a plain object instead of the flat array everywhere else.

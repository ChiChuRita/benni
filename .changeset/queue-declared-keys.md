---
"benni": patch
---

The queue's Lua scripts declare every key they can name, and run on Dragonfly with its default settings.

Every script used to build per-job key names from the queue's prefix inside Lua. Dragonfly refuses a script that touches an undeclared key. `touch` (every `emit()` and heartbeat), `settle`, `retry`, `retryDead`, the new `requeue`, and the new stream read now pass every key in `KEYS`. Three scripts cannot, because they discover key names inside the script: `reserve` (the job ids it promotes, reclaims, and pops), `enqueue`, and `cancel` (an idempotency key read from the record). They keep deriving those keys from the hash tag, which Redis and Redis Cluster allow because they share one slot. They also start with Dragonfly's per-script opt-in, `--!df flags=allow-undeclared-keys`, which Redis reads as a comment.

Verified against `docker.dragonflydb.io/dragonflydb/dragonfly` v2.0 with no server flags: the queue's live test suites pass, and fail with "script tried accessing undeclared key" once the opt-in line is removed. A new live test runs every script against a cluster-enabled Redis node. An older Dragonfly that ignores per-script flags needs `--default_lua_flags=allow-undeclared-keys`.

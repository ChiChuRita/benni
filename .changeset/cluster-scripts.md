---
"benni": patch
---

Lua scripts work on a multi-node Redis Cluster, and survive a `SCRIPT FLUSH` under concurrent callers.

`SCRIPT LOAD` and `SCRIPT EXISTS` carry no key, so a cluster client sends them to any node while `EVALSHA` goes to the node that owns the key. On a three-master cluster most first runs drew `NOSCRIPT`, the reload went back through the same keyless route, and scripts failed intermittently, primitives included. On `NOSCRIPT` the script runner now falls back to `EVAL` with the keys, which runs on the right node and caches the script there for the next `EVALSHA`.

The same change fixes a single-node race: after a `SCRIPT FLUSH` (or a failover), concurrent callers of one script all drew `NOSCRIPT`, and all but the first rethrew it, because the first caller's reload made the others' `SCRIPT EXISTS` probe answer "still there". Each caller now recovers on its own.

A script's own error that merely starts with `NOSCRIPT` is still rethrown rather than re-run; only the server's wording (`NOSCRIPT No matching script`) falls back.

---
"beni": patch
---

Fix WATCH-safety on sessions and three ways a hash write could go wrong.

A `hset(id, value, { ttlSeconds })` issued on a session ran a real MULTI/EXEC, which cleared the connection's watch set: an optimistic transaction around it committed over a concurrent write instead of aborting, or failed with a reply-shape error and wrote nothing. A session that holds a WATCH now batches such a write as a pipeline and leaves the watch armed.

Two `redis.watch` calls sharing one borrowed session no longer interleave their WATCH sets. Each call now holds the session from WATCH to EXEC, so one can no longer abort on a key it never watched while the other commits over a concurrent write. Watches on separate sessions are unaffected.

`hsetex` now rejects a field whose value is `undefined` instead of storing `"undefined"` or `false`, and `hgetex` now rejects an expiry passed with an empty field list instead of dropping it silently.

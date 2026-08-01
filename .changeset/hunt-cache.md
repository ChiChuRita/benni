---
"beni": patch
---

Fix `cache` losing an invalidation to a load that was already running. A loader now publishes only while it still holds the fill lock, and `del()` drops the entry and that lock together, so the usual write-through order (update the row, then invalidate) can no longer be undone by a loader republishing its pre-invalidation snapshot for a full TTL. The same fence stops a loader whose lock has expired from overwriting a newer entry.

Waiters also watch the fill lock instead of only the value: when a lease is handed to a new loader they wait for that loader rather than all giving up on the previous holder's clock and hitting the backend at once. The total wait stays bounded at three lock lifetimes.

One behavior change to note: a load that runs longer than `lockTtlMs` still returns its value to the caller, but no longer caches it, because by then it may be older than whatever replaced it. Set `lockTtlMs` above your slowest load.

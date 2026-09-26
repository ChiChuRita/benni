---
"benni": minor
---

**Breaking:** a queue worker's `heartbeatMs` must now be at most half of its `leaseMs`, and it defaults to a quarter of the lease instead of a fixed 15 seconds.

`leaseMs: 10_000` combined with the old fixed default heartbeat of 15000 meant every job running longer than ten seconds lost its lease before its first renewal and was reclaimed and re-run while still going. Quick tests never catch that. The worker now validates the pair the way `lock` and `semaphore` do, and derives the default from the lease.

```ts
// Before: accepted, and every job over 10s was run twice
jobs.worker(handler, { leaseMs: 10_000 });                    // heartbeat 15000
jobs.worker(handler, { leaseMs: 10_000, heartbeatMs: 8_000 }); // accepted

// After
jobs.worker(handler, { leaseMs: 10_000 });                    // heartbeat 2500
jobs.worker(handler, { leaseMs: 10_000, heartbeatMs: 8_000 }); // ValidationError
```

At the default 60000 lease the default heartbeat is still 15000. A worker that sets `leaseMs` without `heartbeatMs` now renews at a quarter of that lease, so a longer lease also renews less often. A non-streaming handler therefore takes up to `heartbeatMs` to hear about a `cancel()`.

---
"benni": patch
---

The queue decides leases, delays, and timestamps on Redis server time instead of each caller's clock.

Every queue script received `now` from the caller's `Date.now()` and used it to reclaim stalled jobs, stamp lease expiries, and schedule delayed jobs. A worker whose clock ran fast therefore saw leases expire early and reclaimed jobs their holders were still renewing, so a paid generation ran twice. A producer whose clock was off made delayed jobs fire early or late. The scripts now read `TIME` themselves, as `ratelimit` and `semaphore` already did. No script takes a timestamp argument anymore.

- The job record's `createdAt`, `updatedAt`, `startedAt`, and `finishedAt` are server time too.
- Writing after `TIME` relies on effects replication, the default since Redis 5 and the only mode since Redis 7. The queue already requires Redis 6.2.
- Verified live: a worker with `Date.now()` mocked an hour fast does not reclaim a lease another worker holds, and a producer mocked an hour slow cannot make a 400ms delay fire early.

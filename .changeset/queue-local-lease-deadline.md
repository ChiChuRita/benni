---
"benni": patch
---

A queue worker that can no longer reach Redis now aborts its job at the lease deadline instead of generating for a run that has already been reclaimed. Lost leases and refused final writes are reported to `onError`.

A heartbeat that failed over the network only went to `onError`, so a partitioned worker kept streaming tokens from the provider while another worker reclaimed the job and paid for it again. Each running job now tracks its lease on a monotonic clock from the last renewal Redis confirmed, the way `lock` tracks `expiresAt`. Once the lease can no longer have been renewed in time, the worker aborts `job.signal` with a `JobLeaseLostError`, at or before the moment another worker could reclaim the job.

- Lease losses, proven or deadline-driven, are passed to `onError` as a `JobLeaseLostError`. A settle or retry the lease fence refused is reported the same way, where before its result was silently ignored.
- After a deadline-driven loss, a handler that finishes anyway still offers its result to the settle, which is fenced by the lease token. It is recorded only if nobody reclaimed the job, which keeps paid work when a partition heals in time.
- Once the lease is lost, `emit()` and `progress()` throw straight away and `heartbeat()` returns `false`, with no round trip. Automatic heartbeats no longer stack up behind a slow one.
- A cancelled job's signal now aborts with a `JobCancelledError` instead of a plain `Error` with the same message.
- Verified live with a client whose link is cut mid-job: the stalled handler's signal aborts no later than the rescuing worker starts its re-run, and the stalled result is refused once the link heals.

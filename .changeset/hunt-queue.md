---
"beni": minor
---

Fix five queue defects around cancellation, job-id reuse, and retries.

Cancelling an active job now wins atomically: if the handler completes, or throws a retryable error, after `cancel()` returned `true`, the job settles `cancelled` instead of recording a result or scheduling another paid attempt. Previously that only happened once the worker noticed the flag on its next heartbeat, so anything finishing inside the heartbeat window slipped through.

Re-enqueuing an explicit `id` now starts a genuinely clean generation: the old event stream, dead-letter entry, lifecycle memberships, and idempotency mapping are all cleared first, so `watch()` no longer replays the previous generation's terminal event. Reusing an id that is still waiting, scheduled, or active throws a `ValidationError` rather than putting one id in two indexes and running the job twice. This is the breaking part of the release.

A retried attempt now trims its output stream instead of deleting it. Deleting reset the stream's id counter, so the `restarted` marker could be recreated at or below a cursor a watcher already held and the whole second generation went unseen.

An idempotency key is now held for the job's entire run and only starts its `idempotencyTtlMs` retention once the job completes, capped at the record's own TTL. A slow job no longer loses its key mid-flight and lets a duplicate request pay for a second generation.

`RetryJobError` now rejects a non-finite `retryAfterMs`, such as an unparsable `Retry-After` header. Redis refused it as a score only after the retry script had already released the lease, which left the job outside every lifecycle index with nothing able to reserve it.

---
"benni": patch
---

Docs: the queue is documented as at-least-once, which it always was, and the claim that "nothing is double-run" is gone.

`Worker.stop()`'s doc comment and the queue page both said in-flight work keeps its lease, "so nothing is double-run". A crash, a partition, an event-loop stall longer than the lease, or a SIGKILL after a deploy's grace period all start another run of the same job. The [AI Job Queue](https://chichurita.github.io/benni/primitives/queue/) page has a new "What is and isn't guaranteed" section. Guaranteed: one recorded outcome per run, fenced by the lease token; atomic transitions; cancellation that wins; and no silent gaps in a watched stream. Not guaranteed: that the handler runs once, instant cancellation, or unbounded retention. Handler side effects must be idempotent. The README, `llms.txt`, and the AI Apps pattern page say the same.

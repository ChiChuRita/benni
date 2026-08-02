---
"benni": patch
---

Docs: a reference page for the whole error surface, and one recommended `json` form across every entry point.

- New [Errors](https://chichurita.github.io/benni/api/errors/) page under API. It documents every public error class (`ValidationError`, `ReplyShapeError`, `PartialRecordError`, `RedisServerError`, `SessionClosedError`, `WatchRetriesExceededError`, `CrossSlotError`, and the lock, semaphore, queue, idempotency, and budget errors), what throws each one, the structured properties it carries, and how to tell it apart from its siblings. It opens with a "which error should I catch" table and shows a `catch` branching on `RedisServerError.code`. Also covered: `redisErrorCode`, `redisServerError`, and the deliberate exclusion of connection and transport failures from `RedisServerError`.
- The docs quick start now leads with `json(validator)`, matching the README and `llms.txt`. `json<T>()` is shown right after it, labelled as the unchecked escape hatch. Before, the quick start led with the cast and the README led with the validator, so the two disagreed about the recommended default.
- The README philosophy section and `llms.txt` both mention `RedisServerError`, including a rule telling coding agents to branch on `.code` rather than match against message text.

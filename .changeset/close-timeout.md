---
"benni": minor
---

`redis.close({ timeoutMs })` bounds how long queue workers may drain.

`redis.close()` stops every queue worker started through the handle, and each one waited for its in-flight jobs with no limit, so a stuck handler held shutdown open until the platform killed the process. `timeoutMs` is forwarded to each worker's `stop({ timeoutMs })`: once it elapses, jobs still running are aborted and handed back to the queue for another worker. Without it nothing changes.

```ts
process.on("SIGTERM", () => redis.close({ timeoutMs: 10_000 }));
```

---
"benni": minor
---

`queue.wait()` rejects with typed errors: `JobFailedError` for a failed job and `JobCancelledError` for a cancelled one, both carrying `jobId`.

Both were plain `Error`s, so telling "the job failed" from "the connection dropped" meant matching message text. The messages are unchanged: `JobFailedError`'s message is still the recorded failure, verbatim. Both extend `Error`, so existing `catch` blocks keep working.

```ts
try {
  const text = await jobs.wait(id);
} catch (error) {
  if (error instanceof JobCancelledError) return;
  if (error instanceof JobFailedError) log(error.jobId, error.message);
  throw error;
}
```

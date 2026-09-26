---
"benni": minor
---

**Breaking:** `watch()` reports output that the per-job retention cap trimmed away with a new `truncated` event, instead of skipping it silently. `JobEvent` gains that member.

Each job's stream keeps its newest `eventsMaxLen` events (default 10000), and the docs emit one event per token. A long generation outgrew the cap, and a watcher replaying from `"0"` or resuming from an old cursor started mid-output with no way to tell. The `watch()` doc comment even promised "nothing is missed". Now:

- Stream entries carry a per-job sequence number, and the record keeps the id of the last entry the cap removed. The backlog and that watermark are read in one script. `watch()` yields `{ type: "truncated", id }` wherever events are missing: from a cursor the cap trimmed past, from a full replay whose first entry isn't the stream's first, or from a gap that opens while tailing. A restart marker is never reported as a gap. The event's `id` is the position before the gap, so it is safe to store as a cursor.
- The retention cap is applied by the queue itself, in batches of a tenth of `eventsMaxLen`, rather than by `XADD MAXLEN ~`, so it knows exactly how far each trim went.
- `after` is validated as a stream id before anything is sent, because it usually comes straight from an SSE `Last-Event-ID` header. Anything else throws a `ValidationError` instead of reaching Redis.

A `switch` over `event.type` that asserts exhaustiveness with `never` no longer compiles until it handles `truncated`:

```ts
// Before: compiled, and a truncated replay was indistinguishable from a full one
switch (event.type) {
  case "chunk": /* … */ break;
  case "restarted": /* … */ break;
  case "progress": /* … */ break;
  case "completed": case "failed": case "cancelled": /* … */ break;
  default: assertNever(event);
}

// After
switch (event.type) {
  // …
  case "truncated": showNotice("Earlier output was trimmed"); break;
  default: assertNever(event);
}
```

`completed` still carries the whole result, and `wait()` ignores `truncated`. Streams written by 0.1 workers have no sequence numbers and are simply not checked.

---
title: "AI Job Queue"
description: "Run model calls as background jobs that survive refreshes, deploys, and crashes, with a resumable output stream and a Stop button that actually stops the bill."
---

`queue` runs expensive model calls as background jobs, so a generation survives the user refreshing the page, your server deploying, and the request timing out.

```ts
import { queue } from "beni/primitives";

const jobs = queue<{ prompt: string }, string>(client, {
  prefix: "generate"
});
```

## The five bugs you hit in order

Every app with a **Generate** button discovers these in the same sequence:

1. **The request times out.** A long generation outlives your platform's request limit, and the user gets a 504 after paying for 45 seconds of tokens.
2. **A refresh loses everything.** The stream lived in one HTTP response. The tab reloads, the answer is gone, and you generate it again, at full price.
3. **Deploys eat in-flight work.** Every generation running when the container recycles just… disappears. Nobody knows which ones.
4. **Stop doesn't stop.** The user hits Stop, the UI clears, and your server keeps streaming tokens from the provider into a void you are still billed for.
5. **One click bills twice.** A double-click, a client retry, or an at-least-once webhook fires two identical generations, and both run.

Each has a well-known fix. The catch is that they're *five different fixes* (a job queue, a stream buffer, a lease, a cancellation channel, an idempotency store), and they have to agree with each other. Wire them separately and they fight: the classic version of this is a resumable-stream layer that can't tell a user pressing **Stop** apart from a dropped connection, so it dutifully resumes a generation the user cancelled.

`queue` is those five fixes as one thing.

## The whole loop

**Producer**: one atomic round trip. Runs anywhere, including [the edge](/beni/runtime/edge/):

```ts
const { id } = await jobs.enqueue({ prompt }, { idempotencyKey: requestId });
```

**Worker**: a long-lived process:

```ts
import { openai } from "@ai-sdk/openai";
import { streamText } from "ai";

jobs.worker(
  async (job) => {
    const { textStream } = streamText({
      model: openai("gpt-4o-mini"),
      prompt: job.payload.prompt,
      abortSignal: job.signal // Stop actually stops the provider
    });

    let text = "";
    for await (const delta of textStream) {
      text += delta;
      await job.emit(delta); // stream to watchers, and stay alive
    }
    return text;
  },
  { concurrency: 8 }
);
```

**Consumer**: an endpoint that survives reconnects:

```ts
for await (const event of jobs.watch(id, { after: lastSeenEventId })) {
  if (event.type === "chunk") send(event.data, event.id);
  if (event.type === "restarted") clear();
  if (event.type === "completed") return event.result;
}
```

That's the whole thing. All five bugs are gone: the work outlives the request, `after` resumes it, a dead worker's job is reclaimed, `job.signal` propagates Stop, and `idempotencyKey` collapses the duplicate.

The `watch` loop needs no break condition; the iterator ends itself after the job's terminal event.

## What it replaces

| The bug | What you'd otherwise wire | Here |
|---|---|---|
| Request times out | A job queue + a worker | `enqueue` / `worker` |
| Refresh loses the stream | A separate stream buffer keyed by generation id | Every job *has* an output stream |
| Deploy eats in-flight work | Visibility timeouts, stalled-job sweepers | Heartbeat leases, reclaimed automatically |
| Stop doesn't stop | A cancellation channel the worker polls | `cancel()` → `job.signal` aborts |
| Double-billed clicks | An idempotency table with its own TTL rules | `idempotencyKey` |
| A 429 you retried too fast | Backoff logic per provider | `RetryJobError(msg, retryAfterMs)` |

## Three ideas worth knowing

Everything above rests on these, and they're what make it feel different in use.

### Streaming a token *is* the heartbeat

`job.emit(token)` appends to the job's stream **and** renews the lease **and** checks for cancellation: one round trip, no separate keepalive to remember.

This is why a ten-minute generation is ordinary here rather than something you tune around. Most queues detect a dead worker by *idleness*, which is precisely wrong for a worker legitimately blocked on a slow model. A worker here says "still alive" by doing its actual job.

If your handler doesn't stream, an automatic heartbeat covers it; you don't have to call anything.

### Stop stops the bill

```ts
await jobs.cancel(id); // true if the job will not produce a result
```

A job that hasn't started is removed outright. A running job is flagged, and its worker aborts `job.signal` on the next `emit()`, so a `fetch` or AI SDK call wired to that signal tears down mid-stream and you stop paying for a cancelled answer. Either way the job settles `cancelled`, and watchers get a `cancelled` event instead of hanging forever.

Cancelling can't race the worker into a double-settle: only the worker holding the current lease may write a result. Nor can it lose to one. If the handler finishes, or throws a retryable error, after `cancel()` returned `true`, the job still settles `cancelled`: the result is discarded and no further attempt is scheduled, rather than the queue paying for a generation the caller already stopped.

### A retried generation restarts its stream

If attempt 1 dies halfway through `"The capital of"`, attempt 2 starts over. The partial tokens are dropped and a `restarted` event is written first, so a client resuming from a cursor clears its buffer instead of rendering `"The capital ofThe capital of France is Paris"`. The marker is always written above every entry id the previous attempt used, so a resuming cursor can't skip past it.

That's the one event type worth handling deliberately.

## Paying once for duplicate work

```ts
const first = await jobs.enqueue({ prompt }, { idempotencyKey: requestId });
const again = await jobs.enqueue({ prompt }, { idempotencyKey: requestId });
// again.id === first.id, again.deduplicated === true, no second model call
```

The key stays bound to the job for as long as it runs *and after it completes*, so a retry arriving late gets the finished answer rather than starting over. `idempotencyTtlMs` is the retention after completion, not a countdown from enqueue: a job that sits in a backlog for an hour and then streams for ten minutes still holds its key throughout. A job that fails or is cancelled releases its key: there's no answer to hand out, so a genuine retry should be allowed.

## Retries that match how providers actually fail

Everything retries with exponential backoff and full jitter, except what you mark otherwise:

```ts
import { RetryJobError, TerminalJobError } from "beni/primitives";

jobs.worker(async (job) => {
  const response = await fetch(providerUrl, {
    method: "POST",
    body: JSON.stringify(job.payload),
    signal: job.signal
  });

  // 429: the provider told us exactly when to come back. Believe it.
  if (response.status === 429) {
    const retryAfter = Number(response.headers.get("retry-after") ?? 1);
    throw new RetryJobError("rate limited", retryAfter * 1000);
  }

  // 400: a retry reproduces this verbatim. Don't waste three attempts.
  if (response.status === 400) {
    throw new TerminalJobError(`malformed request: ${await response.text()}`);
  }

  return (await response.json()).text;
});
```

Pass `isRetryable` to `worker()` to replace the classification wholesale.

## Awaiting a result

When you just want the answer and don't care about tokens:

```ts
const text = await jobs.wait(id);
```

It checks the record first, so a job that already finished returns immediately instead of waiting for an event that has passed. Unknown ids (and jobs whose `resultTtlMs` has elapsed) reject with `JobNotFoundError`.

## When a worker dies

Nothing to configure. The dead worker's lease expires, the next `reserve` reclaims the job, and it goes back to the ready set, or straight to the dead letter set if it's out of attempts. Because attempts are counted when a job is *reserved*, a handler that reliably crashes the process dead-letters instead of looping forever.

A zombie worker that wakes up later can't clobber the job that replaced it: `emit()`, `progress()`, and the heartbeat all throw `JobLeaseLostError` once the lease token is stale, and settling is refused.

```ts
const worker = jobs.worker(handler, {
  concurrency: 8,
  leaseMs: 120_000, // longer than your slowest generation
  heartbeatMs: 15_000 // comfortably inside the lease
});

// Graceful shutdown: stop taking new work, let in-flight jobs finish.
process.on("SIGTERM", () => void worker.stop());
```

`stop()` never kills a running job: in-flight work keeps its lease and finishes, so nothing is double-run.

## Operating it

```ts
await jobs.stats(); // { waiting, scheduled, active, dead }

for (const id of await jobs.dead({ count: 20 })) {
  await jobs.retryDead(id); // back to the queue with a fresh attempt count
}
```

`retryDead` also clears the failed attempt's output, so a watcher doesn't stop on the stale `failed` event.

Jobs can also be delayed and prioritised, which is how interactive work stays ahead of batch work:

```ts
await jobs.enqueue(payload, { priority: 9 }); // a user is waiting
await jobs.enqueue(payload, { priority: 0, delayMs: 60_000 }); // nightly backfill
```

Passing your own `id` reuses it: once that job has finished, re-enqueuing the id starts a clean generation, dropping the old record, its dead-letter entry, and its output stream so a watcher can't stop on last time's terminal event. Reusing an id that hasn't finished throws instead, because there is no honest way to have one id be two live jobs.

## When you don't need this

Be honest about the fit: it's a worker process to run and monitor:

- **The call is fast and the user is watching.** Under a few seconds, stream it from the request and skip all of this.
- **You've already answered this prompt.** [`cache`](/beni/primitives/cache/) is cheaper than any queue, so check it first and queue only on a miss.
- **You need durable *execution*.** On a retry, your handler re-runs from the top. There is no checkpointing of a half-finished agent loop and no resuming mid-function. If you need "the agent completed 3 of 7 tool calls, resume at 4", model those steps as separate jobs, or use a durable-execution engine.
- **You have no long-lived process.** `worker()` needs one. Producing and watching work fine at the edge; running doesn't.

## How it's built

Job *lifecycle* lives in sorted sets; job *output* lives in a stream. Each is good at exactly one of those.

| Key | Type | Holds |
|---|---|---|
| `{prefix}:ready` | zset | Runnable jobs, priority-major and FIFO within a priority |
| `{prefix}:scheduled` | zset | Delayed jobs and backoff retries, scored by ready time |
| `{prefix}:leases` | zset | Owned jobs, scored by lease expiry |
| `{prefix}:dead` | zset | Dead-lettered jobs |
| `{prefix}:job:<id>` | hash | The job record |
| `{prefix}:events:<id>` | stream | That job's output |
| `{prefix}:signal` | list | A doorbell, so idle workers block instead of polling |

A stream consumer group would hand you recovery via `XAUTOCLAIM`, but it reclaims by *idle time*, the wrong signal for a worker blocked on a slow model, and it gives you no delays, priority, backoff, or dead-lettering. Sorted sets give all four; the stream does what streams are good at.

Every key shares one hash tag, so a queue occupies a single Redis Cluster slot. Every state change is a single Lua script, so there is no window where a job is in two places or none.

## Options

| Option | Default | Notes |
|---|---|---|
| `prefix` | `"queue"` | Key namespace; also the Cluster hash tag |
| `codec` / `resultCodec` | `codecs.json()` | Any [codec](/beni/api/schema-builders/#codecs), including [`zodCodec`](/beni/integrations/zod/) for validated payloads |
| `leaseMs` | `60000` | Ownership without a heartbeat, sized for model calls |
| `maxAttempts` | `3` | Attempts before dead-lettering |
| `backoffMs` / `maxBackoffMs` | `1000` / `60000` | Exponential curve with full jitter |
| `resultTtlMs` | `3600000` | How long a finished record and its stream survive |
| `eventsMaxLen` | `10000` | Retained events per job, so token streams stay bounded |

Worker options: `concurrency`, `leaseMs`, `heartbeatMs`, `pollMs`, `isRetryable`, `onError`.

## Runtime support

`enqueue`, `get`, `cancel`, `wait`, `watch`, `stats`, and `dead` need only `EVALSHA` and stream reads, so they run on every adapter, including [`beni/upstash`](/beni/runtime/edge/) on Cloudflare Workers and Vercel Edge. That's the shape most AI apps want: enqueue from an edge route, run the model on a worker.

`worker()` needs a persistent process. Where the adapter offers a dedicated connection it blocks on the doorbell list, so a job starts a round trip after it's enqueued; otherwise it polls at `pollMs`. `watch()` degrades the same way: blocking `XREAD` on Node and Bun, polling on the edge.

One cost to size for: each live `watch()` holds a connection while it iterates, and each `worker()` holds one for its doorbell. Fine for a worker fleet and a handful of dashboards; an endpoint fanning one generation out to thousands of concurrent viewers would want a connection per viewer. Until a shared tail exists, pass a `pollMs` for those watchers, or fan out from a single server-side `watch()` to your own subscribers.

## See also

- [AI Apps](/beni/patterns/ai-apps/): chat memory, token budgets, and response caching around this queue
- [Cache](/beni/primitives/cache/): answer a repeat prompt without queueing anything
- [Rate Limiting](/beni/primitives/ratelimit/): cap what reaches the queue per user
- [Streams](/beni/data-structures/streams/): the typed API under the job output stream

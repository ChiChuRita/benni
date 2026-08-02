---
title: "Errors"
description: "Every error type Benni throws, what makes it fire, and the structured properties it carries so you can branch on a field instead of matching message text."
---

Benni never casts a bad value and moves on. When something is wrong it throws, and it throws a named class carrying the structured detail you need to decide what to do next. This page is the full public error surface.

## Which Error Should I Catch?

Four questions cover almost every case:

| The failure is | Catch | Where it comes from |
|---|---|---|
| I passed bad input, nothing was sent | `ValidationError` | `benni` |
| Redis answered with an error reply | `RedisServerError` | `benni` |
| A reply or stored value was the wrong shape | `ReplyShapeError` | `benni` |
| A primitive could not give me what I asked for | the primitive's own class | `benni/primitives` |

The first three are the important distinction, and they are mutually exclusive:

- **`ValidationError`** means the command never left the process. Benni refused your arguments.
- **`RedisServerError`** means the command reached Redis and Redis refused it.
- **`ReplyShapeError`** means the command *succeeded* and the value that came back did not match what your schema declared.

Everything else is either a subclass of one of those or a primitive-specific outcome.

One class is deliberately absent: connection and transport failures. A closed socket, a DNS failure, an ioredis `MaxRetriesPerRequestError`, an Upstash HTTP 502, a non-JSON REST body. Benni passes those through from the underlying client untouched, because it has nothing to add and wrapping them would only hide what the client already told you.

## Core Errors

These are exported from the root `benni` entrypoint (and from `benni/core`), with one exception noted below: `CrossSlotError` lives in `benni/cluster`.

### `ValidationError`

Extends `TypeError`. Thrown when caller-supplied input fails validation **before any command is sent to Redis**: an out-of-range count, a non-finite number, a blocking timeout of `0`, a contradictory option combination.

It extends `TypeError` so existing `instanceof TypeError` handling keeps working. Catch `ValidationError` specifically to tell "I passed bad input" apart from a protocol-level failure.

Properties: none beyond `message`. A `ValidationError` is a programming mistake, so the message names the argument and the constraint.

### `ReplyShapeError`

Extends `TypeError`. Thrown when a Redis reply, or a stored value handed to a codec, does not match the shape a decoder expected.

| Property | Type | Meaning |
|---|---|---|
| `reply` | `unknown` | The raw value received, so you can inspect or log it programmatically |

Most messages read `Expected Redis <COMMAND> to return <expected>, got <actual>`. Two common sources:

- A `number()` or `json()` codec decoding a value some other writer stored in a shape the codec cannot read.
- A failing **`json(validator)`** read. When the stored JSON does not satisfy the validator, the read throws `ReplyShapeError` naming the validator vendor and the issues, with the offending value on `.reply`. This is the whole reason to prefer `json(validator)` over `json<T>()`: see [JSON values](/benni/data-structures/json-values/).

`ReplyShapeError` is not a server error. The command succeeded; the *data* was wrong.

### `PartialRecordError`

Extends `ReplyShapeError`. Thrown when a whole-record hash read (`hget(id)` on a `hash()` schema) finds some, but not all, of the declared fields.

| Property | Type | Meaning |
|---|---|---|
| `missing` | `readonly string[]` | The declared fields that were absent |
| `reply` | `unknown` | Inherited: the raw `HMGET` array |

The reply is well formed here, so this is not a protocol or adapter fault. It means the stored record is incomplete, most often because individual fields were given their own TTLs with `hexpire` and some have since lapsed, or because `hdel` removed a declared field.

It extends `ReplyShapeError` so code that already catches that keeps working. Catch `PartialRecordError` specifically to tell an ordinary incomplete record apart from a genuine shape violation, and reach for `hgetall` (which types its result as `Partial`) when incompleteness is expected. See [Hashes](/benni/data-structures/hashes/).

### `RedisServerError`

Extends `Error`. Thrown when the Redis **server** answered with an error reply: `WRONGTYPE` on a key holding another type, `NOSCRIPT`, `OOM`, `READONLY`, `NOAUTH`, or a Lua script's own `redis.error_reply(...)`.

| Property | Type | Meaning |
|---|---|---|
| `code` | `string \| undefined` | The uppercase code the reply opens with, parsed for you |
| `command` | `string \| undefined` | Uppercased name of the command that drew the error, when the throw site could attribute it |
| `cause` | `unknown` | The adapter-native error (or raw payload), set whenever there was one |

#### Why It Exists

This is the one error type **every adapter normalizes to**. Before it, the same `WRONGTYPE` reached you as node-redis's `SimpleError` on `benni/node`, ioredis's `ReplyError` on `benni/ioredis`, Bun's `RedisError` on `benni/bun`, and a bare `Error` built from the REST payload on `benni/upstash`: one taxonomy per runtime, so a `catch` block written against one adapter silently stopped matching after a move to another. That is the opposite of what one typed API across runtimes is supposed to buy.

Now `error instanceof RedisServerError` means the same thing on all four.

#### Branch On `.code`, Not On The Message

`code` is the parsed leading token of the reply: `WRONGTYPE`, `NOSCRIPT`, `NOAUTH`, `OOM`, `READONLY`, `MOVED`, `ASK`, `BUSYGROUP`, `EXECABORT`, and the rest of Redis's vocabulary. Branch on it rather than matching substrings of prose that Redis is free to reword.

```ts
import { RedisServerError } from "benni";

try {
  await redis.query.leaderboard.zadd("global", { member: "ada", score: 1 });
} catch (error) {
  if (!(error instanceof RedisServerError)) throw error;

  switch (error.code) {
    case "WRONGTYPE":
      // that key holds a hash, not a sorted set: a schema or key-prefix bug
      throw new Error(`${error.command} hit the wrong type`, { cause: error });
    case "OOM":
    case "READONLY":
      // the server cannot accept writes right now; shed load and retry later
      return { retryable: true };
    case "NOAUTH":
      // credentials are wrong or missing: not worth retrying
      throw error;
    default:
      throw error;
  }
}
```

`code` is `undefined` when the server's text carries no code, which in practice means a Lua script returned a bare `redis.error_reply("some text")`. Handle that in your `default` branch rather than assuming a code is always present.

`message` is the server's text **verbatim, code first**, so message matching that predates this class keeps working. `cause` holds the adapter-native error, so nothing the underlying client attached is lost.

#### What Is Not A `RedisServerError`

Transport failures are deliberately excluded, because nothing about them came from Redis:

- an Upstash HTTP 502, or a body that is not JSON
- a closed socket, a connection reset, a client shut down mid-command
- an ioredis `MaxRetriesPerRequestError`, a node-redis `ClientClosedError`

Those reach you as whatever the underlying client threw. A `RedisServerError` always means Redis itself formed an answer and that answer was an error.

Cluster redirections (`MOVED`, `ASK`) are followed by the cluster-aware client underneath and normally never surface. One that does reach you is a real failure, and its code is parsed like any other.

### `redisErrorCode(message)`

```ts
function redisErrorCode(message: string): string | undefined;
```

The classifier `RedisServerError` uses on its own message, exported so a caller holding a raw message (from a nested reply, a log line, a script's own output) can classify it the same way. Returns the leading uppercase code, or `undefined` when there is none.

It requires at least three characters, on purpose: the shortest real codes are `ERR`, `OOM`, and `ASK`, and the three-character floor keeps a script's `redis.error_reply("A bad thing")` from reporting `A` as an error code.

### `redisServerError(source, command?)`

```ts
function redisServerError(source: unknown, command?: string): RedisServerError;
```

The normalizer every built-in adapter runs a server error reply through. It preserves the message verbatim, keeps the original as `cause`, and passes an already-normalized `RedisServerError` through untouched, so re-wrapping on the way out of a nested call cannot double-wrap or break identity comparisons.

You only need this if you are **writing an adapter**. Deciding *whether* something is a server error reply stays with each adapter, which knows its client's taxonomy; this function only does the conversion.

`RedisServerErrorOptions` is the exported shape of its second argument on the constructor: `{ command?: string; cause?: unknown }`.

### `SessionClosedError`

Extends `Error`. Thrown by the session command gate for any use of a session after `close()`.

Properties: none. Prefer the boolean `session.closed` for worker loops: a flag is robust where cross-adapter error-class mapping is fragile, and in-flight rejections during a connection drop keep the adapter-native error rather than becoming this. See [Connection Sessions](/benni/advanced/sessions/).

### `WatchRetriesExceededError`

Extends `Error`. Thrown by `redis.watch()` when every attempt aborted because a `WATCH`ed key kept changing under it.

| Property | Type | Meaning |
|---|---|---|
| `attempts` | `number` | The total number of attempts made |

Exhaustion is exceptional, so it throws rather than returning `null`, which keeps the happy path ceremony-free. See [Optimistic Transactions](/benni/advanced/optimistic-transactions/).

### `CrossSlotError`

Extends `ValidationError` (so `instanceof TypeError` still holds). Thrown when a command's keys span two Redis Cluster hash slots, caught **before** the command is sent.

| Property | Type | Meaning |
|---|---|---|
| `command` | `string` | The command that would have been sent |
| `keys` | `readonly [string, string]` | The first key and the key that disagreed with it |
| `slots` | `readonly [number, number]` | Their two slots |

Exported from **`benni/cluster`**, not from the root entrypoint: naming it from the root would put the CRC16 table and the fix-hint prose into every bundle, including the ones that never enable the check. See [Redis Cluster](/benni/advanced/cluster/).

## Primitive Errors

Exported from `benni/primitives`.

### Lock

| Error | Properties | Thrown when |
|---|---|---|
| `LockNotAcquiredError` | `key` | `lock().run()` could not acquire the lock. Acquisition is fail-fast by default (`retries: 0`), so a caller that finds the lock held throws immediately |
| `LockLeaseLostError` | `key` | The lock was lost while `fn` was still running: renewal found the key gone or owned by another token |

`run()` rejects with `LockLeaseLostError` **even when `fn` itself resolved**, because a body that completed without the lock did not complete under the guarantee it was written against. The same error is the abort reason on `handle.signal`. [Distributed Lock](/benni/primitives/lock/#when-the-lock-is-lost) covers the two-pronged detection and how to size `ttlMs` and `heartbeatMs`.

### Semaphore

| Error | Properties | Thrown when |
|---|---|---|
| `SemaphoreNotAcquiredError` | `key`, `limit` | `semaphore().run()` found no slot free |
| `SemaphoreLeaseLostError` | `key`, `limit` | The slot was lost while `fn` was still running: renewal found the lease gone, so it had already been reclaimed |

A lost lock means two callers collided on one key; a lost slot means the semaphore **over-admits**, so a `limit: 20` pool guarding a provider quota quietly runs 21 in flight. As with the lock, `run()` rejects even when `fn` resolved, and the error is the abort reason on `held.signal`. [Semaphore](/benni/primitives/semaphore/#when-the-slot-is-lost) has the detail.

### Queue

| Error | Properties | Thrown when |
|---|---|---|
| `JobNotFoundError` | `jobId` | A job id is not in Redis: it never existed, or it finished and its `resultTtlMs` elapsed |
| `JobLeaseLostError` | `jobId` | Thrown *inside a handler* when this worker no longer owns the job. Its lease expired and another worker reclaimed it, so `emit()`, `progress()`, and the automatic heartbeat all abort the job's signal and throw this rather than let you burn tokens on a run whose result will be discarded |

Two more are errors **you throw**, from inside a handler, to steer the retry machinery:

| Error | Signature | Effect |
|---|---|---|
| `TerminalJobError` | `(message, options?)` | Fail the job immediately, no further attempts. For anything a retry would reproduce verbatim: a malformed request, a content-policy refusal, an unsupported model |
| `RetryJobError` | `(message, retryAfterMs, options?)` | Retry after an explicit delay, overriding the configured backoff. Built for a provider's `Retry-After` header |

`RetryJobError` carries `retryAfterMs` and rejects a non-finite value with a `ValidationError` from its own constructor. A `Retry-After` header parsed straight through can be `NaN`, Redis will not accept that as a sorted-set score, and by then the retry path has already dropped the lease, so the job would be stranded. Refusing it at construction leaves the worker free to fall back to ordinary backoff. See [AI Job Queue](/benni/primitives/queue/).

### Idempotency

| Error | Properties | Thrown when |
|---|---|---|
| `IdempotencyConflictError` | `key` | Another caller holds the key and `onConflict` is `"throw"` |
| `IdempotencyTimeoutError` | `key` | `onConflict: "wait"` gave up before the holder finished |
| `IdempotencyNotRecordedError<T>` | `key`, `value` | The handler succeeded but its result could not be stored |

`IdempotencyNotRecordedError` is the one that needs care. **The side effect happened.** What failed is the record of it, so the running marker will lapse and a later caller with the same key will run the handler again. Treat it as indeterminate rather than as a failure: `value` carries the result if you can still use it (return it to the client, write it somewhere durable), but do not assume a retry is safe. The usual causes are a codec that cannot encode the result, or a Redis blip between finishing the work and recording it. The underlying failure is on `cause`. See [Idempotency](/benni/primitives/idempotency/).

### Budget

| Error | Properties | Thrown when |
|---|---|---|
| `BudgetWindowRolledError` | `id` | The window rolled over under every attempt |

That takes a process stalled for longer than `windowMs` between building the keys and the script running. Nothing was applied, so the call is safe to retry, and a hold whose `settle` throws this is still usable. See [Budget](/benni/primitives/budget/).

## Inheritance At A Glance

```text
TypeError
├── ValidationError
│   └── CrossSlotError
└── ReplyShapeError
    └── PartialRecordError

Error
├── RedisServerError
├── SessionClosedError
├── WatchRetriesExceededError
├── LockNotAcquiredError / LockLeaseLostError
├── SemaphoreNotAcquiredError / SemaphoreLeaseLostError
├── JobNotFoundError / JobLeaseLostError / TerminalJobError / RetryJobError
├── IdempotencyConflictError / IdempotencyTimeoutError / IdempotencyNotRecordedError
└── BudgetWindowRolledError
```

Order your `catch` branches most specific first: `PartialRecordError` before `ReplyShapeError`, `CrossSlotError` before `ValidationError`.

## See Also

- [JSON values](/benni/data-structures/json-values/) for why `json(validator)` throws where `json<T>()` stays quiet
- [Hashes](/benni/data-structures/hashes/#missing-declared-fields-throw) for `PartialRecordError` and the tolerant `hgetall` read
- [Optimistic Transactions](/benni/advanced/optimistic-transactions/) for `WatchRetriesExceededError` and the retry loop around it
- [Redis Cluster](/benni/advanced/cluster/) for `CrossSlotError` and the slot guard
- [Philosophy](/benni/getting-started/philosophy/) for the "nothing is silent" rule these classes implement

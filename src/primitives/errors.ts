import { ValidationError } from "../core/errors.js";

// The primitives' error classes, in a module of their own so that importing
// one (from the root `benni` entry, to `instanceof` it in a catch block) pulls
// in the class and nothing else: none of the primitive's Lua, leases, or
// timers.

// lock

/** Thrown by a lock's `run()` when the lock cannot be acquired. */
export class LockNotAcquiredError extends Error {
  readonly key: string;
  constructor(key: string) {
    super(`Could not acquire lock "${key}"`);
    this.name = "LockNotAcquiredError";
    this.key = key;
  }
}

/**
 * Thrown by a lock's `run()` when the lock was lost while `fn` was still
 * running — renewal found the key gone or owned by another token, so the
 * critical section ran without the mutual exclusion it asked for and someone
 * else may have been inside it at the same time.
 *
 * `run()` rejects with this even when `fn` itself resolved: a body that
 * completed without the lock did not complete under the guarantee it was
 * written against, and reporting success would hide exactly that. The same
 * error is the abort reason on the lock handle's `signal`, so a body that passes
 * the signal to `fetch` or to the AI SDK stops as soon as the lock is gone
 * rather than finishing work that is no longer protected.
 */
export class LockLeaseLostError extends Error {
  readonly key: string;
  constructor(key: string) {
    super(
      `Lost the lock "${key}" before the critical section finished — another caller may hold it now. Raise ttlMs, lower heartbeatMs, or shorten the critical section if this recurs.`
    );
    this.name = "LockLeaseLostError";
    this.key = key;
  }
}

// semaphore

/** Thrown by a semaphore's `run()` when no slot came free. */
export class SemaphoreNotAcquiredError extends Error {
  readonly key: string;
  readonly limit: number;
  constructor(key: string, limit: number) {
    super(`Could not acquire a slot on "${key}" (limit ${limit})`);
    this.name = "SemaphoreNotAcquiredError";
    this.key = key;
    this.limit = limit;
  }
}

/**
 * Thrown by a semaphore's `run()` when the slot was lost while `fn` was still
 * running: renewal found the lease gone, so it had already been reclaimed and
 * handed to someone else.
 *
 * This is where a semaphore differs from a
 * [lock](./lock.js): a lost lock means two callers collided on one key, while a
 * lost slot means the semaphore **over-admits**. The pool believes `limit`
 * callers are inside the critical section and one more (this one) is in there
 * too, so a `limit: 20` semaphore guarding a provider quota quietly runs 21 in
 * flight, which is precisely the 429 it existed to prevent.
 *
 * `run()` rejects with this even when `fn` itself resolved: a body that
 * completed without a slot did not complete under the bound it was written
 * against, and reporting success would hide exactly that. The same error is the
 * abort reason on the semaphore handle's `signal`, so a body that passes the
 * signal to `fetch` or to the AI SDK stops as soon as its slot is gone rather
 * than finishing work that is over the limit.
 */
export class SemaphoreLeaseLostError extends Error {
  readonly key: string;
  readonly limit: number;
  constructor(key: string, limit: number) {
    super(
      `Lost the slot on "${key}" (limit ${limit}) before the critical section finished — the semaphore may now be over its limit. Raise leaseMs, lower heartbeatMs, or shorten the critical section if this recurs.`
    );
    this.name = "SemaphoreLeaseLostError";
    this.key = key;
    this.limit = limit;
  }
}

// cache

/**
 * Thrown by a cache's `get()` when another caller's load was still running after
 * `waitTimeoutMs`. Nothing was loaded by this caller.
 *
 * Loading anyway is what every waiter used to do at the deadline, all at once:
 * a backend already too slow to answer inside the budget got one extra load
 * per waiter, which is the stampede the cache exists to prevent. Serve an
 * error (a 503) or a fallback instead, or raise `waitTimeoutMs`.
 */
export class CacheWaitTimeoutError extends Error {
  readonly key: string;
  constructor(key: string, waitedMs: number) {
    super(
      `Timed out after ${waitedMs}ms waiting for another caller to load cache ` +
        `entry "${key}". Its loader is still running; raise waitTimeoutMs if ` +
        "loads legitimately take this long."
    );
    this.name = "CacheWaitTimeoutError";
    this.key = key;
  }
}

// idempotency

/** Thrown when another caller holds the key and `onConflict` is `"throw"`. */
export class IdempotencyConflictError extends Error {
  readonly key: string;
  constructor(key: string) {
    super(
      `Another request is already running for idempotency key "${key}". ` +
        'Retry once it finishes, or use onConflict: "wait".'
    );
    this.name = "IdempotencyConflictError";
    this.key = key;
  }
}

/**
 * Thrown when the handler succeeded but its result could not be stored, so the
 * call is **not** protected against a repeat.
 *
 * The side effect happened. What failed is the record of it, which means a
 * later caller with the same key will run the handler again. Treat it as
 * indeterminate rather than as a failure: the work is done, and `value` carries
 * the result if you can use it (return it to the client, write it somewhere
 * durable), but do not assume a retry is safe.
 *
 * The usual cause is a codec that cannot encode the result, a Redis blip
 * between finishing the work and recording it, or a lost claim: when `cause`
 * is an {@link IdempotencyLeaseLostError}, another caller may have run the
 * handler too.
 */
export class IdempotencyNotRecordedError<T = unknown> extends Error {
  readonly key: string;
  /** The handler's result. The effect ran; only storing it failed. */
  readonly value: T;
  constructor(key: string, value: T, cause: unknown) {
    super(
      `The handler for idempotency key "${key}" succeeded but its result ` +
        "could not be stored, so a later call with this key will run it " +
        "again. The side effect has already happened.",
      { cause }
    );
    this.name = "IdempotencyNotRecordedError";
    this.key = key;
    this.value = value;
  }
}

/**
 * The running marker lapsed, or was taken over, while the handler was still
 * running: the heartbeat that renews it failed for a whole `runningTtlMs`
 * (Redis unreachable, the event loop blocked) or found another caller's marker
 * in its place.
 *
 * From that moment another caller with the same key may run the handler too.
 * It is the abort reason on the handler's `signal`, so an effect not yet
 * committed can be stopped; if the handler resolves anyway, `run` throws
 * {@link IdempotencyNotRecordedError} with this as its `cause`.
 */
export class IdempotencyLeaseLostError extends Error {
  readonly key: string;
  constructor(key: string) {
    super(
      `Lost the running marker for idempotency key "${key}" while the ` +
        "handler was still running, so another caller may run it too. Raise " +
        "runningTtlMs if Redis blips or the event loop stalls for that long."
    );
    this.name = "IdempotencyLeaseLostError";
    this.key = key;
  }
}

/**
 * Thrown when a key is reused with a different request: the fingerprint
 * passed to `run` does not match the one recorded with the key. Nothing ran.
 *
 * The Stripe contract: an idempotency key names one request, so replaying its
 * stored result for a *different* body would answer a question nobody asked.
 * Treat it as a client bug (HTTP 422), not as a retry.
 */
export class IdempotencyFingerprintMismatchError extends Error {
  readonly key: string;
  constructor(key: string) {
    super(
      `Idempotency key "${key}" was already used with a different request ` +
        "(the fingerprint does not match the one recorded with it). Send a " +
        "new key for a new request."
    );
    this.name = "IdempotencyFingerprintMismatchError";
    this.key = key;
  }
}

/** Thrown when `onConflict: "wait"` gave up before the holder finished. */
export class IdempotencyTimeoutError extends Error {
  readonly key: string;
  constructor(key: string, waitedMs: number) {
    super(
      `Timed out after ${waitedMs}ms waiting on idempotency key "${key}". ` +
        "The original request is still running or died without releasing."
    );
    this.name = "IdempotencyTimeoutError";
    this.key = key;
  }
}

// budget

/**
 * Thrown when the window rolled over under every attempt, which takes a
 * process stalled for longer than `windowMs` between building the keys and the
 * script running. Nothing was applied, so the call is safe to retry, and a
 * hold whose `settle` throws this is still usable.
 */
export class BudgetWindowRolledError extends Error {
  readonly id: string;
  constructor(id: string, windowMs: number) {
    super(
      `The budget window rolled over on every attempt for "${id}". This ` +
        `process was stalled for longer than windowMs (${windowMs}) between ` +
        "building the keys and the script running; nothing was applied."
    );
    this.name = "BudgetWindowRolledError";
    this.id = id;
  }
}

// queue

/**
 * Thrown when a job id is not in Redis — either it never existed, or it
 * finished and its `resultTtlMs` elapsed.
 */
export class JobNotFoundError extends Error {
  readonly jobId: string;
  constructor(jobId: string) {
    super(`Job "${jobId}" not found (unknown id, or its result TTL elapsed)`);
    this.name = "JobNotFoundError";
    this.jobId = jobId;
  }
}

/**
 * Thrown inside a handler when this worker no longer owns the job: Redis
 * reported another token on it, or the lease could not be renewed before it
 * would lapse (a partition, a stalled event loop), so another worker may
 * already be running it. Keep working and you are burning tokens on a run
 * whose result will likely be discarded, so `emit()`, `progress()`, and the
 * automatic heartbeat all abort the job's signal with this. The worker also
 * reports it to `onError`.
 */
export class JobLeaseLostError extends Error {
  readonly jobId: string;
  constructor(jobId: string) {
    super(
      `Lost the lease on job "${jobId}" — another worker may be running it now. Raise leaseMs or lower heartbeatMs if this recurs.`
    );
    this.name = "JobLeaseLostError";
    this.jobId = jobId;
  }
}

/**
 * The reason on a job's signal when `worker.stop({ timeoutMs })` ran out of
 * time and handed the job back to the queue. Another worker re-runs it from the
 * top; nothing this run does from here on is recorded.
 */
export class WorkerStoppedError extends Error {
  readonly jobId: string;
  constructor(jobId: string) {
    super(
      `The worker stopped before job "${jobId}" finished; it was requeued for another worker`
    );
    this.name = "WorkerStoppedError";
    this.jobId = jobId;
  }
}

/**
 * Thrown by `wait()` for a job that failed for good: dead-lettered after its
 * last attempt, or failed by a `TerminalJobError`. `message` is the recorded
 * failure, verbatim.
 */
export class JobFailedError extends Error {
  readonly jobId: string;
  constructor(jobId: string, message: string) {
    super(message);
    this.name = "JobFailedError";
    this.jobId = jobId;
  }
}

/**
 * Thrown by `wait()` for a job that was cancelled, and the reason on a
 * handler's signal when `cancel()` reaches its running job.
 */
export class JobCancelledError extends Error {
  readonly jobId: string;
  constructor(jobId: string) {
    super(`Job "${jobId}" was cancelled`);
    this.name = "JobCancelledError";
    this.jobId = jobId;
  }
}

/**
 * Throw from a handler to fail a job immediately with no further attempts —
 * a malformed request, a content-policy refusal, an unsupported model. Anything
 * a retry would reproduce verbatim belongs here rather than in the backoff.
 */
export class TerminalJobError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "TerminalJobError";
  }
}

/**
 * Throw from a handler to retry after an explicit delay, overriding the
 * configured backoff. Built for provider `Retry-After`: pass the header through
 * and the job comes back exactly when the provider says it may.
 */
export class RetryJobError extends Error {
  readonly retryAfterMs: number;
  constructor(
    message: string,
    retryAfterMs: number,
    options?: { cause?: unknown }
  ) {
    super(message, options);
    this.name = "RetryJobError";
    // A `Retry-After` header parsed straight through can be NaN or Infinity.
    // Redis rejects that as a sorted-set score, and by the time the retry
    // script reaches its ZADD it has already dropped the lease, so the job
    // would be stranded outside every lifecycle index. Refuse it here, where
    // the worker still falls back to the ordinary backoff.
    if (!Number.isFinite(retryAfterMs)) {
      throw new ValidationError(
        `queue retryAfterMs must be a finite number of milliseconds, received ${retryAfterMs}`
      );
    }
    this.retryAfterMs = Math.max(0, retryAfterMs);
  }
}

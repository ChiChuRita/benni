import { type ClientSource, clientArgs } from "../core/client-source.js";
import { codecs } from "../core/codecs.js";
import { ReplyShapeError, ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import { xreadStreamPairs } from "../core/stream.js";
import type {
  Codec,
  InferAnchors,
  RedisClient,
  RedisReply,
  RedisSession
} from "../core/types.js";

const DEFAULT_PREFIX = "queue";
const DEFAULT_LEASE_MS = 60_000;
const DEFAULT_HEARTBEAT_MS = 15_000;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_BACKOFF_MS = 1_000;
const DEFAULT_MAX_BACKOFF_MS = 60_000;
const DEFAULT_RESULT_TTL_MS = 3_600_000;
const DEFAULT_EVENTS_MAX_LEN = 10_000;
const DEFAULT_POLL_MS = 1_000;
const DEFAULT_IDLE_BLOCK_MS = 5_000;
const DEFAULT_CONCURRENCY = 1;
const MAX_PRIORITY = 9;
// Ready scores are priority-major, sequence-minor: (9 - priority) * 1e13 + seq.
// Both terms stay well inside the 2^53 exactly-representable range, so a score
// is an exact integer and ordering is total.
const PRIORITY_STRIDE = 10_000_000_000_000;
// Depth of the doorbell list. It only ever needs one token per idle worker;
// the cap stops an unattended queue from growing it without bound.
const SIGNAL_CAP = "1000";

/** The lifecycle states a job moves through. */
export type JobStatus =
  | "waiting"
  | "scheduled"
  | "active"
  | "completed"
  | "failed"
  | "cancelled";

/** A job record as stored in Redis. */
export type Job<TPayload, TResult> = {
  readonly id: string;
  readonly status: JobStatus;
  readonly payload: TPayload;
  /** Runs so far. `0` until the job is first reserved. */
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly priority: number;
  readonly createdAt: number;
  readonly updatedAt: number;
  /** Set once the job first became active. */
  readonly startedAt: number | null;
  /** Set once the job reached a terminal state. */
  readonly finishedAt: number | null;
  /** Present only when `status` is `"completed"`. */
  readonly result: TResult | null;
  /** The last failure message, kept across retries. */
  readonly error: string | null;
  /** `0`–`1`, as reported by the handler via `progress()`. */
  readonly progress: number;
  readonly idempotencyKey: string | null;
  /** True once `cancel()` has been called, even while still running. */
  readonly cancelRequested: boolean;
};

/**
 * An event on a job's output stream.
 *
 * `restarted` is the one to handle deliberately: the job is being re-attempted,
 * so everything streamed before it belongs to a generation that failed. Clear
 * whatever you have rendered and start again from the chunks that follow.
 */
export type JobEvent<TResult> =
  | { readonly id: string; readonly type: "chunk"; readonly data: string }
  | {
      readonly id: string;
      readonly type: "restarted";
      readonly attempt: number;
    }
  | {
      readonly id: string;
      readonly type: "progress";
      readonly progress: number;
    }
  | {
      readonly id: string;
      readonly type: "completed";
      readonly result: TResult;
    }
  | { readonly id: string; readonly type: "failed"; readonly error: string }
  | { readonly id: string; readonly type: "cancelled" };

/** The terminal event types — a `watch()` iterator ends after one of these. */
export type TerminalJobEvent<TResult> = Extract<
  JobEvent<TResult>,
  { type: "completed" | "failed" | "cancelled" }
>;

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
 * Thrown inside a handler when this worker no longer owns the job — its lease
 * expired and another worker reclaimed it. Keep working and you are burning
 * tokens on a run whose result will be discarded, so `emit()`, `progress()`,
 * and the automatic heartbeat all abort the job's signal and throw this.
 */
export class JobLeaseLostError extends Error {
  readonly jobId: string;
  constructor(jobId: string) {
    super(
      `Lost the lease on job "${jobId}" — another worker has reclaimed it. Raise leaseMs or lower heartbeatMs if this recurs.`
    );
    this.name = "JobLeaseLostError";
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

/** The handler's view of the job it is running. */
export type JobContext<TPayload> = {
  readonly id: string;
  readonly payload: TPayload;
  /** This run's attempt number, starting at `1`. */
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly priority: number;
  readonly createdAt: number;
  /**
   * Aborts when the job is cancelled or its lease is lost. Pass it straight to
   * `fetch`, the AI SDK, or any `AbortSignal`-aware call so a user pressing
   * stop actually stops the model.
   */
  readonly signal: AbortSignal;
  /**
   * Append a chunk to the job's output stream and renew the lease in the same
   * round trip — streaming tokens *is* the heartbeat. Resolves the stream entry
   * id. Throws `JobLeaseLostError` if the lease is gone.
   */
  emit(chunk: string): Promise<string>;
  /** Report progress as `0`–`1`. Also renews the lease. */
  progress(fraction: number): Promise<void>;
  /** Renew the lease explicitly. Returns `false` if the lease is already lost. */
  heartbeat(): Promise<boolean>;
};

export type EnqueueOptions = {
  /**
   * Explicit job id. Default: a random UUID. An id may be reused once its
   * previous job has finished, which starts a clean generation; reusing one
   * that is still waiting, scheduled, or active throws.
   */
  readonly id?: string;
  /** Delay before the job becomes runnable, in milliseconds. */
  readonly delayMs?: number;
  /** `0`–`9`; higher runs first. Default `0`. */
  readonly priority?: number;
  /** Attempts before dead-lettering. Defaults to the queue's `maxAttempts`. */
  readonly maxAttempts?: number;
  /**
   * Collapse duplicate work. While a job with this key is live, enqueuing again
   * returns that job instead of paying for a second generation.
   */
  readonly idempotencyKey?: string;
  /**
   * How long the key is held *after* the job completes, so a late duplicate
   * still gets the finished answer. The key is bound for the whole run however
   * long that takes, and is freed outright if the job fails or is cancelled.
   * Defaults to the queue's `resultTtlMs`, and never outlives the record.
   */
  readonly idempotencyTtlMs?: number;
};

export type EnqueueResult = {
  readonly id: string;
  /** True when an existing job was returned for the idempotency key. */
  readonly deduplicated: boolean;
};

export type QueueOptions<TPayload, TResult> = {
  /** Key namespace. Default `"queue"`. */
  readonly prefix?: string;
  /** Payload codec. Default `codecs.json<TPayload>()`. */
  readonly codec?: Codec<TPayload>;
  /** Result codec. Default `codecs.json<TResult>()`. */
  readonly resultCodec?: Codec<TResult>;
  /**
   * How long a reserved job stays owned without a heartbeat. Default `60000` —
   * sized for model calls, not for CPU work.
   */
  readonly leaseMs?: number;
  /** Default attempts before dead-lettering. Default `3`. */
  readonly maxAttempts?: number;
  /** First retry delay; doubles per attempt. Default `1000`. */
  readonly backoffMs?: number;
  /** Retry delay ceiling. Default `60000`. */
  readonly maxBackoffMs?: number;
  /**
   * How long a finished job's record and output stream survive. Default
   * `3600000` (one hour) — long enough for a client to reconnect and replay.
   */
  readonly resultTtlMs?: number;
  /** Cap on retained events per job. Default `10000`. */
  readonly eventsMaxLen?: number;
};

export type WorkerOptions = {
  /** Jobs to run at once. Default `1`. */
  readonly concurrency?: number;
  /** Override the queue's lease length for this worker. */
  readonly leaseMs?: number;
  /** Automatic heartbeat interval. Default `15000`. */
  readonly heartbeatMs?: number;
  /** Poll interval when no blocking connection is available. Default `1000`. */
  readonly pollMs?: number;
  /**
   * Decide whether a thrown error should be retried. Overrides the default
   * classification (everything retries except `TerminalJobError`).
   */
  readonly isRetryable?: (error: unknown) => boolean;
  /** Called for every unhandled worker-loop error, so failures are never silent. */
  readonly onError?: (error: unknown) => void;
};

export type Worker = {
  /**
   * Stop reserving new jobs and wait for in-flight ones to finish. In-flight
   * jobs are never killed — they keep their lease, so nothing is double-run.
   */
  stop(): Promise<void>;
  /** Jobs currently running on this worker. */
  readonly active: number;
};

export type WatchOptions = {
  /**
   * Resume after this stream entry id — pass the last id the client saw. Use
   * `"0"` (the default) to replay from the beginning.
   */
  readonly after?: string;
  /** Stop watching when this aborts. */
  readonly signal?: AbortSignal;
  /** Poll interval when no blocking connection is available. Default `1000`. */
  readonly pollMs?: number;
};

export type QueueStats = {
  readonly waiting: number;
  readonly scheduled: number;
  readonly active: number;
  readonly dead: number;
};

// ---------------------------------------------------------------------------
// Lua
// ---------------------------------------------------------------------------

// Every queue key shares one hash tag, so a queue occupies a single Cluster
// slot and these scripts may build per-job key names from a base prefix.
// `n()` formats doubles without scientific notation — Lua would render a
// 13-digit millisecond timestamp as "1.7e+12" and Redis would reject it.
const LUA_PRELUDE = `
local base = ARGV[1]
local function n(v) return string.format("%.0f", v) end
local function jobKey(id) return base .. ":job:" .. id end
local function eventsKey(id) return base .. ":events:" .. id end
`;

const enqueueScript = defineScript<
  readonly [
    base: string,
    id: string,
    payload: string,
    now: string,
    delayMs: string,
    priority: string,
    maxAttempts: string,
    idempotencyKey: string,
    idempotencyTtlMs: string,
    signalCap: string
  ],
  { id: string; deduplicated: boolean; liveStatus: string }
>({
  keyCount: 4,
  lua: `${LUA_PRELUDE}
-- @script enqueue
local ready, scheduled, seqKey, signal = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local id = ARGV[2]
local now = tonumber(ARGV[4])
local delay = tonumber(ARGV[5])
local priority = tonumber(ARGV[6])
local idem = ARGV[8]
local key = jobKey(id)

local idemKey = ""
if idem ~= "" then
  idemKey = base .. ":idem:" .. idem
  local existing = redis.call("GET", idemKey)
  if existing then return {existing, 1, ""} end
end

-- Reusing an id that has not finished yet cannot be made safe: the id would sit
-- in two lifecycle indexes at once and the supposedly single job would run
-- twice. Refuse before writing anything, the idempotency mapping included.
local prior = redis.call("HGET", key, "status")
if prior == "waiting" or prior == "scheduled" or prior == "active" then
  return {id, 2, prior}
end

if idemKey ~= "" then
  -- No expiry while the job is live. A mapping that lapsed mid-run let a
  -- duplicate request start a second, paid-for generation; settle starts its
  -- retention once there is a result to hand out, and frees it outright when
  -- there is not.
  redis.call("SET", idemKey, id)
end

-- Re-enqueuing an id that already reached a terminal state has to start from a
-- clean slate. HSET only overwrites the fields it names, so without this the
-- fresh job inherits the dead one's cancelRequested flag (a worker aborts
-- brand-new work on the first heartbeat), its result/finishedAt (get() reports
-- a "waiting" job as finished), and its resultTtlMs expiry (the record dies
-- while the id is still queued, and reserve pops an id with no payload). The
-- previous generation also leaves an event stream whose terminal entry ends a
-- watch() on the new job, a dead-letter entry, and its own idempotency mapping.
local priorIdem = redis.call("HGET", key, "idempotencyKey")
if priorIdem and priorIdem ~= "" and priorIdem ~= idem then
  redis.call("DEL", base .. ":idem:" .. priorIdem)
end
redis.call("ZREM", ready, id)
redis.call("ZREM", scheduled, id)
redis.call("ZREM", base .. ":dead", id)
redis.call("ZREM", base .. ":leases", id)
redis.call("DEL", key, eventsKey(id))

redis.call("HSET", key,
  "id", id,
  "payload", ARGV[3],
  "attempt", "0",
  "maxAttempts", ARGV[7],
  "priority", ARGV[6],
  "createdAt", n(now),
  "updatedAt", n(now),
  "progress", "0",
  "idempotencyKey", idem,
  "idemTtlMs", ARGV[9])

if delay > 0 then
  redis.call("HSET", key, "status", "scheduled")
  redis.call("ZADD", scheduled, n(now + delay), id)
else
  local seq = redis.call("INCR", seqKey)
  redis.call("HSET", key, "status", "waiting")
  redis.call("ZADD", ready, n((${MAX_PRIORITY} - priority) * ${PRIORITY_STRIDE} + seq), id)
  -- Doorbell: wake one blocked worker. Trimmed so an idle queue cannot grow it.
  redis.call("LPUSH", signal, "1")
  redis.call("LTRIM", signal, 0, tonumber(ARGV[10]) - 1)
end
return {id, 0, ""}
`,
  decode: (reply) => {
    const row = expectArray(reply, "enqueue");
    const outcome = toNumber(row[1]);
    return {
      id: expectString(row[0], "enqueue"),
      deduplicated: outcome === 1,
      liveStatus: outcome === 2 ? expectString(row[2], "enqueue") : ""
    };
  }
});

type ReservedRow = {
  readonly id: string;
  readonly payload: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly priority: number;
  readonly createdAt: number;
  readonly token: string;
};

const reserveScript = defineScript<
  readonly [
    base: string,
    now: string,
    leaseMs: string,
    token: string,
    eventsMaxLen: string,
    deadTtlMs: string
  ],
  { job: ReservedRow | null; wakeInMs: number }
>({
  keyCount: 6,
  lua: `${LUA_PRELUDE}
-- @script reserve
local ready, scheduled, leases = KEYS[1], KEYS[2], KEYS[3]
local seqKey, dead, signal = KEYS[4], KEYS[5], KEYS[6]
local now = tonumber(ARGV[2])
local leaseMs = tonumber(ARGV[3])
local token = ARGV[4]
local maxLen = ARGV[5]
local deadTtl = tonumber(ARGV[6])

local function pushReady(id, priority)
  local seq = redis.call("INCR", seqKey)
  redis.call("ZADD", ready, n((${MAX_PRIORITY} - priority) * ${PRIORITY_STRIDE} + seq), id)
end

-- 1. Promote every job whose delay (or backoff) has elapsed.
local due = redis.call("ZRANGEBYSCORE", scheduled, "-inf", n(now), "LIMIT", 0, 100)
for _, id in ipairs(due) do
  redis.call("ZREM", scheduled, id)
  local priority = tonumber(redis.call("HGET", jobKey(id), "priority") or "0")
  redis.call("HSET", jobKey(id), "status", "waiting", "updatedAt", n(now))
  pushReady(id, priority)
end

-- 2. Reclaim jobs whose lease expired — a worker crashed mid-run. Attempts
--    were already counted at reserve, so a crash loop dead-letters rather
--    than spinning forever.
local stalled = redis.call("ZRANGEBYSCORE", leases, "-inf", n(now), "LIMIT", 0, 100)
for _, id in ipairs(stalled) do
  redis.call("ZREM", leases, id)
  local key = jobKey(id)
  local attempt = tonumber(redis.call("HGET", key, "attempt") or "0")
  local maxAttempts = tonumber(redis.call("HGET", key, "maxAttempts") or "1")
  redis.call("HDEL", key, "token")
  if redis.call("HGET", key, "cancelRequested") == "1" then
    -- cancel() returns 3 for an active job: it flags the record and leaves the
    -- owning worker to abort its own signal. If that worker then dies, nobody
    -- settles the job, and reclaiming it as ordinary stalled work started a
    -- fresh, paid-for generation of something the caller already stopped.
    -- Settle it here instead, exactly as cancel() would have.
    redis.call("ZREM", ready, id)
    redis.call("ZREM", scheduled, id)
    redis.call("HSET", key, "status", "cancelled", "updatedAt", n(now),
      "finishedAt", n(now))
    redis.call("XADD", eventsKey(id), "MAXLEN", "~", maxLen, "*",
      "t", "cancelled", "d", "")
    redis.call("PEXPIRE", key, n(deadTtl))
    redis.call("PEXPIRE", eventsKey(id), n(deadTtl))
    local idem = redis.call("HGET", key, "idempotencyKey")
    if idem and idem ~= "" then redis.call("DEL", base .. ":idem:" .. idem) end
  elseif attempt < maxAttempts then
    redis.call("HSET", key, "status", "waiting", "updatedAt", n(now),
      "error", "Worker lease expired before the job finished")
    pushReady(id, tonumber(redis.call("HGET", key, "priority") or "0"))
  else
    redis.call("HSET", key, "status", "failed", "updatedAt", n(now),
      "finishedAt", n(now),
      "error", "Worker lease expired before the job finished")
    redis.call("ZADD", dead, n(now), id)
    redis.call("ZREMRANGEBYSCORE", dead, 0, n(now - deadTtl))
    redis.call("XADD", eventsKey(id), "MAXLEN", "~", maxLen, "*",
      "t", "failed", "d", "Worker lease expired before the job finished")
    redis.call("PEXPIRE", key, n(deadTtl))
    redis.call("PEXPIRE", eventsKey(id), n(deadTtl))
    local idem = redis.call("HGET", key, "idempotencyKey")
    if idem and idem ~= "" then redis.call("DEL", base .. ":idem:" .. idem) end
  end
end

-- 3. Take the head of the ready set.
local head = redis.call("ZPOPMIN", ready)
if not head or #head == 0 then
  -- Nothing to do. Tell the worker how long it may sleep: until the next
  -- scheduled job or the next lease expiry, whichever comes first. -1 means
  -- "nothing pending at all"; 0 means "already due".
  local wake = -1
  local function consider(score)
    local delta = tonumber(score) - now
    if delta < 0 then delta = 0 end
    if wake < 0 or delta < wake then wake = delta end
  end
  local nextScheduled = redis.call("ZRANGE", scheduled, 0, 0, "WITHSCORES")
  if nextScheduled[2] then consider(nextScheduled[2]) end
  local nextLease = redis.call("ZRANGE", leases, 0, 0, "WITHSCORES")
  if nextLease[2] then consider(nextLease[2]) end
  return {0, n(wake)}
end

local id = head[1]
local key = jobKey(id)
local attempt = tonumber(redis.call("HGET", key, "attempt") or "0") + 1

-- A re-attempt regenerates from scratch, so its output starts over too.
-- Leaving the previous attempt's partial chunks in place would make a resuming
-- client concatenate two generations. Announce the restart, then trim
-- everything before the marker: deleting the stream instead would reset the
-- last-generated id, and a marker recreated in the same millisecond can land
-- at or below the cursor a watcher already holds, which drops the restart
-- boundary and every chunk sharing that millisecond.
if attempt > 1 then
  local marker = redis.call("XADD", eventsKey(id), "MAXLEN", "~", maxLen, "*",
    "t", "restarted", "d", n(attempt))
  redis.call("XTRIM", eventsKey(id), "MINID", marker)
end

redis.call("HSET", key,
  "status", "active",
  "attempt", n(attempt),
  "token", token,
  "updatedAt", n(now),
  "startedAt", n(now))
redis.call("ZADD", leases, n(now + leaseMs), id)
-- Consume one doorbell token so it tracks queue depth rather than accumulating.
redis.call("LPOP", signal)

return {1, id,
  redis.call("HGET", key, "payload") or "",
  n(attempt),
  redis.call("HGET", key, "maxAttempts") or "1",
  redis.call("HGET", key, "priority") or "0",
  redis.call("HGET", key, "createdAt") or n(now),
  token}
`,
  decode: (reply) => {
    const row = expectArray(reply, "reserve");
    if (toNumber(row[0]) !== 1) {
      return { job: null, wakeInMs: toNumber(row[1]) };
    }
    return {
      wakeInMs: -1,
      job: {
        id: expectString(row[1], "reserve"),
        payload: expectString(row[2], "reserve"),
        attempt: toNumber(row[3]),
        maxAttempts: toNumber(row[4]),
        priority: toNumber(row[5]),
        createdAt: toNumber(row[6]),
        token: expectString(row[7], "reserve")
      }
    };
  }
});

/**
 * Renew a lease and read the cancel flag in one round trip, optionally
 * appending an event first. `type` is `""` for a bare heartbeat.
 */
const touchScript = defineScript<
  readonly [
    base: string,
    id: string,
    token: string,
    now: string,
    leaseMs: string,
    type: string,
    data: string,
    eventsMaxLen: string
  ],
  { held: boolean; cancelRequested: boolean; eventId: string }
>({
  keyCount: 1,
  lua: `${LUA_PRELUDE}
-- @script touch
local leases = KEYS[1]
local id, token = ARGV[2], ARGV[3]
local now = tonumber(ARGV[4])
local key = jobKey(id)

if redis.call("HGET", key, "token") ~= token then return {0, 0, ""} end
redis.call("ZADD", leases, n(now + tonumber(ARGV[5])), id)

local eventId = ""
local kind = ARGV[6]
if kind ~= "" then
  eventId = redis.call("XADD", eventsKey(id), "MAXLEN", "~", ARGV[8], "*",
    "t", kind, "d", ARGV[7])
  if kind == "progress" then
    redis.call("HSET", key, "progress", ARGV[7])
  end
end
redis.call("HSET", key, "updatedAt", n(now))

local cancelled = redis.call("HGET", key, "cancelRequested")
return {1, cancelled and 1 or 0, eventId}
`,
  decode: (reply) => {
    const row = expectArray(reply, "heartbeat");
    return {
      held: toNumber(row[0]) === 1,
      cancelRequested: toNumber(row[1]) === 1,
      eventId: typeof row[2] === "string" ? row[2] : ""
    };
  }
});

/**
 * Settle a job: 0 = lease lost, 1 = settled, 2 = settled `cancelled` because a
 * cancel had landed while the handler was still running.
 */
const settleScript = defineScript<
  readonly [
    base: string,
    id: string,
    token: string,
    now: string,
    status: string,
    payload: string,
    ttlMs: string,
    eventsMaxLen: string
  ],
  number
>({
  keyCount: 3,
  lua: `${LUA_PRELUDE}
-- @script settle
local leases, dead, ready = KEYS[1], KEYS[2], KEYS[3]
local id, token = ARGV[2], ARGV[3]
local now = tonumber(ARGV[4])
local status = ARGV[5]
local key = jobKey(id)

if redis.call("HGET", key, "token") ~= token then return 0 end

-- Cancellation wins the race with the handler's own outcome. cancel() already
-- promised the caller no result is coming, but the worker only learns of the
-- flag on its next heartbeat, which can be a whole interval after the handler
-- returned. Owning the lease decides who settles, not what they settle as.
local cancelled = 0
if status ~= "cancelled" and redis.call("HGET", key, "cancelRequested") == "1" then
  status = "cancelled"
  cancelled = 1
end

redis.call("ZREM", leases, id)
redis.call("ZREM", ready, id)
redis.call("HDEL", key, "token")
redis.call("HSET", key,
  "status", status,
  "updatedAt", n(now),
  "finishedAt", n(now))

if status == "completed" then
  redis.call("HSET", key, "result", ARGV[6])
  redis.call("XADD", eventsKey(id), "MAXLEN", "~", ARGV[8], "*",
    "t", "completed", "d", ARGV[6])
elseif status == "cancelled" then
  redis.call("XADD", eventsKey(id), "MAXLEN", "~", ARGV[8], "*",
    "t", "cancelled", "d", "")
else
  redis.call("HSET", key, "error", ARGV[6])
  redis.call("ZADD", dead, n(now), id)
  -- The job record expires after resultTtlMs but its dead-letter entry did
  -- not, so the set grew for the life of the deployment and dead() listed ids
  -- whose records were long gone. Trim to the same horizon.
  redis.call("ZREMRANGEBYSCORE", dead, 0, n(now - tonumber(ARGV[7])))
  redis.call("XADD", eventsKey(id), "MAXLEN", "~", ARGV[8], "*",
    "t", "failed", "d", ARGV[6])
end

local ttlMs = tonumber(ARGV[7])
local ttl = n(ttlMs)
redis.call("PEXPIRE", key, ttl)
redis.call("PEXPIRE", eventsKey(id), ttl)

-- An idempotency key points at a job that is in flight or succeeded, so a
-- duplicate request gets the finished answer instead of paying again. A job
-- that failed or was cancelled has no answer to hand out — free the key so the
-- caller can legitimately retry with it.
local idem = redis.call("HGET", key, "idempotencyKey")
if idem and idem ~= "" then
  if status == "completed" then
    -- The mapping was held with no expiry for the whole run; its retention
    -- starts here, now that there is a result behind it. Capped at the
    -- record's own TTL so a deduplicated id can never point at a record that
    -- has already expired.
    local hold = tonumber(redis.call("HGET", key, "idemTtlMs") or "0")
    if hold <= 0 or hold > ttlMs then hold = ttlMs end
    redis.call("PEXPIRE", base .. ":idem:" .. idem, n(hold))
  else
    redis.call("DEL", base .. ":idem:" .. idem)
  end
end
if cancelled == 1 then return 2 end
return 1
`,
  decode: (reply) => toNumber(reply)
});

/**
 * Reschedule a failed attempt. 0 = lease lost, 1 = retry scheduled, 2 = settled
 * `cancelled` instead because a cancel had landed during the attempt.
 */
const retryScript = defineScript<
  readonly [
    base: string,
    id: string,
    token: string,
    now: string,
    delayMs: string,
    error: string,
    ttlMs: string,
    eventsMaxLen: string
  ],
  number
>({
  keyCount: 2,
  lua: `${LUA_PRELUDE}
-- @script retry
local leases, scheduled = KEYS[1], KEYS[2]
local id, token = ARGV[2], ARGV[3]
local now = tonumber(ARGV[4])
local delay = tonumber(ARGV[5])
local key = jobKey(id)

if redis.call("HGET", key, "token") ~= token then return 0 end

-- Validate before the first write. Redis does not roll back what a script
-- already did, so a delay caught at the ZADD would leave the job marked
-- scheduled with no lease and no membership in any lifecycle index: nothing
-- can reserve it and wait() hangs forever.
if not (delay and delay >= 0 and delay < math.huge) then
  return redis.error_reply(
    "benni queue: retry delay must be a finite, non-negative number of milliseconds")
end

redis.call("ZREM", leases, id)
redis.call("HDEL", key, "token")

-- Cancellation wins over a retry too, or the queue schedules another paid
-- generation of work the caller already stopped.
if redis.call("HGET", key, "cancelRequested") == "1" then
  redis.call("HSET", key,
    "status", "cancelled",
    "updatedAt", n(now),
    "finishedAt", n(now),
    "error", ARGV[6])
  redis.call("XADD", eventsKey(id), "MAXLEN", "~", ARGV[8], "*",
    "t", "cancelled", "d", "")
  local ttl = n(tonumber(ARGV[7]))
  redis.call("PEXPIRE", key, ttl)
  redis.call("PEXPIRE", eventsKey(id), ttl)
  local idem = redis.call("HGET", key, "idempotencyKey")
  if idem and idem ~= "" then redis.call("DEL", base .. ":idem:" .. idem) end
  return 2
end

redis.call("HSET", key,
  "status", "scheduled",
  "updatedAt", n(now),
  "error", ARGV[6])
redis.call("ZADD", scheduled, n(now + delay), id)
return 1
`,
  decode: (reply) => toNumber(reply)
});

/**
 * Request cancellation.
 * 0 = unknown id, 1 = cancelled outright, 2 = already terminal, 3 = flagged
 * for the running worker to abort.
 */
const cancelScript = defineScript<
  readonly [
    base: string,
    id: string,
    now: string,
    ttlMs: string,
    eventsMaxLen: string
  ],
  number
>({
  keyCount: 3,
  lua: `${LUA_PRELUDE}
-- @script cancel
local ready, scheduled, leases = KEYS[1], KEYS[2], KEYS[3]
local id = ARGV[2]
local now = tonumber(ARGV[3])
local key = jobKey(id)

local status = redis.call("HGET", key, "status")
if not status then return 0 end
if status == "completed" or status == "failed" or status == "cancelled" then
  return 2
end

redis.call("HSET", key, "cancelRequested", "1", "updatedAt", n(now))

-- Active: the owning worker sees the flag on its next heartbeat or emit and
-- aborts its signal. Settling here would race that worker's own settle.
if status == "active" then return 3 end

redis.call("ZREM", ready, id)
redis.call("ZREM", scheduled, id)
redis.call("ZREM", leases, id)
redis.call("HSET", key, "status", "cancelled", "finishedAt", n(now))
redis.call("XADD", eventsKey(id), "MAXLEN", "~", ARGV[5], "*",
  "t", "cancelled", "d", "")
local ttl = n(tonumber(ARGV[4]))
redis.call("PEXPIRE", key, ttl)
redis.call("PEXPIRE", eventsKey(id), ttl)
local idem = redis.call("HGET", key, "idempotencyKey")
if idem and idem ~= "" then redis.call("DEL", base .. ":idem:" .. idem) end
return 1
`,
  decode: (reply) => toNumber(reply)
});

/** Move a dead-lettered job back to the ready set. 0 = not dead, 1 = requeued. */
const retryDeadScript = defineScript<
  readonly [
    base: string,
    id: string,
    now: string,
    maxAttempts: string,
    signalCap: string
  ],
  number
>({
  keyCount: 4,
  lua: `${LUA_PRELUDE}
-- @script retryDead
local ready, dead, seqKey, signal = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local id = ARGV[2]
local now = tonumber(ARGV[3])
local key = jobKey(id)

if redis.call("ZREM", dead, id) == 0 then return 0 end
if redis.call("EXISTS", key) == 0 then return 0 end

redis.call("HSET", key,
  "status", "waiting",
  "attempt", "0",
  "maxAttempts", ARGV[4],
  "updatedAt", n(now))
redis.call("HDEL", key, "finishedAt", "token", "result")
-- The record carried a result TTL from when it died; it is live again now.
redis.call("PERSIST", key)
-- Discard the failed attempt's output, terminal event included: a watcher
-- must not stop on the old "failed" event, nor render two generations.
redis.call("DEL", eventsKey(id))
local priority = tonumber(redis.call("HGET", key, "priority") or "0")
local seq = redis.call("INCR", seqKey)
redis.call("ZADD", ready, n((${MAX_PRIORITY} - priority) * ${PRIORITY_STRIDE} + seq), id)
-- Ring the doorbell, or an idle worker sleeps out its full block first.
redis.call("LPUSH", signal, "1")
redis.call("LTRIM", signal, 0, tonumber(ARGV[5]) - 1)
return 1
`,
  decode: (reply) => toNumber(reply)
});

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

/**
 * A job queue built for AI work: model calls that run for minutes, stream their
 * output, cost real money per attempt, and get cancelled by users mid-flight.
 *
 * Three things follow from that and shape the design:
 *
 * - **Leases are heartbeats, not idle timers.** A reserved job is owned for
 *   `leaseMs`, renewed while the handler runs. A ten-minute generation is
 *   ordinary, not a stall to be tuned around.
 * - **Every job has an output stream.** `ctx.emit(token)` appends to a capped
 *   per-job Redis stream and renews the lease in the same round trip, so
 *   `queue.watch(id, { after })` is a resumable SSE feed — a client that drops
 *   mid-generation replays from its last entry id instead of paying again.
 * - **Cancellation is first-class.** `queue.cancel(id)` aborts the handler's
 *   `AbortSignal`, so the in-flight `fetch` to the provider actually stops, and
 *   the job settles `cancelled` rather than `failed`.
 *
 * Job lifecycle lives in sorted sets (`ready`, `scheduled`, `leases`, `dead`) —
 * which is what gives delays, exponential backoff, priority, and dead-lettering
 * — while streams carry output, which is what streams are good at.
 *
 * ```ts
 * const jobs = queue<{ prompt: string }, string>(client, { prefix: "generate" });
 *
 * // Producer — runs anywhere, including the edge.
 * const { id } = await jobs.enqueue({ prompt }, { idempotencyKey: requestId });
 *
 * // Worker — a long-lived process.
 * jobs.worker(async (job) => {
 *   const { textStream } = streamText({
 *     model: openai("gpt-4o-mini"),
 *     prompt: job.payload.prompt,
 *     abortSignal: job.signal
 *   });
 *   let text = "";
 *   for await (const delta of textStream) {
 *     text += delta;
 *     await job.emit(delta);
 *   }
 *   return text;
 * }, { concurrency: 8 });
 *
 * // Consumer — resumable, ends on the terminal event.
 * for await (const event of jobs.watch(id, { after: lastSeenId })) {
 *   if (event.type === "chunk") write(event.data);
 * }
 * ```
 *
 * Every key is hash-tagged into one Cluster slot, so the queue is slot-safe.
 * `enqueue`, `cancel`, `get`, and `watch` need only `EVALSHA` plus stream reads
 * and run over `benni/upstash` on the edge; `worker()` needs a long-lived
 * connection and blocks on a doorbell list when the adapter provides
 * `session()`, falling back to polling when it does not.
 */
function createQueue<TPayload, TResult = unknown>(
  client: RedisClient,
  options?: QueueOptions<TPayload, TResult>
) {
  const prefix = options?.prefix ?? DEFAULT_PREFIX;
  const codec = options?.codec ?? codecs.json<TPayload>();
  const resultCodec = options?.resultCodec ?? codecs.json<TResult>();
  const leaseMs = positiveInt(options?.leaseMs ?? DEFAULT_LEASE_MS, "leaseMs");
  const maxAttempts = positiveInt(
    options?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
    "maxAttempts"
  );
  const backoffMs = positiveInt(
    options?.backoffMs ?? DEFAULT_BACKOFF_MS,
    "backoffMs"
  );
  const maxBackoffMs = positiveInt(
    options?.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS,
    "maxBackoffMs"
  );
  const resultTtlMs = positiveInt(
    options?.resultTtlMs ?? DEFAULT_RESULT_TTL_MS,
    "resultTtlMs"
  );
  const eventsMaxLen = positiveInt(
    options?.eventsMaxLen ?? DEFAULT_EVENTS_MAX_LEN,
    "eventsMaxLen"
  );

  // One hash tag over every key keeps the whole queue in a single Cluster slot,
  // which is what lets the scripts derive per-job key names from `base`.
  if (prefix === "" || prefix.startsWith("}")) {
    // Every queue key hangs off this one tag so the scripts can touch several
    // at once. An empty tag is no tag: Redis hashes the whole key instead and
    // the keys scatter, which breaks every script on a cluster.
    throw new ValidationError(
      `queue prefix must form a non-empty Redis hash tag, received ${JSON.stringify(prefix)}`
    );
  }
  const base = `{${prefix}}`;
  const readyKey = `${base}:ready`;
  const scheduledKey = `${base}:scheduled`;
  const leasesKey = `${base}:leases`;
  const deadKey = `${base}:dead`;
  const seqKey = `${base}:seq`;
  const signalKey = `${base}:signal`;
  const jobKey = (id: string) => `${base}:job:${id}`;
  const eventsKey = (id: string) => `${base}:events:${id}`;

  const scripts = createScriptRunner(client);

  async function enqueue(
    payload: TPayload,
    enqueueOptions?: EnqueueOptions
  ): Promise<EnqueueResult> {
    const id = enqueueOptions?.id ?? globalThis.crypto.randomUUID();
    if (id.length === 0) {
      throw new ValidationError("queue job id must not be empty");
    }
    const delayMs = nonNegativeInt(enqueueOptions?.delayMs ?? 0, "delayMs");
    const priority = priorityOf(enqueueOptions?.priority ?? 0);
    const attempts = positiveInt(
      enqueueOptions?.maxAttempts ?? maxAttempts,
      "maxAttempts"
    );
    const idempotencyKey = enqueueOptions?.idempotencyKey ?? "";
    const idempotencyTtlMs = positiveInt(
      enqueueOptions?.idempotencyTtlMs ?? resultTtlMs,
      "idempotencyTtlMs"
    );
    const outcome = await scripts.run(
      enqueueScript,
      [readyKey, scheduledKey, seqKey, signalKey],
      [
        base,
        id,
        codec.encode(payload),
        String(Date.now()),
        String(delayMs),
        String(priority),
        String(attempts),
        idempotencyKey,
        String(idempotencyTtlMs),
        SIGNAL_CAP
      ]
    );
    if (outcome.liveStatus !== "") {
      throw new ValidationError(
        `queue job id ${JSON.stringify(id)} is still live (status "${outcome.liveStatus}"); cancel it or wait for it to finish before reusing the id`
      );
    }
    return { id: outcome.id, deduplicated: outcome.deduplicated };
  }

  async function get(id: string): Promise<Job<TPayload, TResult> | null> {
    const reply = await client.send(["HGETALL", jobKey(id)]);
    const record = toRecord(reply);
    if (record === null || record.id === undefined) return null;
    return decodeJob(record, codec, resultCodec);
  }

  async function cancel(id: string): Promise<boolean> {
    const outcome = await scripts.run(
      cancelScript,
      [readyKey, scheduledKey, leasesKey],
      [base, id, String(Date.now()), String(resultTtlMs), String(eventsMaxLen)]
    );
    // 1 = cancelled outright, 3 = flagged for the running worker. Both mean the
    // job will not produce a result; 0 (unknown) and 2 (already done) do not.
    return outcome === 1 || outcome === 3;
  }

  async function stats(): Promise<QueueStats> {
    const replies = await client.pipeline([
      ["ZCARD", readyKey],
      ["ZCARD", scheduledKey],
      ["ZCARD", leasesKey],
      ["ZCARD", deadKey]
    ]);
    return {
      waiting: toNumber(replies[0]),
      scheduled: toNumber(replies[1]),
      active: toNumber(replies[2]),
      dead: toNumber(replies[3])
    };
  }

  async function dead(listOptions?: { count?: number }): Promise<string[]> {
    const count = positiveInt(listOptions?.count ?? 50, "count");
    const reply = await client.send(["ZRANGE", deadKey, 0, count - 1]);
    if (!Array.isArray(reply)) {
      throw new ReplyShapeError("Expected ZRANGE to return an array", reply);
    }
    return reply.map((entry) => expectString(entry, "dead"));
  }

  async function retryDead(
    id: string,
    retryOptions?: { maxAttempts?: number }
  ): Promise<boolean> {
    const attempts = positiveInt(
      retryOptions?.maxAttempts ?? maxAttempts,
      "maxAttempts"
    );
    const outcome = await scripts.run(
      retryDeadScript,
      [readyKey, deadKey, seqKey, signalKey],
      [base, id, String(Date.now()), String(attempts), SIGNAL_CAP]
    );
    return outcome === 1;
  }

  // -- event stream ---------------------------------------------------------

  async function readEvents(
    id: string,
    after: string
  ): Promise<Array<{ id: string; type: string; data: string }>> {
    const reply = await client.send([
      "XRANGE",
      eventsKey(id),
      exclusive(after),
      "+"
    ]);
    return decodeRawEntries(reply, "XRANGE");
  }

  /** The id of the newest event on a job's stream, or `"0"` if it has none. */
  async function lastEventId(id: string): Promise<string> {
    const reply = await client.send([
      "XREVRANGE",
      eventsKey(id),
      "+",
      "-",
      "COUNT",
      1
    ]);
    return decodeRawEntries(reply, "XREVRANGE")[0]?.id ?? "0";
  }

  /**
   * Async-iterate a job's output, ending after its terminal event. Resumable:
   * pass the last entry id the client received as `after` and nothing is
   * replayed twice and nothing is missed.
   */
  async function* watch(
    id: string,
    watchOptions?: WatchOptions
  ): AsyncGenerator<JobEvent<TResult>, void, undefined> {
    const pollMs = positiveInt(
      watchOptions?.pollMs ?? DEFAULT_POLL_MS,
      "pollMs"
    );
    const signal = watchOptions?.signal;
    let cursor = watchOptions?.after ?? "0";
    let session: RedisSession | null = null;
    // Adapters without a dedicated connection (HTTP/edge) leave `session`
    // undefined; one that has it but cannot lease now falls back the same way
    // rather than failing the whole watch.
    let sessionUnavailable = client.session === undefined;

    /** Yields decoded events; resolves true once a terminal one is seen. */
    async function* emitAll(
      entries: Array<{ id: string; type: string; data: string }>
    ): AsyncGenerator<JobEvent<TResult>, boolean, undefined> {
      for (const entry of entries) {
        cursor = entry.id;
        const event = decodeEvent(entry, resultCodec);
        if (event === null) continue;
        yield event;
        if (isTerminalType(event.type)) return true;
      }
      return false;
    }

    try {
      // Backlog first — everything already written after the caller's cursor.
      if (yield* emitAll(await readEvents(id, cursor))) return;

      // Then tail. A dedicated connection makes this a blocking read; on an
      // adapter without one (HTTP/edge) it degrades to polling.
      for (;;) {
        if (signal?.aborted) return;

        if (session === null && !sessionUnavailable && client.session) {
          try {
            session = await client.session();
          } catch {
            sessionUnavailable = true;
          }
        }

        let entries: Array<{ id: string; type: string; data: string }>;
        if (session !== null && !session.closed) {
          entries = decodeXread(
            await session.send([
              "XREAD",
              "BLOCK",
              pollMs,
              "STREAMS",
              eventsKey(id),
              cursor
            ])
          );
        } else {
          entries = decodeXread(
            await client.send(["XREAD", "STREAMS", eventsKey(id), cursor])
          );
          if (entries.length === 0) await sleep(pollMs);
        }

        if (yield* emitAll(entries)) return;

        // Nothing arrived. If the record is gone the job finished long enough
        // ago that its result TTL elapsed — no terminal event is ever coming.
        if (
          entries.length === 0 &&
          (await client.send(["EXISTS", jobKey(id)])) === 0
        ) {
          throw new JobNotFoundError(id);
        }
      }
    } finally {
      await session?.close().catch(() => {
        // The tail connection is disposable; a failed close must not mask the
        // iteration's own outcome.
      });
    }
  }

  /**
   * Resolve once the job reaches a terminal state. Returns the completed
   * result, or throws with the failure message. Rejects with `JobNotFoundError`
   * if the job is unknown or its result TTL has elapsed.
   */
  async function wait(
    id: string,
    waitOptions?: { signal?: AbortSignal; pollMs?: number }
  ): Promise<TResult> {
    // Read the cursor BEFORE the status: a job that settles between the two
    // calls is caught by the status check, and one that settles after it writes
    // a terminal event past this cursor. Reversing the order would drop both.
    const cursor = await lastEventId(id);
    const existing = await get(id);
    if (existing === null) throw new JobNotFoundError(id);
    if (existing.status === "completed") return existing.result as TResult;
    if (existing.status === "failed") {
      throw new Error(existing.error ?? `Job "${id}" failed`);
    }
    if (existing.status === "cancelled") {
      throw new Error(`Job "${id}" was cancelled`);
    }

    for await (const event of watch(id, {
      after: cursor,
      signal: waitOptions?.signal,
      pollMs: waitOptions?.pollMs
    })) {
      if (event.type === "completed") return event.result;
      if (event.type === "failed") throw new Error(event.error);
      if (event.type === "cancelled") {
        throw new Error(`Job "${id}" was cancelled`);
      }
    }
    // The iterator only ends early when the caller's signal aborted.
    throw new Error(`Stopped waiting for job "${id}"`);
  }

  // -- worker ---------------------------------------------------------------

  /**
   * Run `handler` against jobs from this queue until `stop()`.
   *
   * The handler's return value is the job's result. Throwing retries with
   * exponential backoff until `maxAttempts`, then dead-letters — except
   * `TerminalJobError` (dead-letter immediately) and `RetryJobError` (retry
   * after an explicit delay, for provider `Retry-After`).
   */
  function worker(
    handler: (job: JobContext<TPayload>) => Promise<TResult> | TResult,
    workerOptions?: WorkerOptions
  ): Worker {
    const concurrency = positiveInt(
      workerOptions?.concurrency ?? DEFAULT_CONCURRENCY,
      "concurrency"
    );
    const workerLeaseMs = positiveInt(
      workerOptions?.leaseMs ?? leaseMs,
      "leaseMs"
    );
    const heartbeatMs = positiveInt(
      workerOptions?.heartbeatMs ?? DEFAULT_HEARTBEAT_MS,
      "heartbeatMs"
    );
    const pollMs = positiveInt(
      workerOptions?.pollMs ?? DEFAULT_POLL_MS,
      "pollMs"
    );
    const isRetryable =
      workerOptions?.isRetryable ??
      ((error: unknown) => !(error instanceof TerminalJobError));
    const onError =
      workerOptions?.onError ??
      ((error: unknown) => {
        console.error("[benni queue] worker error", error);
      });

    let running = true;
    const inFlight = new Set<Promise<void>>();
    let doorbell: RedisSession | null = null;
    let doorbellUnavailable = client.session === undefined;
    let slotFreed: (() => void) | null = null;

    function releaseSlot() {
      const notify = slotFreed;
      slotFreed = null;
      notify?.();
    }

    /** Renew the lease, optionally appending an event. */
    async function touch(
      id: string,
      token: string,
      type: "" | "chunk" | "progress",
      data: string
    ) {
      return scripts.run(
        touchScript,
        [leasesKey],
        [
          base,
          id,
          token,
          String(Date.now()),
          String(workerLeaseMs),
          type,
          data,
          String(eventsMaxLen)
        ]
      );
    }

    /**
     * Encode a handler's return value for storage.
     *
     * Two cases the plain codec call got wrong. A `queue<P, void>` handler
     * returns `undefined`, which the default JSON codec refuses — that is a
     * *successful* job, so it is stored as JSON null and reads back as
     * `result: null`. And a genuinely unencodable result is terminal: the
     * handler already ran, so retrying it would repeat the side effect
     * `maxAttempts` times and still dead-letter.
     */
    function encodeResult(id: string, result: TResult): string {
      if (result === undefined) return "null";
      try {
        return resultCodec.encode(result);
      } catch (cause) {
        throw new TerminalJobError(
          `Job "${id}" succeeded but its result could not be encoded, so it ` +
            "cannot be recorded. The handler already ran; it will not be retried.",
          { cause }
        );
      }
    }

    async function run(reserved: ReservedRow): Promise<void> {
      // Decode before the heartbeat starts. A throw here used to escape past
      // the try/finally below with the interval already running, leaving a
      // zombie timer renewing the lease forever — the job stayed `active` and
      // was never reclaimed, retried, or dead-lettered.
      let payload: TPayload;
      try {
        payload = codec.decode(reserved.payload);
      } catch (error) {
        // No attempt can make this payload decodable, so retrying would only
        // hold the lease through every one of them.
        await settle(reserved, "failed", errorMessage(error));
        onError(error);
        return;
      }

      const controller = new AbortController();
      let cancelled = false;
      let leaseLost = false;

      function onCancelled() {
        cancelled = true;
        controller.abort(new Error(`Job "${reserved.id}" was cancelled`));
      }
      function onLeaseLost() {
        leaseLost = true;
        controller.abort(new JobLeaseLostError(reserved.id));
      }

      // Automatic heartbeat: a handler that never emits still keeps its lease,
      // and cancellation still reaches it within one interval.
      const timer = setInterval(() => {
        void touch(reserved.id, reserved.token, "", "").then(
          (state) => {
            if (!state.held) onLeaseLost();
            else if (state.cancelRequested) onCancelled();
          },
          (error) => onError(error)
        );
      }, heartbeatMs);
      // Never keep the process alive for a heartbeat alone.
      (timer as { unref?: () => void }).unref?.();

      const context: JobContext<TPayload> = {
        id: reserved.id,
        payload,
        attempt: reserved.attempt,
        maxAttempts: reserved.maxAttempts,
        priority: reserved.priority,
        createdAt: reserved.createdAt,
        signal: controller.signal,
        async emit(chunk: string) {
          const state = await touch(
            reserved.id,
            reserved.token,
            "chunk",
            chunk
          );
          if (!state.held) {
            onLeaseLost();
            throw new JobLeaseLostError(reserved.id);
          }
          if (state.cancelRequested) onCancelled();
          return state.eventId;
        },
        async progress(fraction: number) {
          const clamped = Math.min(1, Math.max(0, fraction));
          const state = await touch(
            reserved.id,
            reserved.token,
            "progress",
            String(clamped)
          );
          if (!state.held) {
            onLeaseLost();
            throw new JobLeaseLostError(reserved.id);
          }
          if (state.cancelRequested) onCancelled();
        },
        async heartbeat() {
          const state = await touch(reserved.id, reserved.token, "", "");
          if (!state.held) onLeaseLost();
          else if (state.cancelRequested) onCancelled();
          return state.held;
        }
      };

      try {
        const result = await handler(context);
        // A cancel that lands during the final tokens still wins: the user
        // asked to stop, so do not record a result they will not see.
        if (cancelled) {
          await settle(reserved, "cancelled", "");
          return;
        }
        await settle(reserved, "completed", encodeResult(reserved.id, result));
      } catch (error) {
        if (leaseLost) return; // Another worker owns it; touching it would race.
        if (cancelled) {
          await settle(reserved, "cancelled", "");
          return;
        }
        await failed(reserved, error);
      } finally {
        clearInterval(timer);
      }
    }

    async function settle(
      reserved: ReservedRow,
      status: "completed" | "cancelled" | "failed",
      payload: string
    ) {
      try {
        await scripts.run(
          settleScript,
          [leasesKey, deadKey, readyKey],
          [
            base,
            reserved.id,
            reserved.token,
            String(Date.now()),
            status,
            payload,
            String(resultTtlMs),
            String(eventsMaxLen)
          ]
        );
      } catch (error) {
        // The lease is the backstop: an unsettled job is reclaimed and retried
        // rather than lost, so a failed settle must not take down the worker.
        onError(error);
      }
    }

    async function failed(reserved: ReservedRow, error: unknown) {
      const message = errorMessage(error);
      const retry =
        reserved.attempt < reserved.maxAttempts && isRetryable(error);
      if (!retry) {
        await settle(reserved, "failed", message);
        return;
      }
      const delayMs =
        error instanceof RetryJobError
          ? error.retryAfterMs
          : backoffFor(reserved.attempt, backoffMs, maxBackoffMs);
      try {
        await scripts.run(
          retryScript,
          [leasesKey, scheduledKey],
          [
            base,
            reserved.id,
            reserved.token,
            String(Date.now()),
            String(delayMs),
            message,
            String(resultTtlMs),
            String(eventsMaxLen)
          ]
        );
      } catch (scheduleError) {
        onError(scheduleError);
      }
    }

    /** Sleep until there is plausibly work, or until `stop()`. */
    async function waitForWork(wakeInMs: number) {
      if (!running) return;
      // `wakeInMs` is when the next scheduled job or lease expiry comes due;
      // never sleep past it, and never block long enough that stop() drags.
      const blockMs = Math.min(
        DEFAULT_IDLE_BLOCK_MS,
        wakeInMs >= 0 ? Math.max(wakeInMs, 1) : DEFAULT_IDLE_BLOCK_MS
      );
      if (doorbell === null && !doorbellUnavailable && client.session) {
        try {
          doorbell = await client.session();
        } catch {
          // No dedicated connection to spare — poll from here on.
          doorbellUnavailable = true;
        }
      }
      if (!running) return;
      if (doorbell !== null && !doorbell.closed) {
        // BLPOP wakes the instant a producer enqueues, so pickup latency is a
        // round trip rather than a poll interval.
        try {
          await doorbell.send([
            "BLPOP",
            signalKey,
            (blockMs / 1000).toFixed(3)
          ]);
        } catch (error) {
          // stop() closes this connection to cut a blocked BLPOP short; that
          // rejection is the intended signal, not a failure worth reporting.
          if (running) onError(error);
        }
        return;
      }
      await sleep(Math.min(blockMs, pollMs));
    }

    async function dispatch() {
      while (running) {
        if (inFlight.size >= concurrency) {
          await new Promise<void>((resolve) => {
            slotFreed = resolve;
          });
          continue;
        }
        let reserved: ReservedRow | null = null;
        let wakeInMs = -1;
        try {
          const outcome = await scripts.run(
            reserveScript,
            [readyKey, scheduledKey, leasesKey, seqKey, deadKey, signalKey],
            [
              base,
              String(Date.now()),
              String(workerLeaseMs),
              globalThis.crypto.randomUUID(),
              String(eventsMaxLen),
              String(resultTtlMs)
            ]
          );
          reserved = outcome.job;
          wakeInMs = outcome.wakeInMs;
        } catch (error) {
          onError(error);
          await sleep(pollMs);
          continue;
        }

        if (reserved === null) {
          if (!running) break;
          await waitForWork(wakeInMs);
          continue;
        }

        const task = run(reserved)
          .catch(onError)
          .finally(() => {
            inFlight.delete(task);
            releaseSlot();
          });
        inFlight.add(task);
      }
    }

    const loop = dispatch().catch(onError);

    return {
      get active() {
        return inFlight.size;
      },
      async stop() {
        running = false;
        doorbellUnavailable = true; // never lease a replacement while stopping
        releaseSlot();
        // Close the doorbell first: BLPOP would otherwise hold the loop for the
        // rest of its timeout. Adapters reject the in-flight command on close.
        const blocked = doorbell;
        doorbell = null;
        await blocked?.close().catch(() => {});
        await loop;
        await Promise.allSettled([...inFlight]);
      }
    };
  }

  return {
    enqueue,
    get,
    cancel,
    watch,
    wait,
    worker,
    stats,
    dead,
    retryDead,
    /** The Redis key holding a job's record. */
    jobKey,
    /** The Redis key holding a job's output stream. */
    eventsKey
  };
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/** Full jitter over an exponential curve, so retries of a batch fan out. */
function backoffFor(attempt: number, baseMs: number, maxMs: number): number {
  const ceiling = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(ceiling / 2 + Math.random() * (ceiling / 2));
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  return String(error);
}

function isTerminalType(type: string): boolean {
  return type === "completed" || type === "failed" || type === "cancelled";
}

/** XRANGE is inclusive; `(id` asks for strictly-after (Redis 6.2+). */
function exclusive(cursor: string): string {
  return cursor === "0" || cursor === "-" ? "-" : `(${cursor}`;
}

function decodeEvent<TResult>(
  entry: { id: string; type: string; data: string },
  resultCodec: Codec<TResult>
): JobEvent<TResult> | null {
  switch (entry.type) {
    case "chunk":
      return { id: entry.id, type: "chunk", data: entry.data };
    case "restarted":
      return {
        id: entry.id,
        type: "restarted",
        attempt: Number(entry.data) || 0
      };
    case "progress":
      return {
        id: entry.id,
        type: "progress",
        progress: Number(entry.data) || 0
      };
    case "completed":
      return {
        id: entry.id,
        type: "completed",
        result: resultCodec.decode(entry.data)
      };
    case "failed":
      return { id: entry.id, type: "failed", error: entry.data };
    case "cancelled":
      return { id: entry.id, type: "cancelled" };
    default:
      // Forward compatibility: ignore event kinds a newer writer added.
      return null;
  }
}

function decodeRawEntries(
  reply: RedisReply,
  command: string
): Array<{ id: string; type: string; data: string }> {
  if (reply === null) return [];
  if (!Array.isArray(reply)) {
    throw new ReplyShapeError(`Expected ${command} to return an array`, reply);
  }
  return reply.map((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) {
      throw new ReplyShapeError(
        `Expected ${command} to return id/fields pairs`,
        entry
      );
    }
    const fields = entry[1];
    if (!Array.isArray(fields)) {
      throw new ReplyShapeError(
        `Expected ${command} to return field/value pairs`,
        fields
      );
    }
    let type = "";
    let data = "";
    for (let index = 0; index < fields.length - 1; index += 2) {
      if (fields[index] === "t") type = String(fields[index + 1] ?? "");
      if (fields[index] === "d") data = String(fields[index + 1] ?? "");
    }
    return { id: expectString(entry[0], command), type, data };
  });
}

function decodeXread(
  reply: RedisReply
): Array<{ id: string; type: string; data: string }> {
  if (reply === null) return [];
  const pairs = xreadStreamPairs(reply);
  return pairs.flatMap(([, entries]) => decodeRawEntries(entries, "XREAD"));
}

function toRecord(reply: RedisReply): Record<string, string> | null {
  if (reply === null) return null;
  const record: Record<string, string> = {};
  if (reply instanceof Map) {
    for (const [field, value] of reply) {
      record[String(field)] = String(value);
    }
    return record;
  }
  if (!Array.isArray(reply)) {
    throw new ReplyShapeError("Expected HGETALL to return a map", reply);
  }
  if (reply.length === 0) return null;
  for (let index = 0; index < reply.length - 1; index += 2) {
    record[String(reply[index])] = String(reply[index + 1]);
  }
  return record;
}

function decodeJob<TPayload, TResult>(
  record: Record<string, string>,
  codec: Codec<TPayload>,
  resultCodec: Codec<TResult>
): Job<TPayload, TResult> {
  const status = record.status ?? "waiting";
  return {
    id: record.id ?? "",
    status: isJobStatus(status) ? status : "waiting",
    payload: codec.decode(record.payload ?? ""),
    attempt: Number(record.attempt ?? "0"),
    maxAttempts: Number(record.maxAttempts ?? "1"),
    priority: Number(record.priority ?? "0"),
    createdAt: Number(record.createdAt ?? "0"),
    updatedAt: Number(record.updatedAt ?? "0"),
    startedAt: record.startedAt === undefined ? null : Number(record.startedAt),
    finishedAt:
      record.finishedAt === undefined ? null : Number(record.finishedAt),
    result:
      record.result === undefined ? null : resultCodec.decode(record.result),
    error: record.error ?? null,
    progress: Number(record.progress ?? "0"),
    idempotencyKey:
      record.idempotencyKey === undefined || record.idempotencyKey === ""
        ? null
        : record.idempotencyKey,
    cancelRequested: record.cancelRequested === "1"
  };
}

const JOB_STATUSES = new Set<string>([
  "waiting",
  "scheduled",
  "active",
  "completed",
  "failed",
  "cancelled"
]);

function isJobStatus(value: string): value is JobStatus {
  return JOB_STATUSES.has(value);
}

function expectArray(reply: RedisReply, name: string): readonly RedisReply[] {
  if (!Array.isArray(reply)) {
    throw new ReplyShapeError(
      `Expected the queue ${name} script to return an array`,
      reply
    );
  }
  return reply;
}

function expectString(reply: RedisReply, name: string): string {
  if (typeof reply === "string") return reply;
  if (reply instanceof Uint8Array) return new TextDecoder().decode(reply);
  throw new ReplyShapeError(
    `Expected the queue ${name} reply to contain a string`,
    reply
  );
}

function toNumber(reply: RedisReply): number {
  if (typeof reply === "number") return reply;
  if (typeof reply === "bigint") return Number(reply);
  if (typeof reply === "string") return Number(reply);
  return 0;
}

/** The queue {@link queue} returns. */
export type QueueStore<TPayload, TResult = unknown> = ReturnType<
  typeof createQueue<TPayload, TResult>
>;

/** {@link QueueOptions} plus the client, for the single-argument form. */
export type QueueConfig<TPayload, TResult> = QueueOptions<TPayload, TResult> & {
  /** The client, a promise of one, a factory, or a benni handle. */
  readonly client: ClientSource;
};

export function queue<TPayload, TResult = unknown>(
  config: QueueConfig<TPayload, TResult>
): QueueStore<TPayload, TResult>;
export function queue<TPayload, TResult = unknown>(
  client: ClientSource,
  options?: QueueOptions<TPayload, TResult>
): QueueStore<TPayload, TResult>;
export function queue<TPayload, TResult = unknown>(
  source: ClientSource | QueueConfig<TPayload, TResult>,
  options?: QueueOptions<TPayload, TResult>
): QueueStore<TPayload, TResult> {
  const args = clientArgs<QueueOptions<TPayload, TResult>>(source, options);
  return createQueue<TPayload, TResult>(args.client, args.options);
}

/**
 * A queue declared as a schema value, so it lands in `redis.query` next to the
 * data stores and needs no client of its own.
 * @example
 * ```ts
 * // schema.ts
 * export const generate = queue<{ prompt: string }, string>("generate");
 * // app.ts
 * const { id } = await redis.query.generate.enqueue({ prompt });
 * ```
 */
export type QueueSchema<TPayload, TResult> = InferAnchors<TPayload, TResult> &
  QueueOptions<TPayload, TResult> & {
    readonly kind: "queue";
    readonly prefix: string;
  };

const queueBinding: StoreBinding = {
  resource: (ctx, schema: QueueSchema<unknown, unknown>) =>
    createQueue(ctx.client, schema)
};

/** Build a {@link QueueSchema}. Exported as `queue` from `benni/schema`. */
export function defineQueue<TPayload, TResult = unknown>(
  prefix: string,
  options?: QueueOptions<TPayload, TResult>
): QueueSchema<TPayload, TResult> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    ...options,
    kind: "queue",
    prefix
  } as QueueSchema<TPayload, TResult>;
  return withStore(schema, queueBinding);
}

function priorityOf(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0 || value > MAX_PRIORITY) {
    throw new ValidationError(
      `queue priority must be an integer between 0 and ${MAX_PRIORITY}, received ${value}`
    );
  }
  return value;
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `queue ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

function nonNegativeInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ValidationError(
      `queue ${name} must be a non-negative integer, received ${value}`
    );
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

import { codecs } from "../core/codecs.js";
import { ReplyShapeError, ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import {
  type StoreBinding,
  type StoreContext,
  withStore
} from "../core/store.js";
import { xreadStreamPairs } from "../core/stream.js";
import type {
  Codec,
  InferAnchors,
  RedisClient,
  RedisReply,
  RedisSession
} from "../core/types.js";
import {
  JobCancelledError,
  JobFailedError,
  JobLeaseLostError,
  JobNotFoundError,
  RetryJobError,
  TerminalJobError,
  WorkerStoppedError
} from "./errors.js";

const DEFAULT_PREFIX = "queue";
const DEFAULT_LEASE_MS = 60_000;
// The worker renews on a quarter of the lease, the ratio `lock` and `semaphore`
// use (leaseMs 60000 / heartbeatMs 15000 by default): three renewals in a row
// may fail outright before the lease could lapse.
const HEARTBEAT_DIVISOR = 4;
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

/**
 * A job record as stored in Redis. Every timestamp is Redis server time (`TIME`
 * inside the scripts), in epoch milliseconds, so records written by workers
 * with different clocks still order correctly.
 */
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
 *
 * `truncated` means events are missing between the previous event (or your
 * `after` cursor) and the next one: the stream keeps only the newest
 * `eventsMaxLen` events, and the ones you had not read yet were trimmed. What
 * you have rendered is incomplete, and the chunks that follow continue from
 * mid-output. The terminal `completed` event still carries the whole result,
 * so render that, or call `get()`. Its `id` is the position *before* the gap,
 * so storing it as your cursor is safe.
 */
export type JobEvent<TResult> =
  | { readonly id: string; readonly type: "chunk"; readonly data: string }
  | { readonly id: string; readonly type: "truncated" }
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

// TODO(consistency pass): the queue errors extend Error directly because there
// is no shared BenniError base yet. Rebase all of them onto it once it lands.

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
   * Aborts when the job is cancelled, its lease is lost, or `stop()` hands it
   * back to the queue; `signal.reason` is a `JobCancelledError`,
   * `JobLeaseLostError`, or `WorkerStoppedError` respectively. Pass it straight
   * to `fetch`, the AI SDK, or any `AbortSignal`-aware call so a user pressing
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
  /**
   * Events retained per job; older ones are trimmed, in batches of a tenth of
   * the cap. Default `10000`. Emitting a token per event, a long generation
   * can outgrow it: batch several tokens per `emit()`, or raise this. A
   * `watch()` resuming from before the retained window gets a `truncated`
   * event rather than a silent skip.
   */
  readonly eventsMaxLen?: number;
};

export type WorkerOptions = {
  /** Jobs to run at once. Default `1`. */
  readonly concurrency?: number;
  /** Override the queue's lease length for this worker. */
  readonly leaseMs?: number;
  /**
   * Automatic heartbeat interval. Default a quarter of `leaseMs` (`15000` at
   * the default lease). Must be at most half of `leaseMs`, so a renewal and a
   * retry both fit before the lease could lapse; a larger value throws a
   * `ValidationError`. Also bounds how long a `cancel()` takes to reach a
   * handler that does not `emit()`.
   */
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

export type WorkerStopOptions = {
  /**
   * How long to let in-flight jobs finish, in milliseconds. Once it elapses,
   * every job still running has its signal aborted with a `WorkerStoppedError`
   * and is handed back to the queue, attempt refunded, so another worker starts
   * it straight away rather than after its lease lapses. The re-run starts
   * from the top: whatever the interrupted run generated is paid for again.
   * `0` hands everything back immediately. Default: no limit, wait for every
   * in-flight job however long it takes.
   */
  readonly timeoutMs?: number;
};

export type Worker = {
  /**
   * Stop reserving new jobs and wait for in-flight ones to finish, or, with
   * `timeoutMs`, until that elapses and the rest are requeued. Resolves once
   * every in-flight job has finished or been handed back; a handler that
   * ignores its aborted signal may keep running, but can no longer write
   * anything.
   *
   * Without a timeout, a platform that kills the process after its grace
   * period (SIGTERM, then SIGKILL) leaves the job to be reclaimed when its
   * lease lapses, up to `leaseMs` later, and that re-run consumes an attempt.
   */
  stop(options?: WorkerStopOptions): Promise<void>;
  /** Jobs currently running on this worker. */
  readonly active: number;
};

export type WatchOptions = {
  /**
   * Resume after this stream entry id — pass the last id the client saw. Use
   * `"0"` (the default) to replay from the beginning. Usually straight from an
   * SSE `Last-Event-ID` header, so it is validated: anything but a stream id
   * throws a `ValidationError`.
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
// slot. Scripts declare every key they can name up front in KEYS. Three cannot:
// `reserve` discovers the ids it promotes, reclaims, and pops inside the
// script, and `enqueue` and `cancel` release an idempotency key whose name is
// read from the job record. Those derive key names from the `base` in ARGV[1],
// which Redis and Redis Cluster allow because every derived key hashes to the
// same slot as the declared ones.
//
// Dragonfly refuses undeclared keys unless the script opts in, and it only
// reads the opt-in from a comment that precedes the first line of code, so the
// flag has to lead the script. Redis sees an ordinary comment. Verified against
// dragonfly v2.0: without the line the script fails with "script tried
// accessing undeclared key"; with it, it runs.
const UNDECLARED_KEYS = "--!df flags=allow-undeclared-keys\n";

// Server time for every timestamp and lease: TIME inside the script, never the
// caller's Date.now(). A worker whose clock ran fast used to reclaim leases
// other workers still held, running a paid generation twice, and a skewed
// producer made delayed jobs fire early or late. Writing after TIME needs
// effects replication, which is the default from Redis 5 and the only mode from
// Redis 7; the queue already requires 6.2 (exclusive XRANGE, XTRIM MINID), so
// no `redis.replicate_commands()` call is needed.
//
// `n()` formats doubles without scientific notation — Lua would render a
// 13-digit millisecond timestamp as "1.7e+12" and Redis would reject it.
//
// `append` is the only way an event reaches a job's stream. It numbers each
// entry (`n`, one per job, contiguous) so a watcher can tell a gap from a
// quiet spell, and trims the retention cap itself rather than with XADD
// MAXLEN ~, because it has to record how far the trim went: the id of the last
// entry removed goes on the record as `eventsTrimmedThrough`, which is what
// lets `watch()` report a resumed cursor that fell off the retained window
// instead of silently skipping ahead. Trimming in batches of a tenth of the cap
// keeps the XRANGE that finds the cut amortized O(1) per append.
const LUA_CORE = `
local function n(v) return string.format("%.0f", v) end
local function serverNow()
  local t = redis.call("TIME")
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function append(jk, ek, kind, data, maxLen)
  local seq = redis.call("HINCRBY", jk, "eventSeq", 1)
  local entry = redis.call("XADD", ek, "*", "t", kind, "d", data, "n", seq)
  local cap = tonumber(maxLen)
  local excess = redis.call("XLEN", ek) - cap
  if excess >= math.max(1, math.floor(cap / 10)) then
    local head = redis.call("XRANGE", ek, "-", "+", "COUNT", excess + 1)
    redis.call("XTRIM", ek, "MINID", head[#head][1])
    redis.call("HSET", jk, "eventsTrimmedThrough", head[#head - 1][1])
  end
  return entry
end
`;

/** Key-name derivation for the three scripts that cannot declare every key. */
const LUA_DERIVED = `
local base = ARGV[1]
local function jobKey(id) return base .. ":job:" .. id end
local function eventsKey(id) return base .. ":events:" .. id end
local function idemKey(idem) return base .. ":idem:" .. idem end
`;

const enqueueScript = defineScript<
  readonly [
    base: string,
    id: string,
    payload: string,
    delayMs: string,
    priority: string,
    maxAttempts: string,
    idempotencyKey: string,
    idempotencyTtlMs: string,
    signalCap: string
  ],
  { id: string; deduplicated: boolean; liveStatus: string }
>({
  keyCount: 9,
  lua: `${UNDECLARED_KEYS}${LUA_CORE}${LUA_DERIVED}
-- @script enqueue
local ready, scheduled, seqKey, signal = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local key, events, dead, leases, idemKey = KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[9]
local id = ARGV[2]
local now = serverNow()
local delay = tonumber(ARGV[4])
local priority = tonumber(ARGV[5])
local idem = ARGV[7]

if idem ~= "" then
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

if idem ~= "" then
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
-- That mapping's name comes from the old record, so it is the one key here
-- that cannot be declared.
local priorIdem = redis.call("HGET", key, "idempotencyKey")
if priorIdem and priorIdem ~= "" and priorIdem ~= idem then
  redis.call("DEL", base .. ":idem:" .. priorIdem)
end
redis.call("ZREM", ready, id)
redis.call("ZREM", scheduled, id)
redis.call("ZREM", dead, id)
redis.call("ZREM", leases, id)
redis.call("DEL", key, events)

redis.call("HSET", key,
  "id", id,
  "payload", ARGV[3],
  "attempt", "0",
  "maxAttempts", ARGV[6],
  "priority", ARGV[5],
  "createdAt", n(now),
  "updatedAt", n(now),
  "progress", "0",
  "idempotencyKey", idem,
  "idemTtlMs", ARGV[8])

if delay > 0 then
  redis.call("HSET", key, "status", "scheduled")
  redis.call("ZADD", scheduled, n(now + delay), id)
else
  local seq = redis.call("INCR", seqKey)
  redis.call("HSET", key, "status", "waiting")
  redis.call("ZADD", ready, n((${MAX_PRIORITY} - priority) * ${PRIORITY_STRIDE} + seq), id)
  -- Doorbell: wake one blocked worker. Trimmed so an idle queue cannot grow it.
  redis.call("LPUSH", signal, "1")
  redis.call("LTRIM", signal, 0, tonumber(ARGV[9]) - 1)
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
  /** `""` when the job has none. Lets the settling scripts declare its key. */
  readonly idempotencyKey: string;
};

const reserveScript = defineScript<
  readonly [
    base: string,
    leaseMs: string,
    token: string,
    eventsMaxLen: string,
    deadTtlMs: string
  ],
  { job: ReservedRow | null; wakeInMs: number }
>({
  keyCount: 6,
  lua: `${UNDECLARED_KEYS}${LUA_CORE}${LUA_DERIVED}
-- @script reserve
local ready, scheduled, leases = KEYS[1], KEYS[2], KEYS[3]
local seqKey, dead, signal = KEYS[4], KEYS[5], KEYS[6]
local now = serverNow()
local leaseMs = tonumber(ARGV[2])
local token = ARGV[3]
local maxLen = ARGV[4]
local deadTtl = tonumber(ARGV[5])

local function pushReady(id, priority)
  local seq = redis.call("INCR", seqKey)
  redis.call("ZADD", ready, n((${MAX_PRIORITY} - priority) * ${PRIORITY_STRIDE} + seq), id)
end

local function releaseIdem(key)
  local idem = redis.call("HGET", key, "idempotencyKey")
  if idem and idem ~= "" then redis.call("DEL", idemKey(idem)) end
end

-- 1. Promote every job whose delay (or backoff) has elapsed.
local due = redis.call("ZRANGEBYSCORE", scheduled, "-inf", n(now), "LIMIT", 0, 100)
for _, id in ipairs(due) do
  redis.call("ZREM", scheduled, id)
  local priority = tonumber(redis.call("HGET", jobKey(id), "priority") or "0")
  redis.call("HSET", jobKey(id), "status", "waiting", "updatedAt", n(now))
  pushReady(id, priority)
end

-- 2. Reclaim jobs whose lease expired — a worker crashed mid-run, or lost its
--    connection long enough that its own deadline aborted the handler. Server
--    time decides expiry, so a worker with a fast clock cannot reclaim a lease
--    its holder still renews. Attempts were already counted at reserve, so a
--    crash loop dead-letters rather than spinning forever.
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
    append(key, eventsKey(id), "cancelled", "", maxLen)
    redis.call("PEXPIRE", key, n(deadTtl))
    redis.call("PEXPIRE", eventsKey(id), n(deadTtl))
    releaseIdem(key)
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
    append(key, eventsKey(id), "failed",
      "Worker lease expired before the job finished", maxLen)
    redis.call("PEXPIRE", key, n(deadTtl))
    redis.call("PEXPIRE", eventsKey(id), n(deadTtl))
    releaseIdem(key)
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
local events = eventsKey(id)
local attempt = tonumber(redis.call("HGET", key, "attempt") or "0") + 1

-- A re-run regenerates from scratch, so its output starts over too. Leaving
-- the previous run's partial chunks in place would make a resuming client
-- concatenate two generations. Announce the restart, then trim everything
-- before the marker: deleting the stream instead would reset the
-- last-generated id, and a marker recreated in the same millisecond can land
-- at or below the cursor a watcher already holds, which drops the restart
-- boundary and every chunk sharing that millisecond. A run that stop()
-- requeued gets its attempt back, so "re-run" is output on the stream, not an
-- attempt number above one.
if attempt > 1 or redis.call("XLEN", events) > 0 then
  local marker = append(key, events, "restarted", n(attempt), maxLen)
  redis.call("XTRIM", events, "MINID", marker)
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
  token,
  redis.call("HGET", key, "idempotencyKey") or ""}
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
        token: expectString(row[7], "reserve"),
        idempotencyKey:
          row[8] === undefined ? "" : expectString(row[8], "reserve")
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
    id: string,
    token: string,
    leaseMs: string,
    type: string,
    data: string,
    eventsMaxLen: string
  ],
  { held: boolean; cancelRequested: boolean; eventId: string }
>({
  keyCount: 3,
  lua: `${LUA_CORE}
-- @script touch
local leases, key, events = KEYS[1], KEYS[2], KEYS[3]
local id, token = ARGV[1], ARGV[2]

if redis.call("HGET", key, "token") ~= token then return {0, 0, ""} end
local now = serverNow()
redis.call("ZADD", leases, n(now + tonumber(ARGV[3])), id)

local eventId = ""
local kind = ARGV[4]
if kind ~= "" then
  eventId = append(key, events, kind, ARGV[5], ARGV[6])
  if kind == "progress" then
    redis.call("HSET", key, "progress", ARGV[5])
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
    id: string,
    token: string,
    status: string,
    payload: string,
    ttlMs: string,
    eventsMaxLen: string
  ],
  number
>({
  keyCount: 6,
  lua: `${LUA_CORE}
-- @script settle
local leases, dead, ready = KEYS[1], KEYS[2], KEYS[3]
local key, events, idemKey = KEYS[4], KEYS[5], KEYS[6]
local id, token = ARGV[1], ARGV[2]
local status = ARGV[3]

-- The fence: only the holder of the current lease may write an outcome. A
-- worker whose lease lapsed and was reclaimed finds its token gone, so its
-- late result is refused rather than overwriting the run that replaced it.
if redis.call("HGET", key, "token") ~= token then return 0 end
local now = serverNow()

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
  redis.call("HSET", key, "result", ARGV[4])
  append(key, events, "completed", ARGV[4], ARGV[6])
elseif status == "cancelled" then
  append(key, events, "cancelled", "", ARGV[6])
else
  redis.call("HSET", key, "error", ARGV[4])
  redis.call("ZADD", dead, n(now), id)
  -- The job record expires after resultTtlMs but its dead-letter entry did
  -- not, so the set grew for the life of the deployment and dead() listed ids
  -- whose records were long gone. Trim to the same horizon.
  redis.call("ZREMRANGEBYSCORE", dead, 0, n(now - tonumber(ARGV[5])))
  append(key, events, "failed", ARGV[4], ARGV[6])
end

local ttlMs = tonumber(ARGV[5])
local ttl = n(ttlMs)
redis.call("PEXPIRE", key, ttl)
redis.call("PEXPIRE", events, ttl)

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
    redis.call("PEXPIRE", idemKey, n(hold))
  else
    redis.call("DEL", idemKey)
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
    id: string,
    token: string,
    delayMs: string,
    error: string,
    ttlMs: string,
    eventsMaxLen: string
  ],
  number
>({
  keyCount: 5,
  lua: `${LUA_CORE}
-- @script retry
local leases, scheduled = KEYS[1], KEYS[2]
local key, events, idemKey = KEYS[3], KEYS[4], KEYS[5]
local id, token = ARGV[1], ARGV[2]
local delay = tonumber(ARGV[3])

if redis.call("HGET", key, "token") ~= token then return 0 end

-- Validate before the first write. Redis does not roll back what a script
-- already did, so a delay caught at the ZADD would leave the job marked
-- scheduled with no lease and no membership in any lifecycle index: nothing
-- can reserve it and wait() hangs forever.
if not (delay and delay >= 0 and delay < math.huge) then
  return redis.error_reply(
    "benni queue: retry delay must be a finite, non-negative number of milliseconds")
end
local now = serverNow()

redis.call("ZREM", leases, id)
redis.call("HDEL", key, "token")

-- Cancellation wins over a retry too, or the queue schedules another paid
-- generation of work the caller already stopped.
if redis.call("HGET", key, "cancelRequested") == "1" then
  redis.call("HSET", key,
    "status", "cancelled",
    "updatedAt", n(now),
    "finishedAt", n(now),
    "error", ARGV[4])
  append(key, events, "cancelled", "", ARGV[6])
  local ttl = n(tonumber(ARGV[5]))
  redis.call("PEXPIRE", key, ttl)
  redis.call("PEXPIRE", events, ttl)
  local idem = redis.call("HGET", key, "idempotencyKey")
  if idem and idem ~= "" then redis.call("DEL", idemKey) end
  return 2
end

redis.call("HSET", key,
  "status", "scheduled",
  "updatedAt", n(now),
  "error", ARGV[4])
redis.call("ZADD", scheduled, n(now + delay), id)
return 1
`,
  decode: (reply) => toNumber(reply)
});

/**
 * Hand a running job back to the queue because its worker is stopping.
 * 0 = lease already lost, 1 = requeued, 2 = settled `cancelled` instead
 * because a cancel had landed during the run.
 */
const requeueScript = defineScript<
  readonly [
    id: string,
    token: string,
    signalCap: string,
    ttlMs: string,
    eventsMaxLen: string
  ],
  number
>({
  keyCount: 7,
  lua: `${LUA_CORE}
-- @script requeue
local leases, ready, seqKey, signal = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local key, events, idemKey = KEYS[5], KEYS[6], KEYS[7]
local id, token = ARGV[1], ARGV[2]

if redis.call("HGET", key, "token") ~= token then return 0 end
local now = serverNow()
redis.call("ZREM", leases, id)
redis.call("HDEL", key, "token")

-- A cancelled job has no business going back on the queue: the next worker
-- would start a paid generation only to abort it on its first heartbeat.
if redis.call("HGET", key, "cancelRequested") == "1" then
  redis.call("HSET", key, "status", "cancelled", "updatedAt", n(now),
    "finishedAt", n(now))
  append(key, events, "cancelled", "", ARGV[5])
  local ttl = n(tonumber(ARGV[4]))
  redis.call("PEXPIRE", key, ttl)
  redis.call("PEXPIRE", events, ttl)
  local idem = redis.call("HGET", key, "idempotencyKey")
  if idem and idem ~= "" then redis.call("DEL", idemKey) end
  return 2
end

-- A deploy is not the job's fault, so the interrupted run gives its attempt
-- back: three rolling restarts must not dead-letter a healthy job. The next
-- reserve writes a restart marker because the stream is non-empty, not
-- because the attempt number grew.
local attempt = tonumber(redis.call("HGET", key, "attempt") or "1") - 1
if attempt < 0 then attempt = 0 end
redis.call("HSET", key, "status", "waiting", "attempt", n(attempt),
  "updatedAt", n(now))
-- Front of its priority band: whoever is waiting on it has waited longest.
local priority = tonumber(redis.call("HGET", key, "priority") or "0")
redis.call("ZADD", ready, n((${MAX_PRIORITY} - priority) * ${PRIORITY_STRIDE}), id)
redis.call("LPUSH", signal, "1")
redis.call("LTRIM", signal, 0, tonumber(ARGV[3]) - 1)
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
  readonly [base: string, id: string, ttlMs: string, eventsMaxLen: string],
  number
>({
  keyCount: 5,
  lua: `${UNDECLARED_KEYS}${LUA_CORE}${LUA_DERIVED}
-- @script cancel
local ready, scheduled, leases = KEYS[1], KEYS[2], KEYS[3]
local key, events = KEYS[4], KEYS[5]
local id = ARGV[2]

local status = redis.call("HGET", key, "status")
if not status then return 0 end
if status == "completed" or status == "failed" or status == "cancelled" then
  return 2
end
local now = serverNow()

redis.call("HSET", key, "cancelRequested", "1", "updatedAt", n(now))

-- Active: the owning worker sees the flag on its next heartbeat or emit and
-- aborts its signal. Settling here would race that worker's own settle.
if status == "active" then return 3 end

redis.call("ZREM", ready, id)
redis.call("ZREM", scheduled, id)
redis.call("ZREM", leases, id)
redis.call("HSET", key, "status", "cancelled", "finishedAt", n(now))
append(key, events, "cancelled", "", ARGV[4])
local ttl = n(tonumber(ARGV[3]))
redis.call("PEXPIRE", key, ttl)
redis.call("PEXPIRE", events, ttl)
-- The caller names the job, not its idempotency key, so this key comes from
-- the record and is the reason this script needs the undeclared-keys flag.
local idem = redis.call("HGET", key, "idempotencyKey")
if idem and idem ~= "" then redis.call("DEL", idemKey(idem)) end
return 1
`,
  decode: (reply) => toNumber(reply)
});

/** Move a dead-lettered job back to the ready set. 0 = not dead, 1 = requeued. */
const retryDeadScript = defineScript<
  readonly [id: string, maxAttempts: string, signalCap: string],
  number
>({
  keyCount: 6,
  lua: `${LUA_CORE}
-- @script retryDead
local ready, dead, seqKey, signal = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local key, events = KEYS[5], KEYS[6]
local id = ARGV[1]

if redis.call("ZREM", dead, id) == 0 then return 0 end
if redis.call("EXISTS", key) == 0 then return 0 end
local now = serverNow()

redis.call("HSET", key,
  "status", "waiting",
  "attempt", "0",
  "maxAttempts", ARGV[2],
  "updatedAt", n(now))
-- The stream starts over below, so its numbering and trim watermark do too:
-- a stale watermark would report the fresh stream as truncated.
redis.call("HDEL", key, "finishedAt", "token", "result", "eventSeq",
  "eventsTrimmedThrough")
-- The record carried a result TTL from when it died; it is live again now.
redis.call("PERSIST", key)
-- Discard the failed attempt's output, terminal event included: a watcher
-- must not stop on the old "failed" event, nor render two generations.
redis.call("DEL", events)
local priority = tonumber(redis.call("HGET", key, "priority") or "0")
local seq = redis.call("INCR", seqKey)
redis.call("ZADD", ready, n((${MAX_PRIORITY} - priority) * ${PRIORITY_STRIDE} + seq), id)
-- Ring the doorbell, or an idle worker sleeps out its full block first.
redis.call("LPUSH", signal, "1")
redis.call("LTRIM", signal, 0, tonumber(ARGV[3]) - 1)
return 1
`,
  decode: (reply) => toNumber(reply)
});

/**
 * Read a job's backlog and its trim watermark in one atomic step. Read
 * separately, a trim landing between the two would make a complete read look
 * truncated, or a truncated one look complete.
 */
const readEventsScript = defineScript<
  readonly [start: string],
  { trimmedThrough: string; entries: RedisReply }
>({
  keyCount: 2,
  lua: `
-- @script readEvents
local watermark = redis.call("HGET", KEYS[1], "eventsTrimmedThrough")
return {watermark or "", redis.call("XRANGE", KEYS[2], ARGV[1], "+")}
`,
  decode: (reply) => {
    const row = expectArray(reply, "readEvents");
    return {
      trimmedThrough: typeof row[0] === "string" ? row[0] : "",
      entries: row[1] ?? null
    };
  }
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
 * Delivery is **at-least-once**. A handler can run more than once for one job:
 * after a worker crash, a network partition, an event-loop stall longer than
 * the lease, or `stop({ timeoutMs })` requeueing it. What is exactly-once is
 * the *outcome*: only the holder of the current lease can append output or
 * settle the job, so a stale run's result is refused, never recorded twice.
 *
 * Job lifecycle lives in sorted sets (`ready`, `scheduled`, `leases`, `dead`) —
 * which is what gives delays, exponential backoff, priority, and dead-lettering
 * — while streams carry output, which is what streams are good at.
 *
 * ```ts
 * // queue<{ prompt: string }, string>("generate")
 * const jobs = redis.query.generate;
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
export function createQueue<TPayload, TResult = unknown>(
  client: RedisClient,
  options?: QueueOptions<TPayload, TResult>,
  track?: StoreContext["track"]
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
  // With an empty key this names a key no script ever touches: the scripts
  // only use it once the record says the job has an idempotency key.
  const idemKey = (key: string) => `${base}:idem:${key}`;

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
      [
        readyKey,
        scheduledKey,
        seqKey,
        signalKey,
        jobKey(id),
        eventsKey(id),
        deadKey,
        leasesKey,
        idemKey(idempotencyKey)
      ],
      [
        base,
        id,
        codec.encode(payload),
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
      [readyKey, scheduledKey, leasesKey, jobKey(id), eventsKey(id)],
      [base, id, String(resultTtlMs), String(eventsMaxLen)]
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
      [readyKey, deadKey, seqKey, signalKey, jobKey(id), eventsKey(id)],
      [id, String(attempts), SIGNAL_CAP]
    );
    return outcome === 1;
  }

  // -- event stream ---------------------------------------------------------

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
   * replayed twice.
   *
   * Nothing is skipped silently either. A job's stream keeps its newest
   * `eventsMaxLen` events, so a long generation streamed a token at a time can
   * outgrow it, and a cursor from before the retained window cannot be served
   * in full. When that happens the iterator yields a `truncated` event where
   * the missing events would have been, then carries on with what is left; the
   * terminal `completed` event still carries the whole result.
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
    let cursor = streamCursor(watchOptions?.after ?? "0");
    const fromStart = cursor === "0" || cursor === "-";
    // The `n` of the last entry seen, so the next one can be checked for
    // contiguity. Numbering starts at 1 per stream, so replaying from the start
    // expects entry 1; resuming mid-stream learns it from the first entry read.
    // `null` means unknown, which checks nothing rather than guess.
    let lastSeq: number | null = fromStart ? 0 : null;
    let session: RedisSession | null = null;
    // Adapters without a dedicated connection (HTTP/edge) leave `session`
    // undefined; one that has it but cannot lease now falls back the same way
    // rather than failing the whole watch.
    let sessionUnavailable = client.session === undefined;

    /** Yields decoded events; resolves true once a terminal one is seen. */
    async function* emitAll(
      entries: readonly RawEvent[],
      startIntact?: boolean
    ): AsyncGenerator<JobEvent<TResult>, boolean, undefined> {
      for (const [index, entry] of entries.entries()) {
        // A restart marker opens a fresh generation, so whatever preceded it
        // was discarded on purpose and nothing before it is owed to anyone.
        const intact =
          entry.type === "restarted" ||
          (index === 0 && startIntact !== undefined
            ? startIntact
            : lastSeq === null ||
              entry.seq === null ||
              entry.seq === lastSeq + 1);
        if (!intact) yield { id: cursor, type: "truncated" };
        cursor = entry.id;
        lastSeq = entry.seq;
        const event = decodeEvent(entry, resultCodec);
        if (event === null) continue;
        yield event;
        if (isTerminalType(event.type)) return true;
      }
      return false;
    }

    try {
      // Backlog first — everything already written after the caller's cursor,
      // read with the trim watermark in one step. Inclusive of the cursor: if
      // that entry is still retained, nothing after it can have been trimmed,
      // and its `n` is what the next entry must follow.
      const backlog = await scripts.run(
        readEventsScript,
        [jobKey(id), eventsKey(id)],
        [fromStart ? "-" : cursor]
      );
      let entries = decodeRawEntries(backlog.entries, "XRANGE");
      let startIntact: boolean | undefined;
      const head = entries[0];
      if (!fromStart && head && compareStreamIds(head.id, cursor) === 0) {
        lastSeq = head.seq;
        entries = entries.slice(1);
      } else if (!fromStart) {
        // The cursor's own entry is gone. It was trimmed by the retention cap
        // exactly when the watermark (the last entry the cap removed) is at or
        // past it; only strictly past it means an entry the caller never saw
        // went with it. Otherwise it went with a restart or a fresh generation,
        // and whatever follows is complete.
        startIntact = !(
          backlog.trimmedThrough !== "" &&
          compareStreamIds(cursor, backlog.trimmedThrough) < 0
        );
      }
      if (yield* emitAll(entries, startIntact)) return;

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

        let batch: RawEvent[];
        if (session !== null && !session.closed) {
          batch = decodeXread(
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
          batch = decodeXread(
            await client.send(["XREAD", "STREAMS", eventsKey(id), cursor])
          );
          if (batch.length === 0) await sleep(pollMs);
        }

        if (yield* emitAll(batch)) return;

        // Nothing arrived. If the record is gone the job finished long enough
        // ago that its result TTL elapsed — no terminal event is ever coming.
        if (
          batch.length === 0 &&
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
   * result; rejects with `JobFailedError` (message: the recorded failure) or
   * `JobCancelledError`. Rejects with `JobNotFoundError` if the job is unknown
   * or its result TTL has elapsed.
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
      throw new JobFailedError(id, existing.error ?? `Job "${id}" failed`);
    }
    if (existing.status === "cancelled") throw new JobCancelledError(id);

    for await (const event of watch(id, {
      after: cursor,
      signal: waitOptions?.signal,
      pollMs: waitOptions?.pollMs
    })) {
      if (event.type === "completed") return event.result;
      if (event.type === "failed") throw new JobFailedError(id, event.error);
      if (event.type === "cancelled") throw new JobCancelledError(id);
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
   *
   * @throws ValidationError if an option is out of range, including a
   * `heartbeatMs` above half the effective `leaseMs`.
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
    const heartbeatMs = renewalInterval(
      workerOptions?.heartbeatMs,
      workerLeaseMs
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
    /** How to hand each running job back to the queue, keyed by lease token. */
    const interrupts = new Map<string, () => Promise<void>>();
    let doorbell: RedisSession | null = null;
    let doorbellUnavailable = client.session === undefined;
    let slotFreed: (() => void) | null = null;

    function releaseSlot() {
      const notify = slotFreed;
      slotFreed = null;
      notify?.();
    }

    /** The keys a settling script declares for one reserved job. */
    function jobKeys(reserved: ReservedRow) {
      return {
        job: jobKey(reserved.id),
        events: eventsKey(reserved.id),
        idem: idemKey(reserved.idempotencyKey)
      };
    }

    /** Renew the lease, optionally appending an event. */
    async function touch(
      reserved: ReservedRow,
      type: "" | "chunk" | "progress",
      data: string
    ) {
      const keys = jobKeys(reserved);
      return scripts.run(
        touchScript,
        [leasesKey, keys.job, keys.events],
        [
          reserved.id,
          reserved.token,
          String(workerLeaseMs),
          type,
          data,
          String(eventsMaxLen)
        ]
      );
    }

    /** Hand a job back to the ready set. Resolves the script's outcome. */
    async function requeue(reserved: ReservedRow): Promise<number> {
      const keys = jobKeys(reserved);
      return scripts.run(
        requeueScript,
        [
          leasesKey,
          readyKey,
          seqKey,
          signalKey,
          keys.job,
          keys.events,
          keys.idem
        ],
        [
          reserved.id,
          reserved.token,
          SIGNAL_CAP,
          String(resultTtlMs),
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

    async function run(
      reserved: ReservedRow,
      reservedAt: number
    ): Promise<void> {
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
      /** The lease is gone, proven by Redis or by the local deadline. */
      let leaseLost = false;
      /** `stop()` handed the job back to the queue. */
      let interrupted = false;
      /** The handler has returned or thrown; its outcome is being written. */
      let handlerDone = false;
      let heartbeating = false;
      // Monotonic, and measured from before the reserve round trip: the server
      // stamped the lease at some point during it, so this is the earliest the
      // lease can lapse, and a wall-clock jump cannot move it.
      let leaseDeadline = reservedAt + workerLeaseMs;
      let deadlineTimer: ReturnType<typeof setTimeout> | null = null;

      function stopTimers() {
        clearInterval(heartbeatTimer);
        if (deadlineTimer !== null) clearTimeout(deadlineTimer);
        deadlineTimer = null;
      }
      function onCancelled() {
        if (leaseLost || interrupted) return;
        cancelled = true;
        controller.abort(new JobCancelledError(reserved.id));
      }
      function onLeaseLost() {
        if (leaseLost || interrupted) return;
        leaseLost = true;
        stopTimers();
        controller.abort(new JobLeaseLostError(reserved.id));
        // Paid work was just abandoned, possibly to a run that is already
        // underway elsewhere: that belongs in the operator's telemetry.
        onError(new JobLeaseLostError(reserved.id));
      }

      /** One renewal, with the deadline pushed out only on proof. */
      async function renew(type: "" | "chunk" | "progress", data: string) {
        const sentAt = performance.now();
        const state = await touch(reserved, type, data);
        if (!state.held) {
          onLeaseLost();
        } else {
          // Max, not assignment: renewals can overlap (an emit alongside the
          // automatic heartbeat) and finish out of order, and whichever ran
          // last on the server stamped a lease no earlier than either sentAt.
          leaseDeadline = Math.max(leaseDeadline, sentAt + workerLeaseMs);
          if (state.cancelRequested) onCancelled();
        }
        return state;
      }

      // The local deadline. A heartbeat that fails over the network proves
      // nothing, so it used to go to onError and the handler kept generating
      // while another worker reclaimed the job and paid for it again. Once the
      // lease can no longer have been renewed in time, give it up here: abort
      // the signal so the provider call stops. Re-armed lazily, so a renewal
      // per token costs nothing but a number.
      function armDeadline() {
        deadlineTimer = setTimeout(
          () => {
            deadlineTimer = null;
            if (handlerDone || leaseLost || interrupted) return;
            if (performance.now() >= leaseDeadline) onLeaseLost();
            else armDeadline();
          },
          Math.max(0, leaseDeadline - performance.now())
        );
        (deadlineTimer as { unref?: () => void }).unref?.();
      }

      // Automatic heartbeat: a handler that never emits still keeps its lease,
      // and cancellation still reaches it within one interval. One renewal at
      // a time: a round trip slower than the interval would otherwise stack
      // up calls that all re-apply the same lease.
      const heartbeatTimer = setInterval(() => {
        if (heartbeating || leaseLost || interrupted) return;
        heartbeating = true;
        renew("", "")
          .catch((error: unknown) => onError(error))
          .finally(() => {
            heartbeating = false;
          });
      }, heartbeatMs);
      // Never keep the process alive for a heartbeat alone.
      (heartbeatTimer as { unref?: () => void }).unref?.();
      armDeadline();

      interrupts.set(reserved.token, async () => {
        // A handler that already finished is writing its outcome; requeueing
        // under it would throw away a result that was paid for.
        if (handlerDone || interrupted) return;
        interrupted = true;
        stopTimers();
        controller.abort(new WorkerStoppedError(reserved.id));
        try {
          await requeue(reserved);
        } catch (error) {
          // The lease is the backstop: the job is reclaimed when it lapses.
          onError(error);
        }
      });

      /** Why this run can write nothing more, as the error to throw. */
      const stopped = () =>
        controller.signal.reason instanceof Error
          ? controller.signal.reason
          : new JobLeaseLostError(reserved.id);

      const context: JobContext<TPayload> = {
        id: reserved.id,
        payload,
        attempt: reserved.attempt,
        maxAttempts: reserved.maxAttempts,
        priority: reserved.priority,
        createdAt: reserved.createdAt,
        signal: controller.signal,
        async emit(chunk: string) {
          // Once the signal is aborted for loss or shutdown there is nothing to
          // renew, and a round trip would only be refused by the token check.
          if (leaseLost || interrupted) throw stopped();
          const state = await renew("chunk", chunk);
          if (!state.held) throw stopped();
          return state.eventId;
        },
        async progress(fraction: number) {
          if (leaseLost || interrupted) throw stopped();
          const clamped = Math.min(1, Math.max(0, fraction));
          const state = await renew("progress", String(clamped));
          if (!state.held) throw stopped();
        },
        async heartbeat() {
          if (leaseLost || interrupted) return false;
          return (await renew("", "")).held;
        }
      };

      try {
        const result = await handler(context);
        handlerDone = true;
        if (interrupted) return; // stop() requeued it; another worker runs it.
        // A cancel that lands during the final tokens still wins: the user
        // asked to stop, so do not record a result they will not see.
        if (cancelled) {
          await settle(reserved, "cancelled", "");
          return;
        }
        // Attempted even after a lease loss: the write is fenced by the token,
        // so it lands only if nobody reclaimed the job — a partition that
        // healed in time — and a result that was paid for is kept.
        const outcome = await settle(
          reserved,
          "completed",
          encodeResult(reserved.id, result)
        );
        if (outcome === 0 && !leaseLost) {
          onError(new JobLeaseLostError(reserved.id));
        }
      } catch (error) {
        handlerDone = true;
        // Another worker owns it (or stop() handed it back); touching it would
        // race that owner.
        if (leaseLost || interrupted) return;
        if (cancelled) {
          await settle(reserved, "cancelled", "");
          return;
        }
        if ((await failed(reserved, error)) === 0) {
          onError(new JobLeaseLostError(reserved.id));
        }
      } finally {
        handlerDone = true;
        stopTimers();
        interrupts.delete(reserved.token);
      }
    }

    /** Resolves the settle script's outcome, or `null` if the call failed. */
    async function settle(
      reserved: ReservedRow,
      status: "completed" | "cancelled" | "failed",
      payload: string
    ): Promise<number | null> {
      const keys = jobKeys(reserved);
      try {
        return await scripts.run(
          settleScript,
          [leasesKey, deadKey, readyKey, keys.job, keys.events, keys.idem],
          [
            reserved.id,
            reserved.token,
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
        return null;
      }
    }

    /** Resolves the outcome of whichever script ran, or `null` on failure. */
    async function failed(
      reserved: ReservedRow,
      error: unknown
    ): Promise<number | null> {
      const message = errorMessage(error);
      const retry =
        reserved.attempt < reserved.maxAttempts && isRetryable(error);
      if (!retry) return settle(reserved, "failed", message);
      const delayMs =
        error instanceof RetryJobError
          ? error.retryAfterMs
          : backoffFor(reserved.attempt, backoffMs, maxBackoffMs);
      const keys = jobKeys(reserved);
      try {
        return await scripts.run(
          retryScript,
          [leasesKey, scheduledKey, keys.job, keys.events, keys.idem],
          [
            reserved.id,
            reserved.token,
            String(delayMs),
            message,
            String(resultTtlMs),
            String(eventsMaxLen)
          ]
        );
      } catch (scheduleError) {
        onError(scheduleError);
        return null;
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
        const reservedAt = performance.now();
        try {
          const outcome = await scripts.run(
            reserveScript,
            [readyKey, scheduledKey, leasesKey, seqKey, deadKey, signalKey],
            [
              base,
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

        if (!running) {
          // stop() landed while this reserve was in flight. Starting the job
          // now would begin paid work that stop() may not wait for, so hand it
          // straight back, attempt refunded, for another worker.
          await requeue(reserved).catch(onError);
          break;
        }

        const task = run(reserved, reservedAt)
          .catch(onError)
          .finally(() => {
            inFlight.delete(task);
            releaseSlot();
          });
        inFlight.add(task);
      }
    }

    const loop = dispatch().catch(onError);

    const handle: Worker = {
      get active() {
        return inFlight.size;
      },
      async stop(stopOptions?: WorkerStopOptions) {
        const timeoutMs =
          stopOptions?.timeoutMs === undefined
            ? undefined
            : nonNegativeInt(stopOptions.timeoutMs, "timeoutMs");
        running = false;
        doorbellUnavailable = true; // never lease a replacement while stopping
        releaseSlot();
        // Close the doorbell first: BLPOP would otherwise hold the loop for the
        // rest of its timeout. Adapters reject the in-flight command on close.
        const blocked = doorbell;
        doorbell = null;
        await blocked?.close().catch(() => {});
        const drained = (async () => {
          await loop;
          await Promise.allSettled([...inFlight]);
        })();
        if (timeoutMs === undefined) return drained;

        let timer: ReturnType<typeof setTimeout> | undefined;
        const expired = new Promise<true>((resolve) => {
          timer = setTimeout(() => resolve(true), timeoutMs);
        });
        const timedOut = await Promise.race([
          drained.then(() => false),
          expired
        ]);
        clearTimeout(timer);
        if (!timedOut) return;
        // Out of time: abort what is still running and hand it back now, so
        // another worker starts it at once instead of after the lease lapses.
        await Promise.allSettled(
          [...interrupts.values()].map((interrupt) => interrupt())
        );
      }
    };
    // Registered with the handle it was started through, so `redis.close()`
    // stops it; unregistered once a stop() completes.
    const untrack = track?.(handle);
    if (untrack === undefined) return handle;
    const stop = handle.stop;
    handle.stop = (stopOptions) => stop(stopOptions).finally(untrack);
    return handle;
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

/** One raw entry of a job's stream. `seq` is `null` on entries without one. */
type RawEvent = {
  readonly id: string;
  readonly type: string;
  readonly data: string;
  readonly seq: number | null;
};

const STREAM_ID = /^\d{1,20}(-\d{1,20})?$/;

/**
 * Validate a caller's cursor. It usually arrives from an SSE `Last-Event-ID`
 * header, so it is untrusted input bound for XRANGE/XREAD arguments.
 */
function streamCursor(cursor: string): string {
  if (cursor === "-" || STREAM_ID.test(cursor)) return cursor;
  throw new ValidationError(
    `queue watch cursor must be a stream entry id such as "1700000000000-0", or "0" for the beginning, received ${JSON.stringify(cursor)}`
  );
}

/** Order two stream ids (`ms` or `ms-seq`) numerically. */
function compareStreamIds(a: string, b: string): number {
  const [aMs = "0", aSeq = "0"] = a.split("-");
  const [bMs = "0", bSeq = "0"] = b.split("-");
  const byMs = BigInt(aMs) - BigInt(bMs);
  if (byMs !== 0n) return byMs < 0n ? -1 : 1;
  const bySeq = BigInt(aSeq) - BigInt(bSeq);
  return bySeq === 0n ? 0 : bySeq < 0n ? -1 : 1;
}

function decodeEvent<TResult>(
  entry: RawEvent,
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

function decodeRawEntries(reply: RedisReply, command: string): RawEvent[] {
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
    let seq: number | null = null;
    for (let index = 0; index < fields.length - 1; index += 2) {
      if (fields[index] === "t") type = String(fields[index + 1] ?? "");
      if (fields[index] === "d") data = String(fields[index + 1] ?? "");
      if (fields[index] === "n") {
        const parsed = Number(fields[index + 1]);
        seq = Number.isSafeInteger(parsed) ? parsed : null;
      }
    }
    return { id: expectString(entry[0], command), type, data, seq };
  });
}

function decodeXread(reply: RedisReply): RawEvent[] {
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

/** A queue, as `redis.query.<name>` returns it for a {@link QueueSchema}. */
export type QueueStore<TPayload, TResult = unknown> = ReturnType<
  typeof createQueue<TPayload, TResult>
>;

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
    createQueue(ctx.client, schema, ctx.track)
};

/** Build a {@link QueueSchema}. Exported as `queue` from `benni/schema`. */
export function defineQueue<TPayload, TResult = unknown>(
  prefix: string,
  options?: Omit<QueueOptions<TPayload, TResult>, "prefix">
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

/**
 * The worker's heartbeat interval: a quarter of the lease unless the caller
 * chose one, floored at 1ms so a tiny lease still renews rather than spinning.
 *
 * A caller's value has to leave room for a renewal *and* a retry, so half the
 * lease is the ceiling, as in `lock` and `semaphore`. At or above the lease the
 * first heartbeat lands on or after expiry: `leaseMs: 10_000` with the old
 * fixed 15000 default had every job longer than ten seconds reclaimed before
 * its first renewal, a misconfiguration that passes every quick test.
 *
 * The derived default is exempt: for a `leaseMs` of 1 the 1ms floor is the
 * whole lease and can satisfy no ratio, and a working configuration must not
 * start throwing.
 */
function renewalInterval(
  requested: number | undefined,
  leaseMs: number
): number {
  if (requested === undefined) {
    return Math.max(1, Math.floor(leaseMs / HEARTBEAT_DIVISOR));
  }
  const heartbeatMs = positiveInt(requested, "heartbeatMs");
  if (heartbeatMs * 2 > leaseMs) {
    throw new ValidationError(
      `queue heartbeatMs must be at most half of leaseMs (${leaseMs}) so a renewal lands before the lease could lapse, received ${heartbeatMs}`
    );
  }
  return heartbeatMs;
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

import { ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import type { RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "semaphore";
const DEFAULT_LEASE_MS = 60_000;

/**
 * Take a slot if one is free.
 *
 * Holders live in a sorted set scored by lease expiry, so reclaiming the
 * slots of processes that died is just dropping the expired range: there is
 * no sweeper to run and no bookkeeping to get wrong. Server time throughout,
 * because two holders comparing leases against skewed local clocks would
 * disagree about who still owns what.
 */
const acquireScript = defineScript<
  readonly [limit: string, leaseMs: string, token: string],
  number
>({
  keyCount: 1,
  lua: `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local lease = tonumber(ARGV[2])
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, now)
if redis.call("ZCARD", KEYS[1]) >= tonumber(ARGV[1]) then return 0 end
redis.call("ZADD", KEYS[1], now + lease, ARGV[3])
-- Expire the set when its LAST live lease does, never on this lease alone. A
-- plain PEXPIRE lets a short acquisition shorten the whole set's lifetime and
-- delete holders that are still working, which silently blows the limit.
local top = redis.call("ZRANGE", KEYS[1], -1, -1, "WITHSCORES")
redis.call("PEXPIREAT", KEYS[1], math.ceil(tonumber(top[2])))
return 1
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/** Extend our own lease. Returns 0 if the slot was already reclaimed. */
const extendScript = defineScript<
  readonly [leaseMs: string, token: string],
  number
>({
  keyCount: 1,
  lua: `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
-- Presence is not ownership. An expired member sits in the set until some
-- acquire prunes it, so renewing on ZSCORE alone would resurrect a lease we
-- had already lost and hand two callers the same slot.
local score = redis.call("ZSCORE", KEYS[1], ARGV[2])
if score == false or tonumber(score) <= now then return 0 end
local lease = tonumber(ARGV[1])
redis.call("ZADD", KEYS[1], now + lease, ARGV[2])
local top = redis.call("ZRANGE", KEYS[1], -1, -1, "WITHSCORES")
redis.call("PEXPIREAT", KEYS[1], math.ceil(tonumber(top[2])))
return 1
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/** Drop our slot, reporting whether we still actually held it. */
const releaseScript = defineScript<readonly [token: string], number>({
  keyCount: 1,
  lua: `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
-- Presence is not ownership, exactly as in extend. An expired member sits in
-- the set until some acquire prunes it, so a bare ZREM answered "yes, you held
-- it" for a lease that had already lapsed and may already have been handed to
-- someone else. Clear the tombstone either way, but report the truth.
local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
if score == false then return 0 end
redis.call("ZREM", KEYS[1], ARGV[1])
if tonumber(score) <= now then return 0 end
return 1
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/** Live holders, after dropping any whose lease has lapsed. */
const countScript = defineScript<readonly [], number>({
  keyCount: 1,
  lua: `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, now)
return redis.call("ZCARD", KEYS[1])
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

export type SemaphoreOptions = {
  /** How many holders may hold a slot at once. */
  readonly limit: number;
  /** Key namespace; keys are `<prefix>:<id>`. Default `"semaphore"`. */
  readonly prefix?: string;
  /**
   * How long a slot stays held without an {@link SemaphoreHandle.extend}.
   * Default `60000`, sized for model calls rather than CPU work.
   */
  readonly leaseMs?: number;
};

export type SemaphoreAcquireOptions = {
  /** Override the lease for this acquisition. */
  readonly leaseMs?: number;
  /** Retries while every slot is taken. Default `0` (fail fast). */
  readonly retries?: number;
  /** Delay between retries in milliseconds. Default `100`. */
  readonly retryDelayMs?: number;
};

/** Thrown by `semaphore().run()` when no slot came free. */
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

export type SemaphoreHandle = {
  readonly key: string;
  readonly token: string;
  /** Give the slot back; resolves `true` only if we still held it. */
  release(): Promise<boolean>;
  /** Push our lease out; resolves `false` if the slot was already reclaimed. */
  extend(leaseMs?: number): Promise<boolean>;
};

/**
 * A distributed semaphore: at most `limit` holders at once, across processes.
 *
 * This is concurrency, not rate. "100 requests per minute" and "at most 20
 * in flight" are different constraints, and model providers usually impose
 * both: a rate limit protects their billing, a concurrency limit protects
 * their capacity, and exceeding either gets you 429s.
 *
 * ```ts
 * const slots = semaphore(client, { limit: 20 });
 * const answer = await slots.run("openai", async (held) => {
 *   await held.extend();     // heartbeat a long call
 *   return callModel();
 * });
 * ```
 *
 * A holder that crashes frees its slot when its lease lapses, so a dead
 * process cannot wedge the pool. Works over any adapter, including
 * `benni/upstash` on the edge.
 *
 * This is [`lock`](./lock.js) with a number: same handle, same `run`, same
 * retry options. Reach for `lock` when the answer is one, and this when it is
 * a budget.
 */
export function semaphore(client: RedisClient, options: SemaphoreOptions) {
  const limit = positiveInt(options.limit, "limit");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const defaultLeaseMs = positiveInt(
    options.leaseMs ?? DEFAULT_LEASE_MS,
    "leaseMs"
  );
  const scripts = createScriptRunner(client);

  function handleFor(key: string, token: string): SemaphoreHandle {
    return {
      key,
      token,
      async release() {
        return (await scripts.run(releaseScript, [key], [token])) === 1;
      },
      async extend(leaseMs = defaultLeaseMs) {
        const ttl = String(positiveInt(leaseMs, "leaseMs"));
        return (await scripts.run(extendScript, [key], [ttl, token])) === 1;
      }
    };
  }

  async function acquire(
    id: string,
    acquireOptions?: SemaphoreAcquireOptions
  ): Promise<SemaphoreHandle | null> {
    const leaseMs = positiveInt(
      acquireOptions?.leaseMs ?? defaultLeaseMs,
      "leaseMs"
    );
    const retries = acquireOptions?.retries ?? 0;
    const retryDelayMs = acquireOptions?.retryDelayMs ?? 100;
    const key = `${prefix}:${id}`;
    for (let attempt = 0; ; attempt++) {
      const token = globalThis.crypto.randomUUID();
      const args = [String(limit), String(leaseMs), token] as const;
      if ((await scripts.run(acquireScript, [key], args)) === 1) {
        return handleFor(key, token);
      }
      if (attempt >= retries) return null;
      await sleep(retryDelayMs);
    }
  }

  return {
    acquire,
    /** How many slots are currently held, ignoring lapsed leases. */
    async count(id: string): Promise<number> {
      return scripts.run(countScript, [`${prefix}:${id}`], []);
    },
    /**
     * Take a slot, run `fn`, and give the slot back even if `fn` throws.
     * Throws {@link SemaphoreNotAcquiredError} if no slot came free (after any
     * configured retries).
     */
    async run<T>(
      id: string,
      fn: (handle: SemaphoreHandle) => Promise<T> | T,
      acquireOptions?: SemaphoreAcquireOptions
    ): Promise<T> {
      const handle = await acquire(id, acquireOptions);
      if (handle === null) {
        throw new SemaphoreNotAcquiredError(`${prefix}:${id}`, limit);
      }
      try {
        return await fn(handle);
      } finally {
        try {
          await handle.release();
        } catch {
          // A failed release must not mask fn's outcome (or replace its
          // error); the lease is the backstop and frees the slot regardless.
        }
      }
    }
  };
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `semaphore ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

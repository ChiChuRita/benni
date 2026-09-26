import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { RedisClient } from "../core/types.js";
import {
  SemaphoreLeaseLostError,
  SemaphoreNotAcquiredError
} from "./errors.js";
import {
  acquireWithRetry,
  createLease,
  type Lease,
  type LeaseTerms,
  monotonicNow,
  positiveMs,
  renewalInterval,
  runHeld,
  waitPolicy
} from "./lease.js";

const DEFAULT_PREFIX = "semaphore";
// `run()` renews on a quarter of the lease (see `lease.ts`), which at this
// default is exactly the queue's leaseMs 60000 / heartbeatMs 15000.
const DEFAULT_LEASE_MS = 60_000;
const TERMS: LeaseTerms = {
  owner: "semaphore",
  ttlName: "leaseMs",
  held: "slot"
};

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
  /**
   * How many times to retry while every slot is taken. Default `0`, which
   * **fails fast**: a full semaphore makes `acquire()` resolve `null` and
   * `run()` throw {@link SemaphoreNotAcquiredError} instead of waiting. Pass
   * `retries` (and optionally `retryDelayMs`), or `waitTimeoutMs`, to queue
   * behind the current holders instead.
   */
  readonly retries?: number;
  /**
   * Delay between retries in milliseconds. Default `100`. Each wait is
   * jittered over half to one and a half times this, so contenders that
   * collided once do not wake in lockstep and collide again.
   */
  readonly retryDelayMs?: number;
  /**
   * The most time to spend waiting for a slot, in milliseconds, across every
   * retry. Passed alone it means "retry until then"; with `retries` as well,
   * whichever runs out first stops the wait. A final attempt is made at the
   * deadline rather than after it.
   */
  readonly waitTimeoutMs?: number;
};

export type SemaphoreRunOptions = SemaphoreAcquireOptions & {
  /**
   * How often `run()` renews the lease while `fn` is in flight, in
   * milliseconds. Default: a quarter of the effective `leaseMs`. Keep it well
   * under `leaseMs` so a renewal may fail a few times before the slot lapses.
   *
   * A value you pass yourself must be at most **half** the effective `leaseMs`,
   * or `run()` throws a `ValidationError` before taking a slot: a heartbeat at
   * or above the lease puts the first tick on or after expiry, so the slot
   * would be declared lost before a single renewal was even attempted.
   *
   * Pass `false` to opt out of renewal: the slot is then reclaimable `leaseMs`
   * after it was taken, whatever `fn` is still doing.
   */
  readonly heartbeatMs?: number | false;
  /**
   * Called when a renewal round trip *fails* (a dropped connection, a timeout).
   * That is not the same as losing the slot: the next tick retries, and the
   * lease is only declared lost once Redis reports the slot is no longer ours,
   * or the lease window has demonstrably passed with no successful renewal.
   * Without this hook those errors are swallowed, exactly as a failed release
   * is.
   *
   * Telemetry only, and treated as such: a throw from this hook is swallowed
   * rather than allowed to reject the renewal promise `run()` discards (an
   * `unhandledRejection`, fatal in default Node). It is also never called once
   * `run()` has returned — a renewal already in flight when the slot was given
   * back reports nothing, since by then the failure is no longer news.
   */
  readonly onRenewError?: (error: unknown) => void;
};

export type SemaphoreHandle = {
  readonly key: string;
  readonly token: string;
  /**
   * Aborts with a {@link SemaphoreLeaseLostError} the moment this handle is
   * known to have lost its slot: `run()`'s automatic renewal failed, or an
   * `extend()` you made yourself resolved `false`. Pass it to `fetch`, the AI
   * SDK, or any `AbortSignal`-aware call so work stops when the semaphore stops
   * accounting for it.
   *
   * A handle from `acquire()` that you never `extend()` has nothing watching the
   * lease on your behalf, so its signal cannot fire: renew it yourself, or use
   * `run()`, which renews for you.
   */
  readonly signal: AbortSignal;
  /** Give the slot back; resolves `true` only if we still held it. */
  release(): Promise<boolean>;
  /**
   * Push our lease out; resolves `false` if the slot was already reclaimed. A
   * `false` result aborts {@link SemaphoreHandle.signal}. Without an argument
   * it re-applies the lease this acquisition was taken with, not the store
   * default.
   */
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
 * const slots = redis.query.gpuSlots; // semaphore("gpu", { limit: 20 })
 * const answer = await slots.run("openai", async () => callModel());
 * ```
 *
 * A holder that crashes frees its slot when its lease lapses, so a dead
 * process cannot wedge the pool. Works over any adapter, including
 * `benni/upstash` on the edge.
 *
 * Two defaults are worth knowing before you reach for it.
 *
 * **Acquiring fails fast.** `retries` defaults to `0`, so a caller that finds
 * every slot taken does not wait: `acquire()` resolves `null` and `run()` throws
 * {@link SemaphoreNotAcquiredError} straight away. That is the right default for
 * a request handler (shed load rather than pile up), and the wrong one if you
 * meant to *queue* concurrent work:
 *
 * ```ts
 * // Fail fast (default): 25 concurrent callers on a limit of 20 means 5 throw.
 * try {
 *   await slots.run("openai", () => callModel());
 * } catch (error) {
 *   if (error instanceof SemaphoreNotAcquiredError) return busy();
 *   throw error;
 * }
 *
 * // Queue instead: each caller waits for a slot to come free.
 * await slots.run("openai", () => callModel(), {
 *   retries: 100,
 *   retryDelayMs: 50
 * });
 * ```
 *
 * **`run()` renews the lease while your body runs**, every `heartbeatMs` (a
 * quarter of `leaseMs` by default), so a call that outlives `leaseMs` keeps its
 * slot instead of silently losing it and pushing the semaphore over its limit.
 * If renewal finds the slot gone, `held.signal` aborts and `run()` rejects
 * with {@link SemaphoreLeaseLostError} rather than reporting a success that was
 * never inside the bound:
 *
 * ```ts
 * await slots.run("openai", async (held) => {
 *   // Renewed in the background; pass the signal on so a reclaimed slot stops
 *   // the work instead of letting it run over the limit.
 *   await fetch(url, { signal: held.signal });
 * });
 * ```
 *
 * This is [`lock`](./lock.js) with a number: same handle, same `run`, same
 * retry options, same lease renewal. Reach for `lock` when the answer is one,
 * and this when it is a budget.
 */
export function createSemaphore(
  client: RedisClient,
  options: SemaphoreOptions
) {
  const checkMs = (ms: number, name: string) =>
    positiveMs(ms, name, "semaphore");
  const limit = checkMs(options.limit, "limit");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const defaultLeaseMs = checkMs(
    options.leaseMs ?? DEFAULT_LEASE_MS,
    "leaseMs"
  );
  const scripts = createScriptRunner(client);

  function handleFor(
    key: string,
    token: string,
    startedAt: number,
    leaseMs: number
  ): { handle: SemaphoreHandle; lease: Lease } {
    const lease = createLease({
      ttlMs: leaseMs,
      startedAt,
      checkTtl: (ms) => checkMs(ms, "leaseMs"),
      extend: async (ms) =>
        (await scripts.run(extendScript, [key], [String(ms), token])) === 1,
      release: async () =>
        (await scripts.run(releaseScript, [key], [token])) === 1,
      lostError: () => new SemaphoreLeaseLostError(key, limit)
    });
    const handle: SemaphoreHandle = {
      key,
      token,
      signal: lease.signal,
      release: () => lease.release(),
      extend: (ms) => lease.extend(ms)
    };
    return { handle, lease };
  }

  async function acquireLease(
    id: string,
    acquireOptions?: SemaphoreAcquireOptions
  ): Promise<{ handle: SemaphoreHandle; lease: Lease } | null> {
    const leaseMs = checkMs(
      acquireOptions?.leaseMs ?? defaultLeaseMs,
      "leaseMs"
    );
    const policy = waitPolicy(acquireOptions, "semaphore");
    const key = `${prefix}:${id}`;
    return acquireWithRetry(async () => {
      const token = globalThis.crypto.randomUUID();
      const args = [String(limit), String(leaseMs), token] as const;
      const startedAt = monotonicNow();
      if ((await scripts.run(acquireScript, [key], args)) !== 1) return null;
      return handleFor(key, token, startedAt, leaseMs);
    }, policy);
  }

  return {
    /**
     * Take a slot, or resolve `null` if every slot is taken. **Fails fast by
     * default** (`retries: 0`): pass `retries`/`retryDelayMs`, or a
     * `waitTimeoutMs`, to wait for a slot to come free instead of giving up on
     * the first attempt.
     *
     * You own the returned handle: `release()` it in a `finally`, and `extend()`
     * it yourself if the work can outlive `leaseMs`, because nothing renews an
     * `acquire()`d lease in the background and its `signal` cannot fire unless
     * you do. `run()` does both for you.
     */
    acquire(
      id: string,
      acquireOptions?: SemaphoreAcquireOptions
    ): Promise<SemaphoreHandle | null> {
      return acquireLease(id, acquireOptions).then(
        (held) => held?.handle ?? null
      );
    },
    /** How many slots are currently held, ignoring lapsed leases. */
    async count(id: string): Promise<number> {
      return scripts.run(countScript, [`${prefix}:${id}`], []);
    },
    /**
     * Take a slot, run `fn`, and give the slot back even if `fn` throws.
     *
     * **Fails fast by default.** With `retries: 0` (the default) a call that
     * finds every slot taken throws {@link SemaphoreNotAcquiredError}
     * immediately rather than waiting; to queue callers instead, pass
     * `{ retries, retryDelayMs }` or a `waitTimeoutMs`.
     *
     * **The lease is renewed while `fn` runs**, every `heartbeatMs` (a quarter
     * of `leaseMs` by default), so a body that outlives `leaseMs` keeps its
     * slot. If a renewal finds the slot gone, `handle.signal` aborts with a
     * {@link SemaphoreLeaseLostError} and `run()` rejects with it, even if `fn`
     * resolved: the slot had already been handed to another caller, so the
     * semaphore was over its limit for the rest of the body. Pass
     * `heartbeatMs: false` to opt out of renewal.
     *
     * The same verdict is reached without any renewal having run: when `fn`
     * resolves, the lease deadline is checked in that turn and the release's own
     * ownership check is consulted, so a body that blocks the event loop past
     * the lease is reported rather than passed off as inside the limit.
     *
     * @throws SemaphoreNotAcquiredError if no slot came free (after any
     * configured retries).
     * @throws SemaphoreLeaseLostError if the slot was lost while `fn` was
     * running.
     * @throws ValidationError if `leaseMs` is not a positive integer, if
     * `waitTimeoutMs` is negative, or if a `heartbeatMs` was passed that is
     * more than half the effective `leaseMs`.
     */
    async run<T>(
      id: string,
      fn: (handle: SemaphoreHandle) => Promise<T> | T,
      runOptions?: SemaphoreRunOptions
    ): Promise<T> {
      // Validate the renewal settings *before* taking a slot. A throw between
      // the acquire and the try/finally in `runHeld` would hold a slot until
      // its lease lapsed, shrinking the pool over a typo.
      const leaseMs = checkMs(runOptions?.leaseMs ?? defaultLeaseMs, "leaseMs");
      const heartbeatMs = renewalInterval(
        runOptions?.heartbeatMs,
        leaseMs,
        TERMS
      );

      const held = await acquireLease(id, runOptions);
      if (held === null) {
        throw new SemaphoreNotAcquiredError(`${prefix}:${id}`, limit);
      }
      return runHeld(held.lease, () => fn(held.handle), {
        heartbeatMs,
        onRenewError: runOptions?.onRenewError
      });
    }
  };
}

/** A semaphore, as `redis.query.<name>` returns it for a {@link SemaphoreSchema}. */
export type SemaphoreStore = ReturnType<typeof createSemaphore>;

/**
 * A semaphore declared as a schema value, so it lands in `redis.query` next to
 * the data stores and needs no client of its own.
 * @example
 * ```ts
 * // schema.ts
 * export const gpuSlots = semaphore("gpu", { limit: 4 });
 * // app.ts
 * await redis.query.gpuSlots.run("pool", async () => { … });
 * ```
 */
export type SemaphoreSchema = SemaphoreOptions & {
  readonly kind: "semaphore";
  readonly prefix: string;
};

const semaphoreBinding: StoreBinding = {
  resource: (ctx, schema: SemaphoreSchema) =>
    createSemaphore(ctx.client, schema)
};

/**
 * Build a {@link SemaphoreSchema}. Exported as `semaphore` from `benni/schema`.
 */
export function defineSemaphore(
  prefix: string,
  options: Omit<SemaphoreOptions, "prefix">
): SemaphoreSchema {
  return withStore(
    { ...options, kind: "semaphore", prefix } as SemaphoreSchema,
    semaphoreBinding
  );
}

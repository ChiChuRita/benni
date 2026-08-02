import { type ClientSource, clientArgs } from "../core/client-source.js";
import { ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "semaphore";
const DEFAULT_LEASE_MS = 60_000;
// `run()` renews on a quarter of the lease, the same ratio `lock` and the queue
// use (leaseMs 60000 / heartbeatMs 15000, which is exactly this default): three
// renewals in a row may fail outright before the slot could lapse, which is
// what makes a transient blip survivable rather than fatal.
const HEARTBEAT_DIVISOR = 4;

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
   * `retries` (and optionally `retryDelayMs`) to queue behind the current
   * holders instead.
   */
  readonly retries?: number;
  /** Delay between retries in milliseconds. Default `100`. */
  readonly retryDelayMs?: number;
};

export type SemaphoreRunOptions = SemaphoreAcquireOptions & {
  /**
   * How often `run()` renews the lease while `fn` is in flight, in
   * milliseconds. Default: a quarter of the effective `leaseMs`. Keep it well
   * under `leaseMs` so a renewal may fail a few times before the slot lapses.
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
   */
  readonly onRenewError?: (error: unknown) => void;
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

/**
 * Thrown by `semaphore().run()` when the slot was lost while `fn` was still
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
 * abort reason on {@link SemaphoreHandle.signal}, so a body that passes the
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
   * `false` result aborts {@link SemaphoreHandle.signal}.
   */
  extend(leaseMs?: number): Promise<boolean>;
};

/**
 * What we believe about our hold on a slot, tracked alongside the handle so
 * `run()`'s renewal loop and the caller's own `extend()`/`release()` calls share
 * one view of it.
 */
type Lease = {
  /** The lease this acquisition uses, and that renewals re-apply. */
  readonly leaseMs: number;
  /**
   * Local timestamp at which the slot is certainly reclaimable unless it is
   * renewed. Measured from *before* each round trip, so it never overstates how
   * long we hold the slot: the server stamps expiry from its own clock at some
   * point during the call, which is never earlier than this.
   */
  expiresAt: number;
  /** True once we know the slot is no longer ours. */
  lost: boolean;
  /** True once the caller gave the slot up deliberately. */
  released: boolean;
  /** Record the loss, once, and abort the handle's signal. */
  lose(): void;
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
function createSemaphore(client: RedisClient, options: SemaphoreOptions) {
  const limit = positiveInt(options.limit, "limit");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const defaultLeaseMs = positiveInt(
    options.leaseMs ?? DEFAULT_LEASE_MS,
    "leaseMs"
  );
  const scripts = createScriptRunner(client);

  function handleFor(
    key: string,
    token: string,
    acquiredAt: number,
    leaseMs: number
  ): { handle: SemaphoreHandle; lease: Lease } {
    const controller = new AbortController();
    const lease: Lease = {
      leaseMs,
      expiresAt: acquiredAt + leaseMs,
      lost: false,
      released: false,
      lose() {
        if (lease.lost) return;
        lease.lost = true;
        controller.abort(new SemaphoreLeaseLostError(key, limit));
      }
    };
    const handle: SemaphoreHandle = {
      key,
      token,
      signal: controller.signal,
      async release() {
        // Flagged before the round trip: giving the slot up is deliberate, so a
        // renewal that overlaps this call must not report it as a loss.
        lease.released = true;
        return (await scripts.run(releaseScript, [key], [token])) === 1;
      },
      async extend(nextLeaseMs = defaultLeaseMs) {
        const ms = positiveInt(nextLeaseMs, "leaseMs");
        // Time the renewal from before the call, not after: the script stamps
        // the new expiry from server time at some point during it, so
        // `sentAt + ms` is the earliest the slot can be reclaimed. Anything
        // later would let the deadline below claim we still hold a slot that
        // has already been handed on.
        const sentAt = Date.now();
        const args = [String(ms), token] as const;
        const held = (await scripts.run(extendScript, [key], args)) === 1;
        if (held) lease.expiresAt = sentAt + ms;
        else if (!lease.released) lease.lose();
        return held;
      }
    };
    return { handle, lease };
  }

  async function acquireLease(
    id: string,
    acquireOptions?: SemaphoreAcquireOptions
  ): Promise<{ handle: SemaphoreHandle; lease: Lease } | null> {
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
      const sentAt = Date.now();
      if ((await scripts.run(acquireScript, [key], args)) === 1) {
        return handleFor(key, token, sentAt, leaseMs);
      }
      if (attempt >= retries) return null;
      await sleep(retryDelayMs);
    }
  }

  return {
    /**
     * Take a slot, or resolve `null` if every slot is taken. **Fails fast by
     * default** (`retries: 0`): pass `retries`/`retryDelayMs` to wait for a slot
     * to come free instead of giving up on the first attempt.
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
     * `{ retries, retryDelayMs }`.
     *
     * **The lease is renewed while `fn` runs**, every `heartbeatMs` (a quarter
     * of `leaseMs` by default), so a body that outlives `leaseMs` keeps its
     * slot. If a renewal finds the slot gone, `handle.signal` aborts with a
     * {@link SemaphoreLeaseLostError} and `run()` rejects with it, even if `fn`
     * resolved: the slot had already been handed to another caller, so the
     * semaphore was over its limit for the rest of the body. Pass
     * `heartbeatMs: false` to opt out of renewal.
     *
     * @throws SemaphoreNotAcquiredError if no slot came free (after any
     * configured retries).
     * @throws SemaphoreLeaseLostError if the slot was lost while `fn` was
     * running.
     */
    async run<T>(
      id: string,
      fn: (handle: SemaphoreHandle) => Promise<T> | T,
      runOptions?: SemaphoreRunOptions
    ): Promise<T> {
      // Validate the renewal settings *before* taking a slot. A throw between
      // the acquire and the try/finally below would hold a slot until its lease
      // lapsed, shrinking the pool over a typo.
      const leaseMs = positiveInt(
        runOptions?.leaseMs ?? defaultLeaseMs,
        "leaseMs"
      );
      const heartbeatMs =
        runOptions?.heartbeatMs === false
          ? null
          : positiveInt(
              runOptions?.heartbeatMs ?? heartbeatFor(leaseMs),
              "heartbeatMs"
            );
      const onRenewError = runOptions?.onRenewError;

      const held = await acquireLease(id, runOptions);
      if (held === null) {
        throw new SemaphoreNotAcquiredError(`${prefix}:${id}`, limit);
      }
      const { handle, lease } = held;

      let stopped = false;
      let renewing = false;
      const timer =
        heartbeatMs === null
          ? null
          : setInterval(() => {
              if (stopped || lease.lost || lease.released) return;
              // The whole lease window has passed with no successful renewal, so
              // the slot is reclaimable whatever the cause: renewals that keep
              // rejecting, or one still hanging while the guard below skips
              // ticks. Silence here is the bug this renewal exists to fix.
              if (Date.now() >= lease.expiresAt) {
                lease.lose();
                return;
              }
              // One renewal at a time. A round trip slower than the interval
              // would otherwise stack up calls that all re-apply the same lease.
              if (renewing) return;
              renewing = true;
              void handle.extend(lease.leaseMs).then(
                () => {
                  renewing = false;
                },
                (error: unknown) => {
                  renewing = false;
                  // A failed round trip is not proof the slot is gone, so the
                  // next tick retries; the deadline above is what eventually
                  // calls it lost.
                  onRenewError?.(error);
                }
              );
            }, heartbeatMs);
      // Never keep the process alive for a renewal alone: an un-unref'd interval
      // is what makes `node script.js` hang after the work is done.
      (timer as { unref?: () => void } | null)?.unref?.();

      try {
        const result = await fn(handle);
        // `fn` finished, but not under the bound it asked for. Resolving here is
        // what let a lost slot pass for a call inside the limit.
        if (lease.lost) throw new SemaphoreLeaseLostError(handle.key, limit);
        return result;
      } finally {
        stopped = true;
        if (timer !== null) clearInterval(timer);
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

/**
 * The default renewal interval for a lease. Floored at 1ms so an absurdly short
 * `leaseMs` still renews rather than dividing down to a zero-delay spin.
 */
function heartbeatFor(leaseMs: number): number {
  return Math.max(1, Math.floor(leaseMs / HEARTBEAT_DIVISOR));
}

/** The semaphore {@link semaphore} returns. */
export type SemaphoreStore = ReturnType<typeof createSemaphore>;

/** {@link SemaphoreOptions} plus the client, for the single-argument form. */
export type SemaphoreConfig = SemaphoreOptions & {
  /** The client, a promise of one, a factory, or a benni handle. */
  readonly client: ClientSource;
};

export function semaphore(config: SemaphoreConfig): SemaphoreStore;
export function semaphore(
  client: ClientSource,
  options: SemaphoreOptions
): SemaphoreStore;
export function semaphore(
  source: ClientSource | SemaphoreConfig,
  options?: SemaphoreOptions
): SemaphoreStore {
  const args = clientArgs<SemaphoreOptions>(source, options);
  return createSemaphore(args.client, args.options);
}

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
  options: SemaphoreOptions
): SemaphoreSchema {
  return withStore(
    { ...options, kind: "semaphore", prefix } as SemaphoreSchema,
    semaphoreBinding
  );
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

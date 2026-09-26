import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { RedisClient } from "../core/types.js";
import { LockLeaseLostError, LockNotAcquiredError } from "./errors.js";
import {
  acquireWithRetry,
  coLocatedKey,
  createLease,
  extendIfHeldScript,
  type Lease,
  type LeaseTerms,
  monotonicNow,
  positiveMs,
  releaseIfHeldScript,
  renewalInterval,
  runHeld,
  waitPolicy
} from "./lease.js";

const DEFAULT_PREFIX = "lock";
const DEFAULT_TTL_MS = 30_000;
const TERMS: LeaseTerms = { owner: "lock", ttlName: "ttlMs", held: "lock" };

/**
 * Take the lock and draw the next fencing token in one atomic step. Returns
 * the fence (1 or more) if acquired, else 0.
 *
 * The fence has to come from the same script as the `SET NX`: drawn in a
 * second round trip, two holders in quick succession could each take the
 * lock and then draw their fences in the opposite order, and a downstream
 * store would reject the *current* holder's writes. KEYS[2] shares KEYS[1]'s
 * slot (see `coLocatedKey`), so this is legal on a cluster.
 */
const acquireScript = defineScript<
  readonly [token: string, ttlMs: string],
  number
>({
  keyCount: 2,
  lua: 'if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then return redis.call("INCR", KEYS[2]) end return 0',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

export type LockOptions = {
  /**
   * Key namespace; locks are `<prefix>:<id>`, and each id's fence counter is
   * `{<prefix>:<id>}:fence` (the braces keep it in the lock's Cluster slot).
   * Default `"lock"`.
   */
  readonly prefix?: string;
  /** Lock lifetime in milliseconds. Default `30000`. */
  readonly ttlMs?: number;
};

export type AcquireOptions = {
  /** Override the lock lifetime for this acquisition. */
  readonly ttlMs?: number;
  /**
   * How many times to retry while the lock is held. Default `0`, which **fails
   * fast**: a contended `acquire()` resolves `null` and a contended `run()`
   * throws {@link LockNotAcquiredError} instead of waiting. Pass `retries` (and
   * optionally `retryDelayMs`), or `waitTimeoutMs`, to queue behind the
   * current holder instead.
   */
  readonly retries?: number;
  /**
   * Delay between retries in milliseconds. Default `100`. Each wait is
   * jittered over half to one and a half times this, so contenders that
   * collided once do not wake in lockstep and collide again.
   */
  readonly retryDelayMs?: number;
  /**
   * The most time to spend waiting for the lock, in milliseconds, across every
   * retry. Passed alone it means "retry until then"; with `retries` as well,
   * whichever runs out first stops the wait. A final attempt is made at the
   * deadline rather than after it.
   */
  readonly waitTimeoutMs?: number;
};

export type LockRunOptions = AcquireOptions & {
  /**
   * How often `run()` renews the lock while `fn` is in flight, in
   * milliseconds. Default: a quarter of the effective `ttlMs`. Keep it well
   * under `ttlMs` so a renewal may fail a few times before the lock lapses.
   *
   * A value you pass yourself must be at most **half** the effective `ttlMs`,
   * or `run()` throws a `ValidationError` before taking the lock: a heartbeat
   * at or above the TTL puts the first tick on or after expiry, so the lock
   * would be declared lost before a single renewal was even attempted.
   *
   * Pass `false` to opt out of renewal: the lock then expires `ttlMs` after it
   * was taken, whatever `fn` is still doing.
   */
  readonly heartbeatMs?: number | false;
  /**
   * Called when a renewal round trip *fails* — a dropped connection, a
   * timeout. That is not the same as losing the lock: the next tick retries,
   * and the lease is only declared lost once Redis reports another token owns
   * the key, or the TTL window has demonstrably passed with no successful
   * renewal. Without this hook those errors are swallowed, exactly as a failed
   * release is.
   *
   * Telemetry only, and treated as such: a throw from this hook is swallowed
   * rather than allowed to reject the renewal promise `run()` discards (an
   * `unhandledRejection`, fatal in default Node). It is also never called once
   * `run()` has returned — a renewal already in flight when the lock was
   * released reports nothing, since by then the failure is no longer news.
   */
  readonly onRenewError?: (error: unknown) => void;
};

export type LockHandle = {
  readonly key: string;
  readonly token: string;
  /**
   * The fencing token for this acquisition: a number that strictly increases
   * with every acquisition of this lock id, across all processes.
   *
   * A lease cannot stop a paused holder from waking up and writing after its
   * lock expired and someone else took it. A downstream store can: send the
   * fence with every write, and have the store reject any write whose fence is
   * lower than the highest it has already accepted.
   *
   * ```sql
   * UPDATE orders SET status = $1, fence = $2
   *  WHERE id = $3 AND fence < $2;  -- 0 rows: a newer holder already wrote
   * ```
   */
  readonly fence: number;
  /**
   * Aborts with a {@link LockLeaseLostError} the moment this handle is known to
   * have lost the lock: `run()`'s automatic renewal failed, or an `extend()`
   * you made yourself resolved `false`. Pass it to `fetch`, the AI SDK, or any
   * `AbortSignal`-aware call so work stops when it stops being exclusive.
   *
   * A handle from `acquire()` that you never `extend()` has nothing watching
   * the lock on your behalf, so its signal cannot fire — renew it yourself, or
   * use `run()`, which renews for you.
   */
  readonly signal: AbortSignal;
  /** Release the lock; resolves `true` only if we still held it. */
  release(): Promise<boolean>;
  /**
   * Extend the lock's TTL; resolves `true` only if we still held it. A `false`
   * result means the lock is gone, and aborts {@link LockHandle.signal}.
   * Without an argument it re-applies the TTL this acquisition was taken
   * with, not the store default.
   */
  extend(ttlMs?: number): Promise<boolean>;
};

/**
 * A distributed lock over Redis: `SET key token NX PX ttl` to acquire, and an
 * atomic check-and-delete Lua to release, so a caller can never release a lock
 * that expired and was re-acquired elsewhere. Every acquisition also carries a
 * {@link LockHandle.fence | fencing token} for downstream stores to check.
 * Works over any adapter, including `benni/upstash` on the edge.
 *
 * It assumes **one Redis primary**. Replication is asynchronous, so a failover
 * can promote a replica that never saw the `SET`, and grant the lock a second
 * time while the first holder is still inside. There is no Redlock here: when
 * a double grant would be unacceptable, check {@link LockHandle.fence} where
 * the write lands.
 *
 * Two defaults are worth knowing before you reach for it.
 *
 * **Acquiring fails fast.** `retries` defaults to `0`, so a second caller does
 * not wait: `acquire()` resolves `null` and `run()` throws
 * {@link LockNotAcquiredError} straight away. That is the right default for a
 * request handler (return 409 rather than pile up), and the wrong one if you
 * meant to *serialize* concurrent work — for that, ask for retries:
 *
 * ```ts
 * const locks = redis.query.orderLocks; // lock("order", { ttlMs: 10_000 })
 *
 * // Fail fast (default): six concurrent callers means one runs and five throw.
 * try {
 *   await locks.run("order:42", async () => charge(order));
 * } catch (error) {
 *   if (error instanceof LockNotAcquiredError) return conflict();
 *   throw error;
 * }
 *
 * // Serialize instead: each caller waits its turn behind the holder.
 * await locks.run("order:42", async () => charge(order), {
 *   retries: 100,
 *   retryDelayMs: 50
 * });
 * ```
 *
 * **`run()` renews the lock while your body runs**, every `heartbeatMs` (a
 * quarter of `ttlMs` by default), so a critical section that outlives `ttlMs`
 * keeps the lock instead of silently losing it. If renewal finds the lock gone,
 * `handle.signal` aborts and `run()` rejects with {@link LockLeaseLostError}
 * rather than reporting a success that was never exclusive:
 *
 * ```ts
 * await locks.run("order:42", async (handle) => {
 *   // Renewed in the background; pass the signal on so a lost lock stops the
 *   // work instead of letting it finish unprotected.
 *   await fetch(url, { signal: handle.signal });
 * });
 * ```
 */
export function createLock(client: RedisClient, options?: LockOptions) {
  const prefix = options?.prefix ?? DEFAULT_PREFIX;
  const defaultTtlMs = options?.ttlMs ?? DEFAULT_TTL_MS;
  const scripts = createScriptRunner(client);
  const checkTtl = (ms: number) => positiveMs(ms, "ttlMs", "lock");

  function handleFor(
    key: string,
    token: string,
    fence: number,
    startedAt: number,
    ttlMs: number
  ): { handle: LockHandle; lease: Lease } {
    const lease = createLease({
      ttlMs,
      startedAt,
      checkTtl,
      extend: async (ms) =>
        (await scripts.run(extendIfHeldScript, [key], [token, String(ms)])) ===
        1,
      release: async () =>
        (await scripts.run(releaseIfHeldScript, [key], [token])) === 1,
      lostError: () => new LockLeaseLostError(key)
    });
    const handle: LockHandle = {
      key,
      token,
      fence,
      signal: lease.signal,
      release: () => lease.release(),
      extend: (ttl) => lease.extend(ttl)
    };
    return { handle, lease };
  }

  async function acquireLease(
    id: string,
    acquireOptions?: AcquireOptions
  ): Promise<{ handle: LockHandle; lease: Lease } | null> {
    const ttlMs = checkTtl(acquireOptions?.ttlMs ?? defaultTtlMs);
    const policy = waitPolicy(acquireOptions, "lock");
    const key = `${prefix}:${id}`;
    // TODO: one fence counter per lock id, kept forever, because a counter
    // that expired would restart below fences a downstream store has already
    // accepted. Fine for a bounded set of ids; a time-seeded fence would let
    // it expire if the key count ever matters.
    const fenceKey = coLocatedKey(key, "fence", "lock");
    return acquireWithRetry(async () => {
      const token = globalThis.crypto.randomUUID();
      const startedAt = monotonicNow();
      const fence = await scripts.run(
        acquireScript,
        [key, fenceKey],
        [token, String(ttlMs)]
      );
      return fence > 0 ? handleFor(key, token, fence, startedAt, ttlMs) : null;
    }, policy);
  }

  return {
    /**
     * Take the lock, or resolve `null` if someone else holds it. **Fails fast
     * by default** (`retries: 0`): pass `retries`/`retryDelayMs`, or a
     * `waitTimeoutMs`, to wait for the current holder instead of giving up on
     * the first attempt.
     *
     * You own the returned handle: `release()` it in a `finally`, and
     * `extend()` it yourself if the work can outlive `ttlMs` — nothing renews
     * an `acquire()`d lock in the background. `run()` does both for you.
     */
    acquire(
      id: string,
      acquireOptions?: AcquireOptions
    ): Promise<LockHandle | null> {
      return acquireLease(id, acquireOptions).then(
        (held) => held?.handle ?? null
      );
    },
    /**
     * Acquire, run `fn`, and release — even if `fn` throws.
     *
     * **Fails fast by default.** With `retries: 0` (the default) a contended
     * call throws {@link LockNotAcquiredError} immediately rather than waiting;
     * to serialize concurrent callers, pass `{ retries, retryDelayMs }` or a
     * `waitTimeoutMs` so each one queues behind the holder.
     *
     * **The lock is renewed while `fn` runs**, every `heartbeatMs` (a quarter of
     * `ttlMs` by default), so a body that outlives `ttlMs` keeps its lock. If a
     * renewal finds the lock gone, `handle.signal` aborts with a
     * {@link LockLeaseLostError} and `run()` rejects with it — even if `fn`
     * resolved, because a body that finished without the lock did not finish
     * under mutual exclusion. Pass `heartbeatMs: false` to opt out of renewal.
     *
     * The same verdict is reached without any renewal having run: when `fn`
     * resolves, the TTL deadline is checked in that turn and the release's own
     * token check is consulted, so a body that blocks the event loop past the
     * TTL is reported rather than passed off as exclusive.
     *
     * @throws LockNotAcquiredError if the lock cannot be acquired (after any
     * configured retries).
     * @throws LockLeaseLostError if the lock was lost while `fn` was running.
     * @throws ValidationError if `ttlMs` is not a positive integer, if
     * `waitTimeoutMs` is negative, or if a `heartbeatMs` was passed that is
     * more than half the effective `ttlMs`.
     */
    async run<T>(
      id: string,
      fn: (handle: LockHandle) => Promise<T> | T,
      runOptions?: LockRunOptions
    ): Promise<T> {
      // Validate the renewal settings *before* taking the lock. A throw between
      // the acquire and the try/finally in `runHeld` would strand the key until
      // its TTL lapsed, holding up every other caller over a typo.
      const ttlMs = checkTtl(runOptions?.ttlMs ?? defaultTtlMs);
      const heartbeatMs = renewalInterval(
        runOptions?.heartbeatMs,
        ttlMs,
        TERMS
      );

      const held = await acquireLease(id, runOptions);
      if (held === null) {
        throw new LockNotAcquiredError(`${prefix}:${id}`);
      }
      return runHeld(held.lease, () => fn(held.handle), {
        heartbeatMs,
        onRenewError: runOptions?.onRenewError
      });
    }
  };
}

/** A lock set, as `redis.query.<name>` returns it for a {@link LockSchema}. */
export type LockStore = ReturnType<typeof createLock>;

/**
 * A lock set declared as a schema value, so it lands in `redis.query` next to
 * the data stores and needs no client of its own.
 * @example
 * ```ts
 * // schema.ts
 * export const orderLocks = lock("order", { ttlMs: 10_000 });
 * // app.ts
 * await redis.query.orderLocks.run("42", async () => { … });
 * ```
 */
export type LockSchema = LockOptions & {
  readonly kind: "lock";
  readonly prefix: string;
};

const lockBinding: StoreBinding = {
  resource: (ctx, schema: LockSchema) => createLock(ctx.client, schema)
};

/** Build a {@link LockSchema}. Exported as `lock` from `benni/schema`. */
export function defineLock(
  prefix: string,
  options?: Omit<LockOptions, "prefix">
): LockSchema {
  return withStore(
    { ...options, kind: "lock", prefix } as LockSchema,
    lockBinding
  );
}

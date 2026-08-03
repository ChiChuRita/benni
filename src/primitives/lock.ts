import { type ClientSource, clientArgs } from "../core/client-source.js";
import { ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "lock";
const DEFAULT_TTL_MS = 30_000;
// `run()` renews on a quarter of the TTL, the same ratio the queue uses for
// its leases (leaseMs 60000 / heartbeatMs 15000): three renewals in a row may
// fail outright before the lock could lapse, which is what makes a transient
// blip survivable rather than fatal.
const HEARTBEAT_DIVISOR = 4;

// Release only if we still hold the token — never DEL a lock that has expired
// and been re-acquired by someone else. Returns 1 if released, else 0.
const releaseScript = defineScript<readonly [token: string], number>({
  keyCount: 1,
  lua: 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

// Extend the TTL only if we still hold the token. Returns 1 if extended, else 0.
const extendScript = defineScript<
  readonly [token: string, ttlMs: string],
  number
>({
  keyCount: 1,
  lua: 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

export type LockOptions = {
  /** Key namespace; keys are `<prefix>:<id>`. Default `"lock"`. */
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
   * optionally `retryDelayMs`) to queue behind the current holder instead.
   */
  readonly retries?: number;
  /** Delay between retries in milliseconds. Default `100`. */
  readonly retryDelayMs?: number;
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

/** Thrown by `lock().run()` when the lock cannot be acquired. */
export class LockNotAcquiredError extends Error {
  readonly key: string;
  constructor(key: string) {
    super(`Could not acquire lock "${key}"`);
    this.name = "LockNotAcquiredError";
    this.key = key;
  }
}

/**
 * Thrown by `lock().run()` when the lock was lost while `fn` was still
 * running — renewal found the key gone or owned by another token, so the
 * critical section ran without the mutual exclusion it asked for and someone
 * else may have been inside it at the same time.
 *
 * `run()` rejects with this even when `fn` itself resolved: a body that
 * completed without the lock did not complete under the guarantee it was
 * written against, and reporting success would hide exactly that. The same
 * error is the abort reason on {@link LockHandle.signal}, so a body that passes
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

export type LockHandle = {
  readonly key: string;
  readonly token: string;
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
   */
  extend(ttlMs?: number): Promise<boolean>;
};

/**
 * What we believe about our hold on the lock, tracked alongside the handle so
 * `run()`'s renewal loop and the caller's own `extend()`/`release()` calls
 * share one view of it.
 */
type Lease = {
  /** The TTL this acquisition uses, and that renewals re-apply. */
  readonly ttlMs: number;
  /**
   * Local timestamp at which the lock has certainly lapsed unless it is
   * renewed. Measured from *before* each round trip, so it never overstates
   * how long we hold the lock.
   */
  expiresAt: number;
  /** True once we know the lock is no longer ours. */
  lost: boolean;
  /** True once the caller gave the lock up deliberately. */
  released: boolean;
  /** Record the loss, once, and abort the handle's signal. */
  lose(): void;
};

/**
 * A distributed lock over Redis: `SET key token NX PX ttl` to acquire, and an
 * atomic check-and-delete Lua to release, so a caller can never release a lock
 * that expired and was re-acquired elsewhere. Works over any adapter, including
 * `benni/upstash` on the edge.
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
 * const locks = lock(client, { ttlMs: 10_000 });
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

  function handleFor(
    key: string,
    token: string,
    acquiredAt: number,
    ttlMs: number
  ): { handle: LockHandle; lease: Lease } {
    const controller = new AbortController();
    const lease: Lease = {
      ttlMs,
      expiresAt: acquiredAt + ttlMs,
      lost: false,
      released: false,
      lose() {
        if (lease.lost) return;
        lease.lost = true;
        controller.abort(new LockLeaseLostError(key));
      }
    };
    const handle: LockHandle = {
      key,
      token,
      signal: controller.signal,
      async release() {
        // Flagged before the round trip: giving the lock up is deliberate, so
        // a renewal that overlaps this call must not report it as a loss.
        lease.released = true;
        return (await scripts.run(releaseScript, [key], [token])) === 1;
      },
      async extend(nextTtlMs = defaultTtlMs) {
        const ms = positiveMs(nextTtlMs, "ttlMs");
        // Time the renewal from before the call, not after: the server applies
        // PEXPIRE at some point during it, so `sentAt + ms` is the earliest the
        // new TTL can lapse. Anything later would let the deadline below claim
        // we still hold a lock that has already expired.
        const sentAt = Date.now();
        const args = [token, String(ms)] as const;
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
    acquireOptions?: AcquireOptions
  ): Promise<{ handle: LockHandle; lease: Lease } | null> {
    const ttlMs = positiveMs(acquireOptions?.ttlMs ?? defaultTtlMs, "ttlMs");
    const retries = acquireOptions?.retries ?? 0;
    const retryDelayMs = acquireOptions?.retryDelayMs ?? 100;
    const key = `${prefix}:${id}`;
    for (let attempt = 0; ; attempt++) {
      const token = globalThis.crypto.randomUUID();
      const sentAt = Date.now();
      const reply = await client.send(["SET", key, token, "NX", "PX", ttlMs]);
      if (reply !== null) return handleFor(key, token, sentAt, ttlMs);
      if (attempt >= retries) return null;
      await sleep(retryDelayMs);
    }
  }

  return {
    /**
     * Take the lock, or resolve `null` if someone else holds it. **Fails fast
     * by default** (`retries: 0`): pass `retries`/`retryDelayMs` to wait for
     * the current holder instead of giving up on the first attempt.
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
     * to serialize concurrent callers, pass `{ retries, retryDelayMs }` so each
     * one queues behind the holder.
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
     * @throws ValidationError if `ttlMs` is not a positive integer, or if a
     * `heartbeatMs` was passed that is more than half the effective `ttlMs`.
     */
    async run<T>(
      id: string,
      fn: (handle: LockHandle) => Promise<T> | T,
      runOptions?: LockRunOptions
    ): Promise<T> {
      // Validate the renewal settings *before* taking the lock. A throw between
      // the acquire and the try/finally below would strand the key until its
      // TTL lapsed, holding up every other caller over a typo.
      const ttlMs = positiveMs(runOptions?.ttlMs ?? defaultTtlMs, "ttlMs");
      const heartbeatMs = renewalInterval(runOptions?.heartbeatMs, ttlMs);
      const onRenewError = runOptions?.onRenewError;

      const held = await acquireLease(id, runOptions);
      if (held === null) {
        throw new LockNotAcquiredError(`${prefix}:${id}`);
      }
      const { handle, lease } = held;

      let stopped = false;
      let renewing = false;
      let timer: ReturnType<typeof setInterval> | null = null;
      /**
       * Stop renewing, for good. Called both from the tick that notices the
       * lock is gone and from the exit path below, because "the flag is set"
       * is not the same as "the interval is gone": a lease declared lost used
       * to leave the interval armed, and a body that ignores the abort signal
       * and never settles then span on early-returning ticks for the life of
       * the process.
       */
      const stopRenewal = (): void => {
        stopped = true;
        if (timer === null) return;
        clearInterval(timer);
        timer = null;
      };
      /**
       * Hand a failed round trip to the caller's hook, if any, without letting
       * the hook out. A throw from it would reject the renewal promise the tick
       * discards, and an unobserved rejection is fatal in default Node: a
       * telemetry callback must not be able to take the process down.
       */
      const reportRenewError = (error: unknown): void => {
        // Deliberately silent once `run()` has stopped renewing: a round trip
        // still in flight when the lock was released can settle after the call
        // the caller awaited already returned, and reporting a failure to renew
        // a lock we have since given up is noise, not news.
        if (stopped || onRenewError === undefined) return;
        try {
          onRenewError(error);
        } catch {
          // Swallowed exactly as a failed release is. The hook exists to
          // observe renewals, not to decide the fate of the critical section.
        }
      };
      /**
       * One renewal round trip, with every outcome handled *inside* it. The
       * tick discards the returned promise, so anything escaping here would be
       * an unobserved rejection.
       */
      const renewOnce = async (): Promise<void> => {
        try {
          // `extend()` flags the lease lost itself when Redis reports the key
          // is not ours, so a `false` result means there is nothing left to
          // renew and the interval can go now rather than on the next tick.
          if (!(await handle.extend(lease.ttlMs))) stopRenewal();
        } catch (error) {
          // A failed round trip is not proof the lock is gone, so the next tick
          // retries; the deadline below is what eventually calls it lost.
          reportRenewError(error);
        } finally {
          renewing = false;
        }
      };
      if (heartbeatMs !== null) {
        timer = setInterval(() => {
          // Nothing left to renew: `run()` is done, the body gave the lock up,
          // or the lease is gone. Tear the interval down rather than waking up
          // to early-return from here on.
          if (stopped || lease.lost || lease.released) {
            stopRenewal();
            return;
          }
          // The whole TTL window has passed with no successful renewal, so
          // the lock has lapsed whatever the cause: renewals that keep
          // rejecting, or one still hanging while the guard below skips
          // ticks. Silence here is the bug this renewal exists to fix.
          if (Date.now() >= lease.expiresAt) {
            lease.lose();
            stopRenewal();
            return;
          }
          // One renewal at a time. A round trip slower than the interval
          // would otherwise stack up calls that all re-apply the same TTL.
          if (renewing) return;
          renewing = true;
          void renewOnce();
        }, heartbeatMs);
        // Never keep the process alive for a renewal alone: an un-unref'd
        // interval is what makes `node script.js` hang after the work is done.
        (timer as { unref?: () => void }).unref?.();
      }

      // Evidence about our hold on the lock that only exists at the end of the
      // call: the clock as the body finished, whether the body had given the
      // lock up by then, and what the release reported.
      let expiredOnCompletion = false;
      let releasedByBody = false;
      let heldOnRelease: boolean | null = null;
      /**
       * Whether the critical section has to be reported as having run without
       * the lock. `lease.lost` alone is not enough, because it is only ever set
       * from inside the renewal tick: a body that blocks the event loop past
       * the TTL and then returns without awaiting anything never lets the tick
       * run at all, and since a timer is a macrotask while `await fn(handle)`
       * resumes on a microtask, the check below used to win that race and
       * report success for a lock that had already expired.
       */
      const lostTheLock = (): boolean => {
        // Proven: Redis told a renewal the key is no longer ours.
        if (lease.lost) return true;
        // Given up on purpose. Renewals and the second release both find the
        // key gone, and neither of those is a loss. Read from the snapshot, not
        // from `lease.released`: our own release in the exit path sets that flag
        // too, and consulting it live would excuse every lost lock there is.
        if (releasedByBody) return false;
        // Renewal was switched off, so a lock that lapses under a long body is
        // exactly what that opt-out documents.
        if (heartbeatMs === null) return false;
        // The deadline, read in the same turn the body finished rather than
        // only from a tick that may never have got to run.
        if (expiredOnCompletion) return true;
        // The release ran the same token check `extend()` does, so `false` is
        // Redis saying the lock had already moved on. `null` (the round trip
        // itself failed) proves nothing either way.
        return heldOnRelease === false;
      };

      let result: T;
      try {
        result = await fn(handle);
        // Snapshotted before the release, so the round trip it takes cannot push
        // a body that finished comfortably inside its lease past the deadline,
        // and so a lock the body gave up is told apart from one we release here.
        expiredOnCompletion = Date.now() >= lease.expiresAt;
        releasedByBody = lease.released;
      } finally {
        stopRenewal();
        try {
          heldOnRelease = await handle.release();
        } catch {
          // A failed release must not mask fn's outcome (or replace its
          // error); the lock's TTL is the backstop and frees it regardless.
        }
      }
      // Reached only when `fn` resolved: a body that threw propagates its own
      // error, which a lease report would bury. `fn` finished, but not under
      // the guarantee it asked for, and resolving here is what let a lost lock
      // pass for a successful critical section.
      if (lostTheLock()) throw new LockLeaseLostError(handle.key);
      return result;
    }
  };
}

/** The lock set {@link lock} returns. */
export type LockStore = ReturnType<typeof createLock>;

/** {@link LockOptions} plus the client, for the single-argument form. */
export type LockConfig = LockOptions & {
  /** The client, a promise of one, a factory, or a benni handle. */
  readonly client: ClientSource;
};

export function lock(config: LockConfig): LockStore;
export function lock(client: ClientSource, options?: LockOptions): LockStore;
export function lock(
  source: ClientSource | LockConfig,
  options?: LockOptions
): LockStore {
  const args = clientArgs<LockOptions>(source, options);
  return createLock(args.client, args.options);
}

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
export function defineLock(prefix: string, options?: LockOptions): LockSchema {
  return withStore(
    { ...options, kind: "lock", prefix } as LockSchema,
    lockBinding
  );
}

/**
 * The default renewal interval for a TTL. Floored at 1ms so an absurdly short
 * `ttlMs` still renews rather than dividing down to a zero-delay spin.
 */
function heartbeatFor(ttlMs: number): number {
  return Math.max(1, Math.floor(ttlMs / HEARTBEAT_DIVISOR));
}

/**
 * The renewal interval for one `run()`: `null` when the caller opted out, the
 * derived default when they said nothing, and their own value otherwise.
 *
 * Only a value the caller passed is checked against the TTL, and it has to
 * leave room for a renewal *and* a retry, so half the TTL is the ceiling. At or
 * above the TTL the first tick lands on or after expiry, so the lock is
 * declared lost before a single renewal has been attempted — on an uncontended
 * lock, and only once a body is slow enough to reach that first tick, so the
 * misconfiguration passes every quick test and shows up under load.
 *
 * The derived default is deliberately exempt: {@link heartbeatFor} floors at
 * 1ms, which for a `ttlMs` of 1 is the whole TTL and can satisfy no ratio at
 * all, and a working configuration must not start throwing.
 */
function renewalInterval(
  requested: number | false | undefined,
  ttlMs: number
): number | null {
  if (requested === false) return null;
  if (requested === undefined) return heartbeatFor(ttlMs);
  const heartbeatMs = positiveMs(requested, "heartbeatMs");
  if (heartbeatMs * 2 > ttlMs) {
    throw new ValidationError(
      `lock heartbeatMs must be at most half of ttlMs (${ttlMs}) so a renewal lands before the lock could lapse, received ${heartbeatMs}`
    );
  }
  return heartbeatMs;
}

function positiveMs(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `lock ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

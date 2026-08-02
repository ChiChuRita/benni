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
     * @throws LockNotAcquiredError if the lock cannot be acquired (after any
     * configured retries).
     * @throws LockLeaseLostError if the lock was lost while `fn` was running.
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
      const heartbeatMs =
        runOptions?.heartbeatMs === false
          ? null
          : positiveMs(
              runOptions?.heartbeatMs ?? heartbeatFor(ttlMs),
              "heartbeatMs"
            );
      const onRenewError = runOptions?.onRenewError;

      const held = await acquireLease(id, runOptions);
      if (held === null) {
        throw new LockNotAcquiredError(`${prefix}:${id}`);
      }
      const { handle, lease } = held;

      let stopped = false;
      let renewing = false;
      const timer =
        heartbeatMs === null
          ? null
          : setInterval(() => {
              if (stopped || lease.lost || lease.released) return;
              // The whole TTL window has passed with no successful renewal, so
              // the lock has lapsed whatever the cause: renewals that keep
              // rejecting, or one still hanging while the guard below skips
              // ticks. Silence here is the bug this renewal exists to fix.
              if (Date.now() >= lease.expiresAt) {
                lease.lose();
                return;
              }
              // One renewal at a time. A round trip slower than the interval
              // would otherwise stack up calls that all re-apply the same TTL.
              if (renewing) return;
              renewing = true;
              void handle.extend(lease.ttlMs).then(
                () => {
                  renewing = false;
                },
                (error: unknown) => {
                  renewing = false;
                  // A failed round trip is not proof the lock is gone, so the
                  // next tick retries; the deadline above is what eventually
                  // calls it lost.
                  onRenewError?.(error);
                }
              );
            }, heartbeatMs);
      // Never keep the process alive for a renewal alone: an un-unref'd
      // interval is what makes `node script.js` hang after the work is done.
      (timer as { unref?: () => void } | null)?.unref?.();

      try {
        const result = await fn(handle);
        // `fn` finished, but not under the guarantee it asked for. Resolving
        // here is what let a lost lock pass for a successful critical section.
        if (lease.lost) throw new LockLeaseLostError(handle.key);
        return result;
      } finally {
        stopped = true;
        if (timer !== null) clearInterval(timer);
        try {
          await handle.release();
        } catch {
          // A failed release must not mask fn's outcome (or replace its
          // error); the lock's TTL is the backstop and frees it regardless.
        }
      }
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

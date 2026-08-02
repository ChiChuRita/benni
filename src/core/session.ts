import { replyShapeError, ValidationError } from "./errors.js";
import type { SlotGuard } from "./slot.js";
import {
  createWatchedTransaction,
  type WatchedRedisTransaction
} from "./transaction.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  RedisSession
} from "./types.js";

/**
 * Thrown by the session kernel's command gate for any use after close().
 * In-flight rejections during a connection drop keep the adapter-native
 * error; worker loops discriminate with `session.closed`.
 */
export class SessionClosedError extends Error {
  constructor() {
    super("Session is closed");
    this.name = "SessionClosedError";
  }
}

/**
 * Thrown by runWatch()/redis.watch() when every attempt aborted because a
 * WATCHed key kept changing. Carries the total attempt count.
 */
export class WatchRetriesExceededError extends Error {
  readonly attempts: number;

  constructor(attempts: number) {
    super(
      `Watched transaction aborted ${attempts} time${attempts === 1 ? "" : "s"}; retries exceeded`
    );
    this.name = "WatchRetriesExceededError";
    this.attempts = attempts;
  }
}

export type BlockingTimeout = number | "forever";

/**
 * The one timeout shape for every blocking operation. Seconds, matching
 * Redis's own BLPOP semantics; fractional allowed. Blocking forever is
 * spelled `{ timeoutSeconds: "forever" }` — a visible, greppable literal
 * that can never be arrived at via arithmetic.
 */
export type BlockingWait = { readonly timeoutSeconds: BlockingTimeout };

function validateBlockingTimeout(timeout: BlockingTimeout): void {
  if (timeout === "forever") return;
  if (
    typeof timeout !== "number" ||
    Number.isNaN(timeout) ||
    !Number.isFinite(timeout) ||
    timeout <= 0
  ) {
    throw new ValidationError(
      'Blocking timeoutSeconds must be a positive finite number of seconds or "forever"'
    );
  }
}

/**
 * Redis argument for second-based blocking commands (BLPOP, BRPOP, BLMOVE,
 * BZPOPMIN/MAX). Fractional seconds pass through as decimal strings;
 * `"forever"` becomes Redis's block-forever sentinel `"0"`.
 */
export function blockingTimeoutSeconds(timeout: BlockingTimeout): string {
  validateBlockingTimeout(timeout);
  if (timeout === "forever") return "0";
  return String(timeout);
}

/**
 * Redis argument for millisecond-based BLOCK options (XREAD, XREADGROUP).
 * Converts seconds to integer milliseconds, never rounding a positive
 * timeout down to the block-forever sentinel; `"forever"` becomes `"0"`.
 */
export function blockingTimeoutMilliseconds(timeout: BlockingTimeout): string {
  validateBlockingTimeout(timeout);
  if (timeout === "forever") return "0";
  return String(Math.max(1, Math.round(timeout * 1000)));
}

/**
 * The runtime-agnostic session kernel the Benni layer wraps into BenniSession.
 * `client` is the RedisClient-shaped facade the existing store factories
 * consume verbatim; every method funnels through the FIFO command gate.
 */
export type BenniSessionKernel = {
  /** Gated RedisClient facade for the session-bound store accessors. */
  readonly client: RedisClient;
  /** WATCH k1 k2…; throws on empty. */
  watch(keys: readonly string[]): Promise<void>;
  /** UNWATCH. */
  unwatch(): Promise<void>;
  /** Abort-aware builder; exec() resolves the tuple or null on abort. */
  multi(): WatchedRedisTransaction<[]>;
  /** Gated passthrough to the raw session's watchedTransaction. */
  watchedTransaction(
    commands: readonly RedisCommand[]
  ): Promise<RedisReply[] | null>;
  /** Escape hatch to the raw adapter session. */
  readonly raw: RedisSession;
  readonly closed: boolean;
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

/**
 * Core session kernel: FIFO command gate over a raw RedisSession plus the
 * RedisClient-shaped facade, watch/unwatch primitives, the abort-aware
 * multi() builder, and asyncDispose plumbing.
 *
 * The gate is a promise-chain mutex. Single sends hold one slot; the
 * pipeline facade and a watched exec() (whose MULTI+commands+EXEC the
 * adapter enqueues contiguously) hold one slot for their whole
 * multi-command span. Concurrent calls queue in FIFO order — ordinary
 * pipelining semantics — rather than throw. close() bypasses the gate so
 * it can promptly reject an in-flight blocking read; queued-but-unsent
 * work then rejects with SessionClosedError.
 */
export function createBenniSession(
  raw: RedisSession,
  assertSameSlot?: SlotGuard
): BenniSessionKernel {
  let closed = false;
  let tail: Promise<void> = Promise.resolve();
  // WATCH state is connection-wide, so the facade has to know when one is
  // armed: EXEC drops every watched key, and a store batching two commands
  // through transaction() must not do that to the caller's watch.
  let watchArmed = false;

  function run<T>(task: () => Promise<T>): Promise<T> {
    if (closed) return Promise.reject(new SessionClosedError());
    const result = tail.then(() => {
      if (closed) throw new SessionClosedError();
      return task();
    });
    tail = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  async function close(): Promise<void> {
    closed = true;
    await raw.close();
  }

  function pipeline(commands: readonly RedisCommand[]): Promise<RedisReply[]> {
    return run(async () => {
      // Fire without awaiting so the batch is written contiguously in
      // invocation order (the contract's ordered-dispatch clause).
      const settled = await Promise.allSettled(
        commands.map((command) => raw.send(command))
      );
      return settled.map((entry) => {
        if (entry.status === "rejected") throw entry.reason;
        return entry.value;
      });
    });
  }

  const client: RedisClient = {
    send(command) {
      return run(() => raw.send(command));
    },
    pipeline,
    transaction(commands) {
      // Preserve redis.multi()'s empty short-circuit without ever sending a
      // zero-command watched EXEC (contract: core never does).
      if (commands.length === 0) return Promise.resolve([]);
      // A session-bound store reaches this for an internal batch it wants
      // atomic — hset(id, value, { ttlSeconds }) is HSET + EXPIRE. Sending
      // MULTI/EXEC on a connection that holds a WATCH would clear the watch
      // set, so the caller's optimistic transaction would commit over a
      // concurrent write instead of aborting. Losing the batch's atomicity
      // is the far smaller harm, so degrade to the ordered pipeline.
      if (watchArmed) return pipeline(commands);
      return run(async () => {
        const replies = await raw.watchedTransaction(commands);
        if (replies === null) {
          // A bare multi with no WATCH cannot abort; if it does, something
          // armed a watch out of band and loud is right.
          throw replyShapeError("EXEC", "array", replies);
        }
        return replies;
      });
    },
    close
  };

  const kernel: BenniSessionKernel = {
    client,
    async watch(keys) {
      if (keys.length === 0) {
        throw new ValidationError("watch requires at least one key");
      }
      assertSameSlot?.("WATCH", keys);
      // Arm before dispatch: transaction() reads the flag when it is called,
      // which can be while an unawaited WATCH is still queued. Staying armed
      // after a failed WATCH only costs a batch its atomicity.
      watchArmed = true;
      await run(async () => {
        const reply = await raw.send(["WATCH", ...keys]);
        if (reply !== "OK") {
          throw replyShapeError("WATCH", "OK", reply);
        }
      });
    },
    async unwatch() {
      await run(async () => {
        const reply = await raw.send(["UNWATCH"]);
        if (reply !== "OK") {
          throw replyShapeError("UNWATCH", "OK", reply);
        }
        watchArmed = false;
      });
    },
    multi() {
      return createWatchedTransaction(kernel, assertSameSlot);
    },
    watchedTransaction(commands) {
      return run(async () => {
        try {
          return await raw.watchedTransaction(commands);
        } finally {
          // EXEC drops the watch set whether it committed or aborted, and a
          // send that failed outright leaves a session no one can trust.
          watchArmed = false;
        }
      });
    },
    raw,
    get closed() {
      return closed || raw.closed;
    },
    close,
    [Symbol.asyncDispose]: close
  };
  return kernel;
}

/**
 * The slice of a session runWatch() needs; both the core kernel and the
 * Benni layer's BenniSession satisfy it.
 */
export type WatchSession = {
  watch(keys: readonly string[]): Promise<void>;
  unwatch(): Promise<void>;
  close(): Promise<void>;
};

/** Per-session FIFO queue of WATCH windows; see acquireWatchWindow. */
const watchWindows = new WeakMap<WatchSession, Promise<void>>();

/**
 * WATCH belongs to the connection, not to the call: two watch windows running
 * at once on one borrowed session would each see the other's watched keys and
 * be disarmed by the other's EXEC — a spurious abort for one, a lost update
 * for the other. So a window owns its session from WATCH to EXEC and the next
 * one queues behind it. Owned sessions never contend.
 */
async function acquireWatchWindow(session: WatchSession): Promise<() => void> {
  const queued = watchWindows.get(session) ?? Promise.resolve();
  let release!: () => void;
  const window = new Promise<void>((resolve) => {
    release = resolve;
  });
  watchWindows.set(
    session,
    queued.then(() => window)
  );
  await queued;
  return release;
}

export type RunWatchOptions<TSession extends WatchSession> = {
  /** Total attempts, default 5, >= 1. */
  readonly attempts?: number;
  /** Milliseconds to sleep before retry N; default none. */
  readonly backoff?: (attempt: number) => number;
  /** Per-conflict hook — observe contention before it is an incident. */
  readonly onAbort?: (info: { readonly attempt: number }) => void;
  /** Borrow a long-lived session (hot paths); never closed by the helper. */
  readonly session?: TSession;
};

/**
 * Retry loop backing redis.watch(). Per attempt: WATCH keys, run the body
 * (reads via the session), exec the returned builder. `null` from exec is
 * a conflict — fire onAbort, back off, re-WATCH, retry. `null` from the
 * body is an opt-out — UNWATCH and resolve null. A thrown body UNWATCHes
 * best-effort and rethrows. Exhausted attempts throw
 * WatchRetriesExceededError. Owned sessions close in finally on every
 * path; borrowed sessions are never closed but are held exclusively for the
 * length of each window, so concurrent watches on one session queue up.
 */
export async function runWatch<
  TSession extends WatchSession,
  TResults extends readonly unknown[]
>(
  openSession: () => Promise<TSession>,
  keys: string | readonly string[],
  body: (
    session: TSession
  ) => Promise<WatchedRedisTransaction<TResults> | null>,
  options: RunWatchOptions<TSession> = {}
): Promise<TResults | null> {
  const attempts = options.attempts ?? 5;
  if (!Number.isSafeInteger(attempts) || attempts < 1) {
    throw new ValidationError("attempts must be a safe integer >= 1");
  }
  const keyList = typeof keys === "string" ? [keys] : keys;
  if (keyList.length === 0) {
    throw new ValidationError("watch requires at least one key");
  }
  let owned: TSession | undefined;
  let session: TSession;
  if (options.session === undefined) {
    owned = await openSession();
    session = owned;
  } else {
    session = options.session;
  }
  try {
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      // Hold the session for the whole window; the backoff sleep below is
      // outside it, so a queued window is not blocked by another's retries.
      const releaseWindow = await acquireWatchWindow(session);
      let results: TResults | null;
      try {
        await session.watch(keyList);
        let transaction: WatchedRedisTransaction<TResults> | null;
        try {
          transaction = await body(session);
        } catch (error) {
          try {
            await session.unwatch();
          } catch {
            // Best-effort: the body's error is the one worth surfacing.
          }
          throw error;
        }
        if (transaction === null) {
          await session.unwatch();
          return null;
        }
        try {
          results = await transaction.exec();
        } catch (error) {
          // A client-side exec() throw (e.g. the empty-transaction guard)
          // never sent EXEC, so the WATCH stays armed on this — possibly
          // borrowed — session, and a later multi() would see phantom
          // conflicts. UNWATCH after a real EXEC is a harmless no-op, so
          // always clean up.
          try {
            await session.unwatch();
          } catch {
            // Best-effort: the exec error is the one worth surfacing.
          }
          throw error;
        }
      } finally {
        releaseWindow();
      }
      if (results !== null) return results;
      options.onAbort?.({ attempt });
      if (attempt < attempts && options.backoff !== undefined) {
        const delayMs = options.backoff(attempt);
        if (delayMs > 0) await sleep(delayMs);
      }
    }
    throw new WatchRetriesExceededError(attempts);
  } finally {
    if (owned !== undefined) await owned.close();
  }
}

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

import { type ClientSource, clientArgs } from "../core/client-source.js";
import { codecs } from "../core/codecs.js";
import { ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { Codec, InferAnchors, RedisClient } from "../core/types.js";
import {
  createLease,
  extendIfHeldScript,
  heartbeatFor,
  monotonicNow,
  positiveMs,
  releaseIfHeldScript,
  sleep,
  whileRenewing
} from "./lease.js";

const DEFAULT_PREFIX = "idem";
const DEFAULT_TTL_MS = 86_400_000;
const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_MS = 50;

// One string per key, told apart by its first character:
//   R<token><fingerprint?>              running; token is a 36-char UUID
//   D<encoded>                          done, recorded without a fingerprint
//   F<length>:<fingerprint><encoded>    done, with the caller's fingerprint
// A string rather than a hash so records written before fingerprints existed
// (`R<token>`, `D<encoded>`) still read correctly.
const RUNNING = "R";
const DONE = "D";
const DONE_FINGERPRINTED = "F";
const TOKEN_LENGTH = 36;

/**
 * Store the result, but only over our own running marker. Comparing the whole
 * marker (token and fingerprint) is the token check: without it, a slow first
 * caller that already lost its claim would overwrite a *second* caller's
 * in-flight record, or its result.
 */
const completeScript = defineScript<
  readonly [marker: string, record: string, ttlMs: string],
  number
>({
  keyCount: 1,
  lua: `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
redis.call("SET", KEYS[1], ARGV[2], "PX", tonumber(ARGV[3]))
return 1
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

type StoredRecord =
  | { readonly state: "running"; readonly fingerprint?: string }
  | {
      readonly state: "done";
      readonly encoded: string;
      readonly fingerprint?: string;
    }
  | { readonly state: "unknown" };

function parseRecord(held: string): StoredRecord {
  const kind = held[0];
  if (kind === RUNNING) {
    const fingerprint = held.slice(1 + TOKEN_LENGTH);
    return fingerprint === ""
      ? { state: "running" }
      : { state: "running", fingerprint };
  }
  if (kind === DONE) return { state: "done", encoded: held.slice(1) };
  if (kind === DONE_FINGERPRINTED) {
    const colon = held.indexOf(":");
    const length = Number(held.slice(1, colon));
    if (colon > 1 && Number.isSafeInteger(length)) {
      const start = colon + 1;
      return {
        state: "done",
        fingerprint: held.slice(start, start + length),
        encoded: held.slice(start + length)
      };
    }
  }
  return { state: "unknown" };
}

function doneRecord(encoded: string, fingerprint: string | undefined): string {
  return fingerprint === undefined
    ? `${DONE}${encoded}`
    : `${DONE_FINGERPRINTED}${fingerprint.length}:${fingerprint}${encoded}`;
}

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

export type IdempotencyOptions<T> = {
  /** How long a completed result is replayable. Default `86400000` (24h). */
  readonly ttlMs?: number;
  /** Key namespace; keys are `<prefix>:<key>`. Default `"idem"`. */
  readonly prefix?: string;
  /** Result codec. Default `codecs.json<T>()`. */
  readonly codec?: Codec<T>;
  /**
   * How long a caller that died mid-handler blocks the key before another may
   * run it, in milliseconds. Defaults to `waitTimeoutMs`.
   *
   * It does not bound the handler: the marker is renewed every quarter of
   * this while the handler runs, so a 40s charge under the 30s default keeps
   * its claim. It is lost only when renewal fails for this long in a row.
   */
  readonly runningTtlMs?: number;
  /** What to do when another caller is mid-flight. Default `"wait"`. */
  readonly onConflict?: "wait" | "throw";
  /** How long to wait under `onConflict: "wait"`. Default `30000`. */
  readonly waitTimeoutMs?: number;
  /** Poll interval while waiting. Default `50`. */
  readonly pollMs?: number;
};

export type IdempotencyRunOptions = {
  /**
   * A digest of the request the key belongs to, computed by the caller (a
   * SHA-256 of the body, say). Recorded with the key; a later call with the
   * same key and a *different* fingerprint throws
   * {@link IdempotencyFingerprintMismatchError} instead of replaying or waiting.
   *
   * Compared only when both calls passed one, so records written without a
   * fingerprint (or by code that predates it) are still replayed.
   */
  readonly fingerprint?: string;
};

/** What the handler is given. */
export type IdempotencyContext = {
  /**
   * Aborts with {@link IdempotencyLeaseLostError} once the running marker is
   * known to be lost, so another caller may be running the handler too. Pass it
   * to `fetch` or the AI SDK, or check it before committing the effect.
   */
  readonly signal: AbortSignal;
};

export type IdempotentResult<T> = {
  readonly value: T;
  /** True when this is a replay of an earlier call's stored result. */
  readonly replayed: boolean;
};

/**
 * Run a side effect once per caller-supplied idempotency key, and replay its
 * result to every retry.
 *
 * A retried POST must not charge the card twice, and must return the *first*
 * response rather than a fresh one. That is the Stripe `Idempotency-Key`
 * contract, and it is not the same problem as caching: a cache may recompute a
 * pure read whenever it likes, while this must run the effect once and replay
 * whatever it produced.
 *
 * ```ts
 * const once = idempotency<Receipt>({ client });
 * const { value, replayed } = await once.run(
 *   request.headers.get("Idempotency-Key"),
 *   ({ signal }) => chargeCard(order, { signal }),
 *   { fingerprint: await sha256(body) }
 * );
 * ```
 *
 * A losing caller waits for the winner's result by default, so a double-click
 * gets the same receipt rather than a 409. Works over any adapter, including
 * `benni/upstash` on the edge.
 *
 * **What it guarantees, precisely.** At most one caller runs the handler for a
 * key at a time, and once one succeeds its result is replayed for `ttlMs`. The
 * claim is a Redis key renewed while the handler runs, so a slow handler keeps
 * it. That is not exactly-once, and three cases run the handler again:
 *
 * - **The handler throws.** The key is released so the operation can be
 *   retried, which is right for the failures you actually see (a timeout, a
 *   503) and means a handler that fails *after* a partial effect repeats it.
 * - **The process dies mid-handler.** Its claim lapses after `runningTtlMs`
 *   and the next caller runs the handler, whatever the first got done.
 * - **The claim is lost while the handler runs** (renewal failed for
 *   `runningTtlMs`, or a Redis failover dropped the key). The handler's
 *   `signal` aborts, and if it resolves anyway `run` throws
 *   {@link IdempotencyNotRecordedError}, because another caller may be running
 *   it too.
 *
 * Make the effect itself safe to repeat (pass the same key to your payment
 * provider), or record progress inside it. This is an idempotency key, not a
 * transaction.
 */
function createIdempotency<T>(
  client: RedisClient,
  options?: IdempotencyOptions<T>
) {
  const checkMs = (ms: number, name: string) =>
    positiveMs(ms, name, "idempotency");
  const ttlMs = checkMs(options?.ttlMs ?? DEFAULT_TTL_MS, "ttlMs");
  const prefix = options?.prefix ?? DEFAULT_PREFIX;
  const codec = options?.codec ?? codecs.json<T>();
  const onConflict = options?.onConflict ?? "wait";
  const waitTimeoutMs = checkMs(
    options?.waitTimeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS,
    "waitTimeoutMs"
  );
  const runningTtlMs = checkMs(
    options?.runningTtlMs ?? waitTimeoutMs,
    "runningTtlMs"
  );
  const pollMs = checkMs(options?.pollMs ?? DEFAULT_POLL_MS, "pollMs");
  const scripts = createScriptRunner(client);

  const keyFor = (key: string) => `${prefix}:${key}`;

  async function read(key: string): Promise<string | null> {
    const reply = await client.send(["GET", keyFor(key)]);
    return typeof reply === "string" ? reply : null;
  }

  /** Run `fn` under a claim we just took, renewing it while `fn` runs. */
  async function runClaimed(
    key: string,
    marker: string,
    startedAt: number,
    fingerprint: string | undefined,
    fn: (context: IdempotencyContext) => Promise<T> | T
  ): Promise<IdempotentResult<T>> {
    const redisKey = keyFor(key);
    const lease = createLease({
      ttlMs: runningTtlMs,
      startedAt,
      checkTtl: (ms) => checkMs(ms, "runningTtlMs"),
      extend: async (ms) =>
        (await scripts.run(
          extendIfHeldScript,
          [redisKey],
          [marker, String(ms)]
        )) === 1,
      // Guarded by the marker, so we never clear a record some later caller
      // now owns: without it, a slow first caller that already lost its claim
      // would delete a *second* caller's in-flight record on its way out, and
      // the operation would run a third time.
      release: async () =>
        (await scripts.run(releaseIfHeldScript, [redisKey], [marker])) === 1,
      lostError: () => new IdempotencyLeaseLostError(key)
    });
    let value: T;
    try {
      ({ value } = await whileRenewing(
        lease,
        () => fn({ signal: lease.signal }),
        { heartbeatMs: heartbeatFor(runningTtlMs) }
      ));
    } catch (error) {
      // Release so a retry can proceed.
      try {
        await lease.release();
      } catch {
        // The TTL frees it regardless; never mask fn's error.
      }
      throw error;
    }
    try {
      const stored = await lease.close(() =>
        scripts.run(
          completeScript,
          [redisKey],
          [marker, doneRecord(codec.encode(value), fingerprint), String(ttlMs)]
        )
      );
      // 0 means our marker was not there any more: it lapsed or a later
      // caller now owns the key, so nothing was written and that caller may
      // have run the handler too. The script surfaces it as a return value,
      // so a bare `await` let it pass for success.
      if (stored !== 1) throw lease.lostError();
    } catch (cause) {
      // Deliberately NOT swallowed, even though the effect succeeded.
      //
      // This is not lock.run, where a failed release only costs a wasted TTL.
      // Here the unwritten record *is* the guarantee: the next caller with
      // this key re-runs the handler, and reporting plain success would hide
      // that. The error carries the value so a caller who can salvage it may.
      throw new IdempotencyNotRecordedError(key, value, cause);
    }
    return { value, replayed: false };
  }

  return {
    /**
     * Run `fn` for `key` unless a result is already recorded, replaying the
     * stored result thereafter. See {@link idempotency} for exactly what is and
     * is not guaranteed.
     *
     * Passing a nullish key runs `fn` unguarded and reports `replayed: false`,
     * so a handler can forward an optional `Idempotency-Key` header straight
     * through without branching on its presence.
     *
     * @throws IdempotencyFingerprintMismatchError if `key` was used with a
     * different `fingerprint`.
     * @throws IdempotencyConflictError under `onConflict: "throw"` while
     * another caller holds `key`.
     * @throws IdempotencyTimeoutError if `waitTimeoutMs` passed first.
     * @throws IdempotencyNotRecordedError if `fn` succeeded but its result
     * could not be recorded (its `cause` says why).
     */
    async run(
      key: string | null | undefined,
      fn: (context: IdempotencyContext) => Promise<T> | T,
      runOptions?: IdempotencyRunOptions
    ): Promise<IdempotentResult<T>> {
      const fingerprint = runOptions?.fingerprint;
      if (
        fingerprint !== undefined &&
        (typeof fingerprint !== "string" || fingerprint === "")
      ) {
        throw new ValidationError(
          `idempotency fingerprint must be a non-empty string, received ${JSON.stringify(fingerprint)}`
        );
      }
      if (key === null || key === undefined || key === "") {
        const unguarded = new AbortController();
        return {
          value: await fn({ signal: unguarded.signal }),
          replayed: false
        };
      }
      const redisKey = keyFor(key);
      const marker = `${RUNNING}${globalThis.crypto.randomUUID()}${fingerprint ?? ""}`;

      for (const deadline = monotonicNow() + waitTimeoutMs; ; ) {
        const startedAt = monotonicNow();
        const claimed = await client.send([
          "SET",
          redisKey,
          marker,
          "NX",
          "PX",
          runningTtlMs
        ]);
        if (claimed !== null) {
          return runClaimed(key, marker, startedAt, fingerprint, fn);
        }

        const held = await read(key);
        // Someone else is mid-flight, or done (or the record vanished between
        // our SET and our GET, in which case looping re-races for the claim).
        if (held !== null) {
          const record = parseRecord(held);
          if (
            fingerprint !== undefined &&
            record.state !== "unknown" &&
            record.fingerprint !== undefined &&
            record.fingerprint !== fingerprint
          ) {
            throw new IdempotencyFingerprintMismatchError(key);
          }
          if (record.state === "done") {
            return { value: codec.decode(record.encoded), replayed: true };
          }
          if (onConflict === "throw") throw new IdempotencyConflictError(key);
        }
        if (monotonicNow() >= deadline) {
          throw new IdempotencyTimeoutError(key, waitTimeoutMs);
        }
        await sleep(pollMs);
      }
    },

    /** The stored result, or `null` if absent or still running. */
    async peek(key: string): Promise<T | null> {
      const held = await read(key);
      if (held === null) return null;
      const record = parseRecord(held);
      return record.state === "done" ? codec.decode(record.encoded) : null;
    },

    /** Drop the record so the next call runs again. */
    async forget(key: string): Promise<boolean> {
      const reply = await client.send(["DEL", keyFor(key)]);
      return reply === 1;
    }
  };
}

/** The keyed once-only runner {@link idempotency} returns. */
export type IdempotencyStore<T> = ReturnType<typeof createIdempotency<T>>;

/** {@link IdempotencyOptions} plus the client, for the single-argument form. */
export type IdempotencyConfig<T> = IdempotencyOptions<T> & {
  /** The client, a promise of one, a factory, or a benni handle. */
  readonly client: ClientSource;
};

export function idempotency<T>(
  config: IdempotencyConfig<T>
): IdempotencyStore<T>;
export function idempotency<T>(
  client: ClientSource,
  options?: IdempotencyOptions<T>
): IdempotencyStore<T>;
export function idempotency<T>(
  source: ClientSource | IdempotencyConfig<T>,
  options?: IdempotencyOptions<T>
): IdempotencyStore<T> {
  const args = clientArgs<IdempotencyOptions<T>>(source, options);
  return createIdempotency<T>(args.client, args.options);
}

/**
 * A once-only runner declared as a schema value, so it lands in `redis.query`
 * next to the data stores and needs no client of its own.
 * @example
 * ```ts
 * // schema.ts
 * export const charges = idempotency("charge", { codec: json(Receipt) });
 * // app.ts
 * const { value, replayed } = await redis.query.charges.run(key, () => charge(order));
 * ```
 */
export type IdempotencySchema<T> = InferAnchors<T, T> &
  IdempotencyOptions<T> & {
    readonly kind: "idempotency";
    readonly prefix: string;
  };

const idempotencyBinding: StoreBinding = {
  resource: (ctx, schema: IdempotencySchema<unknown>) =>
    createIdempotency(ctx.client, schema)
};

/**
 * Build an {@link IdempotencySchema}. Exported as `idempotency` from
 * `benni/schema`.
 */
export function defineIdempotency<T>(
  prefix: string,
  options?: IdempotencyOptions<T>
): IdempotencySchema<T> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    ...options,
    kind: "idempotency",
    prefix
  } as IdempotencySchema<T>;
  return withStore(schema, idempotencyBinding);
}

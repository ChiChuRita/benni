import { codecs } from "../core/codecs.js";
import { ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import type { Codec, RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "idem";
const DEFAULT_TTL_MS = 86_400_000;
const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_MS = 50;

const RUNNING = "R";
const DONE = "D";

/**
 * Clear the slot only if we are still the one running it.
 *
 * The token check matters: without it, a slow first caller that already lost
 * its claim to a TTL lapse would delete a *second* caller's in-flight record
 * on its way out, and the operation would run a third time.
 */
const abandonScript = defineScript<readonly [token: string], number>({
  keyCount: 1,
  lua: `
local held = redis.call("GET", KEYS[1])
if held == false then return 0 end
if string.sub(held, 1, 1) ~= "${RUNNING}" then return 0 end
if string.sub(held, 2) ~= ARGV[1] then return 0 end
return redis.call("DEL", KEYS[1])
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/** Store the result, but only over our own running marker. */
const completeScript = defineScript<
  readonly [token: string, encoded: string, ttlMs: string],
  number
>({
  keyCount: 1,
  lua: `
local held = redis.call("GET", KEYS[1])
if held == false then return 0 end
if string.sub(held, 1, 1) ~= "${RUNNING}" then return 0 end
if string.sub(held, 2) ~= ARGV[1] then return 0 end
redis.call("SET", KEYS[1], "${DONE}" .. ARGV[2], "PX", tonumber(ARGV[3]))
return 1
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

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
 * The side effect happened. What failed is the record of it, which means the
 * running marker will lapse and a later caller with the same key will run the
 * handler again. Treat it as indeterminate rather than as a failure: the work
 * is done, and `value` carries the result if you can use it (return it to the
 * client, write it somewhere durable), but do not assume a retry is safe.
 *
 * The usual cause is a codec that cannot encode the result, or a Redis blip
 * between finishing the work and recording it.
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
   * How long one caller may hold the slot before another assumes it died.
   * Defaults to `waitTimeoutMs`. Size it to your slowest handler.
   */
  readonly runningTtlMs?: number;
  /** What to do when another caller is mid-flight. Default `"wait"`. */
  readonly onConflict?: "wait" | "throw";
  /** How long to wait under `onConflict: "wait"`. Default `30000`. */
  readonly waitTimeoutMs?: number;
  /** Poll interval while waiting. Default `50`. */
  readonly pollMs?: number;
};

export type IdempotentResult<T> = {
  readonly value: T;
  /** True when this is a replay of an earlier call's stored result. */
  readonly replayed: boolean;
};

/**
 * Exactly-once side effects, keyed by a caller-supplied idempotency key.
 *
 * A retried POST must not charge the card twice, and must return the *first*
 * response rather than a fresh one. That is the Stripe `Idempotency-Key`
 * contract, and it is not the same problem as caching: a cache may recompute a
 * pure read whenever it likes, while this must run the effect once and replay
 * whatever it produced.
 *
 * ```ts
 * const once = idempotency<Receipt>(client);
 * const { value, replayed } = await once.run(
 *   request.headers.get("Idempotency-Key"),
 *   () => chargeCard(order)
 * );
 * ```
 *
 * A losing caller waits for the winner's result by default, so a double-click
 * gets the same receipt rather than a 409. Works over any adapter, including
 * `beni/upstash` on the edge.
 *
 * **If the handler throws, the key is released** so the operation can be
 * retried. That is right for the failures you actually see (a timeout, a 503),
 * but it means a handler that fails *after* a partial side effect will run
 * that part again. This is an idempotency key, not a transaction: make the
 * effect itself safe to repeat, or record progress inside it.
 */
export function idempotency<T>(
  client: RedisClient,
  options?: IdempotencyOptions<T>
) {
  const ttlMs = positiveInt(options?.ttlMs ?? DEFAULT_TTL_MS, "ttlMs");
  const prefix = options?.prefix ?? DEFAULT_PREFIX;
  const codec = options?.codec ?? codecs.json<T>();
  const onConflict = options?.onConflict ?? "wait";
  const waitTimeoutMs = positiveInt(
    options?.waitTimeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS,
    "waitTimeoutMs"
  );
  const runningTtlMs = positiveInt(
    options?.runningTtlMs ?? waitTimeoutMs,
    "runningTtlMs"
  );
  const pollMs = positiveInt(options?.pollMs ?? DEFAULT_POLL_MS, "pollMs");
  const scripts = createScriptRunner(client);

  const keyFor = (key: string) => `${prefix}:${key}`;

  async function read(key: string): Promise<string | null> {
    const reply = await client.send(["GET", keyFor(key)]);
    return typeof reply === "string" ? reply : null;
  }

  return {
    /**
     * Run `fn` at most once for `key`, replaying the stored result thereafter.
     *
     * Passing a nullish key runs `fn` unguarded and reports `replayed: false`,
     * so a handler can forward an optional `Idempotency-Key` header straight
     * through without branching on its presence.
     */
    async run(
      key: string | null | undefined,
      fn: () => Promise<T> | T
    ): Promise<IdempotentResult<T>> {
      if (key === null || key === undefined || key === "") {
        return { value: await fn(), replayed: false };
      }
      const redisKey = keyFor(key);
      const token = globalThis.crypto.randomUUID();

      for (const deadline = Date.now() + waitTimeoutMs; ; ) {
        const claimed = await client.send([
          "SET",
          redisKey,
          `${RUNNING}${token}`,
          "NX",
          "PX",
          runningTtlMs
        ]);

        if (claimed !== null) {
          let value: T;
          try {
            value = await fn();
          } catch (error) {
            // Release so a retry can proceed. Guarded by the token, so we
            // never clear a record some later caller now owns.
            try {
              await scripts.run(abandonScript, [redisKey], [token]);
            } catch {
              // The TTL frees it regardless; never mask fn's error.
            }
            throw error;
          }
          try {
            const stored = await scripts.run(
              completeScript,
              [redisKey],
              [token, codec.encode(value), String(ttlMs)]
            );
            // 0 means the marker lapsed or a later caller now owns the key, so
            // nothing was written. Same broken guarantee as a failed round
            // trip, and reported the same way — the script surfaces it as a
            // return value, so a bare `await` lets it pass for success.
            if (stored !== 1) {
              throw new Error(
                "the running marker lapsed or was taken over by a later caller"
              );
            }
          } catch (cause) {
            // Deliberately NOT swallowed, even though the effect succeeded.
            //
            // This is not lock.run, where a failed release only costs a wasted
            // TTL. Here the unwritten record *is* the guarantee: once the
            // running marker lapses, the next caller with this key re-runs the
            // handler, and reporting plain success would hide that. The error
            // carries the value so a caller who can salvage it may.
            throw new IdempotencyNotRecordedError(key, value, cause);
          }
          return { value, replayed: false };
        }

        const held = await read(key);
        if (held?.startsWith(DONE)) {
          return { value: codec.decode(held.slice(1)), replayed: true };
        }
        // Someone else is mid-flight (or the record vanished between our SET
        // and our GET, in which case looping re-races for the claim).
        if (held !== null && onConflict === "throw") {
          throw new IdempotencyConflictError(key);
        }
        if (Date.now() >= deadline) {
          throw new IdempotencyTimeoutError(key, waitTimeoutMs);
        }
        await sleep(pollMs);
      }
    },

    /** The stored result, or `null` if absent or still running. */
    async peek(key: string): Promise<T | null> {
      const held = await read(key);
      if (held === null || !held.startsWith(DONE)) return null;
      return codec.decode(held.slice(1));
    },

    /** Drop the record so the next call runs again. */
    async forget(key: string): Promise<boolean> {
      const reply = await client.send(["DEL", keyFor(key)]);
      return reply === 1;
    }
  };
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `idempotency ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

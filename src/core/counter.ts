import { ValidationError } from "./errors.js";
import {
  expectNumberLike,
  expectSafeNumber,
  positiveSafeInteger
} from "./helpers.js";
import { defineScript, scriptRunnerFor } from "./script.js";
import type { StoreContext } from "./store.js";
import type { Keyspace, RedisKeyPart } from "./types.js";

/** Options for a counter's `incr`. */
export type CounterIncrOptions = {
  /**
   * Give the key this expiry, in milliseconds, when it has none: the increment
   * that creates the counter starts the window and later increments leave it
   * running, so the key expires `ttlMs` after its first increment (a fixed
   * window). Set in the same round trip as the increment, atomically, so a
   * crash between the two can no longer leave a counter that never expires.
   * A counter that already has an expiry keeps it; call `expire` to move it.
   */
  readonly ttlMs?: number;
};

/**
 * INCR and PEXPIRE in one step. A script rather than MULTI: MULTI needs the
 * client's optional `transaction` capability, and inside a session it would
 * be the session's own EXEC, which ends a WATCH the caller is still holding.
 * EVALSHA is a plain command on every adapter and connection. PTTL rather
 * than `PEXPIRE ... NX` so it also runs on servers older than Redis 7.
 */
const incrWithTtlScript = defineScript<readonly [ttlMs: number], number>({
  keyCount: 1,
  lua: 'local value = redis.call("INCR", KEYS[1]) if redis.call("PTTL", KEYS[1]) == -1 then redis.call("PEXPIRE", KEYS[1], ARGV[1]) end return value',
  decode: (reply) => expectSafeNumber(reply, "INCR")
});

/**
 * The counter commands a kv store carries when its codec is `number()`.
 *
 * The integer commands (`incr`, `incrby`, `decr`, `decrby`) work on a stored
 * integer; `number()` also stores fractions, and Redis rejects the integer
 * commands on one of those with a `RedisServerError` ("value is not an integer
 * or out of range"). `incrbyfloat` works on both. Redis counters are 64-bit, so
 * the integer commands throw a `ReplyShapeError` once the value passes
 * `Number.MAX_SAFE_INTEGER` rather than resolving a rounded number the caller
 * cannot tell apart from the real one.
 */
export function createCounterCommands<TId extends RedisKeyPart = RedisKeyPart>(
  ctx: StoreContext,
  keyspace: Keyspace<number, number, string, TId>
) {
  const { client } = ctx;
  return {
    /**
     * INCR: increment by 1 (a missing key counts as 0) and resolve the new
     * value. With `ttlMs`, also give the key that expiry when it has none, in
     * the same atomic step (see {@link CounterIncrOptions}).
     * @example const hits = await redis.query.views.incr("post-1");
     * @example await redis.query.attempts.incr(ip, { ttlMs: 60_000 });
     */
    async incr(id: TId, options?: CounterIncrOptions): Promise<number> {
      const key = keyspace.key(id);
      if (options?.ttlMs === undefined) {
        return expectSafeNumber(await client.send(["INCR", key]), "INCR");
      }
      const ttlMs = positiveSafeInteger(options.ttlMs, "ttlMs");
      return scriptRunnerFor(ctx).run(incrWithTtlScript, [key], [ttlMs]);
    },
    /** INCRBY: increment by an integer `amount`; resolves the new value. */
    async incrby(id: TId, amount: number): Promise<number> {
      if (!Number.isSafeInteger(amount)) {
        throw new ValidationError("amount must be a safe integer");
      }
      return expectSafeNumber(
        await client.send(["INCRBY", keyspace.key(id), amount]),
        "INCRBY"
      );
    },
    /**
     * INCRBYFLOAT: increment by a finite `amount`, fractional or not; resolves
     * the new value. Works on a stored integer or fraction alike.
     */
    async incrbyfloat(id: TId, amount: number): Promise<number> {
      if (!Number.isFinite(amount)) {
        throw new ValidationError("amount must be a finite number");
      }
      return expectNumberLike(
        await client.send(["INCRBYFLOAT", keyspace.key(id), amount]),
        "INCRBYFLOAT"
      );
    },
    /** DECR: decrement by 1 (a missing key counts as 0); resolves the new value. */
    async decr(id: TId): Promise<number> {
      return expectSafeNumber(
        await client.send(["DECR", keyspace.key(id)]),
        "DECR"
      );
    },
    /** DECRBY: decrement by an integer `amount`; resolves the new value. */
    async decrby(id: TId, amount: number): Promise<number> {
      if (!Number.isSafeInteger(amount)) {
        throw new ValidationError("amount must be a safe integer");
      }
      return expectSafeNumber(
        await client.send(["DECRBY", keyspace.key(id), amount]),
        "DECRBY"
      );
    }
  };
}

/** What a `number()` kv store carries on top of the plain kv commands. */
export type CounterCommands<TId extends RedisKeyPart = RedisKeyPart> =
  ReturnType<typeof createCounterCommands<TId>>;

import { ValidationError } from "./errors.js";
import {
  createKeyLifecycleOps,
  expectNumber,
  expectNumberLike,
  expectSafeNumber
} from "./helpers.js";
import type { Keyspace, RedisClient, RedisKeyPart } from "./types.js";

export function createCounterStore<TId extends RedisKeyPart = RedisKeyPart>(
  client: RedisClient,
  keyspace: Keyspace<number, number, string, TId>
) {
  return {
    ...createKeyLifecycleOps(client, (id: TId) => keyspace.key(id)),
    /**
     * INCR — increment by 1 (creating the key at 0); returns the new value.
     *
     * Redis counters are 64-bit, so the integer commands throw a
     * `ReplyShapeError` once the value passes `Number.MAX_SAFE_INTEGER`
     * rather than resolving a rounded number the caller cannot tell apart
     * from the real one.
     * @example const hits = await redis.counter(views).incr("42");
     */
    async incr(id: TId): Promise<number> {
      return expectSafeNumber(
        await client.send(["INCR", keyspace.key(id)]),
        "INCR"
      );
    },
    /** INCRBY — increment by an integer `amount`; returns the new value. */
    async incrby(id: TId, amount: number): Promise<number> {
      if (!Number.isSafeInteger(amount)) {
        throw new ValidationError("amount must be a safe integer");
      }
      return expectSafeNumber(
        await client.send(["INCRBY", keyspace.key(id), amount]),
        "INCRBY"
      );
    },
    /** INCRBYFLOAT — increment by a float `amount`; returns the new value. */
    async incrbyfloat(id: TId, amount: number): Promise<number> {
      if (!Number.isFinite(amount)) {
        throw new ValidationError("amount must be a finite number");
      }
      return expectNumberLike(
        await client.send(["INCRBYFLOAT", keyspace.key(id), amount]),
        "INCRBYFLOAT"
      );
    },
    /** DECR — decrement by 1 (creating the key at 0); returns the new value. */
    async decr(id: TId): Promise<number> {
      return expectSafeNumber(
        await client.send(["DECR", keyspace.key(id)]),
        "DECR"
      );
    },
    /** DECRBY — decrement by an integer `amount`; returns the new value. */
    async decrby(id: TId, amount: number): Promise<number> {
      if (!Number.isSafeInteger(amount)) {
        throw new ValidationError("amount must be a safe integer");
      }
      return expectSafeNumber(
        await client.send(["DECRBY", keyspace.key(id), amount]),
        "DECRBY"
      );
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", keyspace.key(id)]), "DEL");
    }
  };
}

import { replyShapeError, ValidationError } from "./errors.js";
import { createKeyLifecycleOps, expectNumber } from "./helpers.js";
import type { Codec, RedisClient, RedisKey, RedisKeyPart } from "./types.js";

export type HyperLogLogSchema<
  TInput,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
> = {
  readonly kind: "hll";
  readonly prefix: TPrefix;
  key<TActualId extends TId>(id: TActualId): RedisKey<TPrefix, TActualId>;
  encode(value: TInput): string;
};

export function defineHyperLogLog<
  TPrefix extends string,
  TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  codec: Codec<TInput, unknown>,
  _options?: { readonly ids?: TIds }
): HyperLogLogSchema<TInput, TPrefix, TIds[number]> {
  return {
    kind: "hll",
    prefix,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    },
    encode(value) {
      return codec.encode(value);
    }
  };
}

export function createHyperLogLogStore<
  TInput,
  TId extends RedisKeyPart = RedisKeyPart
>(client: RedisClient, schema: HyperLogLogSchema<TInput, string, TId>) {
  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    /**
     * PFADD — add `values`; `true` if the estimated cardinality changed.
     * @example await redis.hll(visitors).pfadd("2026-07-11", [userId]);
     */
    async pfadd(id: TId, values: readonly TInput[]): Promise<boolean> {
      // Empty input is a no-op: a bare PFADD would create the key.
      if (values.length === 0) return false;
      return (
        expectNumber(
          await client.send([
            "PFADD",
            schema.key(id),
            ...values.map((value) => schema.encode(value))
          ]),
          "PFADD"
        ) === 1
      );
    },
    /** PFCOUNT — estimated cardinality of one id, or of the union of several. */
    async pfcount(ids: TId | readonly TId[]): Promise<number> {
      const list = Array.isArray(ids) ? ids : [ids as TId];
      if (list.length === 0) {
        throw new ValidationError("pfcount requires at least one id");
      }
      return expectNumber(
        await client.send(["PFCOUNT", ...list.map((id) => schema.key(id))]),
        "PFCOUNT"
      );
    },
    /** PFMERGE — merge `sources` into `destination` (union of the estimates). */
    async pfmerge(destination: TId, sources: readonly TId[]): Promise<void> {
      if (sources.length === 0) {
        throw new ValidationError("pfmerge requires at least one source id");
      }
      const reply = await client.send([
        "PFMERGE",
        schema.key(destination),
        ...sources.map((source) => schema.key(source))
      ]);
      if (reply !== "OK") {
        throw replyShapeError("PFMERGE", "OK", reply);
      }
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", schema.key(id)]), "DEL");
    }
  };
}

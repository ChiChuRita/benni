import { replyShapeError, ValidationError } from "./errors.js";
import { createKeyLifecycleOps, expectNumber } from "./helpers.js";
import { type HashTagLayout, type KeyOptions, keyBuilder } from "./keys.js";
import type { SlotGuard } from "./slot.js";
import {
  type StoreBinding,
  type StoreContext,
  withKey,
  withStore
} from "./store.js";
import type { Codec, RedisClient, RedisKey, RedisKeyPart } from "./types.js";

export type HyperLogLogSchema<
  TInput,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart,
  THashTag extends HashTagLayout | undefined = HashTagLayout | undefined
> = {
  readonly kind: "hll";
  readonly prefix: TPrefix;
  readonly hashTag?: THashTag;
  key<TActualId extends TId>(
    id: TActualId
  ): RedisKey<TPrefix, TActualId, THashTag>;
  encode(value: TInput): string;
};

export function defineHyperLogLog<
  TPrefix extends string,
  TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  const THashTag extends HashTagLayout | undefined = undefined
>(
  prefix: TPrefix,
  codec: Codec<TInput, unknown>,
  options?: KeyOptions<TIds, THashTag>
): HyperLogLogSchema<TInput, TPrefix, TIds[number], THashTag> {
  const hashTag = options?.hashTag as THashTag;
  const schema: HyperLogLogSchema<TInput, TPrefix, TIds[number], THashTag> = {
    kind: "hll",
    prefix,
    // Spread so the property is absent, not `undefined`, on the default
    // layout: a schema still enumerates as the plain data it looks like.
    ...(hashTag === undefined ? {} : { hashTag }),
    key: keyBuilder(prefix, hashTag),
    encode(value) {
      return codec.encode(value);
    }
  };
  return withStore(schema, hllBinding);
}

export function createHyperLogLogStore<
  TInput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: HyperLogLogSchema<TInput, string, TId>,
  assertSameSlot?: SlotGuard
) {
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
      const keys = list.map((id) => schema.key(id));
      assertSameSlot?.("PFCOUNT", keys, schema);
      return expectNumber(await client.send(["PFCOUNT", ...keys]), "PFCOUNT");
    },
    /** PFMERGE — merge `sources` into `destination` (union of the estimates). */
    async pfmerge(destination: TId, sources: readonly TId[]): Promise<void> {
      if (sources.length === 0) {
        throw new ValidationError("pfmerge requires at least one source id");
      }
      const keys = [
        schema.key(destination),
        ...sources.map((source) => schema.key(source))
      ];
      assertSameSlot?.("PFMERGE", keys, schema);
      const reply = await client.send(["PFMERGE", ...keys]);
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

/** The hll resource: the store plus the schema's own typed `key()`. */
export function createHllResource<
  TInput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(
  ctx: StoreContext,
  schema: HyperLogLogSchema<TInput, TPrefix, TId, THashTag>
) {
  return withKey(
    schema,
    createHyperLogLogStore(ctx.client, schema, ctx.assertSameSlot)
  );
}

const hllBinding: StoreBinding = { resource: createHllResource };

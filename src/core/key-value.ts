import { replyShapeError, ValidationError } from "./errors.js";
import { createKeyLifecycleOps, expectNumber, ttlSeconds } from "./helpers.js";
import { type HashTagLayout, type KeyOptions, keyBuilder } from "./keys.js";
import type { SlotGuard } from "./slot.js";
import {
  type StoreBinding,
  type StoreContext,
  withKey,
  withStore
} from "./store.js";
import type {
  Codec,
  Keyspace,
  RedisClient,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply
} from "./types.js";

/**
 * Options for {@link createKeyValueStore}'s `set`.
 *
 * `nx` and `xx` are mutually exclusive (write only if absent / only if
 * present), as are `ttlSeconds` and `keepTtl` — both pairs are modeled so
 * the invalid combination is a compile-time error, not a runtime throw.
 * When `nx` or `xx` is set, `set` resolves to whether the write happened.
 */
type SetTtlMode =
  | { readonly ttlSeconds?: number; readonly keepTtl?: never }
  | { readonly keepTtl?: boolean; readonly ttlSeconds?: never };

type SetConditionMode =
  | { readonly nx?: boolean; readonly xx?: never }
  | { readonly xx?: boolean; readonly nx?: never };

export type KeyValueSetOptions = SetTtlMode & SetConditionMode;

type ConditionalSetOptions = SetTtlMode &
  (
    | { readonly nx: true; readonly xx?: never }
    | { readonly xx: true; readonly nx?: never }
  );

function decodeConditionalSetReply(reply: RedisReply): boolean {
  if (reply === "OK") return true;
  if (reply === null) return false;
  throw replyShapeError("SET", "OK or null", reply);
}

/**
 * A member that exists only in the types, to make one specific mistake explain
 * itself. `counter` and `string` are alternate *views* over a kv keyspace rather
 * than kinds of their own, so their commands live on `redis.counter(schema)` and
 * `redis.string(schema)`. Reaching for `incr` on the kv store is the common first
 * guess, and the bare "property does not exist" error answers it by printing
 * every method the store *does* have, which names no fix.
 *
 * Typing the hint as the parameter puts the fix in the error text itself:
 *
 * ```text
 * Argument of type 'string' is not assignable to parameter of type
 * '"INCR is a counter command: use redis.counter(schema).incr(id)"'
 * ```
 *
 * Nothing is added at runtime, so calling one from untyped JavaScript still
 * fails the way an absent method fails.
 */
type ReachThroughAccessor<
  TAccessor extends "counter" | "string",
  TCommand extends string
> = (
  hint: `${TCommand} is a ${TAccessor} command: use redis.${TAccessor}(schema).${Lowercase<TCommand>}(id)`,
  // The rest parameter keeps a two-argument call (`incrby(id, by)`) reporting the
  // hint rather than "Expected 1 arguments, but got 2", which names no fix.
  ...rest: never[]
) => never;

/**
 * The commands a kv store deliberately lacks, each carrying its own fix. Keep
 * this in step with {@link createCounterStore} and {@link createStringStore}:
 * a command that moves onto the kv store should lose its entry here.
 */
type KeyValueViewHints = {
  readonly incr: ReachThroughAccessor<"counter", "INCR">;
  readonly incrby: ReachThroughAccessor<"counter", "INCRBY">;
  readonly incrbyfloat: ReachThroughAccessor<"counter", "INCRBYFLOAT">;
  readonly decr: ReachThroughAccessor<"counter", "DECR">;
  readonly decrby: ReachThroughAccessor<"counter", "DECRBY">;
  readonly append: ReachThroughAccessor<"string", "APPEND">;
  readonly getrange: ReachThroughAccessor<"string", "GETRANGE">;
  readonly setrange: ReachThroughAccessor<"string", "SETRANGE">;
  readonly strlen: ReachThroughAccessor<"string", "STRLEN">;
};

export function createKeyValueStore<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  keyspace: Keyspace<TInput, TOutput, string, TId>,
  assertSameSlot?: SlotGuard
) {
  function set(
    id: TId,
    value: TInput,
    options: ConditionalSetOptions
  ): Promise<boolean>;
  function set(
    id: TId,
    value: TInput,
    options?: KeyValueSetOptions
  ): Promise<void>;
  async function set(
    id: TId,
    value: TInput,
    options: KeyValueSetOptions = {}
  ): Promise<void | boolean> {
    if (options.nx && options.xx) {
      throw new ValidationError("nx cannot be combined with xx");
    }
    if (options.keepTtl && options.ttlSeconds !== undefined) {
      throw new ValidationError("keepTtl cannot be combined with ttlSeconds");
    }
    const command: [string, ...RedisCommandArgument[]] = [
      "SET",
      keyspace.key(id),
      keyspace.encode(value)
    ];
    if (options.nx) command.push("NX");
    if (options.xx) command.push("XX");
    if (options.ttlSeconds !== undefined) {
      command.push("EX", ttlSeconds(options.ttlSeconds));
    }
    if (options.keepTtl) {
      command.push("KEEPTTL");
    }
    const reply = await client.send(command);
    if (options.nx || options.xx) {
      return decodeConditionalSetReply(reply);
    }
    if (reply !== "OK") {
      throw replyShapeError("SET", "OK", reply);
    }
  }

  const store = {
    ...createKeyLifecycleOps(client, (id: TId) => keyspace.key(id)),
    /**
     * `SET key value`. Without `nx`/`xx` resolves once the write is
     * acknowledged. With `nx` (write only if absent) or `xx` (write only if
     * present) resolves to whether the write happened.
     *
     * @example redis.kv(profiles).set("greeting", "hi", { ttlSeconds: 60 })
     * @example const written = await redis.kv(profiles).set("k", "v", { nx: true })
     */
    set,
    /**
     * GET — read the value, decoded, or `null` if the key is missing.
     * @example const greeting = await redis.kv(profiles).get("42");
     */
    async get(id: TId): Promise<TOutput | null> {
      const reply = await client.send(["GET", keyspace.key(id)]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("GET", "string or null", reply);
      }
      return keyspace.decode(reply);
    },
    /** GETDEL — read the value and delete the key; `null` if it was missing. */
    async getdel(id: TId): Promise<TOutput | null> {
      const reply = await client.send(["GETDEL", keyspace.key(id)]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("GETDEL", "string or null", reply);
      }
      return keyspace.decode(reply);
    },
    /** GETSET — write `value` and return the previous value, or `null`. */
    async getset(id: TId, value: TInput): Promise<TOutput | null> {
      const reply = await client.send([
        "GETSET",
        keyspace.key(id),
        keyspace.encode(value)
      ]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("GETSET", "string or null", reply);
      }
      return keyspace.decode(reply);
    },
    /**
     * MGET — read several keys in order (`null` per missing key). Empty input
     * returns `[]` without a round trip.
     * @example const [a, b] = await redis.kv(profiles).mget(["1", "2"]);
     */
    async mget(ids: readonly TId[]): Promise<Array<TOutput | null>> {
      if (ids.length === 0) return [];
      const keys = ids.map((id) => keyspace.key(id));
      assertSameSlot?.("MGET", keys, keyspace);
      const reply = await client.send(["MGET", ...keys]);
      if (!Array.isArray(reply)) {
        throw replyShapeError("MGET", "array", reply);
      }
      return reply.map((value) => {
        if (value === null) return null;
        if (typeof value !== "string") {
          throw replyShapeError("MGET item", "string or null", value);
        }
        return keyspace.decode(value);
      });
    },
    /** MSET — write several id/value pairs atomically. No-op when empty. */
    async mset(
      values: ReadonlyMap<TId, TInput> | readonly [TId, TInput][]
    ): Promise<void> {
      const entries = Array.isArray(values) ? values : [...values.entries()];
      if (entries.length === 0) return;
      const args = entries.flatMap(([id, value]) => [
        keyspace.key(id),
        keyspace.encode(value)
      ]);
      assertSameSlot?.(
        "MSET",
        entries.map(([id]) => keyspace.key(id)),
        keyspace
      );
      const reply = await client.send(["MSET", ...args]);
      if (reply !== "OK") throw replyShapeError("MSET", "OK", reply);
    },
    /**
     * MSETNX — write the pairs only if none of the keys exist; `true` if the
     * write happened. Empty input resolves `true` without a round trip.
     */
    async msetnx(
      values: ReadonlyMap<TId, TInput> | readonly [TId, TInput][]
    ): Promise<boolean> {
      const entries = Array.isArray(values) ? values : [...values.entries()];
      if (entries.length === 0) return true;
      const args = entries.flatMap(([id, value]) => [
        keyspace.key(id),
        keyspace.encode(value)
      ]);
      assertSameSlot?.(
        "MSETNX",
        entries.map(([id]) => keyspace.key(id)),
        keyspace
      );
      return (
        expectNumber(await client.send(["MSETNX", ...args]), "MSETNX") === 1
      );
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      const reply = await client.send(["DEL", keyspace.key(id)]);
      if (typeof reply !== "number") {
        throw replyShapeError("DEL", "number", reply);
      }
      return reply;
    }
  };

  // The hints are type-only: nothing is added to the object above.
  return store as typeof store & KeyValueViewHints;
}

/**
 * The kv resource: the store plus the schema's own typed `key()`.
 * Also serves `redis.query.<name>` for a kv schema.
 */
export function createKvResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(
  ctx: StoreContext,
  schema: Keyspace<TInput, TOutput, TPrefix, TId, THashTag>
) {
  return withKey(
    schema,
    createKeyValueStore(ctx.client, schema, ctx.assertSameSlot)
  );
}

const kvBinding: StoreBinding = { resource: createKvResource };

export function defineKeyspace<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  const THashTag extends HashTagLayout | undefined = undefined
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  options?: KeyOptions<TIds, THashTag>
): Keyspace<TInput, TOutput, TPrefix, TIds[number], THashTag> {
  const hashTag = options?.hashTag as THashTag;
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    kind: "kv",
    prefix,
    // Spread so the property is absent, not `undefined`, on the default
    // layout: a schema still enumerates as the plain data it looks like.
    ...(hashTag === undefined ? {} : { hashTag }),
    key: keyBuilder(prefix, hashTag),
    encode(value) {
      return codec.encode(value);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as Keyspace<TInput, TOutput, TPrefix, TIds[number], THashTag>;
  return withStore(schema, kvBinding);
}

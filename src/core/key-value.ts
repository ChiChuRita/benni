import { type CounterCommands, createCounterCommands } from "./counter.js";
import { replyShapeError, ValidationError } from "./errors.js";
import {
  createKeyLifecycleOps,
  type ExpiryOptions,
  expectNumber,
  expiryArgs,
  ttlSeconds
} from "./helpers.js";
import { type HashTagLayout, type KeyOptions, keyBuilder } from "./keys.js";
import type { SlotGuard } from "./slot.js";
import {
  type StoreBinding,
  type StoreContext,
  withKey,
  withStore
} from "./store.js";
import { createStringCommands, type StringCommands } from "./string.js";
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
 * When `nx` or `xx` is given, `set` resolves to whether the write happened.
 */
type SetTtlMode =
  | { readonly ttlSeconds?: number; readonly keepTtl?: never }
  | { readonly keepTtl?: boolean; readonly ttlSeconds?: never };

type SetConditionMode =
  | { readonly nx?: boolean; readonly xx?: never }
  | { readonly xx?: boolean; readonly nx?: never };

export type KeyValueSetOptions = SetTtlMode & SetConditionMode;

/**
 * A `set` that spells out `nx` or `xx`, literal or computed: it resolves to
 * whether the write happened. Keyed on the flag being present rather than
 * `true`, because a `boolean` flag used to fall through to the `void`
 * overload while the call resolved a boolean whenever the flag was on.
 */
type ConditionalSetOptions = SetTtlMode &
  (
    | { readonly nx: boolean; readonly xx?: never }
    | { readonly xx: boolean; readonly nx?: never }
  );

/**
 * A `set` that provably spells out neither flag. An options value typed
 * {@link KeyValueSetOptions}, whose flags are merely optional, matches neither
 * overload: the reply shape depends on them, so they have to be visible at
 * the call site.
 */
type UnconditionalSetOptions = SetTtlMode & {
  readonly nx?: undefined;
  readonly xx?: undefined;
};

/** GETEX expiry modes; shared with HGETEX (see `ExpiryOptions`). */
export type KeyValueGetExOptions = ExpiryOptions;

function decodeConditionalSetReply(reply: RedisReply): boolean {
  if (reply === "OK") return true;
  if (reply === null) return false;
  throw replyShapeError("SET", "OK or null", reply);
}

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
    options?: UnconditionalSetOptions
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
    // On the flag's presence, not its value, exactly as the overloads read
    // it: `{ nx: false }` is typed Promise<boolean>, so it resolves `true`
    // (a SET without NX always writes) rather than undefined.
    if (options.nx !== undefined || options.xx !== undefined) {
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
     * present) resolves to whether the write happened; a computed
     * `nx: someBoolean` is typed and resolved the same way.
     *
     * @example redis.query.profiles.set("greeting", "hi", { ttlSeconds: 60 })
     * @example const written = await redis.query.profiles.set("k", "v", { nx: true })
     */
    set,
    /**
     * GET — read the value, decoded, or `null` if the key is missing.
     * @example const greeting = await redis.query.profiles.get("42");
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
    /**
     * GETEX — read the value, decoded, while (re)setting its expiry: a bare
     * number of seconds, or any `ExpiryOptions` mode. `null` if the key is
     * missing.
     */
    async getex(
      id: TId,
      ttlOrOptions: number | KeyValueGetExOptions
    ): Promise<TOutput | null> {
      const args =
        typeof ttlOrOptions === "number"
          ? ["EX", ttlSeconds(ttlOrOptions)]
          : expiryArgs(ttlOrOptions);
      const reply = await client.send(["GETEX", keyspace.key(id), ...args]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("GETEX", "string or null", reply);
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
     * @example const [a, b] = await redis.query.profiles.mget(["1", "2"]);
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

  return store;
}

/** The plain kv commands every kv store has, whatever its codec. */
export type KeyValueStore<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
> = ReturnType<typeof createKeyValueStore<TInput, TOutput, TId>>;

/**
 * The commands a kv store carries because of its codec's `format`: the
 * counter commands for `number()`, the string commands for `string()`,
 * nothing extra otherwise. Bracketed so a `format` the type does not know
 * (a plain `Keyspace<number>`) adds nothing rather than a union of both.
 */
export type KeyValueFormatCommands<TFormat, TId extends RedisKeyPart> = [
  TFormat
] extends ["number"]
  ? CounterCommands<TId>
  : [TFormat] extends ["string"]
    ? StringCommands<TId>
    : unknown;

/**
 * What `redis.query.<name>` is for a kv schema: the plain kv commands, the
 * schema's own typed `key()`, and the commands its codec's `format` brings.
 */
export type KvResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined,
  TFormat
> = KeyValueStore<TInput, TOutput, TId> &
  Pick<Keyspace<TInput, TOutput, TPrefix, TId, THashTag>, "key"> &
  KeyValueFormatCommands<TFormat, TId>;

/**
 * The kv resource. Also serves `redis.query.<name>` for a kv schema.
 *
 * The runtime reads the same `format` the type does, so the object has the
 * counter or string commands exactly when its type says so.
 */
export function createKvResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined,
  TFormat
>(
  ctx: StoreContext,
  schema: Keyspace<TInput, TOutput, TPrefix, TId, THashTag, TFormat>
): KvResource<TInput, TOutput, TPrefix, TId, THashTag, TFormat> {
  const store = withKey(
    schema,
    createKeyValueStore(ctx.client, schema, ctx.assertSameSlot)
  );
  const format: unknown = schema.format;
  // The casts follow the format check just made: the codec's `format` is the
  // proof that the values are numbers or plain strings.
  const extra =
    format === "number"
      ? createCounterCommands(
          ctx,
          schema as unknown as Keyspace<number, number, string, TId>
        )
      : format === "string"
        ? createStringCommands(
            ctx.client,
            schema as unknown as Keyspace<string, string, string, TId>,
            ctx.assertSameSlot
          )
        : {};
  return { ...store, ...extra } as KvResource<
    TInput,
    TOutput,
    TPrefix,
    TId,
    THashTag,
    TFormat
  >;
}

const kvBinding: StoreBinding = { resource: createKvResource };

export function defineKeyspace<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  const THashTag extends HashTagLayout | undefined = undefined,
  TFormat = undefined
>(
  prefix: TPrefix,
  // `format` is read off the codec so the store can carry the commands that
  // fit what it stores; see `Keyspace`. Codecs without one infer `undefined`.
  codec: Codec<TInput, TOutput> & { readonly format?: TFormat },
  options?: KeyOptions<TIds, THashTag>
): Keyspace<TInput, TOutput, TPrefix, TIds[number], THashTag, TFormat> {
  const hashTag = options?.hashTag as THashTag;
  const format = codec.format;
  // The inference anchor is type-only and never present — cast the literal.
  const schema = {
    kind: "kv",
    prefix,
    // Spread so the property is absent, not `undefined`, on the default
    // layout: a schema still enumerates as the plain data it looks like.
    ...(hashTag === undefined ? {} : { hashTag }),
    ...(format === undefined ? {} : { format }),
    key: keyBuilder(prefix, hashTag),
    encode(value) {
      return codec.encode(value);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as Keyspace<TInput, TOutput, TPrefix, TIds[number], THashTag, TFormat>;
  return withStore(schema, kvBinding);
}

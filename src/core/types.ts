import type { HashTagLayout } from "./keys.js";

export type { HashTagLayout };

export type RedisCommandArgument = string | number | bigint | Uint8Array;

export type RedisCommand = readonly [
  command: string,
  ...args: RedisCommandArgument[]
];

export type RedisReply =
  | null
  | string
  | number
  | bigint
  | boolean
  | Uint8Array
  | readonly RedisReply[]
  | ReadonlySet<RedisReply>
  | ReadonlyMap<RedisReply, RedisReply>;

/**
 * A dedicated connection leased from a RedisClient, for the two workloads
 * that monopolize a connection: blocking commands (BLPOP, BRPOP, BLMOVE,
 * BZPOPMIN/MAX, XREAD/XREADGROUP BLOCK) and WATCH-based optimistic
 * transactions. Provided by `benni/node`, `benni/ioredis`, and `benni/bun`;
 * `benni/upstash` has no connection to lease and omits `session?()`.
 *
 * Adapter obligations (normative; pinned by the shared contract test):
 * - Exclusivity: the connection belongs to this session alone.
 * - Ordered dispatch: send() calls issued without awaiting are written to
 *   the socket in invocation order. node-redis, ioredis, and Bun all
 *   pipeline this way (verified); core's session pipeline facade relies on
 *   it.
 * - Fail-fast: NO automatic reconnection and no offline queueing. After
 *   close() or a connection drop, `closed` is true and every in-flight and
 *   subsequent send() rejects. A silent reconnect would drop WATCH state
 *   and blocked reads, turning visible failures into correctness bugs.
 * - Prompt close: close() rejects any in-flight command immediately — it
 *   MUST NOT wait out a server-side blocking timeout — and is idempotent.
 *   (node-redis: destroy(); ioredis: disconnect(); Bun: close(). The
 *   graceful variants, node-redis close() and ioredis quit(), provably wait
 *   out the block.)
 * - Leak backstop: the parent client tracks live sessions and force-closes
 *   survivors when the parent close() runs.
 * - Replies follow the same shapes as {@link RedisClient}'s.
 */
export interface RedisSession {
  send(command: RedisCommand): Promise<RedisReply>;
  /**
   * MULTI + the queued commands + EXEC on THIS connection, enqueued
   * contiguously (no interleaving). Resolves the per-command replies, or
   * `null` when EXEC aborted because a key watched on this connection
   * changed. `null` is the single cross-adapter abort signal:
   *   - node-redis: raw EXEC resolves null natively (verified); the multi()
   *     wrapper the adapter uses throws WatchError instead, mapped to null.
   *   - ioredis: multi().exec() resolves null natively (verified).
   *   - Bun: EXEC resolves null (verified on 1.3.14 and 1.4.2, RESP3).
   *     Additionally map an empty-array reply while commands.length > 0 to
   *     null — this defends against the RESP2 *-1 -> [] decode on other
   *     versions.
   * Core NEVER calls this with zero commands (an empty watched transaction
   * throws client-side), which is what keeps [] unambiguous.
   *
   * A per-command runtime error inside a committed EXEC (e.g. WRONGTYPE)
   * MUST reject the returned promise with that command's error. Note the
   * transaction has still committed for the other commands — Redis MULTI
   * has no rollback; core documents this loudly.
   */
  watchedTransaction(
    commands: readonly RedisCommand[]
  ): Promise<RedisReply[] | null>;
  /** True once the connection is gone — closed locally or dropped. */
  readonly closed: boolean;
  close(): Promise<void>;
}

/**
 * The seam every adapter implements and the only thing core talks to:
 * `benni/node` (node-redis), `benni/ioredis`, `benni/bun` (Bun's built-in
 * client), and `benni/upstash` (the Upstash REST protocol over fetch). A
 * hand-written adapter that honours this contract gets the whole typed API.
 *
 * `send`, `pipeline`, and `close` are required. The optional members each
 * gate one feature, and core checks for them at call time, throwing
 * `UnsupportedCapabilityError` (a `TypeError`) when one is missing:
 *
 * | adapter | transaction       | session | subscriber (patterns) |
 * | ------- | ----------------- | ------- | --------------------- |
 * | node    | yes               | yes     | yes (yes)             |
 * | ioredis | yes               | yes     | yes (yes)             |
 * | bun     | yes               | yes     | yes (no)              |
 * | upstash | yes (/multi-exec) | no      | no                    |
 *
 * Reply shapes (normative; pinned for every adapter by the shared contract
 * test). Replies are RESP2-shaped whatever the adapter speaks underneath, so
 * `redis.raw.send()` and a user's own decoders see the same value on every
 * adapter:
 * - Simple and bulk strings: a JS `string`, UTF-8 decoded. Bytes that are
 *   not valid UTF-8 decode lossily (U+FFFD) on every adapter; binary data
 *   belongs in the `bytes()` codec, which stores base64.
 * - Integers: a `number`.
 * - Nil (null bulk string or null array): `null`, never `undefined`.
 * - Arrays: arrays, element by element under these same rules.
 * - Doubles: the decimal `string` RESP2 sends, e.g. ZSCORE -> `"1.5"`,
 *   and `"inf"`/`"-inf"` for the infinities. This covers ZSCORE, ZMSCORE,
 *   ZINCRBY, ZADD INCR, every WITHSCORES score, ZPOPMIN/MAX, BZPOPMIN/MAX,
 *   ZMPOP/BZMPOP, ZRANK/ZREVRANK WITHSCORE (`[rank, "score"]`, the rank
 *   stays a number), GEOPOS, and GEOSEARCH/GEORADIUS WITHCOORD. The digits
 *   may differ from the server's own formatting but parse to the same
 *   number.
 * - Maps: a flat `[field, value, field, value, ...]` array (HGETALL, CONFIG
 *   GET, XINFO STREAM, HRANDFIELD WITHVALUES), except XREAD/XREADGROUP,
 *   whose RESP2 reply is an array of `[stream, entries]` pairs.
 * - Scored and paired replies are flat: ZRANGE ... WITHSCORES and ZPOPMIN
 *   with a count are `[member, score, member, score, ...]`, not RESP3's
 *   nested pairs. ZMPOP/BZMPOP keep RESP2's own `[key, [[member, score],
 *   ...]]` nesting.
 * - Sets: an array.
 * - Error replies are never values: they reject, normalized to
 *   `RedisServerError` (see core/errors.ts). Transport failures (a dropped
 *   socket, an HTTP 401 or 5xx, a timeout) reject as plain `Error`s.
 *
 * How each adapter meets that: node pins RESP2 on the connection; ioredis
 * speaks RESP2 and bypasses its own reply transformers by uppercasing the
 * command name; Upstash's REST JSON is RESP2-derived (the adapter asks for
 * base64 strings and decodes them); Bun speaks only RESP3, so the Bun
 * adapter reshapes each reply per command. `RedisReply` still admits Maps,
 * Sets, booleans, bigints, and bytes so a hand-written adapter that passes
 * something through is not a type error, and the typed stores tolerate a
 * Map, but raw callers and user decoders can only rely on the shapes above.
 *
 * Batches:
 * - pipeline(): one reply per command, in command order, not atomic. If any
 *   command fails, the promise rejects with the first failing command's
 *   error (in command order) and no replies are returned; the other
 *   commands still ran.
 * - transaction(): the same, inside MULTI/EXEC. A per-command runtime error
 *   rejects even though the other commands committed (no rollback).
 */
export interface RedisClient {
  send(command: RedisCommand): Promise<RedisReply>;
  pipeline(commands: readonly RedisCommand[]): Promise<RedisReply[]>;
  transaction?(commands: readonly RedisCommand[]): Promise<RedisReply[]>;
  /**
   * Optional, like transaction?(). Lease a dedicated connection. The caller
   * owns close(). Adapters that cannot provide one leave this undefined;
   * redis.session()/redis.watch() then throw at call time.
   */
  session?(): Promise<RedisSession>;
  /**
   * Optional, like session?(). Lease a connection put into subscriber mode.
   * Core leases at most one per client, lazily on first subscribe, and closes
   * it when the last subscription goes away — so adapters do no bookkeeping.
   * Adapters that cannot hold a connection (HTTP) leave this undefined;
   * subscribing then throws at call time, like the session guard.
   */
  subscriber?(): Promise<RedisSubscriber>;
  /**
   * Final and idempotent. Force-closes every session and subscriber leased
   * from this client, then the client itself (an adopted ioredis client is
   * borrowed and left open). Afterwards every command, and every lease,
   * rejects rather than quietly opening a connection nothing will close.
   */
  close(): Promise<void>;
}

/**
 * A connection in subscriber mode. Core registers exactly ONE listener per
 * channel/pattern and fans out to its own handlers, so implementations never
 * need to track multiple listeners for the same name.
 *
 * psubscribe/punsubscribe are optional: an adapter whose pattern support is
 * missing or broken omits them (Bun does), and pattern subscribes throw a
 * clear TypeError instead of hanging.
 *
 * Unlike a session, a subscriber connection is expected to survive a drop:
 * the adapter reconnects and resubscribes every channel and pattern it still
 * holds, so core's subscriptions keep delivering. node-redis and ioredis do
 * this natively; the Bun adapter does it itself, because Bun's client
 * reconnects without resubscribing. Messages published while the connection
 * was down are lost (Redis Pub/Sub is at-most-once) and nothing signals the
 * gap.
 */
export interface RedisSubscriber {
  subscribe(
    channel: string,
    listener: (message: string) => void
  ): Promise<void>;
  unsubscribe(channel: string): Promise<void>;
  psubscribe?(
    pattern: string,
    listener: (message: string, channel: string) => void
  ): Promise<void>;
  punsubscribe?(pattern: string): Promise<void>;
  /**
   * True once the connection is gone for good: closed locally, or the
   * underlying client gave up reconnecting (node-redis `isOpen` false,
   * ioredis status "end", Bun `onclose`). NOT true during a reconnect
   * window, or core would abandon subscriptions that are about to recover.
   * Once it is true, core drops the lease and the next subscribe leases a
   * fresh connection.
   */
  readonly closed: boolean;
  close(): Promise<void>;
}

export type RedisKeyPart = string | number | bigint;

/**
 * The key a schema builds for an id, in the schema's own hash-tag layout.
 *
 * The bracketed `[T] extends [X]` comparisons are load-bearing: a naked
 * conditional distributes, so a union-valued layout would silently produce a
 * union of all three key shapes.
 */
export type RedisKey<
  TPrefix extends string,
  TId extends RedisKeyPart = RedisKeyPart,
  THashTag extends HashTagLayout | undefined = undefined
> = [THashTag] extends ["prefix"]
  ? `{${TPrefix}}:${TId}`
  : [THashTag] extends ["id"]
    ? `${TPrefix}:{${TId}}`
    : `${TPrefix}:${TId}`;

export type StoreSetOptions = {
  readonly ttlSeconds?: number;
};

export interface Codec<TInput, TOutput = TInput> {
  encode(input: TInput): string;
  decode(stored: string): TOutput;
}

export type Keyspace<
  TInput,
  TOutput = TInput,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart,
  THashTag extends HashTagLayout | undefined = HashTagLayout | undefined
> = InferAnchors<TInput, TOutput> & {
  readonly kind: "kv";
  readonly prefix: TPrefix;
  readonly hashTag?: THashTag;
  key<TActualId extends TId>(
    id: TActualId
  ): RedisKey<TPrefix, TActualId, THashTag>;
  encode(value: TInput): string;
  decode(stored: string): TOutput;
};

export type FieldCodecs = Record<string, Codec<any, any>>;

/**
 * Flattens an alias instantiation or an intersection into one object literal
 * type. Type-only: it changes what an editor hover prints (`{ name: string }`
 * rather than `InferHashOutput<{ name: Codec<string, string> }>`), never what
 * is assignable to what.
 */
export type Simplify<T> = { [K in keyof T]: T[K] } & {};

// The phantom key behind InferInput/InferOutput. `declare`d, so it is never
// emitted, and not exported, so no code can index a schema with it: the
// anchor exists in the type system only, and it is optional, so the type does
// not claim a property the runtime object lacks.
declare const inferTypes: unique symbol;

/**
 * Type-only inference anchor, for schemas whose value types are not an
 * `encode`/`decode` pair of their own (hash, stream, the primitives) and the
 * codec-backed stores built alongside them. {@link InferInput} and
 * {@link InferOutput} read it where it exists and fall back to
 * `encode`/`decode` where it does not (geo, hll, channel, pattern, a bare
 * codec), so they work on every schema that carries values. There is no
 * property to access at runtime.
 */
export type InferAnchors<TInput, TOutput> = {
  readonly [inferTypes]?: {
    readonly input: TInput;
    readonly output: TOutput;
  };
};

/**
 * The write-side value type of any Benni schema or codec.
 * @example
 * ```ts
 * const users = hash("user", { name: string(), score: number() });
 * type NewUser = InferInput<typeof users>; // { name: string; score: number }
 * ```
 */
export type InferInput<TSchema> = typeof inferTypes extends keyof TSchema
  ? TSchema extends { readonly [inferTypes]?: { readonly input: infer TInput } }
    ? TInput
    : never
  : TSchema extends { encode(input: infer TInput): string }
    ? TInput
    : never;

/**
 * The read-side value type of any Benni schema or codec.
 * @example
 * ```ts
 * const profiles = kv("profile", json(Profile));
 * type StoredProfile = InferOutput<typeof profiles>; // Profile
 * ```
 */
export type InferOutput<TSchema> = typeof inferTypes extends keyof TSchema
  ? TSchema extends {
      readonly [inferTypes]?: { readonly output: infer TOutput };
    }
    ? TOutput
    : never
  : TSchema extends { decode(stored: string): infer TOutput }
    ? TOutput
    : never;

type FieldInput<TCodec> = TCodec extends Codec<infer TInput, any>
  ? TInput
  : never;

type FieldOutput<TCodec> = TCodec extends Codec<any, infer TOutput>
  ? TOutput
  : never;

export type InferHashInput<TFields extends FieldCodecs> = {
  [K in keyof TFields]: FieldInput<TFields[K]>;
} & {};

export type InferHashOutput<TFields extends FieldCodecs> = {
  [K in keyof TFields]: FieldOutput<TFields[K]>;
} & {};

/** One declared field's read-side type. */
export type HashFieldOutput<
  TFields extends FieldCodecs,
  TField extends keyof TFields
> = FieldOutput<TFields[TField]>;

/**
 * Any subset of a hash's fields, each at its own read-side type, absent when
 * not stored: what `hgetall` returns.
 */
export type PartialHashOutput<TFields extends FieldCodecs> = {
  [K in keyof TFields]?: FieldOutput<TFields[K]>;
} & {};

export type HashSchema<
  TFields extends FieldCodecs,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart,
  THashTag extends HashTagLayout | undefined = HashTagLayout | undefined
> = InferAnchors<InferHashInput<TFields>, InferHashOutput<TFields>> & {
  readonly kind: "hash";
  readonly prefix: TPrefix;
  readonly hashTag?: THashTag;
  readonly fields: TFields;
  key<TActualId extends TId>(
    id: TActualId
  ): RedisKey<TPrefix, TActualId, THashTag>;
};

export type SetSchema<
  TInput,
  TOutput = TInput,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart,
  THashTag extends HashTagLayout | undefined = HashTagLayout | undefined
> = InferAnchors<TInput, TOutput> & {
  readonly kind: "set";
  readonly prefix: TPrefix;
  readonly hashTag?: THashTag;
  key<TActualId extends TId>(
    id: TActualId
  ): RedisKey<TPrefix, TActualId, THashTag>;
  encode(member: TInput): string;
  decode(stored: string): TOutput;
};

export type ListSchema<
  TInput,
  TOutput = TInput,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart,
  THashTag extends HashTagLayout | undefined = HashTagLayout | undefined
> = InferAnchors<TInput, TOutput> & {
  readonly kind: "list";
  readonly prefix: TPrefix;
  readonly hashTag?: THashTag;
  key<TActualId extends TId>(
    id: TActualId
  ): RedisKey<TPrefix, TActualId, THashTag>;
  encode(value: TInput): string;
  decode(stored: string): TOutput;
};

export type SortedSetSchema<
  TInput,
  TOutput = TInput,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart,
  THashTag extends HashTagLayout | undefined = HashTagLayout | undefined
> = InferAnchors<TInput, TOutput> & {
  readonly kind: "zset";
  readonly prefix: TPrefix;
  readonly hashTag?: THashTag;
  key<TActualId extends TId>(
    id: TActualId
  ): RedisKey<TPrefix, TActualId, THashTag>;
  encode(member: TInput): string;
  decode(stored: string): TOutput;
};

export type SortedSetEntry<T> = {
  readonly member: T;
  readonly score: number;
};

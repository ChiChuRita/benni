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
 * transactions.
 *
 * Adapter obligations (normative; pinned by the shared contract test):
 * - Exclusivity: the connection belongs to this session alone.
 * - Ordered dispatch: send() calls issued without awaiting are written to
 *   the socket in invocation order. Both current adapters auto-pipeline
 *   this way (verified); core's session pipeline facade relies on it.
 * - Fail-fast: NO automatic reconnection and no offline queueing. After
 *   close() or a connection drop, `closed` is true and every in-flight and
 *   subsequent send() rejects. A silent reconnect would drop WATCH state
 *   and blocked reads, turning visible failures into correctness bugs.
 * - Prompt close: close() rejects any in-flight command immediately — it
 *   MUST NOT wait out a server-side blocking timeout — and is idempotent.
 *   (Node: destroy(); graceful close() provably waits out the block.)
 * - Leak backstop: the parent client tracks live sessions and force-closes
 *   survivors when the parent close() runs.
 */
export interface RedisSession {
  send(command: RedisCommand): Promise<RedisReply>;
  /**
   * MULTI + the queued commands + EXEC on THIS connection, enqueued
   * contiguously (no interleaving). Resolves the per-command replies, or
   * `null` when EXEC aborted because a key watched on this connection
   * changed. `null` is the single cross-adapter abort signal:
   *   - node-redis: raw EXEC resolves null natively (verified); if the
   *     multi() wrapper is used instead, map WatchError -> null.
   *   - Bun: EXEC resolves null (verified on 1.3.14 / RESP3). Additionally
   *     map an empty-array reply while commands.length > 0 to null — this
   *     defends against the RESP2 *-1 -> [] decode on other versions.
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

export interface RedisClient {
  send(command: RedisCommand): Promise<RedisReply>;
  pipeline(commands: readonly RedisCommand[]): Promise<RedisReply[]>;
  transaction?(commands: readonly RedisCommand[]): Promise<RedisReply[]>;
  /**
   * NEW — optional, like transaction?. Lease a dedicated connection.
   * The caller owns close(). Adapters that cannot provide one leave this
   * undefined; redis.session()/redis.watch() then throw TypeError at call time
   * (same style as the existing transaction guard).
   */
  session?(): Promise<RedisSession>;
  /**
   * Optional, like session?(). Lease a connection put into subscriber mode.
   * Core leases at most one per client, lazily on first subscribe, and closes
   * it when the last subscription goes away — so adapters do no bookkeeping.
   * Adapters that cannot hold a connection (HTTP) leave this undefined;
   * subscribing then throws TypeError at call time, like the session guard.
   */
  subscriber?(): Promise<RedisSubscriber>;
  close(): Promise<void>;
}

/**
 * A connection in subscriber mode. Core registers exactly ONE listener per
 * channel/pattern and fans out to its own handlers, so implementations never
 * need to track multiple listeners for the same name.
 *
 * psubscribe/punsubscribe are optional: an adapter whose pattern support is
 * missing or broken omits them, and pattern subscribes throw a clear TypeError
 * instead of hanging.
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
  /** True once the connection is gone — closed locally or dropped. */
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
 * Type-only inference anchors, present on every value-carrying schema.
 * `typeof profiles.$inferInput` / `.$inferOutput` name the schema's value
 * types the way Drizzle's `$inferSelect` does. The properties never exist at
 * runtime — accessing them outside a type position is always a bug.
 */
export type InferAnchors<TInput, TOutput> = {
  /** Type-only: the write-side value type. Never exists at runtime. */
  readonly $inferInput: TInput;
  /** Type-only: the read-side value type. Never exists at runtime. */
  readonly $inferOutput: TOutput;
};

/**
 * The write-side value type of any Beni schema or codec.
 * @example
 * ```ts
 * const users = hash("user", { name: string(), score: number() });
 * type NewUser = InferInput<typeof users>; // { name: string; score: number }
 * ```
 */
export type InferInput<TSchema> = TSchema extends {
  readonly $inferInput: infer TInput;
}
  ? TInput
  : TSchema extends { encode(input: infer TInput): string }
    ? TInput
    : never;

/**
 * The read-side value type of any Beni schema or codec.
 * @example
 * ```ts
 * const profiles = kv("profile", json<Profile>());
 * type StoredProfile = InferOutput<typeof profiles>; // Profile
 * ```
 */
export type InferOutput<TSchema> = TSchema extends {
  readonly $inferOutput: infer TOutput;
}
  ? TOutput
  : TSchema extends { decode(stored: string): infer TOutput }
    ? TOutput
    : never;

export type InferHashInput<TFields extends FieldCodecs> = {
  [K in keyof TFields]: TFields[K] extends Codec<infer TInput, any>
    ? TInput
    : never;
};

export type InferHashOutput<TFields extends FieldCodecs> = {
  [K in keyof TFields]: TFields[K] extends Codec<any, infer TOutput>
    ? TOutput
    : never;
};

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

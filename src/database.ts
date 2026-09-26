// Every `create*Resource` below is imported TYPE-ONLY. QueryResource and
// BenniSession name them through `ReturnType<typeof …>` to keep the public
// types byte-identical, while the runtime dispatch goes through the store
// binding each schema carries — so a bundler only ever pulls in the store
// modules whose schemas the app actually declares. Turning any of these into
// a value import silently re-pins every store to the root entry.
import type { BitmapSchema, createBitmapResource } from "./core/bitmap.js";
import {
  type ClientSource,
  resolveClient,
  SESSION_UNSUPPORTED
} from "./core/client-source.js";
// A value import, but errors.js is a leaf the root entry already pulls in for
// the exported error classes, so this pins nothing new into the bundle.
import { UnsupportedCapabilityError } from "./core/errors.js";
import type { createGeoResource, GeoSetSchema } from "./core/geo.js";
import type { createHashResource } from "./core/hash.js";
import type {
  createHllResource,
  HyperLogLogSchema
} from "./core/hyperloglog.js";
import type { KvResource } from "./core/key-value.js";
import type { HashTagLayout, SameSlotArg, SameSlotList } from "./core/keys.js";
import type {
  createListResource,
  createListSessionAccessor
} from "./core/list.js";
import type {
  ChannelName,
  createPatternResource,
  PubSubChannel,
  PubSubChannelResource,
  PubSubPattern
} from "./core/pubsub.js";
import {
  type HashScanEntry,
  type ScanMemberOptions,
  type ScanOptions,
  scanHash,
  scanKeys,
  scanKeyspace,
  scanSet,
  scanSortedSet
} from "./core/scan.js";
import type { createScriptResource, ScriptSchema } from "./core/script.js";
import {
  createBenniSession,
  type RunWatchOptions,
  runWatch
} from "./core/session.js";
import type { createSetResource } from "./core/set.js";
import type { SlotGuard } from "./core/slot.js";
import type {
  createZsetResource,
  createZsetSessionAccessor
} from "./core/sorted-set.js";
import {
  type BoundSchema,
  createStoreContext,
  PUBSUB_HUB_KEY,
  resolveSessionStore,
  resolveStore,
  STORE,
  type StoreContext
} from "./core/store.js";
import type { StreamSchema } from "./core/stream.js";
import type {
  createStreamResource,
  createStreamSessionAccessor
} from "./core/stream-resource.js";
import {
  createTransaction,
  type RedisTransaction,
  type WatchedRedisTransaction
} from "./core/transaction.js";
import type {
  FieldCodecs,
  FullRedisClient,
  HashSchema,
  Keyspace,
  ListSchema,
  RedisClient,
  RedisKeyPart,
  RedisPatternSubscriber,
  RedisSession,
  RedisSubscriber,
  SetSchema,
  SortedSetEntry,
  SortedSetSchema
} from "./core/types.js";
// Type-only, like the store factories above: naming a primitive as a value
// here would pin its module (and its Lua) into every bundle that binds a
// client. The runtime path goes through each primitive schema's own store
// binding, exactly as the data stores do.
import type { BudgetStore } from "./primitives/budget.js";
import type { CacheSchema, CacheStore } from "./primitives/cache.js";
import type {
  IdempotencySchema,
  IdempotencyStore
} from "./primitives/idempotency.js";
import type { LockStore } from "./primitives/lock.js";
import type { QueueSchema, QueueStore } from "./primitives/queue.js";
import type { RatelimitStore } from "./primitives/ratelimit.js";
import type { SemaphoreStore } from "./primitives/semaphore.js";

export type BenniSchema = Record<string, unknown>;

/**
 * The schema module this app binds, declared once so every `Benni` type can
 * find it without being handed `typeof schema` again at each call site.
 *
 * ```ts
 * declare module "benni" {
 *   interface Register {
 *     schema: typeof import("./schema");
 *   }
 * }
 * ```
 *
 * With that in place `Benni` alone is the fully typed handle, so a helper
 * signature reads `function handlers(redis: Benni)`. Without it nothing
 * changes: `Benni` stays generic and `Benni<typeof schema>` still works.
 *
 * **Apps only, once per program, never in a library.** The augmentation is
 * global to the compilation: a second one is a compile error (TS2717,
 * "subsequent property declarations must have the same type"), and a library
 * that augments it retypes the bare `Benni` of every app that imports it. In
 * a monorepo, declare it in the app packages only; shared packages take
 * {@link AnyBenni} or an explicit `Benni<typeof schema>`.
 */
// biome-ignore lint/suspicious/noEmptyInterface: the augmentation target.
export interface Register {}

/**
 * The registered schema module, or the open `BenniSchema` when the app has not
 * declared one. Written as a conditional over {@link Register} so an empty
 * interface (the unaugmented default) falls through to the open type.
 */
export type RegisteredSchema = Register extends {
  readonly schema: infer TSchema;
}
  ? TSchema extends BenniSchema
    ? TSchema
    : BenniSchema
  : BenniSchema;

/** The options `benni()` takes besides the client and the schema. */
export type BenniOptions = {
  /**
   * Called when a Pub/Sub handler throws or rejects. Delivery to the other
   * handlers continues either way. Without this, a failing handler is rethrown
   * asynchronously rather than swallowed.
   */
  readonly onPubSubError?: (error: unknown) => void;
  /**
   * The Redis Cluster slot guard, which checks before sending that every key
   * in a multi-key command hashes to one slot and throws `CrossSlotError` when
   * it does not.
   *
   * ```ts
   * import { assertSameSlot } from "benni/cluster";
   * const redis = benni({ client, schema, cluster: assertSameSlot });
   * ```
   *
   * You pass the checker rather than `true` so the CRC16 table and the error's
   * fix-hint prose live in `benni/cluster` instead of the root entry. `benni()`
   * has to reference the guard to install it, so a boolean would pull all of
   * it into every bundle, including the ones that never turn the check on.
   *
   * **This validates your keys; it does not route them.** Benni models slot
   * co-location, not cluster topology: routing comes from the cluster-aware
   * client underneath.
   *
   * Omitted by default, because cross-slot multi-key commands are perfectly
   * legal on a single-node Redis and enabling this unconditionally would break
   * every such caller. Turn it on in development and CI.
   */
  readonly cluster?: SlotGuard;
};

/**
 * The kinds a schema builder stamps onto its result, used to dispatch a
 * schema to its matching store in the `redis.query` registry.
 */
export type SchemaKind =
  | "kv"
  | "hash"
  | "set"
  | "list"
  | "zset"
  | "stream"
  | "bitmap"
  | "geo"
  | "hll"
  | "channel"
  | "pattern"
  | "script"
  // The primitives declare themselves the same way, so a cache or a queue is
  // reachable by name through `redis.query` like any other store.
  | "cache"
  | "ratelimit"
  | "queue"
  | "lock"
  | "semaphore"
  | "idempotency"
  | "budget";

/**
 * {@link SchemaKind} at runtime. `buildQuery` needs it to tell a benni schema
 * that lost its store binding (a copy) from a foreign object that merely has a
 * `kind` property.
 */
const SCHEMA_KINDS: ReadonlySet<unknown> = new Set<SchemaKind>([
  "kv",
  "hash",
  "set",
  "list",
  "zset",
  "stream",
  "bitmap",
  "geo",
  "hll",
  "channel",
  "pattern",
  "script",
  "cache",
  "ratelimit",
  "queue",
  "lock",
  "semaphore",
  "idempotency",
  "budget"
]);

/**
 * The kinds a session reaches through `session.query` and `session.store()`:
 * the data stores. Primitives, channels, patterns, and scripts stay on the
 * handle. They gain nothing from a dedicated connection, and a primitive
 * that manages its own connections or timers (a queue worker, a lock's
 * renewal) must not be tied to one that closes with the session.
 */
export type SessionSchemaKind =
  | "kv"
  | "hash"
  | "set"
  | "list"
  | "zset"
  | "stream"
  | "bitmap"
  | "geo"
  | "hll";

const SESSION_KINDS: ReadonlySet<unknown> = new Set<SessionSchemaKind>([
  "kv",
  "hash",
  "set",
  "list",
  "zset",
  "stream",
  "bitmap",
  "geo",
  "hll"
]);

/** A client that can lease a session: `session()`, `watch()`, blocking reads. */
type SessionCapable = { session(): Promise<RedisSession> };
/** A client that can lease a subscriber connection: `subscribe()`. */
type SubscriberCapable = { subscriber(): Promise<RedisSubscriber> };
/** A client whose subscriber connection can pattern-subscribe. */
type PatternCapable = { subscriber(): Promise<RedisPatternSubscriber> };

/**
 * A channel resource over a client that cannot subscribe: publishing is a
 * plain stateless command and works everywhere; `subscribe()` and `stream()`
 * need a held connection, so they are not part of this type.
 */
export interface PubSubPublisher<
  TInput,
  TName extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
> {
  /** PUBLISH — a stateless command, so it rides the bound client. */
  publish(message: TInput): Promise<number>;
  /** This resource's own channel, with no id: `name`. */
  channelName(): TName;
  /** The per-entity channel for `id`: `name:id`. */
  channelName<TActualId extends TId>(
    id: TActualId
  ): ChannelName<TName, TActualId>;
  /** The same publisher for the per-entity channel `name:id`. */
  at<TActualId extends TId>(
    id: TActualId
  ): PubSubPublisher<TInput, ChannelName<TName, TActualId>, TId>;
}

/**
 * The channel resource a handle over `TClient` exposes: the full resource when
 * the client can hold a subscriber connection, the publish-only one when not.
 */
export type ChannelResourceFor<
  TClient extends RedisClient,
  TInput,
  TOutput,
  TName extends string,
  TId extends RedisKeyPart
> = TClient extends SubscriberCapable
  ? PubSubChannelResource<TInput, TOutput, TName, TId>
  : PubSubPublisher<TInput, TName, TId>;

/**
 * Maps one declared schema to the typed resource `redis.query.<name>` exposes
 * for it, which is also what `redis.store(schema)` returns.
 * Dispatch is on the schema's literal `kind`, so structurally-identical schema
 * shapes (kv/set/list/zset/geo) still resolve to distinct resources. `TClient`
 * only matters for channels, which lose `subscribe()` on a client that cannot
 * hold a subscriber connection.
 */
export type QueryResource<
  T,
  TClient extends RedisClient = FullRedisClient
> = T extends { readonly kind: "hash" }
  ? T extends HashSchema<
      infer TFields extends FieldCodecs,
      infer TPrefix extends string,
      infer TId,
      infer THashTag extends HashTagLayout | undefined
    >
    ? ReturnType<typeof createHashResource<TFields, TPrefix, TId, THashTag>>
    : never
  : T extends { readonly kind: "stream" }
    ? T extends StreamSchema<
        infer TFields,
        infer TPrefix extends string,
        infer TId,
        infer THashTag extends HashTagLayout | undefined
      >
      ? ReturnType<typeof createStreamResource<TFields, TPrefix, TId, THashTag>>
      : never
    : T extends { readonly kind: "kv" }
      ? T extends Keyspace<
          infer TInput,
          infer TOutput,
          infer TPrefix extends string,
          infer TId,
          infer THashTag extends HashTagLayout | undefined,
          infer TFormat
        >
        ? KvResource<TInput, TOutput, TPrefix, TId, THashTag, TFormat>
        : never
      : T extends { readonly kind: "set" }
        ? T extends SetSchema<
            infer TInput,
            infer TOutput,
            infer TPrefix extends string,
            infer TId,
            infer THashTag extends HashTagLayout | undefined
          >
          ? ReturnType<
              typeof createSetResource<TInput, TOutput, TPrefix, TId, THashTag>
            >
          : never
        : T extends { readonly kind: "list" }
          ? T extends ListSchema<
              infer TInput,
              infer TOutput,
              infer TPrefix extends string,
              infer TId,
              infer THashTag extends HashTagLayout | undefined
            >
            ? ReturnType<
                typeof createListResource<
                  TInput,
                  TOutput,
                  TPrefix,
                  TId,
                  THashTag
                >
              >
            : never
          : T extends { readonly kind: "zset" }
            ? T extends SortedSetSchema<
                infer TInput,
                infer TOutput,
                infer TPrefix extends string,
                infer TId,
                infer THashTag extends HashTagLayout | undefined
              >
              ? ReturnType<
                  typeof createZsetResource<
                    TInput,
                    TOutput,
                    TPrefix,
                    TId,
                    THashTag
                  >
                >
              : never
            : T extends { readonly kind: "bitmap" }
              ? T extends BitmapSchema<
                  infer TPrefix extends string,
                  infer TId,
                  infer THashTag extends HashTagLayout | undefined
                >
                ? ReturnType<
                    typeof createBitmapResource<TPrefix, TId, THashTag>
                  >
                : never
              : T extends { readonly kind: "geo" }
                ? T extends GeoSetSchema<
                    infer TInput,
                    infer TOutput,
                    infer TPrefix extends string,
                    infer TId,
                    infer THashTag extends HashTagLayout | undefined
                  >
                  ? ReturnType<
                      typeof createGeoResource<
                        TInput,
                        TOutput,
                        TPrefix,
                        TId,
                        THashTag
                      >
                    >
                  : never
                : T extends { readonly kind: "hll" }
                  ? T extends HyperLogLogSchema<
                      infer TInput,
                      infer TPrefix extends string,
                      infer TId,
                      infer THashTag extends HashTagLayout | undefined
                    >
                    ? ReturnType<
                        typeof createHllResource<TInput, TPrefix, TId, THashTag>
                      >
                    : never
                  : T extends { readonly kind: "channel" }
                    ? T extends PubSubChannel<
                        infer TInput,
                        infer TOutput,
                        infer TName extends string,
                        infer TId extends RedisKeyPart
                      >
                      ? ChannelResourceFor<TClient, TInput, TOutput, TName, TId>
                      : never
                    : T extends { readonly kind: "pattern" }
                      ? T extends PubSubPattern<infer TOutput, string>
                        ? ReturnType<typeof createPatternResource<TOutput>>
                        : never
                      : T extends { readonly kind: "script" }
                        ? T extends ScriptSchema<
                            string,
                            infer TKeys,
                            infer TArgs,
                            infer TResult
                          >
                          ? ReturnType<
                              typeof createScriptResource<
                                string,
                                TKeys,
                                TArgs,
                                TResult
                              >
                            >
                          : never
                        : PrimitiveResource<T>;

/**
 * The {@link QueryResource} tail for the primitive kinds, split out so the
 * data-store chain above stays readable. Same dispatch on the literal `kind`,
 * and the same `never` for a kind this build does not know.
 */
export type PrimitiveResource<T> = T extends { readonly kind: "cache" }
  ? T extends CacheSchema<infer TValue>
    ? CacheStore<TValue>
    : never
  : T extends { readonly kind: "queue" }
    ? T extends QueueSchema<infer TPayload, infer TResult>
      ? QueueStore<TPayload, TResult>
      : never
    : T extends { readonly kind: "idempotency" }
      ? T extends IdempotencySchema<infer TValue>
        ? IdempotencyStore<TValue>
        : never
      : // The rest carry no value type, so the kind alone resolves the store.
        T extends { readonly kind: "ratelimit" }
        ? RatelimitStore
        : T extends { readonly kind: "lock" }
          ? LockStore
          : T extends { readonly kind: "semaphore" }
            ? SemaphoreStore
            : T extends { readonly kind: "budget" }
              ? BudgetStore
              : never;

/**
 * The `redis.query` registry: every schema exported from the bound schema module,
 * keyed by its export name, resolved to its typed resource. Entries that are
 * not schemas (a re-exported type, a helper) are dropped, and so are pattern
 * subscriptions when the client cannot pattern-subscribe, since a pattern
 * resource has nothing else to offer.
 */
export type QueryRegistry<
  TSchema extends BenniSchema,
  TClient extends RedisClient = FullRedisClient
> = {
  [K in keyof TSchema as TSchema[K] extends { readonly kind: SchemaKind }
    ? TClient extends PatternCapable
      ? K
      : TSchema[K] extends { readonly kind: "pattern" }
        ? never
        : K
    : never]: QueryResource<TSchema[K], TClient>;
};

/**
 * The schemas `redis.store()` accepts over a client of type `TClient`: any
 * benni schema, except a pattern subscription when the client cannot
 * pattern-subscribe, since a pattern resource has nothing else to offer.
 */
export type StorableSchema<TClient extends RedisClient = FullRedisClient> = {
  readonly kind: TClient extends PatternCapable
    ? SchemaKind
    : Exclude<SchemaKind, "pattern">;
};

/**
 * What `session.query.<name>` and `session.store(schema)` resolve a data-store
 * schema to: the shared resource, except that list, zset, and stream get the
 * blocking superset (blpop/brpop/blmove/blmpop, bzpopmin/bzpopmax/bzmpop,
 * xread with a timeout, the blocking consumer-group read), bound to the
 * session's own connection.
 */
export type SessionQueryResource<T> = T extends { readonly kind: "list" }
  ? T extends ListSchema<
      infer TInput,
      infer TOutput,
      infer TPrefix extends string,
      infer TId,
      infer THashTag extends HashTagLayout | undefined
    >
    ? ReturnType<
        typeof createListSessionAccessor<
          TInput,
          TOutput,
          TPrefix,
          TId,
          THashTag
        >
      >
    : never
  : T extends { readonly kind: "zset" }
    ? T extends SortedSetSchema<
        infer TInput,
        infer TOutput,
        infer TPrefix extends string,
        infer TId,
        infer THashTag extends HashTagLayout | undefined
      >
      ? ReturnType<
          typeof createZsetSessionAccessor<
            TInput,
            TOutput,
            TPrefix,
            TId,
            THashTag
          >
        >
      : never
    : T extends { readonly kind: "stream" }
      ? T extends StreamSchema<
          infer TFields,
          infer TPrefix extends string,
          infer TId,
          infer THashTag extends HashTagLayout | undefined
        >
        ? ReturnType<
            typeof createStreamSessionAccessor<TFields, TPrefix, TId, THashTag>
          >
        : never
      : QueryResource<T>;

/**
 * `session.query`: the data-store schemas of the bound schema module, keyed by
 * export name, bound to the session's connection. See {@link SessionSchemaKind}
 * for why the primitives and pub/sub are not in it.
 */
export type SessionQueryRegistry<TSchema extends BenniSchema> = {
  [K in keyof TSchema as TSchema[K] extends {
    readonly kind: SessionSchemaKind;
  }
    ? K
    : never]: SessionQueryResource<TSchema[K]>;
};

/**
 * A dedicated connection leased from the handle, for the two workloads that
 * monopolize one: blocking reads and WATCH-then-commit. `query` and `store()`
 * reach the same data stores as the handle's, bound to this connection, with
 * the blocking commands added to lists, sorted sets, and streams. `scan`,
 * pub/sub, scripts, and the primitives are intentionally absent: they have no
 * session-specific semantics, and the smaller surface keeps the session's
 * purpose legible.
 */
export interface BenniSession<TSchema extends BenniSchema = RegisteredSchema> {
  /**
   * The bound schema module's data stores by export name, on this
   * connection: `s.query.users.hget("42")` inside `redis.watch()`.
   */
  readonly query: SessionQueryRegistry<TSchema>;
  /** A data-store schema outside the bound module, on this connection. */
  store<T extends { readonly kind: SessionSchemaKind }>(
    schema: T
  ): SessionQueryResource<T>;

  /** WATCH k1 k2…; throws on empty. Keys must share one Cluster hash slot. */
  watch<const TKeys extends readonly string[]>(
    keys: TKeys & SameSlotList<TKeys>
  ): Promise<void>;
  /** UNWATCH. */
  unwatch(): Promise<void>;
  /** Abort-aware builder; exec() resolves the tuple or null on abort. */
  multi(): WatchedRedisTransaction<[]>;

  /** Escape hatch to the raw adapter session. */
  readonly raw: RedisSession;
  readonly closed: boolean;
  close(): Promise<void>;
  /** Alias of close(); enables `await using`. */
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * The redis.watch policy layer options: the retry loop lives in core
 * (runWatch); the borrow-a-session escape hatch is typed here in the Benni
 * handle's BenniSession.
 */
export type BenniWatchOptions<TSchema extends BenniSchema = RegisteredSchema> =
  Omit<RunWatchOptions<BenniSession<TSchema>>, "session"> & {
    /** Borrow a long-lived session (hot paths); never closed by the helper. */
    readonly session?: BenniSession<TSchema>;
  };

/** Options for `redis.close()`. */
export type BenniCloseOptions = {
  /**
   * How long queue workers started through this handle may spend finishing
   * their in-flight jobs, in milliseconds, forwarded to each worker's
   * `stop({ timeoutMs })`: once it elapses, jobs still running are aborted
   * and handed back to the queue. Default: no limit, as for `worker.stop()`.
   */
  readonly timeoutMs?: number;
};

/**
 * Memoizes resolved stores per schema object, so `redis.store(schema)` hands
 * back the very resource `redis.query` holds for it, and a second call the
 * same one. A primitive keeps per-instance state (the cache's single-flight
 * map, a queue's workers), which a fresh store per call would silently split.
 */
function storeCache(
  resolve: (schema: object, label: string) => unknown
): (schema: unknown, label: string) => unknown {
  const resolved = new WeakMap<object, unknown>();
  return (schema, label) => {
    // Non-objects have no binding; resolving them throws the usual message.
    if (typeof schema !== "object" || schema === null) {
      return resolve(schema as object, label);
    }
    if (!resolved.has(schema)) resolved.set(schema, resolve(schema, label));
    return resolved.get(schema);
  };
}

/**
 * A session over its private connection. It gets its own store context, so
 * everything a store builds lazily (the script runner behind `incr` with a
 * TTL) is bound to this connection rather than the shared client's.
 */
function createBenniSessionFacade(
  raw: RedisSession,
  parent: StoreContext,
  queryable: readonly (readonly [string, unknown])[],
  onClose: (session: BenniSession<BenniSchema>) => void
): BenniSession<BenniSchema> {
  const kernel = createBenniSession(raw, parent.assertSameSlot);
  const close = () => {
    onClose(session);
    return kernel.close();
  };
  const ctx = createStoreContext(
    kernel.client,
    parent.onPubSubError,
    parent.assertSameSlot
  );
  const resolve = storeCache((schema, label) => {
    const kind = (schema as { readonly kind?: unknown } | null)?.kind;
    if (SCHEMA_KINDS.has(kind) && !SESSION_KINDS.has(kind)) {
      throw new TypeError(
        `${label} is a ${String(kind)} schema. A session reaches only the data stores (${[...SESSION_KINDS].join(", ")}); use the handle for primitives, channels, and scripts.`
      );
    }
    return resolveSessionStore(schema, ctx, label);
  });
  // Getters, resolved on first use: a watch retry opens a session per
  // attempt, and building every store up front would allocate the whole
  // registry each time for the one or two the body reads.
  const query: Record<string, unknown> = {};
  for (const [name, schema] of queryable) {
    Object.defineProperty(query, name, {
      enumerable: true,
      get: () => resolve(schema, `schema.${name}`)
    });
  }
  const session: BenniSession<BenniSchema> = {
    query: query as SessionQueryRegistry<BenniSchema>,
    store: ((schema: unknown) =>
      resolve(schema, "session.store() schema")) as BenniSession["store"],
    watch: kernel.watch,
    unwatch: kernel.unwatch,
    multi: kernel.multi,
    raw: kernel.raw,
    get closed() {
      return kernel.closed;
    },
    close,
    [Symbol.asyncDispose]: close
  };
  return session;
}

/** `redis.scan`: cursor-driven iteration, without blocking the server. */
export interface BenniScan {
  /** SCAN over the whole keyspace. */
  keys(scanOptions?: ScanOptions): AsyncIterable<string>;
  /** SCAN over one keyspace's keys (`prefix:*`). */
  kv<TInput, TOutput>(
    keyspace: Keyspace<TInput, TOutput>,
    scanOptions?: ScanOptions
  ): AsyncIterable<string>;
  /** SSCAN over one set's members. */
  set<TInput, TOutput, TId extends RedisKeyPart>(
    schema: SetSchema<TInput, TOutput, string, TId>,
    id: NoInfer<TId>,
    scanOptions?: ScanMemberOptions
  ): AsyncIterable<TOutput>;
  /** HSCAN over one hash's fields. */
  hash<TFields extends FieldCodecs, TId extends RedisKeyPart>(
    schema: HashSchema<TFields, string, TId>,
    id: NoInfer<TId>,
    scanOptions?: ScanMemberOptions
  ): AsyncIterable<HashScanEntry<TFields>>;
  /** ZSCAN over one sorted set's members and scores. */
  zset<TInput, TOutput, TId extends RedisKeyPart>(
    schema: SortedSetSchema<TInput, TOutput, string, TId>,
    id: NoInfer<TId>,
    scanOptions?: ScanMemberOptions
  ): AsyncIterable<SortedSetEntry<TOutput>>;
}

/** `redis.pubsub`: the handle's subscriber connection. */
export interface BenniPubSub {
  /**
   * Drop every subscription and close the leased subscriber connection.
   * Publishing keeps working — it rides the bound client. `redis.close()`
   * does this first.
   */
  close(): Promise<void>;
}

/** `redis.session()` and `redis.watch()`, present when the client can lease a session. */
export interface BenniSessions<TSchema extends BenniSchema = RegisteredSchema> {
  /**
   * Lease a dedicated connection, for blocking reads and WATCH. The callback
   * form closes it when the callback settles; otherwise close it yourself (or
   * `await using`). `redis.close()` closes any still open.
   */
  session(): Promise<BenniSession<TSchema>>;
  session<T>(fn: (s: BenniSession<TSchema>) => Promise<T>): Promise<T>;
  /**
   * The retrying optimistic-transaction helper. Per attempt: (open or borrow
   * a session) → WATCH keys → run the body (reads via `s.query`) → the body
   * returns the built, un-executed multi → the helper calls exec(). A
   * conflict (null) fires onAbort, backs off, and re-WATCHes; a body that
   * returns null opts out (UNWATCH, resolve null); exhausted attempts throw
   * WatchRetriesExceededError. Owned sessions close in finally; a borrowed
   * options.session is never closed.
   *
   * Keys are checked for a shared Cluster hash tag wherever that is provable
   * from their types; see {@link SameSlotList}. Keys built from runtime ids
   * are not provable and pass silently.
   */
  watch<
    const TKeys extends string | readonly string[],
    TResults extends readonly unknown[]
  >(
    // The naked TKeys member is mandatory: TypeScript cannot infer through a
    // conditional, so without it the check silently never fires.
    keys: TKeys & SameSlotArg<TKeys>,
    body: (
      s: BenniSession<TSchema>
    ) => Promise<WatchedRedisTransaction<TResults> | null>,
    watchOptions?: BenniWatchOptions<TSchema>
  ): Promise<TResults | null>;
}

// biome-ignore lint/suspicious/noEmptyInterface: named, not `{}`, so the handle's type prints as `Benni<…>` in hovers and errors instead of dissolving into its structure.
export interface BenniNoSessions {}

/**
 * The members every handle has, whatever its client can do. {@link Benni}
 * adds `session()`/`watch()` when the client supports them.
 */
export interface BenniBase<
  TSchema extends BenniSchema,
  TClient extends RedisClient
> {
  /** The schema module passed as `schema`, or undefined when none was. */
  readonly schema: TSchema | undefined;
  /**
   * The client, for raw commands (`redis.raw.send(["PING"])`). It refuses
   * commands once `redis.close()` has run, and its `close()` is
   * `redis.close()`.
   */
  readonly raw: TClient;
  /**
   * The schema registry: `redis.query.<exportName>` resolves each schema from
   * the bound `{ schema }` module to its typed resource, dispatched by the
   * schema's `kind`. This is the one path to every store and primitive:
   * declare schemas once, reach each by name with full inference.
   */
  readonly query: QueryRegistry<TSchema, TClient>;
  /**
   * The same resource `redis.query` would give, for a schema that is not in
   * the bound module: one declared inside a library, or a handle built with
   * no `schema`. For a schema that is in it, this is the very object
   * `redis.query` holds.
   *
   * ```ts
   * const locks = benni({ client }).store(lock("order", { ttlMs: 10_000 }));
   * ```
   */
  store<T extends StorableSchema<TClient>>(
    schema: T
  ): QueryResource<T, TClient>;
  readonly scan: BenniScan;
  readonly pubsub: BenniPubSub;
  /**
   * Typed MULTI/EXEC builder (shared-client form; for WATCH-based
   * optimistic transactions use `redis.watch()` or a session's `multi()`).
   */
  multi(): RedisTransaction<[]>;
  /**
   * Shut the handle down, in order: the Pub/Sub subscriptions, then queue
   * workers started through it (each drains its in-flight jobs, as
   * `worker.stop()` does, bounded by `timeoutMs` when given), then sessions
   * still open, then the client. Only what this handle opened is closed: a
   * handle built over another handle leaves that handle's client open, and an
   * ioredis client you adopted is never closed. Idempotent; commands issued
   * once it has run reject.
   */
  close(options?: BenniCloseOptions): Promise<void>;
  /** Alias of close(); enables `await using redis = benni(…)`. */
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * The type of the bound handle `benni()` returns — name it in your own
 * signatures the way you would Drizzle's `NodePgDatabase`.
 *
 * `TSchema` defaults to whatever the app declared through {@link Register},
 * so with that augmentation in place the bare `Benni` is already the fully
 * typed handle. Pass `typeof schema` explicitly when you have not registered
 * one, or for a second handle on a different module.
 *
 * `TClient` is the adapter's client type and decides what the handle offers:
 * without sessions (`benni/upstash`) there is no `session()` or `watch()`,
 * without a subscriber connection no `subscribe()`, without pattern support
 * (`benni/bun`) no pattern entries in `redis.query`. It defaults to a client that can
 * do everything, as `benni/node` and `benni/ioredis` can; on another adapter
 * name its client: `Benni<typeof schema, UpstashClient>`. Libraries that
 * accept any handle take {@link AnyBenni}.
 * @example
 * ```ts
 * import * as schema from "./schema";
 * export function makeHandlers(redis: Benni<typeof schema>) { ... }
 * ```
 */
export type Benni<
  TSchema extends BenniSchema = RegisteredSchema,
  TClient extends RedisClient = FullRedisClient
> = BenniBase<TSchema, TClient> &
  (TClient extends SessionCapable ? BenniSessions<TSchema> : BenniNoSessions);

/**
 * Any handle, whatever its schema and client: the parameter type for a
 * library that takes a handle from its caller. It offers the capability-free
 * surface, so a library that needs sessions or subscribing should say so with
 * a narrower `Benni<BenniSchema, TClient>`.
 */
export type AnyBenni = Benni<BenniSchema, RedisClient>;

/**
 * What `benni()` takes: the client, the schema module, and the options.
 *
 * `schema` is required whenever `TSchema` names one, so an explicit
 * `benni<typeof schema>({ client })` cannot compile into a handle whose
 * `redis.query` is empty at runtime.
 */
export type BenniConfig<
  TSchema extends BenniSchema = BenniSchema,
  TClient extends RedisClient = RedisClient
> = BenniOptions & {
  /**
   * An adapter's client (`node({ url })`, `ioredis(…)`, `bun()`,
   * `upstash(…)`), or another benni handle to share its client with.
   */
  readonly client: ClientSource<TClient>;
} & (BenniSchema extends TSchema
    ? { readonly schema?: TSchema }
    : { readonly schema: TSchema });

/** Why a command reached a handle whose close() already ran. */
const HANDLE_CLOSED =
  "This benni handle is closed: redis.close() already ran. Commands after close() are refused rather than reopening a connection nothing would close.";

/**
 * The client the handle and its stores talk to: `underlying`, refusing work
 * once the handle is shutting down.
 *
 * Two stages, because shutdown needs the connection for a while. Queue
 * workers drain their in-flight jobs during close() and those jobs still write
 * their results, so plain commands flow until the workers and sessions are
 * done. New connections (a session, a subscriber) are refused from the moment
 * close() starts, since nothing would close them.
 *
 * Built member by member so the optional capabilities are present exactly
 * when the underlying client has them; feature checks such as
 * `transactionOrPipeline` see the same client either way.
 */
function guardClient(
  underlying: RedisClient,
  state: { closing: boolean; closed: boolean },
  close: () => Promise<void>
): RedisClient {
  const refused = () => Promise.reject(new Error(HANDLE_CLOSED));
  const guarded: RedisClient = {
    send: (command) => (state.closed ? refused() : underlying.send(command)),
    pipeline: (commands) =>
      state.closed ? refused() : underlying.pipeline(commands),
    close
  };
  const { transaction, session, subscriber } = underlying;
  if (transaction !== undefined) {
    guarded.transaction = (commands) =>
      state.closed ? refused() : transaction.call(underlying, commands);
  }
  if (session !== undefined) {
    guarded.session = () =>
      state.closing ? refused() : session.call(underlying);
  }
  if (subscriber !== undefined) {
    guarded.subscriber = () =>
      state.closing ? refused() : subscriber.call(underlying);
  }
  return guarded;
}

/** What `track` registers: a queue worker, stoppable with a drain timeout. */
type Stoppable = { stop(options?: BenniCloseOptions): Promise<void> };

function createBenni<TSchema extends BenniSchema, TClient extends RedisClient>(
  source: ClientSource<TClient>,
  options: BenniOptions & { readonly schema?: TSchema }
): Benni<TSchema, TClient> {
  const underlying: RedisClient = resolveClient(source);
  // A client handed over directly is this handle's to close; one taken from
  // another handle belongs to that handle.
  const ownsClient = underlying === source;
  const state = { closing: false, closed: false };
  const workers = new Set<Stoppable>();
  const sessions = new Set<BenniSession<BenniSchema>>();
  let closing: Promise<void> | undefined;

  function close(closeOptions?: BenniCloseOptions): Promise<void> {
    // Memoized, so a second close() awaits the first one's teardown rather
    // than resolving while it is still running, and the client is closed once.
    // A second call's timeoutMs therefore has no effect: the workers were
    // already told how long they have.
    closing ??= (async () => {
      state.closing = true;
      const failures: unknown[] = [];
      // Each stage runs even if an earlier one failed: a subscriber that
      // would not close must not leave the workers and the client running.
      const settle = async (work: Promise<unknown>[]) => {
        for (const result of await Promise.allSettled(work)) {
          if (result.status === "rejected") failures.push(result.reason);
        }
      };
      const stopOptions =
        closeOptions?.timeoutMs === undefined
          ? undefined
          : { timeoutMs: closeOptions.timeoutMs };
      const hub = ctx.peek<{ close(): Promise<void> }>(PUBSUB_HUB_KEY);
      await settle(hub === undefined ? [] : [hub.close()]);
      await settle([...workers].map((worker) => worker.stop(stopOptions)));
      await settle([...sessions].map((session) => session.close()));
      state.closed = true;
      if (ownsClient) await settle([underlying.close()]);
      if (failures.length > 0) throw failures[0];
    })();
    return closing;
  }

  const client = guardClient(underlying, state, () => close());
  const ctx = createStoreContext(
    client,
    options.onPubSubError,
    options.cluster,
    (worker) => {
      workers.add(worker);
      return () => workers.delete(worker);
    }
  );
  const resolve = storeCache((schema, label) =>
    resolveStore(schema, ctx, label)
  );
  // The bound module's schemas, checked once here. A session builds its own
  // `query` from the data-store entries of this list.
  const bound = boundSchemas(options.schema, ctx);
  const sessionQueryable = bound.filter(([, schema]) =>
    SESSION_KINDS.has((schema as { readonly kind?: unknown }).kind)
  );

  async function openSession(): Promise<BenniSession<BenniSchema>> {
    if (client.session === undefined) {
      // The runtime backstop: a handle over a client without sessions has no
      // session() in its type.
      throw new UnsupportedCapabilityError(SESSION_UNSUPPORTED, "session");
    }
    const raw = await client.session();
    const leased = createBenniSessionFacade(
      raw,
      ctx,
      sessionQueryable,
      (closed) => sessions.delete(closed)
    );
    // Leased while close() was already draining the sessions: nothing would
    // close this one.
    if (state.closing) {
      await leased.close();
      throw new Error(HANDLE_CLOSED);
    }
    sessions.add(leased);
    return leased;
  }

  type Session = BenniSession<BenniSchema>;
  function session(): Promise<Session>;
  function session<T>(fn: (s: Session) => Promise<T>): Promise<T>;
  function session<T>(fn?: (s: Session) => Promise<T>): Promise<Session | T> {
    if (fn === undefined) return openSession();
    return openSession().then(async (leased) => {
      try {
        return await fn(leased);
      } finally {
        await leased.close();
      }
    });
  }

  const query: Record<string, unknown> = {};
  for (const [name, schema] of bound) {
    query[name] = resolve(schema, `schema.${name}`);
  }

  // Typed as the everything-capable handle over an open schema: the
  // implementation always has every member, with the runtime guards as the
  // backstop. The return type is what narrows it to `TSchema` and to what
  // `TClient` can do.
  const handle: BenniBase<BenniSchema, FullRedisClient> &
    BenniSessions<BenniSchema> = {
    schema: options.schema,
    raw: client as FullRedisClient,
    query,
    store: ((schema: unknown) =>
      resolve(schema, "redis.store() schema")) as BenniBase<
      BenniSchema,
      FullRedisClient
    >["store"],
    scan: {
      keys(scanOptions) {
        return scanKeys(client, scanOptions);
      },
      kv(keyspace, scanOptions) {
        return scanKeyspace(client, keyspace, scanOptions);
      },
      set(schema, id, scanOptions) {
        return scanSet(client, schema, id, scanOptions);
      },
      hash(schema, id, scanOptions) {
        return scanHash(client, schema, id, scanOptions);
      },
      zset(schema, id, scanOptions) {
        return scanSortedSet(client, schema, id, scanOptions);
      }
    },
    pubsub: {
      /**
       * Peeks rather than resolves: the hub is created on first subscribe, so
       * closing a handle that never subscribed must not create one (and must
       * not make this module import the pub/sub code).
       */
      close(): Promise<void> {
        const hub = ctx.peek<{ close(): Promise<void> }>(PUBSUB_HUB_KEY);
        return hub === undefined ? Promise.resolve() : hub.close();
      }
    },
    session,
    watch(keys, body, watchOptions = {}) {
      return runWatch(openSession, keys, body, watchOptions);
    },
    multi() {
      return createTransaction(client, ctx.assertSameSlot);
    },
    close,
    [Symbol.asyncDispose]: () => close()
  };
  return handle as unknown as Benni<TSchema, TClient>;
}

/**
 * The benni schemas a bound module exports, as `[exportName, schema]` pairs.
 *
 * The store binding is what makes an export a benni schema, not a `kind`
 * property: Valibot stamps `kind` on every schema and ArkType on every
 * type(), and both are ordinary co-exports of a schema module (that is how
 * `json(validator)` is used), so claiming every kind-bearing object would
 * kill benni() at bind time on a module that is perfectly valid. A copy of a
 * real schema keeps its kind but drops the non-enumerable binding; that one
 * must still fail here, at bind time, naming the export, rather than at first
 * call.
 */
function boundSchemas(
  schema: BenniSchema | undefined,
  ctx: StoreContext
): Array<readonly [string, BoundSchema]> {
  const found: Array<readonly [string, BoundSchema]> = [];
  if (!schema) return found;
  for (const name of Object.keys(schema)) {
    const value = schema[name];
    if (
      (value as Partial<BoundSchema> | null | undefined)?.[STORE] === undefined
    ) {
      if (
        SCHEMA_KINDS.has((value as { readonly kind?: unknown } | null)?.kind)
      ) {
        // Throws, naming the export.
        resolveStore(value, ctx, `schema.${name}`);
      }
      continue;
    }
    found.push([name, value as BoundSchema]);
  }
  return found;
}

/**
 * Bind a Redis client to create the typed `redis` handle. Reach every schema
 * the bound `{ schema }` module exports by its export name through
 * `redis.query` (dispatched on each schema's `kind`); a schema declared
 * elsewhere goes through `redis.store(schema)`, which returns the same
 * resource.
 *
 * The adapter returns its client synchronously and connects on the first
 * command, so this needs no top-level `await` and opens nothing at import.
 * To find a bad URL at startup rather than at the first request, send one
 * command: `await redis.raw.send(["PING"])`.
 * @example
 * ```ts
 * import * as schema from "./schema";
 *
 * export const redis = benni({ client: node({ url }), schema });
 *
 * await redis.query.users.hset("42", { name: "Ada", score: 10 });
 * await redis.close();
 * ```
 */
export function benni<
  TSchema extends BenniSchema = BenniSchema,
  TClient extends RedisClient = RedisClient
>(config: BenniConfig<TSchema, TClient>): Benni<TSchema, TClient> {
  if (typeof config !== "object" || config === null || !("client" in config)) {
    throw new TypeError(
      "benni() takes one config object: benni({ client: node({ url }), schema }). The positional benni(client, { schema }) form was removed in 0.2."
    );
  }
  return createBenni<TSchema, TClient>(config.client, config);
}

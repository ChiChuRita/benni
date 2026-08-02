// Every `create*Resource` below is imported TYPE-ONLY. QueryResource and
// BenniSession name them through `ReturnType<typeof …>` to keep the public
// types byte-identical, while the runtime dispatch goes through the store
// binding each schema carries — so a bundler only ever pulls in the store
// modules whose schemas the app actually declares. Turning any of these into
// a value import silently re-pins every store to the root entry.
import type { BitmapSchema, createBitmapResource } from "./core/bitmap.js";
import {
  type ClientSource,
  clientArgs,
  SESSION_UNSUPPORTED
} from "./core/client-source.js";
import { createCounterStore } from "./core/counter.js";
import type { createGeoResource, GeoSetSchema } from "./core/geo.js";
import type { createHashResource } from "./core/hash.js";
import type {
  createHllResource,
  HyperLogLogSchema
} from "./core/hyperloglog.js";
import type { createKvResource } from "./core/key-value.js";
import type { HashTagLayout, SameSlotArg, SameSlotList } from "./core/keys.js";
import type {
  createListResource,
  createListSessionAccessor
} from "./core/list.js";
import type {
  ChannelName,
  createChannelResource,
  createPatternResource,
  PubSubChannel,
  PubSubChannelResource,
  PubSubPattern
} from "./core/pubsub.js";
import {
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
  type StoreContext,
  withKey
} from "./core/store.js";
import type { StreamSchema } from "./core/stream.js";
import type {
  createStreamResource,
  createStreamSessionAccessor
} from "./core/stream-resource.js";
import { createStringStore } from "./core/string.js";
import {
  createTransaction,
  type WatchedRedisTransaction
} from "./core/transaction.js";
import type {
  FieldCodecs,
  HashSchema,
  Keyspace,
  ListSchema,
  RedisClient,
  RedisKeyPart,
  RedisSession,
  SetSchema,
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

export type BenniOptions<TSchema extends BenniSchema = BenniSchema> = {
  readonly schema?: TSchema;
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
   * const redis = benni(client, { cluster: assertSameSlot });
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
 * The data-store accessors shared by `benni()` and every session — each bound
 * to the connection passed in. `benni()` binds them to the shared client; a
 * session rebinds the identical set to its private connection so
 * `session.kv(x)` and `redis.kv(x)` behave the same. The list/zset/stream
 * accessors returned here are the base (non-blocking) shape; sessions swap in
 * the blocking supersets (see createBenniSessionFacade).
 *
 * Each accessor resolves the schema's own store binding rather than naming a
 * store factory, so this module pulls in no store code. The casts restore the
 * precise resource type the binding is known to produce — the accessor
 * signatures, and therefore the public API, are unchanged.
 *
 * `counter` and `string` are the exceptions: they are alternate views over a
 * plain kv keyspace rather than a kind of their own, so they cannot dispatch
 * through the schema and stay bound directly.
 */
function createStoreAccessors(ctx: StoreContext) {
  return {
    kv: <
      TInput,
      TOutput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: Keyspace<TInput, TOutput, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "kv schema") as ReturnType<
        typeof createKvResource<TInput, TOutput, TPrefix, TId, THashTag>
      >,
    hash: <
      TFields extends FieldCodecs,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: HashSchema<TFields, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "hash schema") as ReturnType<
        typeof createHashResource<TFields, TPrefix, TId, THashTag>
      >,
    list: <
      TInput,
      TOutput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: ListSchema<TInput, TOutput, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "list schema") as ReturnType<
        typeof createListResource<TInput, TOutput, TPrefix, TId, THashTag>
      >,
    set: <
      TInput,
      TOutput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: SetSchema<TInput, TOutput, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "set schema") as ReturnType<
        typeof createSetResource<TInput, TOutput, TPrefix, TId, THashTag>
      >,
    zset: <
      TInput,
      TOutput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: SortedSetSchema<TInput, TOutput, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "zset schema") as ReturnType<
        typeof createZsetResource<TInput, TOutput, TPrefix, TId, THashTag>
      >,
    hll: <
      TInput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: HyperLogLogSchema<TInput, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "hll schema") as ReturnType<
        typeof createHllResource<TInput, TPrefix, TId, THashTag>
      >,
    stream: <
      TFields extends FieldCodecs,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: StreamSchema<TFields, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "stream schema") as ReturnType<
        typeof createStreamResource<TFields, TPrefix, TId, THashTag>
      >,
    bitmap: <
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: BitmapSchema<TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "bitmap schema") as ReturnType<
        typeof createBitmapResource<TPrefix, TId, THashTag>
      >,
    geo: <
      TInput,
      TOutput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: GeoSetSchema<TInput, TOutput, TPrefix, TId, THashTag>
    ) =>
      resolveStore(schema, ctx, "geo schema") as ReturnType<
        typeof createGeoResource<TInput, TOutput, TPrefix, TId, THashTag>
      >,
    counter: <
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: Keyspace<number, number, TPrefix, TId, THashTag>
    ) => withKey(schema, createCounterStore(ctx.client, schema)),
    string: <
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(
      schema: Keyspace<string, string, TPrefix, TId, THashTag>
    ) =>
      withKey(schema, createStringStore(ctx.client, schema, ctx.assertSameSlot))
  };
}

type StoreAccessors = ReturnType<typeof createStoreAccessors>;

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
 * Maps one declared schema to the typed resource `redis.query.<name>` exposes
 * for it — the same resource the matching `redis.<kind>(schema)` accessor
 * returns.
 * Dispatch is on the schema's literal `kind`, so structurally-identical schema
 * shapes (kv/set/list/zset/geo) still resolve to distinct resources.
 */
export type QueryResource<T> = T extends { readonly kind: "hash" }
  ? T extends HashSchema<
      infer TFields,
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
          infer THashTag extends HashTagLayout | undefined
        >
        ? ReturnType<
            typeof createKvResource<TInput, TOutput, TPrefix, TId, THashTag>
          >
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
                      ? ReturnType<
                          typeof createChannelResource<
                            TInput,
                            TOutput,
                            TName,
                            TId
                          >
                        >
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
 * not schemas (a re-exported type, a helper) are dropped.
 */
export type QueryRegistry<TSchema extends BenniSchema> = {
  [K in keyof TSchema as TSchema[K] extends { readonly kind: SchemaKind }
    ? K
    : never]: QueryResource<TSchema[K]>;
};

/**
 * A dedicated connection leased from the Benni handle, shaped like the same
 * store surface. Exposes the same data-store accessors bound to its private
 * connection — where the
 * list/zset/stream accessors are supersets that also expose the blocking
 * variants (blpop/brpop/blmove/blmpop, bzpopmin/bzpopmax/bzmpop,
 * xread with a timeout) and the
 * blocking consumer-group read — plus the WATCH primitives. `scan`, `pubsub`,
 * and `script` are intentionally absent: they have no session-specific
 * semantics, and the smaller surface keeps the session's purpose legible —
 * block, or watch-then-commit.
 */
export type BenniSession = Omit<StoreAccessors, "list" | "zset" | "stream"> & {
  list<
    TInput,
    TOutput,
    TPrefix extends string,
    TId extends RedisKeyPart,
    THashTag extends HashTagLayout | undefined
  >(
    schema: ListSchema<TInput, TOutput, TPrefix, TId, THashTag>
  ): ReturnType<
    typeof createListSessionAccessor<TInput, TOutput, TPrefix, TId, THashTag>
  >;
  zset<
    TInput,
    TOutput,
    TPrefix extends string,
    TId extends RedisKeyPart,
    THashTag extends HashTagLayout | undefined
  >(
    schema: SortedSetSchema<TInput, TOutput, TPrefix, TId, THashTag>
  ): ReturnType<
    typeof createZsetSessionAccessor<TInput, TOutput, TPrefix, TId, THashTag>
  >;
  stream<
    TFields extends FieldCodecs,
    TPrefix extends string,
    TId extends RedisKeyPart,
    THashTag extends HashTagLayout | undefined
  >(
    schema: StreamSchema<TFields, TPrefix, TId, THashTag>
  ): ReturnType<
    typeof createStreamSessionAccessor<TFields, TPrefix, TId, THashTag>
  >;

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
};

/**
 * The redis.watch policy layer options: the retry loop lives in core
 * (runWatch); the borrow-a-session escape hatch is typed here in the Benni
 * handle's BenniSession.
 */
export type BenniWatchOptions = Omit<
  RunWatchOptions<BenniSession>,
  "session"
> & {
  /** Borrow a long-lived session (hot paths); never closed by the helper. */
  readonly session?: BenniSession;
};

/**
 * A session's own accessors: identical to the shared set, except that list,
 * zset, and stream resolve the schema's *session* binding — the blocking
 * superset — over the session's private connection.
 */
function createBenniSessionFacade(
  raw: RedisSession,
  parent: StoreContext
): BenniSession {
  const kernel = createBenniSession(raw, parent.assertSameSlot);
  // A session shares the parent's singletons (hub, script runner) but binds
  // every store to its own connection.
  const ctx: StoreContext = { ...parent, client: kernel.client };
  const base = createStoreAccessors(ctx);
  const accessors: BenniSession = {
    kv: base.kv,
    hash: base.hash,
    set: base.set,
    hll: base.hll,
    bitmap: base.bitmap,
    geo: base.geo,
    counter: base.counter,
    string: base.string,
    list<
      TInput,
      TOutput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(schema: ListSchema<TInput, TOutput, TPrefix, TId, THashTag>) {
      return resolveSessionStore(schema, ctx, "list schema") as ReturnType<
        typeof createListSessionAccessor<
          TInput,
          TOutput,
          TPrefix,
          TId,
          THashTag
        >
      >;
    },
    zset<
      TInput,
      TOutput,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(schema: SortedSetSchema<TInput, TOutput, TPrefix, TId, THashTag>) {
      return resolveSessionStore(schema, ctx, "zset schema") as ReturnType<
        typeof createZsetSessionAccessor<
          TInput,
          TOutput,
          TPrefix,
          TId,
          THashTag
        >
      >;
    },
    stream<
      TFields extends FieldCodecs,
      TPrefix extends string,
      TId extends RedisKeyPart,
      THashTag extends HashTagLayout | undefined
    >(schema: StreamSchema<TFields, TPrefix, TId, THashTag>) {
      return resolveSessionStore(schema, ctx, "stream schema") as ReturnType<
        typeof createStreamSessionAccessor<TFields, TPrefix, TId, THashTag>
      >;
    },
    watch: kernel.watch,
    unwatch: kernel.unwatch,
    multi: kernel.multi,
    raw: kernel.raw,
    get closed() {
      return kernel.closed;
    },
    close: kernel.close,
    [Symbol.asyncDispose]: kernel.close
  };
  return accessors;
}

function createBenni<TSchema extends BenniSchema = BenniSchema>(
  client: RedisClient,
  options: BenniOptions<TSchema> = {}
) {
  const ctx = createStoreContext(
    client,
    options.onPubSubError,
    options.cluster
  );
  const accessors = createStoreAccessors(ctx);

  async function openSession(): Promise<BenniSession> {
    if (client.session === undefined) {
      throw new TypeError(SESSION_UNSUPPORTED);
    }
    return createBenniSessionFacade(await client.session(), ctx);
  }

  function session(): Promise<BenniSession>;
  function session<T>(fn: (s: BenniSession) => Promise<T>): Promise<T>;
  function session<T>(
    fn?: (s: BenniSession) => Promise<T>
  ): Promise<BenniSession | T> {
    if (fn === undefined) return openSession();
    return openSession().then(async (leased) => {
      try {
        return await fn(leased);
      } finally {
        await leased.close();
      }
    });
  }

  /**
   * `redis.pubsub.channel(schema)` addresses the channel the schema names;
   * `redis.pubsub.channel(schema, id)` addresses the per-entity channel
   * `name:id` — one channel per room, per user, per job.
   *
   * Declared as overloads (rather than one optional-id signature) so the
   * resolved channel stays visible in the type: with an id the resource is
   * typed `events:room:42`, without one it is still `events:room`.
   *
   * The id is applied by the resource's own `at()` rather than being passed
   * into the store factory, because a schema's store binding is invoked with
   * `(ctx, schema)` and nothing else. Reaching past that would mean naming the
   * pub/sub resource factory here as a value, which is exactly what the binding
   * indirection exists to avoid — it would pin the whole pub/sub module into
   * every bundle that imports `benni()`.
   */
  function pubsubChannel<
    TInput,
    TOutput,
    TName extends string,
    TId extends RedisKeyPart
  >(
    channel: PubSubChannel<TInput, TOutput, TName, TId>
  ): PubSubChannelResource<TInput, TOutput, TName, TId>;
  function pubsubChannel<
    TInput,
    TOutput,
    TName extends string,
    TId extends RedisKeyPart,
    TActualId extends TId
  >(
    channel: PubSubChannel<TInput, TOutput, TName, TId>,
    id: TActualId
  ): PubSubChannelResource<TInput, TOutput, ChannelName<TName, TActualId>, TId>;
  function pubsubChannel(
    channel: PubSubChannel<unknown, unknown, string, RedisKeyPart>,
    id?: RedisKeyPart
  ): PubSubChannelResource<unknown, unknown> {
    const resource = resolveStore(
      channel,
      ctx,
      "channel schema"
    ) as PubSubChannelResource<unknown, unknown>;
    return id === undefined ? resource : resource.at(id);
  }

  function buildQuery(): QueryRegistry<TSchema> {
    const registry: Record<string, unknown> = {};
    const schema = options.schema;
    if (schema) {
      for (const name of Object.keys(schema)) {
        const value = (schema as Record<string, unknown>)[name];
        // The store binding is what makes an export a benni schema, not a
        // `kind` property: Valibot stamps `kind` on every schema and ArkType
        // on every type(), and both are ordinary co-exports of a schema module
        // (that is how `json(validator)` is used), so claiming every
        // kind-bearing object would kill benni() at bind time on a module that
        // is perfectly valid.
        if (
          (value as Partial<BoundSchema> | null | undefined)?.[STORE] ===
          undefined
        ) {
          // A copy of a real schema keeps its kind but drops the
          // non-enumerable binding. That one must still fail here, at bind
          // time, naming the export, rather than at first call.
          if (
            SCHEMA_KINDS.has(
              (value as { readonly kind?: unknown } | null)?.kind
            )
          ) {
            resolveStore(value, ctx, `schema.${name}`);
          }
          continue;
        }
        registry[name] = resolveStore(value, ctx, `schema.${name}`);
      }
    }
    return registry as QueryRegistry<TSchema>;
  }

  return {
    schema: options.schema,
    raw: client,
    ...accessors,
    /**
     * The schema registry: `redis.query.<exportName>` resolves each schema from
     * the bound `{ schema }` module to its typed resource, dispatched by the
     * schema's `kind`. This is the drizzle-style headline surface — declare
     * schemas once, reach every store by name with full inference.
     */
    query: buildQuery(),
    scan: {
      keys(scanOptions?: ScanOptions): AsyncIterable<string> {
        return scanKeys(client, scanOptions);
      },
      kv<TInput, TOutput>(
        keyspace: Keyspace<TInput, TOutput>,
        scanOptions?: ScanOptions
      ): AsyncIterable<string> {
        return scanKeyspace(client, keyspace, scanOptions);
      },
      set<TInput, TOutput, TId extends RedisKeyPart>(
        schema: SetSchema<TInput, TOutput, string, TId>,
        id: NoInfer<TId>,
        scanOptions?: ScanMemberOptions
      ) {
        return scanSet(client, schema, id, scanOptions);
      },
      hash<TFields extends FieldCodecs, TId extends RedisKeyPart>(
        schema: HashSchema<TFields, string, TId>,
        id: NoInfer<TId>,
        scanOptions?: ScanMemberOptions
      ) {
        return scanHash(client, schema, id, scanOptions);
      },
      zset<TInput, TOutput, TId extends RedisKeyPart>(
        schema: SortedSetSchema<TInput, TOutput, string, TId>,
        id: NoInfer<TId>,
        scanOptions?: ScanMemberOptions
      ) {
        return scanSortedSet(client, schema, id, scanOptions);
      }
    },
    pubsub: {
      channel: pubsubChannel,
      pattern<TOutput>(pattern: PubSubPattern<TOutput>) {
        return resolveStore(pattern, ctx, "pattern schema") as ReturnType<
          typeof createPatternResource<TOutput>
        >;
      },
      /**
       * Drop every subscription and close the leased subscriber connection.
       * Publishing keeps working — it rides the bound client.
       *
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
    /**
     * The retrying optimistic-transaction helper, discoverable next to
     * redis.multi(). Per attempt: (open or borrow a session) → WATCH keys → run
     * the body (reads via the session accessors) → the body returns the
     * built, un-executed multi → the helper calls exec(). A conflict (null)
     * fires onAbort, backs off, and re-WATCHes; a body that returns null
     * opts out (UNWATCH, resolve null); exhausted attempts throw
     * WatchRetriesExceededError. Owned sessions close in finally; a borrowed
     * options.session is never closed.
     */
    /**
     * WATCH the keys, run `body`, and EXEC its transaction, retrying on abort.
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
        s: BenniSession
      ) => Promise<WatchedRedisTransaction<TResults> | null>,
      watchOptions: BenniWatchOptions = {}
    ): Promise<TResults | null> {
      return runWatch(openSession, keys, body, watchOptions);
    },
    /**
     * Typed MULTI/EXEC builder (shared-client form; for WATCH-based
     * optimistic transactions use `redis.watch()` or a session's `multi()`).
     */
    multi() {
      return createTransaction(client, ctx.assertSameSlot);
    },
    script<
      TName extends string,
      TKeys extends readonly string[],
      TArgs extends FieldCodecs,
      TResult
    >(schema: ScriptSchema<TName, TKeys, TArgs, TResult>) {
      return resolveStore(schema, ctx, "script schema") as ReturnType<
        typeof createScriptResource<TName, TKeys, TArgs, TResult>
      >;
    }
  };
}

/**
 * The type of the bound handle `benni()` returns — name it in your own
 * signatures the way you would Drizzle's `NodePgDatabase`.
 *
 * The parameter defaults to whatever the app declared through
 * {@link Register}, so with that augmentation in place the bare `Benni` is
 * already the fully typed handle. Pass `typeof schema` explicitly when you
 * have not registered one, or for a second handle on a different module.
 * @example
 * ```ts
 * import * as schema from "./schema";
 * export function makeHandlers(redis: Benni<typeof schema>) { ... }
 * ```
 */
export type Benni<TSchema extends BenniSchema = RegisteredSchema> = ReturnType<
  typeof createBenni<TSchema>
>;

/** {@link BenniOptions} plus the client, for the single-argument form. */
export type BenniConfig<TSchema extends BenniSchema = BenniSchema> =
  BenniOptions<TSchema> & {
    /**
     * The client, a promise of one, or a factory. A promise or factory is
     * resolved once on first use, so an adapter never has to be awaited at
     * module scope.
     */
    readonly client: ClientSource;
  };

/**
 * Bind a Redis client to create the typed `redis` handle. Reach every schema
 * the bound `{ schema }` module exports by its export name through
 * `redis.query` (dispatched on each schema's `kind`), or address a store
 * directly by kind — `redis.hash(schema)`, `redis.zset(schema)`,
 * `redis.scan.*`, `redis.session()`.
 *
 * Takes either shape. The single-argument form needs no top-level `await`,
 * because the adapter's promise is resolved on first command:
 * @example
 * ```ts
 * import * as schema from "./schema";
 *
 * export const redis = benni({ client: node({ url }), schema });
 * // or, with a client you already awaited:
 * export const redis = benni(client, { schema });
 *
 * await redis.query.users.hset("42", { name: "Ada", score: 10 });
 * await redis.hash(schema.users).hset("42", "score", 11);
 * ```
 */
export function benni<TSchema extends BenniSchema = BenniSchema>(
  config: BenniConfig<TSchema>
): Benni<TSchema>;
export function benni<TSchema extends BenniSchema = BenniSchema>(
  client: ClientSource,
  options?: BenniOptions<TSchema>
): Benni<TSchema>;
export function benni<TSchema extends BenniSchema = BenniSchema>(
  source: ClientSource | BenniConfig<TSchema>,
  options?: BenniOptions<TSchema>
): Benni<TSchema> {
  const args = clientArgs<BenniOptions<TSchema>>(source, options);
  return createBenni<TSchema>(args.client, args.options);
}

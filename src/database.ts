import { type BitmapSchema, createBitmapStore } from "./core/bitmap.js";
import { createCounterStore } from "./core/counter.js";
import { replyShapeError } from "./core/errors.js";
import { createGeoStore, type GeoSetSchema } from "./core/geo.js";
import { createHashStore } from "./core/hash.js";
import type { HyperLogLogSchema } from "./core/hyperloglog.js";
import { createHyperLogLogStore } from "./core/hyperloglog.js";
import { createKeyValueStore } from "./core/key-value.js";
import { createBlockingListOps, createListStore } from "./core/list.js";
import type {
  InferPubSubInput,
  PubSubChannel,
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
import { createScriptRunner } from "./core/script.js";
import {
  createBeniSession,
  type RunWatchOptions,
  runWatch
} from "./core/session.js";
import { createSetStore } from "./core/set.js";
import {
  createBlockingSortedSetOps,
  createSortedSetStore
} from "./core/sorted-set.js";
import {
  createBlockingStreamOps,
  createStreamStore,
  type StreamSchema
} from "./core/stream.js";
import {
  createBlockingStreamGroupOps,
  createStreamGroupOps
} from "./core/stream-group.js";
import { createStringStore } from "./core/string.js";
import {
  createTransaction,
  type WatchedRedisTransaction
} from "./core/transaction.js";
import type {
  FieldCodecs,
  HashSchema,
  InferHashInput,
  Keyspace,
  ListSchema,
  RedisClient,
  RedisKeyPart,
  RedisSession,
  SetSchema,
  SortedSetSchema
} from "./core/types.js";
import type { ScriptSchema } from "./schema.js";

export type BeniSchema = Record<string, unknown>;

export type BeniOptions<TSchema extends BeniSchema = BeniSchema> = {
  readonly schema?: TSchema;
  readonly pubsub?: {
    publish<TChannel extends PubSubChannel<any>>(
      channel: TChannel,
      message: InferPubSubInput<TChannel>
    ): Promise<number>;
    subscribe<TOutput>(
      channel: PubSubChannel<any, TOutput>,
      handler: (message: TOutput) => void | Promise<void>
    ): Promise<{ unsubscribe(): Promise<void> }>;
    subscribePattern?<TOutput>(
      pattern: PubSubPattern<TOutput>,
      handler: (message: TOutput, channel: string) => void | Promise<void>
    ): Promise<{ unsubscribe(): Promise<void> }>;
  };
};

// Carry the schema's own key() type through (not a widened `(id) => string`)
// so `redis.kv(s).key("42")` keeps the `"prefix:42"` template-literal type
// the schemas advertise.
function withKey<
  TId extends RedisKeyPart,
  TSchema extends { key(id: TId): string },
  TStore extends object
>(schema: TSchema, store: TStore): TStore & Pick<TSchema, "key"> {
  return {
    ...store,
    key: schema.key.bind(schema) as TSchema["key"]
  };
}

/**
 * The data-store accessors shared by `beni()` and every session — each
 * bound to the connection passed in. `beni()` binds them to the shared
 * client; a session rebinds the identical set to its private connection so
 * `session.kv(x)` and `redis.kv(x)` behave the same. The list/zset/stream
 * accessors returned here are the base (non-blocking) shape; sessions spread
 * the blocking supersets over them (see createSessionAccessors).
 *
 * The shared `stream` accessor exposes `.group(name)` for consumer groups —
 * non-blocking group ops bind to whatever connection they are given.
 */
function createKvResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: Keyspace<TInput, TOutput, TPrefix, TId>) {
  return withKey(schema, createKeyValueStore(client, schema));
}

function createHashResource<
  TFields extends FieldCodecs,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: HashSchema<TFields, TPrefix, TId>) {
  return withKey(schema, createHashStore(client, schema));
}

function createSetResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: SetSchema<TInput, TOutput, TPrefix, TId>) {
  return withKey(schema, createSetStore(client, schema));
}

function createListResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: ListSchema<TInput, TOutput, TPrefix, TId>) {
  return withKey(schema, createListStore(client, schema));
}

function createZsetResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: SortedSetSchema<TInput, TOutput, TPrefix, TId>) {
  return withKey(schema, createSortedSetStore(client, schema));
}

function createHllResource<
  TInput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: HyperLogLogSchema<TInput, TPrefix, TId>) {
  return withKey(schema, createHyperLogLogStore(client, schema));
}

function createStreamResource<
  TFields extends FieldCodecs,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: StreamSchema<TFields, TPrefix, TId>) {
  const store = createStreamStore(client, schema);
  return {
    ...withKey(schema, store),
    ...createStreamGroupOps(client, schema)
  };
}

function createBitmapResource<TPrefix extends string, TId extends RedisKeyPart>(
  client: RedisClient,
  schema: BitmapSchema<TPrefix, TId>
) {
  return withKey(schema, createBitmapStore(client, schema));
}

function createGeoResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: GeoSetSchema<TInput, TOutput, TPrefix, TId>) {
  return withKey(schema, createGeoStore(client, schema));
}

function createCounterResource<
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: Keyspace<number, number, TPrefix, TId>) {
  return withKey(schema, createCounterStore(client, schema));
}

function createStringResource<TPrefix extends string, TId extends RedisKeyPart>(
  client: RedisClient,
  schema: Keyspace<string, string, TPrefix, TId>
) {
  return withKey(schema, createStringStore(client, schema));
}

function createStoreAccessors(client: RedisClient) {
  return {
    kv: <TInput, TOutput, TPrefix extends string, TId extends RedisKeyPart>(
      schema: Keyspace<TInput, TOutput, TPrefix, TId>
    ) => createKvResource(client, schema),
    hash: <
      TFields extends FieldCodecs,
      TPrefix extends string,
      TId extends RedisKeyPart
    >(
      schema: HashSchema<TFields, TPrefix, TId>
    ) => createHashResource(client, schema),
    list: <TInput, TOutput, TPrefix extends string, TId extends RedisKeyPart>(
      schema: ListSchema<TInput, TOutput, TPrefix, TId>
    ) => createListResource(client, schema),
    set: <TInput, TOutput, TPrefix extends string, TId extends RedisKeyPart>(
      schema: SetSchema<TInput, TOutput, TPrefix, TId>
    ) => createSetResource(client, schema),
    zset: <TInput, TOutput, TPrefix extends string, TId extends RedisKeyPart>(
      schema: SortedSetSchema<TInput, TOutput, TPrefix, TId>
    ) => createZsetResource(client, schema),
    hll: <TInput, TPrefix extends string, TId extends RedisKeyPart>(
      schema: HyperLogLogSchema<TInput, TPrefix, TId>
    ) => createHllResource(client, schema),
    stream: <
      TFields extends FieldCodecs,
      TPrefix extends string,
      TId extends RedisKeyPart
    >(
      schema: StreamSchema<TFields, TPrefix, TId>
    ) => createStreamResource(client, schema),
    bitmap: <TPrefix extends string, TId extends RedisKeyPart>(
      schema: BitmapSchema<TPrefix, TId>
    ) => createBitmapResource(client, schema),
    geo: <TInput, TOutput, TPrefix extends string, TId extends RedisKeyPart>(
      schema: GeoSetSchema<TInput, TOutput, TPrefix, TId>
    ) => createGeoResource(client, schema),
    counter: <TPrefix extends string, TId extends RedisKeyPart>(
      schema: Keyspace<number, number, TPrefix, TId>
    ) => createCounterResource(client, schema),
    string: <TPrefix extends string, TId extends RedisKeyPart>(
      schema: Keyspace<string, string, TPrefix, TId>
    ) => createStringResource(client, schema)
  };
}

type BeniPubSubAdapter = BeniOptions["pubsub"];

/**
 * A pub/sub channel resource: publish through the adapter (or the raw client
 * when no adapter is configured) and subscribe through the adapter's dedicated
 * subscriber connection.
 */
function createChannelResource<TInput, TOutput>(
  client: RedisClient,
  pubsub: BeniPubSubAdapter,
  channel: PubSubChannel<TInput, TOutput>
) {
  return {
    publish(message: TInput): Promise<number> {
      if (pubsub) return pubsub.publish(channel, message);
      return client
        .send(["PUBLISH", channel.name, channel.encode(message)])
        .then((reply) => {
          if (typeof reply !== "number") {
            throw replyShapeError("PUBLISH", "number", reply);
          }
          return reply;
        });
    },
    subscribe(handler: (message: TOutput) => void | Promise<void>) {
      if (!pubsub) {
        throw new TypeError("Pub/Sub subscribe requires a pubsub adapter");
      }
      return pubsub.subscribe(channel, handler);
    }
  };
}

/** A pub/sub pattern resource: pattern-subscribe through the adapter. */
function createPatternResource<TOutput>(
  pubsub: BeniPubSubAdapter,
  pattern: PubSubPattern<TOutput>
) {
  return {
    subscribe(
      handler: (message: TOutput, channel: string) => void | Promise<void>
    ) {
      if (!pubsub?.subscribePattern) {
        throw new TypeError(
          "Pub/Sub pattern subscribe requires a pubsub adapter"
        );
      }
      return pubsub.subscribePattern(pattern, handler);
    }
  };
}

/** A typed Lua script resource: run with named keys and args. */
function createScriptResource<
  TName extends string,
  TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
>(
  scriptRunner: ReturnType<typeof createScriptRunner>,
  schema: ScriptSchema<TName, TKeys, TArgs, TResult>
) {
  return {
    run(input: {
      readonly keys: { readonly [K in TKeys[number]]: string };
      readonly args: InferHashInput<TArgs>;
    }): Promise<TResult> {
      return scriptRunner.run(
        schema,
        schema.keys.map((key) => input.keys[key as TKeys[number]]),
        schema.encodeArgs(input.args)
      );
    }
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
  | "script";

/**
 * Maps one declared schema to the typed resource `redis.query.<name>` exposes
 * for it — the same resource the matching `redis.<kind>(schema)` accessor
 * returns.
 * Dispatch is on the schema's literal `kind`, so structurally-identical schema
 * shapes (kv/set/list/zset/geo) still resolve to distinct resources.
 */
export type QueryResource<T> = T extends { readonly kind: "hash" }
  ? T extends HashSchema<infer TFields, infer TPrefix extends string, infer TId>
    ? ReturnType<typeof createHashResource<TFields, TPrefix, TId>>
    : never
  : T extends { readonly kind: "stream" }
    ? T extends StreamSchema<
        infer TFields,
        infer TPrefix extends string,
        infer TId
      >
      ? ReturnType<typeof createStreamResource<TFields, TPrefix, TId>>
      : never
    : T extends { readonly kind: "kv" }
      ? T extends Keyspace<
          infer TInput,
          infer TOutput,
          infer TPrefix extends string,
          infer TId
        >
        ? ReturnType<typeof createKvResource<TInput, TOutput, TPrefix, TId>>
        : never
      : T extends { readonly kind: "set" }
        ? T extends SetSchema<
            infer TInput,
            infer TOutput,
            infer TPrefix extends string,
            infer TId
          >
          ? ReturnType<typeof createSetResource<TInput, TOutput, TPrefix, TId>>
          : never
        : T extends { readonly kind: "list" }
          ? T extends ListSchema<
              infer TInput,
              infer TOutput,
              infer TPrefix extends string,
              infer TId
            >
            ? ReturnType<
                typeof createListResource<TInput, TOutput, TPrefix, TId>
              >
            : never
          : T extends { readonly kind: "zset" }
            ? T extends SortedSetSchema<
                infer TInput,
                infer TOutput,
                infer TPrefix extends string,
                infer TId
              >
              ? ReturnType<
                  typeof createZsetResource<TInput, TOutput, TPrefix, TId>
                >
              : never
            : T extends { readonly kind: "bitmap" }
              ? T extends BitmapSchema<infer TPrefix extends string, infer TId>
                ? ReturnType<typeof createBitmapResource<TPrefix, TId>>
                : never
              : T extends { readonly kind: "geo" }
                ? T extends GeoSetSchema<
                    infer TInput,
                    infer TOutput,
                    infer TPrefix extends string,
                    infer TId
                  >
                  ? ReturnType<
                      typeof createGeoResource<TInput, TOutput, TPrefix, TId>
                    >
                  : never
                : T extends { readonly kind: "hll" }
                  ? T extends HyperLogLogSchema<
                      infer TInput,
                      infer TPrefix extends string,
                      infer TId
                    >
                    ? ReturnType<typeof createHllResource<TInput, TPrefix, TId>>
                    : never
                  : T extends { readonly kind: "channel" }
                    ? T extends PubSubChannel<
                        infer TInput,
                        infer TOutput,
                        string
                      >
                      ? ReturnType<
                          typeof createChannelResource<TInput, TOutput>
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
                        : never;

/**
 * The `redis.query` registry: every schema exported from the bound schema module,
 * keyed by its export name, resolved to its typed resource. Entries that are
 * not schemas (a re-exported type, a helper) are dropped.
 */
export type QueryRegistry<TSchema extends BeniSchema> = {
  [K in keyof TSchema as TSchema[K] extends { readonly kind: SchemaKind }
    ? K
    : never]: QueryResource<TSchema[K]>;
};

/**
 * A dedicated connection leased from the Beni handle, shaped like the same
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
export type BeniSession = Omit<StoreAccessors, "list" | "zset" | "stream"> & {
  list<TInput, TOutput, TPrefix extends string, TId extends RedisKeyPart>(
    schema: ListSchema<TInput, TOutput, TPrefix, TId>
  ): ReturnType<
    typeof createListSessionAccessor<TInput, TOutput, TPrefix, TId>
  >;
  zset<TInput, TOutput, TPrefix extends string, TId extends RedisKeyPart>(
    schema: SortedSetSchema<TInput, TOutput, TPrefix, TId>
  ): ReturnType<
    typeof createZsetSessionAccessor<TInput, TOutput, TPrefix, TId>
  >;
  stream<
    TFields extends FieldCodecs,
    TPrefix extends string,
    TId extends RedisKeyPart
  >(
    schema: StreamSchema<TFields, TPrefix, TId>
  ): ReturnType<typeof createStreamSessionAccessor<TFields, TPrefix, TId>>;

  /** WATCH k1 k2…; throws on empty. */
  watch(keys: readonly string[]): Promise<void>;
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
 * (runWatch); the borrow-a-session escape hatch is typed here in the Beni
 * handle's BeniSession.
 */
export type BeniWatchOptions = Omit<RunWatchOptions<BeniSession>, "session"> & {
  /** Borrow a long-lived session (hot paths); never closed by the helper. */
  readonly session?: BeniSession;
};

/**
 * Session list accessor: the base store spread with the blocking pops. Its
 * inferred return type drives BeniSession["list"], so leftPopBlocking &
 * friends are structurally present on a session and absent on the shared
 * Beni handle.
 */
function createListSessionAccessor<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: ListSchema<TInput, TOutput, TPrefix, TId>) {
  const store = createListStore(client, schema);
  return {
    ...withKey(schema, store),
    ...createBlockingListOps(client, schema)
  };
}

/** Session zset accessor: base store spread with the blocking pops. */
function createZsetSessionAccessor<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: SortedSetSchema<TInput, TOutput, TPrefix, TId>) {
  const store = createSortedSetStore(client, schema);
  return {
    ...withKey(schema, store),
    ...createBlockingSortedSetOps(client, schema)
  };
}

/**
 * Session stream accessor: base store + blocking XREAD + the blocking-consumer
 * group superset. createBlockingStreamGroupOps is spread last so its group()
 * (returning the full BlockingStreamGroup) wins the `group` key.
 */
function createStreamSessionAccessor<
  TFields extends FieldCodecs,
  TPrefix extends string,
  TId extends RedisKeyPart
>(client: RedisClient, schema: StreamSchema<TFields, TPrefix, TId>) {
  const store = createStreamStore(client, schema);
  return {
    ...withKey(schema, store),
    ...createBlockingStreamOps(client, schema),
    ...createBlockingStreamGroupOps(client, schema)
  };
}

function createBeniSessionFacade(raw: RedisSession): BeniSession {
  const kernel = createBeniSession(raw);
  const client = kernel.client;
  const base = createStoreAccessors(client);
  const accessors: BeniSession = {
    kv: base.kv,
    hash: base.hash,
    set: base.set,
    hll: base.hll,
    bitmap: base.bitmap,
    geo: base.geo,
    counter: base.counter,
    string: base.string,
    list(schema) {
      return createListSessionAccessor(client, schema);
    },
    zset(schema) {
      return createZsetSessionAccessor(client, schema);
    },
    stream(schema) {
      return createStreamSessionAccessor(client, schema);
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

/**
 * Bind a Redis client to create the typed `redis` handle. Access data stores by
 * kind — `redis.hash(schema)`, `redis.zset(schema)`, `redis.scan.*`,
 * `redis.session()` —
 * or, when a `{ schema }` module is passed, reach every bound schema by its
 * export name through the `redis.query` registry (dispatched on each schema's
 * `kind`).
 * @example
 * ```ts
 * import * as schema from "./schema";
 * const redis = beni(client, { schema });
 * await redis.query.users.set("42", { name: "Ada", score: 10 });
 * await redis.hash(schema.users).hset("42", "score", 11);
 * ```
 */
export function beni<TSchema extends BeniSchema = BeniSchema>(
  client: RedisClient,
  options: BeniOptions<TSchema> = {}
) {
  const scriptRunner = createScriptRunner(client);
  const accessors = createStoreAccessors(client);

  async function openSession(): Promise<BeniSession> {
    if (client.session === undefined) {
      throw new TypeError("Redis client does not support sessions");
    }
    return createBeniSessionFacade(await client.session());
  }

  function session(): Promise<BeniSession>;
  function session<T>(fn: (s: BeniSession) => Promise<T>): Promise<T>;
  function session<T>(
    fn?: (s: BeniSession) => Promise<T>
  ): Promise<BeniSession | T> {
    if (fn === undefined) return openSession();
    return openSession().then(async (leased) => {
      try {
        return await fn(leased);
      } finally {
        await leased.close();
      }
    });
  }

  function buildQuery(): QueryRegistry<TSchema> {
    const registry: Record<string, unknown> = {};
    const schema = options.schema;
    if (schema) {
      for (const name of Object.keys(schema)) {
        const value = (
          schema as Record<string, { readonly kind?: SchemaKind }>
        )[name];
        switch (value?.kind) {
          case "kv":
            registry[name] = createKvResource(client, value as never);
            break;
          case "hash":
            registry[name] = createHashResource(client, value as never);
            break;
          case "set":
            registry[name] = createSetResource(client, value as never);
            break;
          case "list":
            registry[name] = createListResource(client, value as never);
            break;
          case "zset":
            registry[name] = createZsetResource(client, value as never);
            break;
          case "stream":
            registry[name] = createStreamResource(client, value as never);
            break;
          case "bitmap":
            registry[name] = createBitmapResource(client, value as never);
            break;
          case "geo":
            registry[name] = createGeoResource(client, value as never);
            break;
          case "hll":
            registry[name] = createHllResource(client, value as never);
            break;
          case "channel":
            registry[name] = createChannelResource(
              client,
              options.pubsub,
              value as never
            );
            break;
          case "pattern":
            registry[name] = createPatternResource(
              options.pubsub,
              value as never
            );
            break;
          case "script":
            registry[name] = createScriptResource(scriptRunner, value as never);
            break;
          default:
            break;
        }
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
      channel<TInput, TOutput>(channel: PubSubChannel<TInput, TOutput>) {
        return createChannelResource(client, options.pubsub, channel);
      },
      pattern<TOutput>(pattern: PubSubPattern<TOutput>) {
        return createPatternResource(options.pubsub, pattern);
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
    watch<TResults extends readonly unknown[]>(
      keys: string | readonly string[],
      body: (
        s: BeniSession
      ) => Promise<WatchedRedisTransaction<TResults> | null>,
      watchOptions: BeniWatchOptions = {}
    ): Promise<TResults | null> {
      return runWatch(openSession, keys, body, watchOptions);
    },
    /**
     * Typed MULTI/EXEC builder (shared-client form; for WATCH-based
     * optimistic transactions use `redis.watch()` or a session's `multi()`).
     */
    multi() {
      return createTransaction(client);
    },
    script<
      TName extends string,
      TKeys extends readonly string[],
      TArgs extends FieldCodecs,
      TResult
    >(schema: ScriptSchema<TName, TKeys, TArgs, TResult>) {
      return createScriptResource(scriptRunner, schema);
    }
  };
}

/**
 * The type of the bound handle `beni()` returns — name it in your own
 * signatures the way you would Drizzle's `NodePgDatabase`.
 * @example
 * ```ts
 * import * as schema from "./schema";
 * export function makeHandlers(redis: Beni<typeof schema>) { ... }
 * ```
 */
export type Beni<TSchema extends BeniSchema = BeniSchema> = ReturnType<
  typeof beni<TSchema>
>;

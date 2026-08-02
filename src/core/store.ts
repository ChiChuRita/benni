import type { SlotGuard } from "./slot.js";
import type { RedisClient, RedisKeyPart } from "./types.js";

/**
 * The store binding every schema carries, stamped by the `define*` builder in
 * that kind's own module.
 *
 * This is what keeps the root entry tree-shakable. `benni()` used to dispatch
 * with a `switch (schema.kind)` naming all twelve store factories, so a
 * bundler had to retain every store even for an app that declares one hash.
 * Dispatching through the schema means the only module that names
 * `createHashStore` is `core/hash.ts` — reachable only when the app actually
 * calls `schema.hash(...)`.
 */
export const STORE: unique symbol = Symbol.for("benni.store");

/**
 * Keys for the per-handle singletons in `StoreContext.shared`. They live here,
 * not in pubsub.ts/script.ts, so `benni()` can peek at a hub that may never
 * have been created without importing the module that would create one.
 */
export const PUBSUB_HUB_KEY = "pubsub";
export const SCRIPT_RUNNER_KEY = "script";

/**
 * What a store factory is handed. `shared`/`peek` memoize the per-handle
 * singletons (the pub/sub hub, the script runner) so that `benni()` itself
 * never names their constructors — keeping those modules out of the baseline
 * too.
 */
export type StoreContext = {
  readonly client: RedisClient;
  readonly onPubSubError?: (error: unknown) => void;
  /**
   * The cross-slot guard, present only under `benni(client, { cluster: assertSameSlot })`.
   *
   * Every multi-key call site invokes it as `assertSameSlot?.(…)`, so when it
   * is undefined the optional call short-circuits argument evaluation and the
   * key array is never even built. That is what makes the default path free.
   */
  readonly assertSameSlot?: SlotGuard;
  /** Get or create the memoized singleton stored under `key`. */
  shared<T>(key: string, create: () => T): T;
  /** The memoized singleton if it was ever created; never creates one. */
  peek<T>(key: string): T | undefined;
};

/**
 * A schema's store factories. `session` is present only for the kinds whose
 * session accessor is a superset of the shared one (list, zset, stream — the
 * blocking variants); everything else reuses `resource` on both surfaces.
 */
export type StoreBinding = {
  readonly resource: (ctx: StoreContext, schema: never) => unknown;
  readonly session?: (ctx: StoreContext, schema: never) => unknown;
};

/** A schema with its store binding attached. */
export type BoundSchema = { readonly [STORE]: StoreBinding };

/**
 * Attach a store binding to a freshly built schema. Non-enumerable so the
 * schema still spreads, logs, and compares as the plain data it looks like.
 */
export function withStore<T extends object>(
  schema: T,
  binding: StoreBinding
): T {
  return Object.defineProperty(schema, STORE, {
    value: binding,
    enumerable: false,
    writable: false,
    configurable: false
  });
}

/**
 * Carry the schema's own key() type through (not a widened `(id) => string`)
 * so `redis.kv(s).key("42")` keeps the `"prefix:42"` template-literal type the
 * schemas advertise.
 */
export function withKey<
  TId extends RedisKeyPart,
  TSchema extends { key(id: TId): string },
  TStore extends object
>(schema: TSchema, store: TStore): TStore & Pick<TSchema, "key"> {
  return {
    ...store,
    key: schema.key.bind(schema) as TSchema["key"]
  };
}

function bindingOf(schema: unknown, label: string): StoreBinding {
  const binding = (schema as Partial<BoundSchema> | null | undefined)?.[STORE];
  if (binding === undefined) {
    throw new TypeError(
      `${label} was not built by a benni schema builder, so it carries no store binding. ` +
        "Pass the schema object returned by `schema.hash(...)`, `schema.kv(...)`, etc. " +
        "directly — copying one (object spread, structuredClone, JSON round-trip) drops it."
    );
  }
  return binding;
}

/** Resolve a schema to its shared-connection resource. */
export function resolveStore(
  schema: unknown,
  ctx: StoreContext,
  label: string
): unknown {
  return bindingOf(schema, label).resource(ctx, schema as never);
}

/**
 * Resolve a schema to its session resource — the blocking superset where the
 * kind has one, otherwise the same resource the shared handle uses.
 */
export function resolveSessionStore(
  schema: unknown,
  ctx: StoreContext,
  label: string
): unknown {
  const binding = bindingOf(schema, label);
  return (binding.session ?? binding.resource)(ctx, schema as never);
}

/** Build the per-handle context that store factories are dispatched with. */
export function createStoreContext(
  client: RedisClient,
  onPubSubError?: (error: unknown) => void,
  assertSameSlot?: SlotGuard
): StoreContext {
  const singletons = new Map<string, unknown>();
  return {
    client,
    onPubSubError,
    assertSameSlot,
    shared<T>(key: string, create: () => T): T {
      if (!singletons.has(key)) singletons.set(key, create());
      return singletons.get(key) as T;
    },
    peek<T>(key: string): T | undefined {
      return singletons.get(key) as T | undefined;
    }
  };
}

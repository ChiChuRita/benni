import { type ClientSource, clientArgs } from "../core/client-source.js";
import { codecs } from "../core/codecs.js";
import { ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { Codec, InferAnchors, RedisClient } from "../core/types.js";
import { createLock } from "./lock.js";

const DEFAULT_PREFIX = "cache";
const DEFAULT_LOCK_TTL_MS = 10_000;
const DEFAULT_POLL_MS = 50;
// How many lease lifetimes a waiter will sit through in total. The wait window
// restarts whenever the fill lease changes hands, so waiters re-collapse onto
// the new holder; this caps the restarts so an endless chain of dying holders
// cannot keep get() waiting forever.
const MAX_WAITED_LEASES = 3;

// Publish only while the loader still holds the fill lease. An unconditional
// SET republished a value snapshotted before a concurrent del() — with a full
// TTL, so one correctly ordered invalidation served stale data for the whole
// ttlMs — and let a holder whose lease had already expired overwrite a newer
// fill. Both keys carry the same {id} hash tag, so this is one Cluster slot.
// Returns 1 if published, else 0.
const publishScript = defineScript<
  readonly [token: string, value: string, ttlMs: string],
  number
>({
  keyCount: 2,
  lua: 'if redis.call("GET", KEYS[2]) ~= ARGV[1] then return 0 end redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3]) return 1',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

// Drop the entry and break any in-flight fill lease in one atomic step, so an
// invalidation always beats a load that is already running: the loader's
// fenced publish then finds a token that is gone and drops its stale value.
// Returns the entry's deleted count, never the lock's.
const invalidateScript = defineScript<readonly [], number>({
  keyCount: 2,
  lua: 'redis.call("DEL", KEYS[2]) return redis.call("DEL", KEYS[1])',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

export type CacheOptions<T> = {
  /** Entry lifetime in milliseconds. */
  readonly ttlMs: number;
  /** Key namespace; keys are `<prefix>:<id>`. Default `"cache"`. */
  readonly prefix?: string;
  /** Value codec. Default `codecs.json<T>()`. */
  readonly codec?: Codec<T>;
  /**
   * How long a single loader may hold the fill lock before another caller
   * gives up waiting and loads for itself. A loader whose lock has expired no
   * longer publishes its result (it would overwrite whatever replaced it), so
   * set this above your slowest load. Default `10000`.
   */
  readonly lockTtlMs?: number;
  /** Poll interval while waiting on another caller's load. Default `50`. */
  readonly pollMs?: number;
};

/**
 * A read-through cache with stampede protection. On a miss, exactly one caller
 * runs the loader (single-flight via a distributed [lock](./lock.js)); everyone
 * else polls for the filled value instead of hammering the backend. If the
 * loader holder dies, its lock TTL expires and waiters fail open by loading
 * for themselves — a stampede can degrade back to plain load, never deadlock.
 *
 * A loader publishes only while it still holds the fill lock, so `del()` beats
 * a load that is already in flight and a stale result can never overwrite a
 * fresher one.
 *
 * Works over any adapter, including `benni/upstash` on the edge (needs only
 * `GET`/`SET`/`DEL` and `SET NX`/`EVALSHA` for the fill lease).
 *
 * ```ts
 * const profiles = cache<Profile>({ client, ttlMs: 60_000 });
 * const profile = await profiles.get(userId, () => db.loadProfile(userId));
 * ```
 */
function createCache<T>(client: RedisClient, options: CacheOptions<T>) {
  const ttlMs = positiveInt(options.ttlMs, "ttlMs");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const codec = options.codec ?? codecs.json<T>();
  const lockTtlMs = positiveInt(
    options.lockTtlMs ?? DEFAULT_LOCK_TTL_MS,
    "lockTtlMs"
  );
  const pollMs = positiveInt(options.pollMs ?? DEFAULT_POLL_MS, "pollMs");
  const scripts = createScriptRunner(client);
  // The fill lock lives next to the entries: <prefix>:lock:{<id>}.
  const fillLocks = createLock(client, {
    prefix: `${prefix}:lock`,
    ttlMs: lockTtlMs
  });

  // The id carries the hash tag, so an entry and its own fill lock always land
  // on the same Cluster node while the cache itself still spreads across the
  // keyspace — which is the one property a cache must keep. Tagging the prefix
  // instead would pin every entry to a single node.
  const tagged = (id: string) => `{${id}}`;
  const key = (id: string) => `${prefix}:${tagged(id)}`;
  // Mirrors the key `fillLocks` builds, for the two places that must name the
  // lease without holding it: the poll loop and del().
  const lockKey = (id: string) => `${prefix}:lock:${tagged(id)}`;

  async function read(id: string): Promise<{ hit: boolean; value?: T }> {
    const reply = await client.send(["GET", key(id)]);
    if (typeof reply !== "string") return { hit: false };
    return { hit: true, value: codec.decode(reply) };
  }

  /** The token of whoever holds the fill lease, or null if nobody does. */
  async function leaseHolder(id: string): Promise<string | null> {
    const reply = await client.send(["GET", lockKey(id)]);
    return typeof reply === "string" ? reply : null;
  }

  async function write(id: string, value: T): Promise<void> {
    await client.send(["SET", key(id), codec.encode(value), "PX", ttlMs]);
  }

  async function fillUnlocked(
    id: string,
    loader: () => Promise<T> | T
  ): Promise<T> {
    const value = await loader();
    // NX: without a lease there is nothing to fence on, and whatever is
    // already cached was published by a loader that did hold one, so it is at
    // least as fresh as this value.
    await client.send(["SET", key(id), codec.encode(value), "NX", "PX", ttlMs]);
    return value;
  }

  type FillLock = NonNullable<Awaited<ReturnType<typeof fillLocks.acquire>>>;

  async function loadUnder(
    handle: FillLock,
    id: string,
    loader: () => Promise<T> | T
  ): Promise<T> {
    try {
      // Double-check: another caller may have filled between miss and lock.
      const second = await read(id);
      if (second.hit) return second.value as T;
      const value = await loader();
      // Fenced on the lease: a del() during the load, or a lease that expired
      // and moved on to a fresher loader, drops this write rather than
      // resurrecting a value the caller has already invalidated or replaced.
      await scripts.run(
        publishScript,
        [key(id), handle.key],
        [handle.token, codec.encode(value), String(ttlMs)]
      );
      return value;
    } finally {
      try {
        await handle.release();
      } catch {
        // A failed release must not mask the load's outcome; the fill
        // lock's TTL frees it regardless.
      }
    }
  }

  return {
    /**
     * Read `id`, running `loader` on a miss. Concurrent misses collapse to one
     * loader call; the others wait for the filled value.
     */
    async get(id: string, loader: () => Promise<T> | T): Promise<T> {
      const first = await read(id);
      if (first.hit) return first.value as T;

      const handle = await fillLocks.acquire(tagged(id));
      if (handle) return loadUnder(handle, id, loader);

      // Someone else is loading; poll until they fill or their lock frees up.
      let leaseToken: string | null = null;
      let deadline = Date.now() + lockTtlMs;
      // The wait restarts on every handoff, so cap the total: an endless chain
      // of dying holders must not keep get() waiting forever.
      const hardDeadline = Date.now() + lockTtlMs * MAX_WAITED_LEASES;
      while (Date.now() < deadline) {
        await sleep(pollMs);
        const polled = await read(id);
        if (polled.hit) return polled.value as T;
        // Watch the lease, not just the value. A holder whose loader threw
        // releases within milliseconds; polling the value alone left every
        // waiter asleep for the whole lockTtlMs and then let all of them load
        // at once — one backend 503 turned into a full-TTL stall followed by
        // an unthrottled stampede. Taking the free lock here keeps
        // single-flight across a failed load.
        const token = await leaseHolder(id);
        if (token === null) {
          const retry = await fillLocks.acquire(tagged(id));
          if (retry) return loadUnder(retry, id, loader);
        }
        if (token !== leaseToken) {
          // The lease changed hands, so wait on the new holder's clock. A
          // deadline frozen at our first failed acquire expired on the dead
          // holder's, and every waiter but the successor then bypassed a
          // loader it could plainly see was running.
          leaseToken = token;
          deadline = Math.min(Date.now() + lockTtlMs, hardDeadline);
        }
      }
      // Fail open — the holder died without releasing; load for ourselves
      // rather than error. Worst case is a brief duplicate load, never a
      // deadlock.
      return fillUnlocked(id, loader);
    },
    /** Read without loading. */
    async peek(id: string): Promise<T | null> {
      const result = await read(id);
      return result.hit ? (result.value as T) : null;
    },
    /** Write an entry directly (with the configured TTL). */
    set: write,
    /**
     * Drop an entry (returns the deleted count); the next `get` reloads it.
     * This also breaks any fill lease in flight, so a loader that read its
     * value before the invalidation cannot publish it afterwards.
     */
    async del(id: string): Promise<number> {
      return scripts.run(invalidateScript, [key(id), lockKey(id)], []);
    }
  };
}

/** The read-through cache {@link cache} returns. */
export type CacheStore<T> = ReturnType<typeof createCache<T>>;

/** {@link CacheOptions} plus the client, for the single-argument form. */
export type CacheConfig<T> = CacheOptions<T> & {
  /** The client, a promise of one, a factory, or a benni handle. */
  readonly client: ClientSource;
};

export function cache<T>(config: CacheConfig<T>): CacheStore<T>;
export function cache<T>(
  client: ClientSource,
  options: CacheOptions<T>
): CacheStore<T>;
export function cache<T>(
  source: ClientSource | CacheConfig<T>,
  options?: CacheOptions<T>
): CacheStore<T> {
  const args = clientArgs<CacheOptions<T>>(source, options);
  return createCache<T>(args.client, args.options);
}

/**
 * A cache declared as a schema value, so it lands in `redis.query` next to the
 * data stores and needs no client of its own.
 * @example
 * ```ts
 * // schema.ts
 * export const profiles = cache("profile", { ttlMs: 60_000, codec: json(Profile) });
 * // app.ts
 * const profile = await redis.query.profiles.get(id, () => db.load(id));
 * ```
 */
export type CacheSchema<T> = InferAnchors<T, T> &
  CacheOptions<T> & {
    readonly kind: "cache";
    readonly prefix: string;
  };

const cacheBinding: StoreBinding = {
  resource: (ctx, schema: CacheSchema<unknown>) =>
    createCache(ctx.client, schema)
};

/** Build a {@link CacheSchema}. Exported as `cache` from `benni/schema`. */
export function defineCache<T>(
  prefix: string,
  options: CacheOptions<T>
): CacheSchema<T> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    ...options,
    kind: "cache",
    prefix
  } as CacheSchema<T>;
  return withStore(schema, cacheBinding);
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `cache ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

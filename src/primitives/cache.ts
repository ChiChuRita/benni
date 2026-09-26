import { type ClientSource, clientArgs } from "../core/client-source.js";
import { codecs } from "../core/codecs.js";
import { ReplyShapeError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { Codec, InferAnchors, RedisClient } from "../core/types.js";
import {
  assertCoLocated,
  createLease,
  extendIfHeldScript,
  heartbeatFor,
  jittered,
  monotonicNow,
  positiveMs,
  releaseIfHeldScript,
  sleep,
  whileRenewing
} from "./lease.js";

const DEFAULT_PREFIX = "cache";
const DEFAULT_LOCK_TTL_MS = 10_000;
const DEFAULT_POLL_MS = 50;
// A waiter's default budget, in fill-lock lifetimes: long enough to sit out a
// loader that died (its lock lapses within one) and take over.
const DEFAULT_WAITED_LEASES = 3;
// Waiters back off from `pollMs` up to this multiple of it. A 40s load with
// 500 waiters used to cost 20,000 GETs a second; at the cap it is ~1,250
// single round trips.
const MAX_POLL_BACKOFF = 8;

type Claim =
  | { readonly state: "hit"; readonly value: string }
  | { readonly state: "claimed" }
  | { readonly state: "held" };

/**
 * One round trip for a miss: the entry if it is there, else the fill lock if
 * it is free. Checking and claiming in one script is the double-check a
 * separate GET used to do after winning the lock, and it is what a waiter
 * polls with, so it takes over a dead loader's lock the moment it lapses.
 *
 * KEYS[1] entry, KEYS[2] fill lock (same `{id}` slot). ARGV[1] our token,
 * ARGV[2] lockTtlMs. Returns `{1, value}` hit, `{2}` claimed, `{0}` held.
 */
const claimScript = defineScript<
  readonly [token: string, lockTtlMs: string],
  Claim
>({
  keyCount: 2,
  lua: `
local value = redis.call("GET", KEYS[1])
if value then return {1, value} end
if redis.call("SET", KEYS[2], ARGV[1], "NX", "PX", ARGV[2]) then return {2} end
return {0}
`,
  decode: (reply) => {
    if (!Array.isArray(reply)) {
      throw new ReplyShapeError(
        "Expected cache claim to return an array",
        reply
      );
    }
    const state = Number(reply[0]);
    if (state === 1 && typeof reply[1] === "string") {
      return { state: "hit", value: reply[1] };
    }
    return state === 2 ? { state: "claimed" } : { state: "held" };
  }
});

// Publish only while the loader still holds the fill lock, and give the lock
// up in the same step. The fill token is the entry's generation: `set()` and
// `del()` delete it, and a lease that lapsed is gone or someone else's, so a
// value loaded before any of those finds a token that is not current and is
// dropped rather than resurrecting data the caller has already replaced.
// Returns 1 if published, else 0.
const publishScript = defineScript<
  readonly [token: string, value: string, ttlMs: string],
  number
>({
  keyCount: 2,
  lua: `
if redis.call("GET", KEYS[2]) ~= ARGV[1] then return 0 end
redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
redis.call("DEL", KEYS[2])
return 1
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

// Write the entry and break any in-flight fill in one atomic step, so a
// slower loader that read its value before this write cannot overwrite it.
const writeScript = defineScript<
  readonly [value: string, ttlMs: string],
  number
>({
  keyCount: 2,
  lua: 'redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2]) redis.call("DEL", KEYS[2]) return 1',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

// Drop the entry and break any in-flight fill lease in one atomic step, so an
// invalidation always beats a load that is already running. Returns the
// entry's deleted count, never the lock's.
const invalidateScript = defineScript<readonly [], number>({
  keyCount: 2,
  lua: 'redis.call("DEL", KEYS[2]) return redis.call("DEL", KEYS[1])',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/**
 * Thrown by `cache().get()` when another caller's load was still running after
 * `waitTimeoutMs`. Nothing was loaded by this caller.
 *
 * Loading anyway is what every waiter used to do at the deadline, all at once:
 * a backend already too slow to answer inside the budget got one extra load
 * per waiter, which is the stampede the cache exists to prevent. Serve an
 * error (a 503) or a fallback instead, or raise `waitTimeoutMs`.
 */
export class CacheWaitTimeoutError extends Error {
  readonly key: string;
  constructor(key: string, waitedMs: number) {
    super(
      `Timed out after ${waitedMs}ms waiting for another caller to load cache ` +
        `entry "${key}". Its loader is still running; raise waitTimeoutMs if ` +
        "loads legitimately take this long."
    );
    this.name = "CacheWaitTimeoutError";
    this.key = key;
  }
}

export type CacheOptions<T> = {
  /** Entry lifetime in milliseconds. */
  readonly ttlMs: number;
  /**
   * Key namespace; entries are `<prefix>:{<id>}` and fill locks
   * `<prefix>:lock:{<id>}`, so both share the id's Cluster slot. Default
   * `"cache"`.
   */
  readonly prefix?: string;
  /** Value codec. Default `codecs.json<T>()`. */
  readonly codec?: Codec<T>;
  /**
   * The fill lock's lease, in milliseconds. Default `10000`.
   *
   * The lock is renewed every quarter of this while the loader runs, so it
   * does not bound the load: a 40s load under the default keeps its lock. It
   * is how long a loader that *died* holds everyone else up before a waiter
   * takes over.
   */
  readonly lockTtlMs?: number;
  /**
   * How long a `get()` waits on another caller's load before throwing
   * {@link CacheWaitTimeoutError}, in milliseconds. Default three times
   * `lockTtlMs`.
   */
  readonly waitTimeoutMs?: number;
  /**
   * First poll interval while waiting on another caller's load, in
   * milliseconds. Default `50`. Each poll is one round trip; the interval
   * doubles (with jitter) up to eight times this.
   */
  readonly pollMs?: number;
};

/**
 * A read-through cache with stampede protection. On a miss, exactly one caller
 * runs the loader, under a fill lock renewed for as long as the load takes;
 * everyone else polls for the filled value instead of hammering the backend.
 * If the loader throws or its process dies, a waiter takes the lock over and
 * loads in its place, so one failure costs one reload, not a stampede.
 *
 * A loader publishes only if nothing wrote or deleted the entry since it
 * started: `set()` and `del()` both invalidate an in-flight fill, and so does
 * a fill lock that lapsed, so a stale result can never overwrite a fresher one
 * or undo an invalidation.
 *
 * Works over any adapter, including `benni/upstash` on the edge (it needs only
 * `GET` and `EVALSHA`).
 *
 * ```ts
 * const profiles = cache<Profile>({ client, ttlMs: 60_000 });
 * const profile = await profiles.get(userId, () => db.loadProfile(userId));
 * ```
 */
function createCache<T>(client: RedisClient, options: CacheOptions<T>) {
  const checkMs = (ms: number, name: string) => positiveMs(ms, name, "cache");
  const ttlMs = checkMs(options.ttlMs, "ttlMs");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const codec = options.codec ?? codecs.json<T>();
  const lockTtlMs = checkMs(
    options.lockTtlMs ?? DEFAULT_LOCK_TTL_MS,
    "lockTtlMs"
  );
  const waitTimeoutMs = checkMs(
    options.waitTimeoutMs ?? lockTtlMs * DEFAULT_WAITED_LEASES,
    "waitTimeoutMs"
  );
  const pollMs = checkMs(options.pollMs ?? DEFAULT_POLL_MS, "pollMs");
  const scripts = createScriptRunner(client);

  // The id carries the hash tag, so an entry and its own fill lock always land
  // on the same Cluster node while the cache itself still spreads across the
  // keyspace — which is the one property a cache must keep. Tagging the prefix
  // instead would pin every entry to a single node.
  function keysFor(id: string): readonly [entry: string, lock: string] {
    const keys = [`${prefix}:{${id}}`, `${prefix}:lock:{${id}}`] as const;
    assertCoLocated(keys, "cache", id);
    return keys;
  }

  async function read(entry: string): Promise<{ hit: boolean; value?: T }> {
    const reply = await client.send(["GET", entry]);
    if (typeof reply !== "string") return { hit: false };
    return { hit: true, value: codec.decode(reply) };
  }

  async function loadUnder(
    keys: readonly [entry: string, lock: string],
    token: string,
    startedAt: number,
    loader: () => Promise<T> | T
  ): Promise<T> {
    const [entry, lock] = keys;
    const lease = createLease({
      ttlMs: lockTtlMs,
      startedAt,
      checkTtl: (ms) => checkMs(ms, "lockTtlMs"),
      extend: async (ms) =>
        (await scripts.run(extendIfHeldScript, [lock], [token, String(ms)])) ===
        1,
      release: async () =>
        (await scripts.run(releaseIfHeldScript, [lock], [token])) === 1,
      lostError: () =>
        new Error(`Lost the fill lock for cache entry "${entry}"`)
    });
    let closed = false;
    try {
      const { value } = await whileRenewing(lease, loader, {
        heartbeatMs: heartbeatFor(lockTtlMs)
      });
      // Returned to this caller either way. When the publish finds its token
      // gone (a set(), a del(), a lapsed lease) the value is simply not
      // cached, because by then it may be older than what replaced it.
      await lease.close(() =>
        scripts.run(
          publishScript,
          [entry, lock],
          [token, codec.encode(value), String(ttlMs)]
        )
      );
      closed = true;
      return value;
    } finally {
      // The loader threw (or the publish never reached Redis): free the lock
      // now so a waiter can take over, rather than after lockTtlMs.
      if (!closed) {
        try {
          await lease.release();
        } catch {
          // A failed release must not mask the load's outcome; the fill
          // lock's TTL frees it regardless.
        }
      }
    }
  }

  return {
    /**
     * Read `id`, running `loader` on a miss. Concurrent misses collapse to one
     * loader call; the others wait for the filled value.
     *
     * @throws CacheWaitTimeoutError if another caller's load was still running
     * after `waitTimeoutMs`.
     * @throws ValidationError if `id` would split the entry and its fill lock
     * across Cluster slots (an empty id, or one starting with `}`).
     */
    async get(id: string, loader: () => Promise<T> | T): Promise<T> {
      const keys = keysFor(id);
      const first = await read(keys[0]);
      if (first.hit) return first.value as T;

      const deadline = monotonicNow() + waitTimeoutMs;
      for (let delay = pollMs; ; ) {
        const token = globalThis.crypto.randomUUID();
        const startedAt = monotonicNow();
        const claim = await scripts.run(claimScript, keys, [
          token,
          String(lockTtlMs)
        ]);
        if (claim.state === "hit") return codec.decode(claim.value);
        if (claim.state === "claimed") {
          return loadUnder(keys, token, startedAt, loader);
        }
        // Someone else is loading, and renewing while they do. Wait for their
        // value; if they die or their loader throws, the lock frees and the
        // next poll takes it over.
        const remaining = deadline - monotonicNow();
        if (remaining <= 0)
          throw new CacheWaitTimeoutError(keys[0], waitTimeoutMs);
        await sleep(Math.min(jittered(delay), remaining));
        delay = Math.min(delay * 2, pollMs * MAX_POLL_BACKOFF);
      }
    },
    /** Read without loading. */
    async peek(id: string): Promise<T | null> {
      const result = await read(keysFor(id)[0]);
      return result.hit ? (result.value as T) : null;
    },
    /**
     * Write an entry directly (with the configured TTL). This also breaks any
     * fill in flight, so a loader that read its value before this write
     * cannot overwrite it afterwards.
     */
    async set(id: string, value: T): Promise<void> {
      await scripts.run(writeScript, keysFor(id), [
        codec.encode(value),
        String(ttlMs)
      ]);
    },
    /**
     * Drop an entry (returns the deleted count); the next `get` reloads it.
     * This also breaks any fill in flight, so a loader that read its value
     * before the invalidation cannot publish it afterwards.
     */
    async del(id: string): Promise<number> {
      return scripts.run(invalidateScript, keysFor(id), []);
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

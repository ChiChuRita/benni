import { codecs } from "../core/codecs.js";
import { ValidationError } from "../core/errors.js";
import type { Codec, RedisClient } from "../core/types.js";
import { lock } from "./lock.js";

const DEFAULT_PREFIX = "cache";
const DEFAULT_LOCK_TTL_MS = 10_000;
const DEFAULT_POLL_MS = 50;

export type CacheOptions<T> = {
  /** Entry lifetime in milliseconds. */
  readonly ttlMs: number;
  /** Key namespace; keys are `<prefix>:<id>`. Default `"cache"`. */
  readonly prefix?: string;
  /** Value codec. Default `codecs.json<T>()`. */
  readonly codec?: Codec<T>;
  /**
   * How long a single loader may hold the fill lock before another caller
   * gives up waiting and loads for itself. Default `10000`.
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
 * Works over any adapter, including `beni/upstash` on the edge (needs only
 * `GET`/`SET`/`DEL` and the lock's `SET NX`/`EVALSHA`).
 *
 * ```ts
 * const profiles = cache<Profile>(client, { ttlMs: 60_000 });
 * const profile = await profiles.get(userId, () => db.loadProfile(userId));
 * ```
 */
export function cache<T>(client: RedisClient, options: CacheOptions<T>) {
  const ttlMs = positiveInt(options.ttlMs, "ttlMs");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const codec = options.codec ?? codecs.json<T>();
  const lockTtlMs = positiveInt(
    options.lockTtlMs ?? DEFAULT_LOCK_TTL_MS,
    "lockTtlMs"
  );
  const pollMs = positiveInt(options.pollMs ?? DEFAULT_POLL_MS, "pollMs");
  // The fill lock lives next to the entries: <prefix>:lock:<id>.
  const fillLocks = lock(client, {
    prefix: `${prefix}:lock`,
    ttlMs: lockTtlMs
  });

  const key = (id: string) => `${prefix}:${id}`;

  async function read(id: string): Promise<{ hit: boolean; value?: T }> {
    const reply = await client.send(["GET", key(id)]);
    if (typeof reply !== "string") return { hit: false };
    return { hit: true, value: codec.decode(reply) };
  }

  async function write(id: string, value: T): Promise<void> {
    await client.send(["SET", key(id), codec.encode(value), "PX", ttlMs]);
  }

  async function fill(id: string, loader: () => Promise<T> | T): Promise<T> {
    const value = await loader();
    await write(id, value);
    return value;
  }

  return {
    /**
     * Read `id`, running `loader` on a miss. Concurrent misses collapse to one
     * loader call; the others wait for the filled value.
     */
    async get(id: string, loader: () => Promise<T> | T): Promise<T> {
      const first = await read(id);
      if (first.hit) return first.value as T;

      const handle = await fillLocks.acquire(id);
      if (handle) {
        try {
          // Double-check: another caller may have filled between miss and lock.
          const second = await read(id);
          if (second.hit) return second.value as T;
          return await fill(id, loader);
        } finally {
          try {
            await handle.release();
          } catch {
            // A failed release must not mask the load's outcome; the fill
            // lock's TTL frees it regardless.
          }
        }
      }

      // Someone else is loading; poll until they fill or their lock expires.
      const deadline = Date.now() + lockTtlMs;
      while (Date.now() < deadline) {
        await sleep(pollMs);
        const polled = await read(id);
        if (polled.hit) return polled.value as T;
      }
      // Fail open — the holder died; load for ourselves rather than
      // error. Worst case is a brief duplicate load, never a deadlock.
      return fill(id, loader);
    },
    /** Read without loading. */
    async peek(id: string): Promise<T | null> {
      const result = await read(id);
      return result.hit ? (result.value as T) : null;
    },
    /** Write an entry directly (with the configured TTL). */
    set: write,
    /** Drop an entry (returns the deleted count); the next `get` reloads it. */
    async del(id: string): Promise<number> {
      const reply = await client.send(["DEL", key(id)]);
      return typeof reply === "number" ? reply : 0;
    }
  };
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

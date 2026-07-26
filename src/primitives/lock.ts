import { ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import type { RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "lock";
const DEFAULT_TTL_MS = 30_000;

// Release only if we still hold the token — never DEL a lock that has expired
// and been re-acquired by someone else. Returns 1 if released, else 0.
const releaseScript = defineScript<readonly [token: string], number>({
  keyCount: 1,
  lua: 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

// Extend the TTL only if we still hold the token. Returns 1 if extended, else 0.
const extendScript = defineScript<
  readonly [token: string, ttlMs: string],
  number
>({
  keyCount: 1,
  lua: 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

export type LockOptions = {
  /** Key namespace; keys are `<prefix>:<id>`. Default `"lock"`. */
  readonly prefix?: string;
  /** Lock lifetime in milliseconds. Default `30000`. */
  readonly ttlMs?: number;
};

export type AcquireOptions = {
  /** Override the lock lifetime for this acquisition. */
  readonly ttlMs?: number;
  /** Retries if the lock is held. Default `0` (fail fast). */
  readonly retries?: number;
  /** Delay between retries in milliseconds. Default `100`. */
  readonly retryDelayMs?: number;
};

/** Thrown by `lock().run()` when the lock cannot be acquired. */
export class LockNotAcquiredError extends Error {
  readonly key: string;
  constructor(key: string) {
    super(`Could not acquire lock "${key}"`);
    this.name = "LockNotAcquiredError";
    this.key = key;
  }
}

export type LockHandle = {
  readonly key: string;
  readonly token: string;
  /** Release the lock; resolves `true` only if we still held it. */
  release(): Promise<boolean>;
  /** Extend the lock's TTL; resolves `true` only if we still held it. */
  extend(ttlMs?: number): Promise<boolean>;
};

/**
 * A distributed lock over Redis: `SET key token NX PX ttl` to acquire, and an
 * atomic check-and-delete Lua to release, so a caller can never release a lock
 * that expired and was re-acquired elsewhere. Works over any adapter, including
 * `beni/upstash` on the edge.
 *
 * ```ts
 * const locks = lock(client, { ttlMs: 10_000 });
 * await locks.run("order:42", async () => {
 *   // critical section — the lock is released automatically
 * });
 * ```
 */
export function lock(client: RedisClient, options?: LockOptions) {
  const prefix = options?.prefix ?? DEFAULT_PREFIX;
  const defaultTtlMs = options?.ttlMs ?? DEFAULT_TTL_MS;
  const scripts = createScriptRunner(client);

  function handleFor(key: string, token: string): LockHandle {
    return {
      key,
      token,
      async release() {
        return (await scripts.run(releaseScript, [key], [token])) === 1;
      },
      async extend(ttlMs = defaultTtlMs) {
        const args = [token, String(positiveMs(ttlMs, "ttlMs"))] as const;
        return (await scripts.run(extendScript, [key], args)) === 1;
      }
    };
  }

  async function acquire(
    id: string,
    acquireOptions?: AcquireOptions
  ): Promise<LockHandle | null> {
    const ttlMs = positiveMs(acquireOptions?.ttlMs ?? defaultTtlMs, "ttlMs");
    const retries = acquireOptions?.retries ?? 0;
    const retryDelayMs = acquireOptions?.retryDelayMs ?? 100;
    const key = `${prefix}:${id}`;
    for (let attempt = 0; ; attempt++) {
      const token = globalThis.crypto.randomUUID();
      const reply = await client.send(["SET", key, token, "NX", "PX", ttlMs]);
      if (reply !== null) return handleFor(key, token);
      if (attempt >= retries) return null;
      await sleep(retryDelayMs);
    }
  }

  return {
    acquire,
    /**
     * Acquire, run `fn`, and release — even if `fn` throws. Throws if the lock
     * can't be acquired (after any configured retries).
     */
    async run<T>(
      id: string,
      fn: (handle: LockHandle) => Promise<T> | T,
      acquireOptions?: AcquireOptions
    ): Promise<T> {
      const handle = await acquire(id, acquireOptions);
      if (handle === null) {
        throw new LockNotAcquiredError(`${prefix}:${id}`);
      }
      try {
        return await fn(handle);
      } finally {
        try {
          await handle.release();
        } catch {
          // A failed release must not mask fn's outcome (or replace its
          // error); the lock's TTL is the backstop and frees it regardless.
        }
      }
    }
  };
}

function positiveMs(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `lock ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

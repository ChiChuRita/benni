import { ReplyShapeError, ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import type { RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "ratelimit";

/**
 * Sliding-window log in a single sorted set (one key, so it is Redis Cluster
 * safe). Drop entries older than the window, count what remains, and admit +
 * record the request if under the limit. Decodes to the internal
 * `{ allowed, remaining, reset }` tuple that `check()` maps onto the public
 * `RatelimitResult` (`{ success, limit, remaining, resetMs }`).
 */
const slidingWindowScript = defineScript<
  readonly [nowMs: string, windowMs: string, limit: string, member: string],
  { allowed: number; remaining: number; reset: number }
>({
  keyCount: 1,
  lua: `
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, now - window)
local count = redis.call("ZCARD", KEYS[1])
if count < limit then
  redis.call("ZADD", KEYS[1], now, ARGV[4])
  redis.call("PEXPIRE", KEYS[1], window)
  return {1, limit - count - 1, now + window}
end
local oldest = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
local reset = now + window
if oldest[2] then reset = tonumber(oldest[2]) + window end
return {0, 0, reset}
`,
  decode: (reply) => {
    if (!Array.isArray(reply)) {
      throw new ReplyShapeError(
        "Expected ratelimit script to return an array",
        reply
      );
    }
    return {
      allowed: Number(reply[0]),
      remaining: Number(reply[1]),
      reset: Number(reply[2])
    };
  }
});

export type RatelimitOptions = {
  /** Maximum requests allowed within the window. */
  readonly limit: number;
  /** Window length in milliseconds. */
  readonly windowMs: number;
  /** Key namespace; keys are `<prefix>:<id>`. Default `"ratelimit"`. */
  readonly prefix?: string;
};

export type RatelimitResult = {
  /** Whether this request is allowed. */
  readonly success: boolean;
  /** The configured limit. */
  readonly limit: number;
  /** Requests left in the current window (0 when denied). */
  readonly remaining: number;
  /** Epoch-ms when the window next frees a slot. */
  readonly resetMs: number;
};

/**
 * A sliding-window rate limiter. Each `check(id)` is one atomic Lua round trip.
 * Works over any adapter, including `beni/upstash` on the edge.
 *
 * ```ts
 * const limiter = ratelimit(client, { limit: 10, windowMs: 60_000 });
 * const { success, remaining } = await limiter.check(userId);
 * if (!success) throw new Response("Too Many Requests", { status: 429 });
 * ```
 *
 * The window is a log of request timestamps in one sorted set — exact, and
 * bounded by `limit` entries per key. For very high per-key rates prefer a
 * counter-based limiter.
 */
export function ratelimit(client: RedisClient, options: RatelimitOptions) {
  const limit = positiveInt(options.limit, "limit");
  const windowMs = positiveInt(options.windowMs, "windowMs");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const scripts = createScriptRunner(client);

  return {
    async check(id: string): Promise<RatelimitResult> {
      const now = Date.now();
      const key = `${prefix}:${id}`;
      const member = `${now}-${globalThis.crypto.randomUUID()}`;
      const result = await scripts.run(
        slidingWindowScript,
        [key],
        [String(now), String(windowMs), String(limit), member]
      );
      return {
        success: result.allowed === 1,
        limit,
        remaining: result.remaining,
        resetMs: result.reset
      };
    }
  };
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `ratelimit ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

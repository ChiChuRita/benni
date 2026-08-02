import { type ClientSource, clientArgs } from "../core/client-source.js";
import { ReplyShapeError, ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import { type StoreBinding, withStore } from "../core/store.js";
import type { RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "ratelimit";

/**
 * Sliding-window log in a single sorted set (one key, so it is Redis Cluster
 * safe). Drop entries older than the window, count what remains, and admit +
 * record the request if under the limit. Decodes to the internal
 * `{ allowed, remaining, reset, retryAfter }` tuple that `check()` maps onto
 * the public `RatelimitResult`.
 */
const slidingWindowScript = defineScript<
  readonly [windowMs: string, limit: string, member: string],
  { allowed: number; remaining: number; reset: number; retryAfter: number }
>({
  keyCount: 1,
  lua: `
-- Server time, not the caller's. Two app servers with skewed clocks would
-- otherwise disagree about where the window starts, and the limit would be
-- enforced differently depending on which one answered the request.
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, now - window)
local count = redis.call("ZCARD", KEYS[1])
if count < limit then
  redis.call("ZADD", KEYS[1], now, ARGV[3])
  redis.call("PEXPIRE", KEYS[1], window)
  -- reset is when a slot next frees, which in a sliding window is the oldest
  -- entry ageing out -- not a full window from now. Reporting now + window on
  -- the allowed path put X-RateLimit-Reset up to a whole window late, so a
  -- client that waited for it waited longer than it had to. After the ZADD the
  -- set is never empty; if we are the only entry the two agree anyway.
  local first = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
  local nextFree = now + window
  if first[2] then nextFree = tonumber(first[2]) + window end
  return {1, limit - count - 1, nextFree, 0}
end
local oldest = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
local reset = now + window
if oldest[2] then reset = tonumber(oldest[2]) + window end
-- retryAfter is derived here, from the same clock as reset, so the caller
-- never has to difference a server timestamp against its own clock.
local retryAfter = reset - now
if retryAfter < 0 then retryAfter = 0 end
return {0, 0, reset, retryAfter}
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
      reset: Number(reply[2]),
      retryAfter: Number(reply[3])
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
  /**
   * Server epoch-ms when the window next frees a slot. Compare it against your
   * own clock only if you know the two agree; prefer {@link retryAfterMs}.
   */
  readonly resetMs: number;
  /**
   * How long to wait before retrying, in milliseconds (`0` when allowed).
   *
   * Derived server-side from the same clock as `resetMs`, so it is immune to
   * skew between your process and Redis. This is what the `Retry-After` header
   * wants.
   */
  readonly retryAfterMs: number;
};

/**
 * A sliding-window rate limiter. Each `check(id)` is one atomic Lua round trip.
 * Works over any adapter, including `benni/upstash` on the edge.
 *
 * ```ts
 * const limiter = ratelimit({ client, limit: 10, windowMs: 60_000 });
 * const { success, remaining } = await limiter.check(userId);
 * if (!success) throw new Response("Too Many Requests", { status: 429 });
 * ```
 *
 * The window is a log of request timestamps in one sorted set — exact, and
 * bounded by `limit` entries per key. For very high per-key rates prefer a
 * counter-based limiter.
 */
function createRatelimit(client: RedisClient, options: RatelimitOptions) {
  const limit = positiveInt(options.limit, "limit");
  const windowMs = positiveInt(options.windowMs, "windowMs");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const scripts = createScriptRunner(client);

  return {
    async check(id: string): Promise<RatelimitResult> {
      const key = `${prefix}:${id}`;
      // Uniqueness only: the timestamp comes from the server inside the script.
      const member = globalThis.crypto.randomUUID();
      const result = await scripts.run(
        slidingWindowScript,
        [key],
        [String(windowMs), String(limit), member]
      );
      return {
        success: result.allowed === 1,
        limit,
        remaining: result.remaining,
        resetMs: result.reset,
        retryAfterMs: result.retryAfter
      };
    }
  };
}

/** The limiter {@link ratelimit} returns. */
export type RatelimitStore = ReturnType<typeof createRatelimit>;

/** {@link RatelimitOptions} plus the client, for the single-argument form. */
export type RatelimitConfig = RatelimitOptions & {
  /** The client, a promise of one, a factory, or a benni handle. */
  readonly client: ClientSource;
};

export function ratelimit(config: RatelimitConfig): RatelimitStore;
export function ratelimit(
  client: ClientSource,
  options: RatelimitOptions
): RatelimitStore;
export function ratelimit(
  source: ClientSource | RatelimitConfig,
  options?: RatelimitOptions
): RatelimitStore {
  const args = clientArgs<RatelimitOptions>(source, options);
  return createRatelimit(args.client, args.options);
}

/**
 * A limiter declared as a schema value, so it lands in `redis.query` next to
 * the data stores and needs no client of its own.
 * @example
 * ```ts
 * // schema.ts
 * export const apiLimit = ratelimit("api", { limit: 10, windowMs: 60_000 });
 * // app.ts
 * const { success } = await redis.query.apiLimit.check(userId);
 * ```
 */
export type RatelimitSchema = RatelimitOptions & {
  readonly kind: "ratelimit";
  readonly prefix: string;
};

const ratelimitBinding: StoreBinding = {
  resource: (ctx, schema: RatelimitSchema) =>
    createRatelimit(ctx.client, schema)
};

/**
 * Build a {@link RatelimitSchema}. Exported as `ratelimit` from `benni/schema`.
 */
export function defineRatelimit(
  prefix: string,
  options: RatelimitOptions
): RatelimitSchema {
  return withStore(
    { ...options, kind: "ratelimit", prefix } as RatelimitSchema,
    ratelimitBinding
  );
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `ratelimit ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

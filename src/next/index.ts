import type { RedisClient, RedisCommand } from "../core/index.js";
import { type RatelimitResult, ratelimit } from "../primitives/index.js";

export type { RatelimitResult } from "../primitives/index.js";

const DEFAULT_CACHE_PREFIX = "next-cache";
const DEFAULT_RATELIMIT_PREFIX = "next-ratelimit";

/**
 * A connected {@link RedisClient}, a promise of one, or a lazy factory. A
 * factory is called (and awaited) once on first use and the client is cached —
 * handy in `cache-handler.mjs`, which Next.js loads at build time when no
 * Redis connection should be opened yet.
 */
export type RedisClientSource =
  | RedisClient
  | Promise<RedisClient>
  | (() => RedisClient | Promise<RedisClient>);

/**
 * The stored shape of one cache entry, mirroring what Next.js hands to
 * `set()` and expects back from `get()`: the payload, its write timestamp,
 * and the tags it was written under.
 */
export type NextCacheEntry = {
  /** The Next.js cache payload (PAGE, ROUTE, and FETCH kinds), stored as-is. */
  readonly value: unknown;
  /** Epoch-ms timestamp of the write; Next.js uses it for staleness checks. */
  readonly lastModified: number;
  /** The tags the entry was written under (from `ctx.tags`). */
  readonly tags: readonly string[];
};

/**
 * The subset of the `set()` context the handler reads. Next.js passes more
 * properties; they are ignored.
 */
export type NextCacheHandlerContext = {
  /** Cache tags for the entry (explicit and implicit). */
  readonly tags?: readonly string[];
  /** Revalidate period in seconds, or `false` for "cache forever". */
  readonly revalidate?: number | false;
};

/**
 * A minimal structural interface for a Next.js custom cache handler. Matches
 * the shape Next.js 14.1+ expects (`ctx.tags: string[]`,
 * `ctx.revalidate?: number | false`, entries as
 * `{ value, lastModified, tags }`); `resetRequestCache()` is called by
 * Next.js 15 and is a harmless no-op on 14. Deliberately not imported from
 * `"next"` so `beni/next` has zero dependencies.
 */
export interface NextCacheHandler {
  get(key: string): Promise<NextCacheEntry | null>;
  set(key: string, data: unknown, ctx?: NextCacheHandlerContext): Promise<void>;
  revalidateTag(tag: string | readonly string[]): Promise<void>;
  resetRequestCache(): void;
}

/** Options for {@link cacheHandler}. */
export type CacheHandlerOptions = {
  /** A connected {@link RedisClient}, or a lazy factory (awaited once, cached). */
  readonly client: RedisClientSource;
  /** Key namespace. Default `"next-cache"`. */
  readonly prefix?: string;
  /**
   * Extra safety cap on entry TTL in seconds, applied when `ctx.revalidate`
   * is absent or `false`. Without it those entries live forever.
   */
  readonly defaultTtlSeconds?: number;
};

/**
 * A Next.js ISR/App-Router cache handler backed by Redis, so cached pages,
 * route handlers, and `fetch` data survive deploys and are shared across
 * every instance of the app.
 *
 * Returns a **class** (Next.js instantiates the default export of the
 * cache-handler module) closed over the options. Entries live at
 * `<prefix>:entry:<key>` as JSON; each tag keeps a set of its keys at
 * `<prefix>:tag:<tag>`, so `revalidateTag` is a set lookup plus one `DEL`.
 * Entries with a numeric `revalidate` get that TTL via `SET ... EX`.
 *
 * Only `send`/`pipeline` are used, so it works over every adapter —
 * including [`beni/upstash`](../upstash/index.js). PAGE, ROUTE, and FETCH
 * payloads all round-trip as JSON (Next.js base64-encodes route bodies
 * itself). Reads fail open: an entry that does not decode is a miss, never an
 * error. Only `ctx.tags` feeds the tag index — tags carried solely in
 * response headers of externally-revalidated payloads are not indexed.
 *
 * @example
 * ```ts
 * // cache-handler.mjs
 * import { cacheHandler } from "beni/next";
 * import { upstash } from "beni/upstash";
 *
 * export default cacheHandler({
 *   client: () => upstash({
 *     url: process.env.UPSTASH_URL,
 *     token: process.env.UPSTASH_TOKEN
 *   })
 * });
 *
 * // next.config.ts
 * const nextConfig = {
 *   cacheHandler: require.resolve("./cache-handler.mjs"),
 *   cacheMaxMemorySize: 0 // disable the in-memory cache
 * };
 * ```
 */
export function cacheHandler(
  options: CacheHandlerOptions
): new () => NextCacheHandler {
  const prefix = options.prefix ?? DEFAULT_CACHE_PREFIX;
  const defaultTtlSeconds = options.defaultTtlSeconds;
  const getClient = createClientResolver(options.client);

  const entryKey = (key: string) => `${prefix}:entry:${key}`;
  const tagKey = (tag: string) => `${prefix}:tag:${tag}`;

  const ttlSecondsFor = (revalidate: number | false | undefined) => {
    if (typeof revalidate === "number" && revalidate > 0) {
      return Math.ceil(revalidate);
    }
    return defaultTtlSeconds;
  };

  return class BeniCacheHandler implements NextCacheHandler {
    async get(key: string): Promise<NextCacheEntry | null> {
      const client = await getClient();
      const reply = await client.send(["GET", entryKey(key)]);
      if (typeof reply !== "string") return null;
      // A cache must fail open: anything that does not decode is a miss.
      try {
        const parsed: unknown = JSON.parse(reply);
        if (typeof parsed !== "object" || parsed === null) return null;
        return parsed as NextCacheEntry;
      } catch {
        return null;
      }
    }

    async set(
      key: string,
      data: unknown,
      ctx?: NextCacheHandlerContext
    ): Promise<void> {
      const tags = [...(ctx?.tags ?? [])];
      const entry: NextCacheEntry = {
        value: data,
        lastModified: Date.now(),
        tags
      };
      let payload: string;
      try {
        payload = JSON.stringify(entry);
      } catch {
        // Unserializable payload (circular, BigInt, ...): skip caching.
        return;
      }
      const ttl = ttlSecondsFor(ctx?.revalidate);
      const commands: RedisCommand[] = [
        ttl === undefined
          ? ["SET", entryKey(key), payload]
          : ["SET", entryKey(key), payload, "EX", ttl]
      ];
      for (const tag of tags) {
        commands.push(["SADD", tagKey(tag), key]);
      }
      const client = await getClient();
      await client.pipeline(commands);
    }

    async revalidateTag(tag: string | readonly string[]): Promise<void> {
      const tags = typeof tag === "string" ? [tag] : [...tag];
      if (tags.length === 0) return;
      const client = await getClient();
      const tagKeys = tags.map(tagKey);
      const memberReplies = await client.pipeline(
        tagKeys.map((key): RedisCommand => ["SMEMBERS", key])
      );
      const doomed: string[] = [];
      for (const reply of memberReplies) {
        for (const member of iterateMembers(reply)) {
          doomed.push(entryKey(member));
        }
      }
      await client.send(["DEL", ...doomed, ...tagKeys]);
    }

    resetRequestCache(): void {
      // Next.js 15 resets its per-request in-memory cache here; this handler
      // keeps no request-local state, so there is nothing to reset.
    }
  };
}

function iterateMembers(reply: unknown): string[] {
  if (Array.isArray(reply) || reply instanceof Set) {
    return [...reply].filter((member) => typeof member === "string");
  }
  return [];
}

/** Options for {@link rateLimit}. */
export type NextRateLimitOptions = {
  /** A connected {@link RedisClient}, or a lazy factory (awaited once, cached). */
  readonly client: RedisClientSource;
  /** Maximum requests allowed within the window. */
  readonly limit: number;
  /** Window length in milliseconds. */
  readonly windowMs: number;
  /** Key namespace; keys are `<prefix>:<identity>`. Default `"next-ratelimit"`. */
  readonly prefix?: string;
  /**
   * Extract the identity to limit on from the `Request`. Default: the first
   * hop of `x-forwarded-for`, else `"anonymous"`.
   */
  readonly identify?: (request: Request) => string | Promise<string>;
};

/**
 * The function {@link rateLimit} returns: call it with a `Request` to gate
 * middleware and route handlers, or call `.check(identity)` directly where
 * there is no `Request` (Server Actions).
 */
export type NextRateLimitHandler = ((
  request: Request
) => Promise<Response | null>) & {
  /** Run the limiter for an explicit identity (e.g. a user id). */
  check(identity: string): Promise<RatelimitResult>;
};

/**
 * A sliding-window rate limiter for Next.js middleware, route handlers, and
 * Server Actions, built on the [`ratelimit`](../primitives/ratelimit.js)
 * primitive (one atomic Lua round trip per check).
 *
 * The returned function takes a web-standard `Request` and resolves `null`
 * when the request is allowed, or a ready-to-return `429 Response` with
 * `Retry-After` (seconds) and `X-RateLimit-Limit` / `X-RateLimit-Remaining` /
 * `X-RateLimit-Reset` (epoch seconds) headers when it is not. Only
 * web-standard APIs are used, so it runs in Edge middleware and Node route
 * handlers alike — pair it with `beni/upstash` on the edge.
 *
 * @example
 * ```ts
 * // middleware.ts
 * import { rateLimit } from "beni/next";
 * import { upstash } from "beni/upstash";
 *
 * const limiter = rateLimit({
 *   client: () => upstash({
 *     url: process.env.UPSTASH_URL,
 *     token: process.env.UPSTASH_TOKEN
 *   }),
 *   limit: 20,
 *   windowMs: 10_000
 * });
 *
 * export async function middleware(request: Request) {
 *   const denied = await limiter(request);
 *   if (denied) return denied;
 * }
 *
 * export const config = { matcher: "/api/:path*" };
 * ```
 *
 * @example
 * ```ts
 * // A Server Action has no Request; limit on the user id instead.
 * "use server";
 *
 * export async function submitComment(formData: FormData) {
 *   const { success, resetMs } = await limiter.check(await getUserId());
 *   if (!success) {
 *     return { error: "Too many comments, try again shortly.", resetMs };
 *   }
 *   // ...
 * }
 * ```
 */
export function rateLimit(options: NextRateLimitOptions): NextRateLimitHandler {
  const prefix = options.prefix ?? DEFAULT_RATELIMIT_PREFIX;
  const identify = options.identify ?? identifyByForwardedFor;
  const getClient = createClientResolver(options.client);

  let limiter: Promise<ReturnType<typeof ratelimit>> | undefined;
  const getLimiter = () => {
    if (!limiter) {
      limiter = getClient().then((client) =>
        ratelimit(client, {
          limit: options.limit,
          windowMs: options.windowMs,
          prefix
        })
      );
      limiter.catch(() => {
        limiter = undefined;
      });
    }
    return limiter;
  };

  const check = async (identity: string): Promise<RatelimitResult> => {
    const instance = await getLimiter();
    return instance.check(identity);
  };

  const handler = async (request: Request): Promise<Response | null> => {
    const result = await check(await identify(request));
    if (result.success) return null;
    const retryAfterSeconds = Math.max(
      0,
      Math.ceil((result.resetMs - Date.now()) / 1000)
    );
    return new Response("Too Many Requests", {
      status: 429,
      headers: {
        "Retry-After": String(retryAfterSeconds),
        "X-RateLimit-Limit": String(result.limit),
        "X-RateLimit-Remaining": String(result.remaining),
        "X-RateLimit-Reset": String(Math.ceil(result.resetMs / 1000))
      }
    });
  };

  return Object.assign(handler, { check });
}

function identifyByForwardedFor(request: Request): string {
  const forwarded = request.headers.get("x-forwarded-for");
  const firstHop = forwarded?.split(",")[0]?.trim();
  return firstHop || "anonymous";
}

function createClientResolver(
  source: RedisClientSource
): () => Promise<RedisClient> {
  let cached: Promise<RedisClient> | undefined;
  return () => {
    if (!cached) {
      cached = Promise.resolve(
        typeof source === "function" ? source() : source
      );
      // Do not cache a failed connection attempt; the next call retries.
      cached.catch(() => {
        cached = undefined;
      });
    }
    return cached;
  };
}

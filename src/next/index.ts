import { type ClientSource, resolveClient } from "../core/client-source.js";
import type { RedisClient, RedisCommand } from "../core/index.js";
// Type-only: the limiter arrives already built, from the handle, so this
// module never pulls the rate-limit primitive in itself.
import type {
  RatelimitResult,
  RatelimitStore
} from "../primitives/ratelimit.js";

export type { RatelimitResult } from "../primitives/ratelimit.js";

const DEFAULT_CACHE_PREFIX = "next-cache";
/** Keys per DEL in revalidateTag, so one popular tag cannot block the server. */
const DEL_CHUNK = 500;
/**
 * The header Next.js records a page's or route handler's tags in, implicit
 * path tags (`_N_T_/blog`) included. Their `set()` context carries no tags.
 */
const CACHE_TAGS_HEADER = "x-next-cache-tags";
/**
 * How long a tag's revalidation is remembered: Next.js's
 * `CACHE_ONE_YEAR_SECONDS`, the longest `revalidate` it ever writes on a
 * fetch entry (`force-cache` and `revalidate: false` both become one year).
 * Fetch entries are capped to it too, so a marker always outlives every entry
 * written before it.
 */
const MARKER_TTL_SECONDS = 31_536_000;
/**
 * Bumped whenever the stored layout changes, so `get` treats an entry written
 * by an older layout as a miss instead of handing Next.js something it cannot
 * render. 0.1.0 stored plain JSON (no `v`), which turned every Buffer into
 * `{ type, data }` and every Map into `{}`.
 */
const FORMAT_VERSION = 2;

/**
 * Adds one key to a tag set and gives the set the expiry it should have, in a
 * single atomic step. `ARGV[2]` is the entry's TTL in seconds, or 0 when the
 * entry never expires.
 *
 * A tag set has to outlive its longest-lived member, so the expiry may be
 * extended but never shortened, and an entry that never expires has to leave
 * the set permanent. `EXPIRE ... GT` alone cannot do that: Redis compares
 * against a missing expiry as if it were infinite, so GT refuses to install
 * the first TTL on the set the SADD just created, and the sets grew for the
 * life of the deployment. Bootstrapping with `NX` instead would put an expiry
 * back on a set that a permanent entry deliberately PERSISTed. Only next to
 * the SADD can the two TTL-less cases be told apart, hence the script: a set
 * this call brings into existence gets the TTL outright, one that was already
 * there only ever has it extended.
 */
const TAG_MEMBER_LUA = `local ttl = tonumber(ARGV[2])
local fresh = redis.call("EXISTS", KEYS[1]) == 0
redis.call("SADD", KEYS[1], ARGV[1])
if ttl <= 0 then
  redis.call("PERSIST", KEYS[1])
elseif fresh then
  redis.call("EXPIRE", KEYS[1], ttl)
else
  redis.call("EXPIRE", KEYS[1], ttl, "GT")
end
return 1`;

/**
 * A {@link RedisClient} from a benni adapter, or the handle `benni()`
 * returned. Adapters connect on first use, so `cache-handler.mjs`, which
 * Next.js loads at build time, opens no connection just by being imported.
 */
export type RedisClientSource = ClientSource;

/**
 * The shape `get()` resolves to and Next.js reads back: the payload exactly
 * as `set()` received it (Buffers and Maps included), its write timestamp,
 * and the tags it was indexed under.
 */
export type NextCacheEntry = {
  /** The Next.js cache value (APP_PAGE, APP_ROUTE, PAGES, FETCH, ...). */
  readonly value: unknown;
  /** Epoch-ms timestamp of the write; Next.js uses it for staleness checks. */
  readonly lastModified: number;
  /** The tags the entry was indexed under. */
  readonly tags: readonly string[];
};

/**
 * The subset of the `set()` context the handler reads. Next.js passes more
 * properties; they are ignored.
 */
export type NextCacheHandlerContext = {
  /** Fetch entries: the tags given to `fetch(url, { next: { tags } })`. */
  readonly tags?: readonly string[];
  /** Next.js 15.3+, pages and route handlers: seconds, `false` = never. */
  readonly cacheControl?: {
    readonly revalidate: number | false;
    readonly expire?: number | undefined;
  };
  /** Next.js 15.0-15.2, pages and route handlers: seconds, `false` = never. */
  readonly revalidate?: number | false;
  /** `true` for a fetch entry. */
  readonly fetchCache?: boolean;
};

/**
 * The subset of the `get()` context the handler reads: for a fetch entry,
 * its own tags and the implicit tags of the route doing the fetching.
 */
export type NextCacheGetContext = {
  readonly kind?: string;
  readonly tags?: readonly string[];
  readonly softTags?: readonly string[];
};

/**
 * A minimal structural interface for a Next.js custom cache handler, matching
 * the `cacheHandler` contract of Next.js 15 and 16. Deliberately not imported
 * from `"next"` so `benni/next` has zero dependencies.
 */
export interface NextCacheHandler {
  get(key: string, ctx?: NextCacheGetContext): Promise<NextCacheEntry | null>;
  set(key: string, data: unknown, ctx?: NextCacheHandlerContext): Promise<void>;
  revalidateTag(
    tag: string | readonly string[],
    durations?: { readonly expire?: number }
  ): Promise<void>;
  resetRequestCache(): void;
}

/** Options for {@link cacheHandler}. */
export type CacheHandlerOptions = {
  /** A {@link RedisClient} from a benni adapter, or a benni handle. */
  readonly client: RedisClientSource;
  /** Key namespace. Default `"next-cache"`. */
  readonly prefix?: string;
  /**
   * TTL in seconds for an entry Next.js gives no lifetime (`revalidate:
   * false` with no `expire`). Without it those entries live until evicted
   * or revalidated.
   */
  readonly defaultTtlSeconds?: number;
};

/**
 * A Next.js ISR/App-Router cache handler backed by Redis, so cached pages,
 * route handlers, and `fetch` data survive deploys and are shared across
 * every instance of the app. Supports Next.js 15 and 16.
 *
 * Returns a **class** (Next.js instantiates the default export of the
 * cache-handler module) closed over the options. Entries live at
 * `<prefix>:entry:<key>`; each tag keeps a set of its keys at
 * `<prefix>:tag:<tag>`, so `revalidateTag` (and `revalidatePath`, which is a
 * tag underneath) is a set lookup plus a `DEL`.
 *
 * - **Values round-trip exactly.** Buffers (`rscData`, route `body`), Maps
 *   (`segmentData`), and everything nested are stored in a tagged JSON
 *   encoding, and come back as Buffers and Maps.
 * - **TTL follows Next.js.** A page or route handler expires at its
 *   `cacheControl.expire` (the point where Next.js would stop serving it
 *   stale), falling back to `revalidate` on Next.js 15, which does not pass
 *   `expire`; a fetch entry expires at its `revalidate`.
 * - **Tags come from where Next.js puts them:** the fetch's `ctx.tags`, and
 *   the `x-next-cache-tags` header of a page or route value.
 * - **Fetch entries honour implicit tags.** `revalidateTag` also records when
 *   each tag was revalidated, and a fetch `get` checks its own and its
 *   route's tags against that in the same round trip, so `revalidatePath`
 *   reaches the fetches a page made, on every instance.
 *
 * Only `send`/`pipeline` are used, so it works over every adapter —
 * including [`benni/upstash`](../upstash/index.js). Reads fail open: an entry
 * that does not decode is a miss, never an error.
 *
 * @example
 * ```ts
 * // cache-handler.mjs
 * import { cacheHandler } from "benni/next";
 * import { upstash } from "benni/upstash";
 *
 * export default cacheHandler({
 *   client: upstash({
 *     url: process.env.UPSTASH_URL,
 *     token: process.env.UPSTASH_TOKEN
 *   })
 * });
 *
 * // next.config.mjs
 * const nextConfig = {
 *   cacheHandler: fileURLToPath(new URL("./cache-handler.mjs", import.meta.url)),
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

  // One hash tag over every key keeps the whole cache in a single Cluster
  // slot, which is what lets revalidateTag DEL entries and tag sets together.
  // Without it that DEL is CROSSSLOT and the handler is simply broken on a
  // cluster; on a single node the layout behaves identically.
  const base = `{${prefix}}`;
  const entryKey = (key: string) => `${base}:entry:${key}`;
  const tagKey = (tag: string) => `${base}:tag:${tag}`;
  const markerKey = (tag: string) => `${base}:revalidated:${tag}`;

  const ttlSecondsFor = (
    data: unknown,
    ctx: NextCacheHandlerContext | undefined
  ): number | undefined => {
    if (isFetchValue(data) || ctx?.fetchCache === true) {
      const revalidate = isFetchValue(data) ? data.revalidate : undefined;
      return typeof revalidate === "number" && revalidate > 0
        ? Math.min(Math.ceil(revalidate), MARKER_TTL_SECONDS)
        : MARKER_TTL_SECONDS;
    }
    // Past `expire` Next.js re-renders before responding; before it, a stale
    // entry is still served while it regenerates in the background. So the
    // entry has to live until `expire`, not `revalidate`, or every
    // revalidation turns into a blocking render. Next.js 15 keeps `expire`
    // in its prerender manifest and never passes it, so there the entry can
    // only go at `revalidate`: the first request after that renders fresh.
    const expire = ctx?.cacheControl?.expire;
    if (typeof expire === "number" && expire > 0) return Math.ceil(expire);
    const revalidate = ctx?.cacheControl?.revalidate ?? ctx?.revalidate;
    if (typeof revalidate === "number" && revalidate > 0) {
      return Math.ceil(revalidate);
    }
    return defaultTtlSeconds;
  };

  return class BenniCacheHandler implements NextCacheHandler {
    async get(
      key: string,
      ctx?: NextCacheGetContext
    ): Promise<NextCacheEntry | null> {
      const tags = unique([...(ctx?.tags ?? []), ...(ctx?.softTags ?? [])]);
      const client = await getClient();
      const [reply, markers] =
        tags.length === 0
          ? [await client.send(["GET", entryKey(key)]), undefined]
          : await client.pipeline([
              ["GET", entryKey(key)],
              ["MGET", ...tags.map(markerKey)]
            ]);
      const entry = decodeEntry(reply);
      if (!entry) return null;
      // A tag revalidated at or after this write means the data predates it.
      // Only fetch lookups carry tags here: page and route entries are found
      // and deleted through their tag sets instead.
      if (Array.isArray(markers)) {
        for (const marker of markers) {
          if (marker != null && Number(marker) >= entry.lastModified) {
            return null;
          }
        }
      }
      return entry;
    }

    async set(
      key: string,
      data: unknown,
      ctx?: NextCacheHandlerContext
    ): Promise<void> {
      const tags = unique([...(ctx?.tags ?? []), ...headerTags(data)]);
      const entry: NextCacheEntry & { readonly v: number } = {
        v: FORMAT_VERSION,
        value: data,
        lastModified: Date.now(),
        tags
      };
      let payload: string;
      try {
        payload = JSON.stringify(pack(entry));
      } catch {
        // Unserializable payload (circular, BigInt, ...): skip caching.
        return;
      }
      const ttl = ttlSecondsFor(data, ctx);
      const commands: RedisCommand[] = [
        ttl === undefined
          ? ["SET", entryKey(key), payload]
          : ["SET", entryKey(key), payload, "EX", ttl]
      ];
      for (const tag of tags) {
        // Sent as EVAL rather than run through the script runner so the whole
        // write stays one pipeline, and so this module still speaks nothing
        // but send/pipeline. See TAG_MEMBER_LUA for what it decides.
        commands.push(["EVAL", TAG_MEMBER_LUA, 1, tagKey(tag), key, ttl ?? 0]);
      }
      const client = await getClient();
      await client.pipeline(commands);
    }

    /**
     * Deletes every entry indexed under the tags. Next.js 16 also passes
     * `durations` for a stale-while-revalidate profile (`revalidateTag(tag,
     * "max")`); this handler expires immediately either way, so the next
     * request renders fresh rather than being served stale once.
     */
    async revalidateTag(tag: string | readonly string[]): Promise<void> {
      const tags = typeof tag === "string" ? [tag] : [...tag];
      if (tags.length === 0) return;
      const client = await getClient();
      const tagKeys = tags.map(tagKey);
      const now = Date.now();
      // TODO: the markers compare this clock with the writer's `lastModified`,
      // so instances skewed by more than the gap between a write and a
      // revalidation can keep that write alive; stamp both from Redis TIME
      // inside scripts if deployments turn out to need it.
      const replies = await client.pipeline([
        ...tagKeys.map((key): RedisCommand => ["SMEMBERS", key]),
        ...tags.map(
          (name): RedisCommand => [
            "SET",
            markerKey(name),
            now,
            "EX",
            MARKER_TTL_SECONDS
          ]
        )
      ]);
      const perTag = tagKeys.map((key, index) => ({
        key,
        members: iterateMembers(replies[index])
      }));
      const doomed = perTag.flatMap(({ members }) => members.map(entryKey));
      // Entries first, then the tag memberships, as one pipeline. A popular
      // tag can name tens of thousands of entries, and one DEL over all of
      // them is a multi-megabyte command that blocks the server, so chunk it.
      // The order matters if we die partway: a tag pointing at already-deleted
      // entries is harmless and self-healing, while entries whose tag is gone
      // can never be revalidated.
      const commands: RedisCommand[] = [];
      for (let index = 0; index < doomed.length; index += DEL_CHUNK) {
        commands.push(["DEL", ...doomed.slice(index, index + DEL_CHUNK)]);
      }
      // SREM exactly what SMEMBERS returned, rather than DEL-ing the set. A
      // concurrent set() can SADD to this tag between the read above and here,
      // and DEL would drop that membership while the new entry itself survives
      // — the "can never be revalidated" case the paragraph above is about.
      // Redis drops a set once its last member goes, so this still cleans up.
      for (const { key, members } of perTag) {
        for (let index = 0; index < members.length; index += DEL_CHUNK) {
          commands.push([
            "SREM",
            key,
            ...members.slice(index, index + DEL_CHUNK)
          ]);
        }
      }
      if (commands.length > 0) await client.pipeline(commands);
    }

    resetRequestCache(): void {
      // Next.js resets its per-request in-memory cache here; this handler
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

function unique(tags: readonly string[]): string[] {
  return [...new Set(tags)].filter((tag) => tag.length > 0);
}

function isFetchValue(data: unknown): data is { revalidate?: unknown } {
  return (
    typeof data === "object" &&
    data !== null &&
    (data as { kind?: unknown }).kind === "FETCH"
  );
}

/** The tags Next.js recorded on a page or route value, implicit ones included. */
function headerTags(data: unknown): string[] {
  if (typeof data !== "object" || data === null) return [];
  const headers = (data as { headers?: unknown }).headers;
  if (typeof headers !== "object" || headers === null) return [];
  const value = (headers as Record<string, unknown>)[CACHE_TAGS_HEADER];
  return typeof value === "string" ? value.split(",") : [];
}

function decodeEntry(reply: unknown): NextCacheEntry | null {
  if (typeof reply !== "string") return null;
  // A cache must fail open: anything that does not decode is a miss.
  try {
    const parsed = unpack(JSON.parse(reply)) as Partial<
      NextCacheEntry & { v: number }
    > | null;
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      parsed.v !== FORMAT_VERSION ||
      typeof parsed.lastModified !== "number"
    ) {
      return null;
    }
    return {
      value: parsed.value ?? null,
      lastModified: parsed.lastModified,
      tags: Array.isArray(parsed.tags) ? parsed.tags : []
    };
  } catch {
    return null;
  }
}

// The tagged encoding. Plain JSON turns a Buffer into `{ type, data }` and a
// Map into `{}`, and Next.js cache values are full of both: `rscData`, a route
// handler's `body`, and `segmentData: Map<string, Buffer>`. So a Buffer (any
// Uint8Array) becomes `{ "$b": base64 }` and a Map `{ "$m": [[k, v], ...] }`.
// A plain object that happens to look like one of those, a single key named
// `$b`, `$m`, or `$o`, is wrapped as `{ "$o": object }`, so nothing a caller
// stores can be mistaken for a tag on the way back.
const BYTES = "$b";
const MAP = "$m";
const OBJECT = "$o";

function isTagShaped(keys: readonly string[]): boolean {
  return (
    keys.length === 1 &&
    (keys[0] === BYTES || keys[0] === MAP || keys[0] === OBJECT)
  );
}

function pack(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    // Next.js runs cache handlers on the Node.js runtime, where Buffer exists.
    const bytes = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    return { [BYTES]: bytes.toString("base64") };
  }
  if (value instanceof Map) {
    return {
      [MAP]: Array.from(value, ([key, item]) => [pack(key), pack(item)])
    };
  }
  if (Array.isArray(value)) return value.map(pack);
  if (typeof value !== "object" || value === null) return value;
  // Anything with its own JSON form (a Date, say) keeps it.
  if (typeof (value as { toJSON?: unknown }).toJSON === "function") {
    return value;
  }
  // Skip what JSON.stringify would drop, so the key count checked below is
  // the key count that lands in Redis.
  const fields = Object.entries(value)
    .filter(
      ([, item]) =>
        item !== undefined &&
        typeof item !== "function" &&
        typeof item !== "symbol"
    )
    .map(([key, item]) => [key, pack(item)] as const);
  // fromEntries defines keys rather than assigning them, so an own
  // `__proto__` key stays data instead of becoming the object's prototype.
  const out = Object.fromEntries(fields);
  return isTagShaped(fields.map(([key]) => key)) ? { [OBJECT]: out } : out;
}

function unpack(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(unpack);
  if (typeof value !== "object" || value === null) return value;
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record);
  if (isTagShaped(keys)) {
    const inner = record[keys[0] as string];
    if (keys[0] === BYTES && typeof inner === "string") {
      return Buffer.from(inner, "base64");
    }
    if (keys[0] === MAP && Array.isArray(inner)) {
      return new Map(
        inner.map((pair) => {
          const [key, item] = Array.isArray(pair) ? pair : [];
          return [unpack(key), unpack(item)];
        })
      );
    }
    if (keys[0] === OBJECT && typeof inner === "object" && inner !== null) {
      return unpackFields(inner as Record<string, unknown>);
    }
  }
  return unpackFields(record);
}

function unpackFields(
  record: Record<string, unknown>
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [key, unpack(item)])
  );
}

/** Options for {@link rateLimitMiddleware}. */
export type NextRateLimitOptions = {
  /**
   * The limiter to count requests against: a `ratelimit` schema reached
   * through the handle, e.g. `redis.query.apiLimit`. Its limit, window, and
   * key prefix are the ones declared on the schema.
   */
  readonly limiter: RatelimitStore;
  /**
   * Extract the identity to limit on from the `Request`. Required, and
   * deliberately so: there is no request property a limiter can trust without
   * knowing the deployment. `x-forwarded-for` is set by the client on a
   * self-hosted deploy, and appended to (rather than replaced) by many
   * proxies, so defaulting to it would let a caller pick its own identity and
   * nullify the limit by varying one header, minting a fresh Redis key each
   * time. Pass the value your deployment actually verifies.
   *
   * @example
   * ```ts
   * // On Vercel, which overwrites the header at the edge:
   * identify: (request) =>
   *   request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"
   * // Best: an identity you authenticated yourself.
   * identify: async (request) => (await auth(request)).userId
   * ```
   */
  readonly identify: (request: Request) => string | Promise<string>;
};

/**
 * The function {@link rateLimitMiddleware} returns: call it with a `Request` to gate
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
 * Server Actions, over a `ratelimit` schema reached through the handle (one
 * atomic Lua round trip per check).
 *
 * The returned function takes a web-standard `Request` and resolves `null`
 * when the request is allowed, or a ready-to-return `429 Response` with
 * `Retry-After` (seconds) and `X-RateLimit-Limit` / `X-RateLimit-Remaining` /
 * `X-RateLimit-Reset` (epoch seconds) headers when it is not. Only
 * web-standard APIs are used, so it runs in Edge middleware and Node route
 * handlers alike — pair it with `benni/upstash` on the edge.
 *
 * @example
 * ```ts
 * // schema.ts
 * export const apiLimit = ratelimit("api", { limit: 20, windowMs: 10_000 });
 *
 * // middleware.ts
 * import { rateLimitMiddleware } from "benni/next";
 * import { redis } from "./redis";
 *
 * const limiter = rateLimitMiddleware({
 *   limiter: redis.query.apiLimit,
 *   // On Vercel, which overwrites the header at the edge.
 *   identify: (request) =>
 *     request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"
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
export function rateLimitMiddleware(
  options: NextRateLimitOptions
): NextRateLimitHandler {
  const limiter: unknown = options.limiter;
  // Checked here, not on the first request, so a 0.1-style
  // `{ client, limit, windowMs }` config fails at startup with the fix in the
  // message rather than as "check is not a function" under traffic.
  if (
    typeof (limiter as Partial<RatelimitStore> | null)?.check !== "function"
  ) {
    throw new TypeError(
      'rateLimitMiddleware() takes the limiter itself: declare `export const apiLimit = ratelimit("api", { limit, windowMs })` in your schema module and pass `limiter: redis.query.apiLimit`. The { client, limit, windowMs, prefix } options were removed in 0.2.'
    );
  }
  const identify = options.identify;
  const check = (identity: string): Promise<RatelimitResult> =>
    options.limiter.check(identity);

  const handler = async (request: Request): Promise<Response | null> => {
    const result = await check(await identify(request));
    if (result.success) return null;
    // retryAfterMs is a server-derived duration, so this never differences a
    // Redis timestamp against a possibly-skewed local clock.
    const retryAfterSeconds = Math.ceil(result.retryAfterMs / 1000);
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

/**
 * The source narrowed to a client, behind a promise so the call sites here
 * can stay `await getClient()`.
 */
function createClientResolver(
  source: RedisClientSource
): () => Promise<RedisClient> {
  const client = resolveClient(source);
  return () => Promise.resolve(client);
}

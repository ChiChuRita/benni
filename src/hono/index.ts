import type { Context, MiddlewareHandler } from "hono";
import type { RedisClient } from "../core/types.js";
import { ratelimit as createRatelimiter } from "../primitives/ratelimit.js";

/**
 * A Redis client, however you have it: the client itself, a promise of one
 * (e.g. a top-level `connect()` call), or a factory that produces one. The
 * source is awaited once on first use and cached for every later request.
 */
export type ClientSource =
  | RedisClient
  | Promise<RedisClient>
  | (() => Promise<RedisClient>);

function resolveOnce(source: ClientSource): () => Promise<RedisClient> {
  let cached: Promise<RedisClient> | undefined;
  return () => {
    if (!cached) {
      cached = Promise.resolve(
        typeof source === "function" ? source() : source
      );
      // A failed resolution must not poison every later request.
      cached.catch(() => {
        cached = undefined;
      });
    }
    return cached;
  };
}

/**
 * A `RedisClient` facade over a lazily resolved source, so the primitives can
 * be constructed (and their options validated) eagerly while the underlying
 * client connects on first use.
 */
function lazyClient(getClient: () => Promise<RedisClient>): RedisClient {
  return {
    send: async (command) => (await getClient()).send(command),
    pipeline: async (commands) => (await getClient()).pipeline(commands),
    close: async () => (await getClient()).close()
  };
}

export type RatelimitOptions = {
  /** The Redis client (or a promise/factory of one; awaited once, cached). */
  readonly client: ClientSource;
  /** Maximum requests allowed within the window. */
  readonly limit: number;
  /** Window length in milliseconds. */
  readonly windowMs: number;
  /** Key namespace; keys are `<prefix>:<id>`. Default `"ratelimit"`. */
  readonly prefix?: string;
  /**
   * Derives the rate-limit subject from the request. Default: the first hop
   * of `x-forwarded-for`, else `cf-connecting-ip`, else `"anonymous"`.
   */
  readonly key?: (c: Context) => string | Promise<string>;
};

function defaultRatelimitKey(c: Context): string {
  const forwarded = c.req.header("x-forwarded-for");
  if (forwarded) {
    const first = forwarded.split(",")[0]?.trim();
    if (first) return first;
  }
  return c.req.header("cf-connecting-ip") ?? "anonymous";
}

/**
 * Sliding-window rate limiting as Hono middleware, built on the
 * [`ratelimit` primitive](../primitives/ratelimit.js) — one atomic Lua round
 * trip per request. Allowed requests carry `X-RateLimit-Limit`, `-Remaining`,
 * and `-Reset` (epoch seconds) headers; denied requests get a JSON 429 with
 * `Retry-After`.
 *
 * @example
 * ```ts
 * import { Hono } from "hono";
 * import { ratelimit } from "beni/hono";
 *
 * const app = new Hono();
 * app.use("*", ratelimit({ client, limit: 100, windowMs: 60_000 }));
 * app.get("/", (c) => c.text("hello"));
 * ```
 */
export function ratelimit(options: RatelimitOptions): MiddlewareHandler {
  const getClient = resolveOnce(options.client);
  const limiter = createRatelimiter(lazyClient(getClient), {
    limit: options.limit,
    windowMs: options.windowMs,
    ...(options.prefix !== undefined && { prefix: options.prefix })
  });
  const key = options.key ?? defaultRatelimitKey;

  return async (c, next) => {
    const result = await limiter.check(await key(c));
    if (!result.success) {
      const retryAfterSeconds = Math.max(
        0,
        Math.ceil((result.resetMs - Date.now()) / 1000)
      );
      c.header("Retry-After", String(retryAfterSeconds));
      return c.json({ error: "rate limit exceeded" }, 429);
    }
    c.header("X-RateLimit-Limit", String(result.limit));
    c.header("X-RateLimit-Remaining", String(result.remaining));
    c.header("X-RateLimit-Reset", String(Math.ceil(result.resetMs / 1000)));
    await next();
  };
}

export type CacheOptions = {
  /** The Redis client (or a promise/factory of one; awaited once, cached). */
  readonly client: ClientSource;
  /** Entry lifetime in milliseconds. */
  readonly ttlMs: number;
  /** Key namespace; keys are `<prefix>:<key>`. Default `"hono-cache"`. */
  readonly prefix?: string;
  /**
   * Derives the cache key from the request. Default:
   * `method + ":" + path + query`.
   */
  readonly key?: (c: Context) => string;
  /**
   * Header names folded into the cache key, so responses that differ by
   * these headers (e.g. `accept-language`) are cached separately.
   */
  readonly vary?: readonly string[];
};

type CacheEntry = {
  readonly status: number;
  readonly headers: Record<string, string>;
  readonly body: string;
};

function defaultCacheKey(c: Context): string {
  const url = new URL(c.req.url);
  return `${c.req.method}:${url.pathname}${url.search}`;
}

/**
 * Read-through response caching as Hono middleware. `GET`/`HEAD` responses
 * are stored in Redis as `{ status, headers, body }` JSON (`SET PX ttlMs`)
 * and replayed on hit with an `X-Beni-Cache: hit` header. Other methods
 * pass straight through, as do error responses and anything carrying
 * `set-cookie`. Every Redis failure fails open — the request always runs.
 *
 * The body is stored as text, so this is for text-ish responses (JSON, HTML),
 * not streaming or binary payloads.
 *
 * @example
 * ```ts
 * import { Hono } from "hono";
 * import { cache } from "beni/hono";
 *
 * const app = new Hono();
 * app.get("/report", cache({ client, ttlMs: 30_000 }), (c) =>
 *   c.json({ generatedAt: Date.now() })
 * );
 * ```
 */
export function cache(options: CacheOptions): MiddlewareHandler {
  const getClient = resolveOnce(options.client);
  const client = lazyClient(getClient);
  const prefix = options.prefix ?? "hono-cache";
  const key = options.key ?? defaultCacheKey;
  const vary = options.vary ?? [];

  return async (c, next) => {
    if (c.req.method !== "GET" && c.req.method !== "HEAD") {
      await next();
      return;
    }

    let cacheKey = `${prefix}:${key(c)}`;
    for (const name of vary) {
      cacheKey += `|${name.toLowerCase()}=${c.req.header(name) ?? ""}`;
    }

    try {
      const reply = await client.send(["GET", cacheKey]);
      if (typeof reply === "string") {
        const entry = JSON.parse(reply) as CacheEntry;
        return new Response(entry.body, {
          status: entry.status,
          headers: { ...entry.headers, "X-Beni-Cache": "hit" }
        });
      }
    } catch {
      // Fail open: a Redis read failure must never break the request.
    }

    await next();

    const res = c.res;
    if (!res.ok || res.headers.get("set-cookie") !== null) return;
    try {
      const contentType = res.headers.get("content-type");
      const entry: CacheEntry = {
        status: res.status,
        headers: contentType ? { "content-type": contentType } : {},
        body: await res.clone().text()
      };
      await client.send([
        "SET",
        cacheKey,
        JSON.stringify(entry),
        "PX",
        options.ttlMs
      ]);
    } catch {
      // Fail open: a Redis write failure must never break the response.
    }
  };
}

/**
 * The per-request session bag exposed by the [`session`](#session) middleware
 * via `c.get("session")` / [`getSession`](#getsession). Values are `unknown`
 * per key — the session is a convenience bag; codec-level typing belongs to
 * your beni schemas.
 */
export type Session = {
  /** Read a value. `T` is a caller assertion, not a validation. */
  get<T = unknown>(key: string): T | undefined;
  /** Write a value; marks the session dirty (persisted after the handler). */
  set(key: string, value: unknown): void;
  /** Remove one key; marks the session dirty. */
  delete(key: string): void;
  /** Remove every key; the stored record is deleted after the handler. */
  clear(): void;
  /** The session id (the cookie value). */
  readonly id: string;
  /** True when no existing session record backed this request. */
  readonly isNew: boolean;
};

export type SessionOptions = {
  /** The Redis client (or a promise/factory of one; awaited once, cached). */
  readonly client: ClientSource;
  /** Session lifetime in seconds; refreshed on every write. Default `86400`. */
  readonly ttlSeconds?: number;
  /** Key namespace; keys are `<prefix>:<id>`. Default `"hono-session"`. */
  readonly prefix?: string;
  /** Session cookie name. Default `"sid"`. */
  readonly cookieName?: string;
  /**
   * Cookie attributes. Defaults: `path: "/"`, `httpOnly: true`,
   * `sameSite: "Lax"`, `secure: false` (enable `secure` in production).
   */
  readonly cookie?: {
    readonly path?: string;
    readonly httpOnly?: boolean;
    readonly secure?: boolean;
    readonly sameSite?: "Strict" | "Lax" | "None";
  };
};

function readCookie(
  header: string | undefined,
  name: string
): string | undefined {
  if (!header) return undefined;
  for (const part of header.split(";")) {
    const separator = part.indexOf("=");
    if (separator === -1) continue;
    if (part.slice(0, separator).trim() === name) {
      return decodeURIComponent(part.slice(separator + 1).trim());
    }
  }
  return undefined;
}

/**
 * Redis-backed sessions as Hono middleware. The session record is a JSON
 * object stored under `<prefix>:<id>`, loaded from the `sid` cookie before
 * the handler and persisted after it — but only when the handler actually
 * wrote something (`SET ... EX ttlSeconds`, so the TTL rolls on every write).
 * New sessions get a `crypto.randomUUID()` id and a `Set-Cookie` header;
 * `clear()` deletes the stored record.
 *
 * Values are `unknown` per key (`get<T>` is a convenience assertion) — for
 * typed data, reach for your beni schemas instead.
 *
 * @example
 * ```ts
 * import { Hono } from "hono";
 * import { getSession, session } from "beni/hono";
 *
 * const app = new Hono();
 * app.use("*", session({ client }));
 * app.post("/login", (c) => {
 *   getSession(c).set("userId", "u1");
 *   return c.text("welcome");
 * });
 * ```
 */
export function session(options: SessionOptions): MiddlewareHandler {
  const getClient = resolveOnce(options.client);
  const client = lazyClient(getClient);
  const ttlSeconds = options.ttlSeconds ?? 86_400;
  const prefix = options.prefix ?? "hono-session";
  const cookieName = options.cookieName ?? "sid";
  const cookie = options.cookie ?? {};

  return async (c, next) => {
    const sid = readCookie(c.req.header("Cookie"), cookieName);
    let data: Record<string, unknown> = {};
    let id: string | undefined;

    if (sid) {
      const reply = await client.send(["GET", `${prefix}:${sid}`]);
      if (typeof reply === "string") {
        data = JSON.parse(reply) as Record<string, unknown>;
        id = sid;
      }
    }

    // An unknown sid gets a fresh id (never adopt an unverified cookie
    // value as a session id — that would invite session fixation).
    const isNew = id === undefined;
    if (id === undefined) id = globalThis.crypto.randomUUID();

    let dirty = false;
    const bag: Session = {
      get: <T = unknown>(key: string) => data[key] as T | undefined,
      set(key, value) {
        data[key] = value;
        dirty = true;
      },
      delete(key) {
        delete data[key];
        dirty = true;
      },
      clear() {
        data = {};
        dirty = true;
      },
      id,
      isNew
    };
    c.set("session", bag);

    await next();

    if (!dirty) return;
    const recordKey = `${prefix}:${id}`;
    if (Object.keys(data).length === 0) {
      if (!isNew) await client.send(["DEL", recordKey]);
      return;
    }
    await client.send([
      "SET",
      recordKey,
      JSON.stringify(data),
      "EX",
      ttlSeconds
    ]);
    if (isNew) {
      const attributes = [
        `${cookieName}=${id}`,
        `Path=${cookie.path ?? "/"}`,
        `SameSite=${cookie.sameSite ?? "Lax"}`
      ];
      if (cookie.httpOnly ?? true) attributes.push("HttpOnly");
      if (cookie.secure ?? false) attributes.push("Secure");
      c.header("Set-Cookie", attributes.join("; "), { append: true });
    }
  };
}

/**
 * Typed accessor for the request's [`Session`](#session-1). Requires the
 * [`session`](#session) middleware upstream.
 *
 * @example
 * ```ts
 * app.get("/me", (c) => {
 *   const userId = getSession(c).get<string>("userId");
 *   return userId ? c.text(userId) : c.text("anonymous", 401);
 * });
 * ```
 */
export function getSession(c: Context): Session {
  const bag = c.get("session") as Session | undefined;
  if (!bag) {
    throw new TypeError(
      "getSession(c) requires the session() middleware to run first"
    );
  }
  return bag;
}

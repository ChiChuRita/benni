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
   * Derives the rate-limit subject from the request. Required, and
   * deliberately so: there is no request property a limiter can trust without
   * knowing the deployment. `x-forwarded-for` and `cf-connecting-ip` are set
   * by the client on a direct deploy, and appended to (rather than replaced)
   * by many proxies, so defaulting to either would let a caller pick its own
   * identity and nullify the limit by varying one header. Pass the value your
   * deployment actually verifies: an authenticated user or API key id where
   * you have one, or the client address your platform exposes.
   *
   * @example
   * ```ts
   * // Behind a proxy you control, which overwrites the header:
   * key: (c) => c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"
   * // On Cloudflare Workers:
   * key: (c) => c.req.header("cf-connecting-ip") ?? "anonymous"
   * // Best: something you authenticated yourself.
   * key: (c) => c.get("userId")
   * ```
   */
  readonly key: (c: Context) => string | Promise<string>;
};

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
 * app.use(
 *   "*",
 *   ratelimit({
 *     client,
 *     limit: 100,
 *     windowMs: 60_000,
 *     key: (c) => c.get("userId")
 *   })
 * );
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
  const key = options.key;

  return async (c, next) => {
    const result = await limiter.check(await key(c));
    if (!result.success) {
      // retryAfterMs is a server-derived duration, so this never differences
      // a Redis timestamp against a possibly-skewed local clock.
      const retryAfterSeconds = Math.ceil(result.retryAfterMs / 1000);
      c.header("Retry-After", String(retryAfterSeconds));
      return c.json({ error: "rate limit exceeded" }, 429);
    }
    await next();
    // After next(), not before: headers set before the handler runs land in
    // Hono's prepared-header bag, which is dropped whenever a downstream
    // handler assigns a fresh Response to c.res (beni's own cache() does that
    // on every hit). Setting them on the finalized response instead makes the
    // documented headers survive whatever the handler returned.
    c.header("X-RateLimit-Limit", String(result.limit));
    c.header("X-RateLimit-Remaining", String(result.remaining));
    c.header("X-RateLimit-Reset", String(Math.ceil(result.resetMs / 1000)));
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
  // The origin is part of the key: one app bound to several hostnames (a
  // multi-tenant Worker, a wildcard-domain SaaS) must not share one entry per
  // path across them, whichever host happens to warm it first.
  return `${c.req.method}:${url.origin}${url.pathname}${url.search}`;
}

/**
 * Response headers carried through into the stored entry. Everything else is
 * dropped, but a replay has to keep telling downstream caches and clients the
 * truth about these.
 */
const STORED_HEADERS = [
  "content-type",
  "cache-control",
  "vary",
  "etag",
  "last-modified"
];

/** True when the response's own directives forbid a shared cache storing it. */
function forbidsSharedStorage(res: Response): boolean {
  const control = res.headers.get("cache-control");
  if (control === null) return false;
  for (const directive of control.split(",")) {
    // `private` and `no-cache` can carry a field list (`private="set-cookie"`),
    // so compare the directive name only.
    const name = directive.split("=")[0]?.trim().toLowerCase();
    if (name === "no-store" || name === "no-cache" || name === "private") {
      return true;
    }
  }
  return false;
}

/** True when the response varies by something the cache key does not fold in. */
function variesBeyondKey(res: Response, vary: readonly string[]): boolean {
  const declared = res.headers.get("vary");
  if (declared === null) return false;
  const covered = new Set(vary.map((name) => name.toLowerCase()));
  for (const name of declared.split(",")) {
    const trimmed = name.trim().toLowerCase();
    if (trimmed === "") continue;
    // `Vary: *` means never reusable, whatever the key folds in.
    if (trimmed === "*" || !covered.has(trimmed)) return true;
  }
  return false;
}

/**
 * Set by `session()` on its bag so `cache()` can tell whether the response it
 * is about to store was derived from session state, and so `getSession()` can
 * mark the bag touched from the outside. Module-local symbols, not part of the
 * public `Session` shape.
 */
const SESSION_TOUCHED = Symbol("beni.hono.sessionTouched");
const SESSION_TOUCH = Symbol("beni.hono.sessionTouch");

type TouchTracked = {
  readonly [SESSION_TOUCHED]?: () => boolean;
  readonly [SESSION_TOUCH]?: () => void;
};

/** True when the handler actually read or wrote the session on this request. */
function sessionWasTouched(c: Context): boolean {
  const bag = c.get("session") as (Session & TouchTracked) | undefined;
  const probe = bag?.[SESSION_TOUCHED];
  return typeof probe === "function" && probe();
}

/**
 * Read-through response caching as Hono middleware. `GET`/`HEAD` responses
 * are stored in Redis as `{ status, headers, body }` JSON (`SET PX ttlMs`)
 * and replayed on hit with an `X-Beni-Cache: hit` header. Other methods pass
 * straight through, as do ranged requests, anything but a plain `200`, and
 * responses carrying `set-cookie`, a `no-store`/`no-cache`/`private`
 * `Cache-Control`, or a `Vary` naming a header the key does not fold in.
 * Every Redis failure fails open — the request always runs.
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
    // A shared cache must not store or replay a response to a request that
    // carried credentials. Pass those straight through.
    if (c.req.header("Authorization") !== undefined) {
      await next();
      return;
    }
    // Range is not part of the key, so a ranged request is uncacheable in
    // both directions: its partial response must not be stored under the
    // plain URL key, and a stored whole body is not what it asked for.
    if (c.req.header("Range") !== undefined) {
      await next();
      return;
    }

    let cacheKey = `${prefix}:${key(c)}`;
    for (const name of vary) {
      // Length-prefixed, so a header value containing the separator cannot
      // make two different header sets build the same key.
      const value = c.req.header(name) ?? "";
      cacheKey += `|${name.toLowerCase()}=${value.length}:${value}`;
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
    // Only a plain 200 is storable. Response.ok spans 200-299, which let a
    // 206 built for someone else's Range header be stored under the plain URL
    // key and replayed, Content-Range stripped, to clients that sent none.
    if (res.status !== 200 || res.headers.get("set-cookie") !== null) return;
    // The set-cookie check alone is not enough to catch a per-user response.
    // A returning visitor already has their sid cookie, so session() sets no
    // Set-Cookie at all, and when session() is the outer middleware (the
    // documented `app.use("*", session())` shape) it appends its header after
    // this runs anyway. Either way the guard never fires and one user's
    // authenticated body gets stored under a key that does not vary by
    // session, then served to everyone else. Ask the session itself instead.
    if (sessionWasTouched(c)) return;
    // The handler's own directives win over anything inferred here: no-store,
    // no-cache, or private all say "not yours to share", and a Vary naming a
    // header the key does not fold in says this body is not reusable as keyed.
    if (forbidsSharedStorage(res) || variesBeyondKey(res, vary)) return;
    try {
      const headers: Record<string, string> = {};
      for (const name of STORED_HEADERS) {
        const value = res.headers.get(name);
        if (value !== null) headers[name] = value;
      }
      const entry: CacheEntry = {
        status: res.status,
        headers,
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
  /**
   * Mint a fresh session id, keeping the current data. The record under the
   * old id is deleted after the handler and a new `Set-Cookie` is issued.
   * Call this on login and on any privilege change: it is what stops a
   * session id an attacker planted in the browser from becoming the id the
   * authenticated data lives under.
   */
  regenerate(): void;
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
      const raw = part.slice(separator + 1).trim();
      try {
        return decodeURIComponent(raw);
      } catch {
        // A stray "%" makes decodeURIComponent throw a URIError. That is a
        // malformed cookie, not a server fault, so treat it as no cookie
        // rather than turning every request from that browser into a 500.
        return undefined;
      }
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
 * `clear()` deletes the stored record; `regenerate()` rotates the id (call it
 * on login and on any privilege change).
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
 *   const bag = getSession(c);
 *   bag.regenerate();
 *   bag.set("userId", "u1");
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
        try {
          const parsed = JSON.parse(reply) as unknown;
          // Anything that is not a plain object (a corrupt record, a key
          // collision with another writer) would otherwise throw out of the
          // middleware and 500 the request. Start a fresh session instead.
          if (parsed !== null && typeof parsed === "object") {
            data = parsed as Record<string, unknown>;
            id = sid;
          }
        } catch {
          // Same: an unparseable record is a fresh session, not a 500.
        }
      }
    }

    // An unknown sid gets a fresh id: adopting an unverified cookie value
    // would let anyone pick their own key namespace. It is not on its own a
    // fixation defence, since an attacker can always mint a real id first and
    // plant that. `regenerate()` is what closes fixation.
    const loadedId = id;
    const isNew = loadedId === undefined;
    let currentId = loadedId ?? globalThis.crypto.randomUUID();
    // Ids abandoned by regenerate(), deleted after the handler.
    const staleIds: string[] = [];

    let dirty = false;
    // Reads count too, not just writes: a handler that only *reads* the
    // session still produces a per-user response, which is what cache() has
    // to know before it stores anything. The id and isNew are accessors for
    // the same reason - reading the identity is reading the session.
    let touched = false;
    const bag: Session = {
      get: <T = unknown>(key: string) => {
        touched = true;
        return data[key] as T | undefined;
      },
      set(key, value) {
        touched = true;
        data[key] = value;
        dirty = true;
      },
      delete(key) {
        touched = true;
        delete data[key];
        dirty = true;
      },
      clear() {
        touched = true;
        data = {};
        dirty = true;
      },
      regenerate() {
        touched = true;
        // Only an id that actually backs a record needs deleting; one this
        // request minted was never written.
        if (currentId === loadedId) staleIds.push(currentId);
        currentId = globalThis.crypto.randomUUID();
        // The new id has to reach both Redis and the browser even if the
        // handler writes nothing else.
        dirty = true;
      },
      get id() {
        touched = true;
        return currentId;
      },
      get isNew() {
        touched = true;
        return isNew;
      }
    };
    Object.defineProperty(bag, SESSION_TOUCHED, {
      value: () => touched,
      enumerable: false
    });
    Object.defineProperty(bag, SESSION_TOUCH, {
      value: () => {
        touched = true;
      },
      enumerable: false
    });
    c.set("session", bag);

    await next();

    for (const stale of staleIds) {
      await client.send(["DEL", `${prefix}:${stale}`]);
    }
    if (!dirty) return;
    const recordKey = `${prefix}:${currentId}`;
    if (Object.keys(data).length === 0) {
      if (currentId === loadedId) await client.send(["DEL", recordKey]);
      return;
    }
    const record = JSON.stringify(data);
    // Writing back to the record this request loaded is conditional on it
    // still being there. A concurrent logout may have deleted it while the
    // handler ran, and an unconditional SET would resurrect the stale
    // snapshot with a fresh full lifetime, re-authenticating the sid the user
    // just logged out of. XX makes that write a no-op instead.
    await client.send(
      currentId === loadedId
        ? ["SET", recordKey, record, "EX", ttlSeconds, "XX"]
        : ["SET", recordKey, record, "EX", ttlSeconds]
    );
    // Whenever the browser is not already carrying this id: a brand new
    // session, or one rotated by regenerate().
    if (currentId !== sid) {
      const attributes = [
        `${cookieName}=${currentId}`,
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
  const bag = c.get("session") as (Session & TouchTracked) | undefined;
  if (!bag) {
    throw new TypeError(
      "getSession(c) requires the session() middleware to run first"
    );
  }
  // Reaching for the bag at all counts as a touch, so cache() stays safe
  // however the handler uses it. Marking only on the members that exist today
  // leaves the guard one new property away from being defeated again.
  bag[SESSION_TOUCH]?.();
  return bag;
}

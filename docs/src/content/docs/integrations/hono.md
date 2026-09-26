---
title: "Hono"
description: "Rate limiting, response caching, and sessions as Hono middleware: one stack that runs on Node, Bun, Deno, and Cloudflare Workers."
---

`benni/hono` packages rate limiting, response caching, and sessions as drop-in [Hono](https://hono.dev) middleware. They run over any adapter, so the same middleware stack runs everywhere Hono does: Node, Bun, Deno, and Cloudflare Workers. On Workers, pair it with [`benni/upstash`](/benni/runtime/edge/).

```ts
// schema.ts
import { ratelimit } from "benni/schema";

export const apiLimit = ratelimit("api", { limit: 100, windowMs: 60_000 });
```

```ts
// app.ts
import { Hono } from "hono";
import { benni } from "benni";
import { upstash } from "benni/upstash";
import { rateLimitMiddleware } from "benni/hono";
import * as schema from "./schema";

const redis = benni({
  client: upstash({
    url: process.env.UPSTASH_REDIS_REST_URL as string,
    token: process.env.UPSTASH_REDIS_REST_TOKEN as string
  }),
  schema
});

const app = new Hono();
app.use(
  "*",
  rateLimitMiddleware({
    limiter: redis.query.apiLimit,
    key: (c) => c.get("userId")
  })
);
```

The rate limiter takes the [`ratelimit` primitive](/benni/primitives/ratelimit/) itself, declared in your schema module, so the limit lives in one place. The cache and session middleware keep their own storage and take `client`: an adapter's client or a Benni handle. Adapters connect on the first command, so building any of them at module scope opens nothing.

The exports are named for what they are, `rateLimitMiddleware`, `cacheMiddleware`, and `sessionMiddleware`, so they cannot be confused with the `ratelimit`/`cache` schema builders or with `redis.session()`.

## Rate limiting

Sliding-window rate limiting, one atomic Lua round trip per request: a [`ratelimit` primitive](/benni/primitives/ratelimit/) from your schema module, behind a middleware. Allowed requests carry `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` (epoch seconds); denied requests get a JSON `429` with `Retry-After`.

```ts
import { Hono } from "hono";
import { rateLimitMiddleware } from "benni/hono";

const app = new Hono();

app.use(
  "/api/*",
  rateLimitMiddleware({
    limiter: redis.query.apiLimit, // ratelimit("api", { limit: 100, windowMs: 60_000 })
    key: (c) => c.req.header("x-api-key") ?? "anonymous"
  })
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `limiter` | - | The limiter, e.g. `redis.query.apiLimit`. Its limit, window, and key prefix are the ones declared on the schema. |
| `key` | - | `(c) => string \| Promise<string>`, the rate-limit subject. Required: see below. |
| `failOpen` | `false` | Let requests through, unlimited, while Redis is failing. See [When Redis fails](#when-redis-fails). |

`key` is required on purpose. There is no request property a limiter can trust without knowing the deployment: `x-forwarded-for` and `cf-connecting-ip` are set by the client on a direct deploy, and appended to rather than replaced by many proxies, so a default built on either would let a caller pick its own identity and nullify the limit by varying one header. Pass the value your deployment actually verifies: an authenticated user or API key id where you have one, otherwise the client address your platform exposes.

```ts
// Behind a proxy you control, which overwrites the header:
key: (c) => c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"
// On Cloudflare Workers:
key: (c) => c.req.header("cf-connecting-ip") ?? "anonymous"
```

## Response caching

Read-through caching for `GET`/`HEAD` responses (other methods pass through). On a hit the stored response is replayed with an `X-Benni-Cache: hit` header; on a miss the handler runs and successful responses are stored with `SET PX ttlMs`. **Every Redis failure fails open**: the request always runs.

The cache key is the full URL (`method:origin+path+query`) plus any `vary` headers, so it is a *shared* cache, and one app bound to several hostnames keeps one entry per host.

A request that carries credentials passes straight through, neither read from nor stored in the cache:

- an `Authorization` header, always;
- a `Cookie` header, unless you opt in. A route authenticated by a cookie through middleware Benni cannot see (your own auth, a third-party session library) sends no `Set-Cookie` and touches no Benni session, so nothing about its response says "per user", and a shared cache would store the first visitor's page and replay it to everyone. Two ways to cache cookie-carrying requests anyway: list `cookie` in `vary`, which keys the entry on the exact `Cookie` header so each visitor only ever sees their own response, or set `ignoreCookies: true`, which shares one entry across all cookies and is only safe when no response behind the middleware depends on one (analytics cookies on a public page, say).

A ranged request (`Range` header) also passes straight through. Beyond that, a response is never stored when any of these hold:

- the response is anything but a plain `200`;
- the handler or an inner middleware set a cookie on the response;
- the response carries a `no-store`, `no-cache`, or `private` `Cache-Control`, or a `Vary` naming a header you did not list in `vary` (`Vary: *` is never storable);
- the handler read or wrote the [`session`](#sessions) in any way, including reading `id` or `isNew` (`cacheMiddleware()` asks the session bag directly, so this holds whichever order the two middlewares are composed in).

That last rule keeps a route that reads Benni's own session safe even under `ignoreCookies`: a returning visitor already has their `sid`, so `sessionMiddleware()` emits no `Set-Cookie` and there is nothing for a cookie check to see. If a route varies by anything else the cache cannot observe (a header you did not list in `vary` and the response does not declare in `Vary`), do not put `cacheMiddleware()` on it, or give it a `key` that includes the distinguishing value.

Stored entries keep `content-type`, `cache-control`, `vary`, `etag`, and `last-modified`, so a replay stays honest to the browser and to any CDN in front of you. Every other response header is dropped.

```ts
import { Hono } from "hono";
import { cacheMiddleware } from "benni/hono";

const app = new Hono();

app.get(
  "/report",
  cacheMiddleware({ client, ttlMs: 30_000, vary: ["accept-language"] }),
  async (c) => c.json(await buildExpensiveReport())
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `ttlMs` | - | Entry lifetime in milliseconds. |
| `prefix` | `"hono-cache"` | Key namespace; keys are `<prefix>:<key>`. |
| `key` | `method + ":" + origin + path + query` | `(c) => string`, the cache key. |
| `vary` | `[]` | Header names folded into the key. Include `cookie` to cache cookie-carrying requests per visitor. |
| `ignoreCookies` | `false` | Cache cookie-carrying requests in the one shared entry. |

Bodies are stored as their exact bytes (`{ status, headers, body64 }` JSON, the body base64-encoded), so images and other binary responses replay byte for byte. The body is buffered in full, so this is not for streaming responses. Entries written by an earlier version in the old text form read as a miss and are replaced on the next request.

## Sessions

These are **cookie-backed user sessions**, not [`redis.session()`](/benni/advanced/sessions/) connection leases, so they work on Workers and other edge runtimes with `benni/upstash`.

Redis-backed sessions behind a `sid` cookie. The record is a JSON object under `<prefix>:<id>`, loaded before your handler and persisted after it, but only when the handler actually wrote something (`SET ... EX ttlSeconds`, so the TTL rolls on every write). New sessions get a `crypto.randomUUID()` id and a `Set-Cookie` header; `clear()` deletes the stored record.

A write back to a record this request loaded is conditional (`SET ... XX`), so a request that was already in flight when a concurrent `clear()` deleted the record cannot resurrect it. Beyond that, writes are last-writer-wins over the whole record: two overlapping requests that each set a different key can still lose one of the two writes.

Call `regenerate()` on login and on any privilege change. It mints a fresh id, carries the data over, deletes the record under the old id, and issues a new `Set-Cookie`. This is the defence against session fixation: without it, a session id an attacker planted in the victim's browser stays the id the authenticated data lives under, and replaying that cookie is enough to become the victim.

```ts
import { Hono } from "hono";
import { getSession, sessionMiddleware } from "benni/hono";

const app = new Hono();
app.use("*", sessionMiddleware({ client, ttlSeconds: 86_400 }));

app.post("/login", async (c) => {
  const user = await authenticate(c);
  const bag = getSession(c);
  // Never keep the pre-login id once the session becomes privileged.
  bag.regenerate();
  bag.set("userId", user.id);
  return c.json({ ok: true });
});

app.get("/me", (c) => {
  const userId = getSession(c).get<string>("userId");
  return userId ? c.text(userId) : c.text("anonymous", 401);
});

app.post("/logout", (c) => {
  getSession(c).clear();
  return c.text("bye");
});
```

| Option | Default | Meaning |
| --- | --- | --- |
| `ttlSeconds` | `86400` | Session lifetime in seconds; refreshed on every write. |
| `prefix` | `"hono-session"` | Key namespace; keys are `<prefix>:<id>`. |
| `cookieName` | `"sid"` | Session cookie name. |
| `cookie` | `path: "/"`, `httpOnly: true`, `sameSite: "Lax"`, `secure: false` | Cookie attributes; enable `secure` in production. |

Session values are `unknown` per key; `get<T>` is a convenience assertion, not a validation. The session is a convenience bag; codec-level typing belongs to your [Benni schemas](/benni/core-concepts/defining-schemas/).

## When Redis fails

Each middleware takes the side of the trade its job calls for:

| Middleware | On a Redis error | Why |
| --- | --- | --- |
| `cacheMiddleware` | Fails open: a miss, the handler runs. Not configurable. | A cache is an optimization; an outage should cost latency, never availability. |
| `rateLimitMiddleware` | Fails closed by default: the error propagates, so Hono answers `500` (or your `app.onError`). `failOpen: true` lets the request through without `X-RateLimit-*` headers instead. | A limiter that cannot count should not silently stop limiting; choose availability explicitly when that is the better trade. |
| `sessionMiddleware` | Fails closed: the error propagates. Not configurable. | Failing open would treat a signed-in visitor as anonymous and silently drop whatever the handler wrote. |

`failOpen` covers only the limiter's own Redis round trip: errors from `key` and from your handler propagate either way. It also swallows the Redis error, so watch Redis health somewhere else if you turn it on.

## Putting it together

```ts
import { Hono } from "hono";
import { benni } from "benni";
import { upstash } from "benni/upstash";
import {
  cacheMiddleware,
  getSession,
  rateLimitMiddleware,
  sessionMiddleware
} from "benni/hono";
import * as schema from "./schema"; // exports apiLimit = ratelimit("api", …)

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});
const redis = benni({ client, schema });

const app = new Hono();

app.use(
  "*",
  rateLimitMiddleware({
    limiter: redis.query.apiLimit,
    key: (c) => c.get("userId")
  })
);
app.use("*", sessionMiddleware({ client }));

// Returning visitors carry the sid cookie, which cacheMiddleware() bypasses by
// default. This page depends on no cookie, and session reads stay guarded, so
// opt in.
app.get(
  "/pricing",
  cacheMiddleware({ client, ttlMs: 60_000, ignoreCookies: true }),
  (c) => c.json({ plans: ["free", "pro"] })
);

app.post("/login", (c) => {
  const bag = getSession(c);
  bag.regenerate();
  bag.set("userId", "u1");
  return c.json({ ok: true });
});

export default app;
```

The same file deploys to a Node server, a Bun process, Deno Deploy, or a Cloudflare Worker; only the adapter changes.

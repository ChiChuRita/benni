---
title: "Hono"
description: "Rate limiting, response caching, and sessions as Hono middleware: one stack that runs on Node, Bun, Deno, and Cloudflare Workers."
---

`benni/hono` packages the [primitives](/benni/primitives/ratelimit/) as drop-in [Hono](https://hono.dev) middleware: rate limiting, response caching, and sessions. Because they take any `RedisClient`, the same middleware stack runs everywhere Hono does: Node, Bun, Deno, and Cloudflare Workers. On Workers, pair it with [`benni/upstash`](/benni/runtime/edge/).

```ts
import { Hono } from "hono";
import { upstash } from "benni/upstash";
import { ratelimit } from "benni/hono";

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});

const app = new Hono();
app.use(
  "*",
  ratelimit({ client, limit: 100, windowMs: 60_000, key: (c) => c.get("userId") })
);
```

Every middleware accepts `client` as a `RedisClient`, a `Promise<RedisClient>`, or a `() => Promise<RedisClient>` factory, awaited once on first use and cached, so lazy connection setups just work.

## Rate limiting

Sliding-window rate limiting, one atomic Lua round trip per request, the [`ratelimit` primitive](/benni/primitives/ratelimit/) behind a middleware. Allowed requests carry `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` (epoch seconds); denied requests get a JSON `429` with `Retry-After`.

```ts
import { Hono } from "hono";
import { ratelimit } from "benni/hono";

const app = new Hono();

app.use(
  "/api/*",
  ratelimit({
    client,
    limit: 100,
    windowMs: 60_000,
    key: (c) => c.req.header("x-api-key") ?? "anonymous"
  })
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `limit` | - | Maximum requests allowed within the window. |
| `windowMs` | - | Window length in milliseconds. |
| `prefix` | `"ratelimit"` | Key namespace; keys are `<prefix>:<id>`. |
| `key` | - | `(c) => string \| Promise<string>`, the rate-limit subject. Required: see below. |

`key` is required on purpose. There is no request property a limiter can trust without knowing the deployment: `x-forwarded-for` and `cf-connecting-ip` are set by the client on a direct deploy, and appended to rather than replaced by many proxies, so a default built on either would let a caller pick its own identity and nullify the limit by varying one header. Pass the value your deployment actually verifies: an authenticated user or API key id where you have one, otherwise the client address your platform exposes.

```ts
// Behind a proxy you control, which overwrites the header:
key: (c) => c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"
// On Cloudflare Workers:
key: (c) => c.req.header("cf-connecting-ip") ?? "anonymous"
```

## Response caching

Read-through caching for `GET`/`HEAD` responses (other methods pass through). On a hit the stored response is replayed with an `X-Benni-Cache: hit` header; on a miss the handler runs and successful responses are stored with `SET PX ttlMs`. **Every Redis failure fails open**: the request always runs.

The cache key is the full URL (`method:origin+path+query`) plus any `vary` headers, so it is a *shared* cache, and one app bound to several hostnames keeps one entry per host. A response is never stored when any of these hold:

- the request carried an `Authorization` or a `Range` header;
- the response is anything but a plain `200`;
- the handler or an inner middleware set a cookie on the response;
- the response carries a `no-store`, `no-cache`, or `private` `Cache-Control`, or a `Vary` naming a header you did not list in `vary` (`Vary: *` is never storable);
- the handler read or wrote the [`session`](#sessions) in any way, including reading `id` or `isNew` (`cache()` asks the session bag directly, so this holds whichever order the two middlewares are composed in).

That last rule is what keeps a per-user route safe. Note the cookie check alone would not: a returning visitor already has their `sid`, so `session()` emits no `Set-Cookie` and there is nothing for a cookie check to see. If a route varies by anything the cache cannot observe (a header you did not list in `vary` and the response does not declare in `Vary`, a value read straight from `c.req.header("Cookie")`), do not put `cache()` on it, or give it a `key` that includes the distinguishing value.

Stored entries keep `content-type`, `cache-control`, `vary`, `etag`, and `last-modified`, so a replay stays honest to the browser and to any CDN in front of you. Every other response header is dropped.

```ts
import { Hono } from "hono";
import { cache } from "benni/hono";

const app = new Hono();

app.get(
  "/report",
  cache({ client, ttlMs: 30_000, vary: ["accept-language"] }),
  async (c) => c.json(await buildExpensiveReport())
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `ttlMs` | - | Entry lifetime in milliseconds. |
| `prefix` | `"hono-cache"` | Key namespace; keys are `<prefix>:<key>`. |
| `key` | `method + ":" + origin + path + query` | `(c) => string`, the cache key. |
| `vary` | `[]` | Header names folded into the key. |

Bodies are stored as text (`{ status, headers, body }` JSON), so this is for text-ish responses (JSON, HTML), not streaming or binary payloads.

## Sessions

These are **cookie-backed user sessions**, not [`redis.session()`](/benni/advanced/sessions/) connection leases, so they work on Workers and other edge runtimes with `benni/upstash`.

Redis-backed sessions behind a `sid` cookie. The record is a JSON object under `<prefix>:<id>`, loaded before your handler and persisted after it, but only when the handler actually wrote something (`SET ... EX ttlSeconds`, so the TTL rolls on every write). New sessions get a `crypto.randomUUID()` id and a `Set-Cookie` header; `clear()` deletes the stored record.

A write back to a record this request loaded is conditional (`SET ... XX`), so a request that was already in flight when a concurrent `clear()` deleted the record cannot resurrect it. Beyond that, writes are last-writer-wins over the whole record: two overlapping requests that each set a different key can still lose one of the two writes.

Call `regenerate()` on login and on any privilege change. It mints a fresh id, carries the data over, deletes the record under the old id, and issues a new `Set-Cookie`. This is the defence against session fixation: without it, a session id an attacker planted in the victim's browser stays the id the authenticated data lives under, and replaying that cookie is enough to become the victim.

```ts
import { Hono } from "hono";
import { getSession, session } from "benni/hono";

const app = new Hono();
app.use("*", session({ client, ttlSeconds: 86_400 }));

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

## Putting it together

```ts
import { Hono } from "hono";
import { upstash } from "benni/upstash";
import { cache, getSession, ratelimit, session } from "benni/hono";

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});

const app = new Hono();

app.use(
  "*",
  ratelimit({ client, limit: 100, windowMs: 60_000, key: (c) => c.get("userId") })
);
app.use("*", session({ client }));

app.get("/pricing", cache({ client, ttlMs: 60_000 }), (c) =>
  c.json({ plans: ["free", "pro"] })
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

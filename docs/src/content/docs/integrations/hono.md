---
title: "Hono"
description: "Rate limiting, response caching, and sessions as Hono middleware: one stack that runs on Node, Bun, Deno, and Cloudflare Workers."
---

`beni/hono` packages the [primitives](/beni/primitives/ratelimit/) as drop-in [Hono](https://hono.dev) middleware: rate limiting, response caching, and sessions. Because they take any `RedisClient`, the same middleware stack runs everywhere Hono does: Node, Bun, Deno, and Cloudflare Workers. On Workers, pair it with [`beni/upstash`](/beni/runtime/edge/).

```ts
import { Hono } from "hono";
import { upstash } from "beni/upstash";
import { ratelimit } from "beni/hono";

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});

const app = new Hono();
app.use("*", ratelimit({ client, limit: 100, windowMs: 60_000 }));
```

Every middleware accepts `client` as a `RedisClient`, a `Promise<RedisClient>`, or a `() => Promise<RedisClient>` factory, awaited once on first use and cached, so lazy connection setups just work.

## Rate limiting

Sliding-window rate limiting, one atomic Lua round trip per request, the [`ratelimit` primitive](/beni/primitives/ratelimit/) behind a middleware. Allowed requests carry `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` (epoch seconds); denied requests get a JSON `429` with `Retry-After`.

```ts
import { Hono } from "hono";
import { ratelimit } from "beni/hono";

const app = new Hono();

app.use(
  "/api/*",
  ratelimit({
    client,
    limit: 100,
    windowMs: 60_000,
    // Default: first x-forwarded-for hop, else cf-connecting-ip, else "anonymous".
    key: (c) => c.req.header("x-api-key") ?? "anonymous"
  })
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `limit` | - | Maximum requests allowed within the window. |
| `windowMs` | - | Window length in milliseconds. |
| `prefix` | `"ratelimit"` | Key namespace; keys are `<prefix>:<id>`. |
| `key` | client IP | `(c) => string \| Promise<string>`, the rate-limit subject. |

## Response caching

Read-through caching for `GET`/`HEAD` responses (other methods pass through). On a hit the stored response is replayed with an `X-Beni-Cache: hit` header; on a miss the handler runs and successful responses are stored with `SET PX ttlMs`. Responses that set cookies are never cached, and **every Redis failure fails open**: the request always runs.

```ts
import { Hono } from "hono";
import { cache } from "beni/hono";

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
| `key` | `method + ":" + path + query` | `(c) => string`, the cache key. |
| `vary` | `[]` | Header names folded into the key. |

Bodies are stored as text (`{ status, headers, body }` JSON), so this is for text-ish responses (JSON, HTML), not streaming or binary payloads.

## Sessions

These are **cookie-backed user sessions**, not [`redis.session()`](/beni/advanced/sessions/) connection leases, so they work on Workers and other edge runtimes with `beni/upstash`.

Redis-backed sessions behind a `sid` cookie. The record is a JSON object under `<prefix>:<id>`, loaded before your handler and persisted after it, but only when the handler actually wrote something (`SET ... EX ttlSeconds`, so the TTL rolls on every write). New sessions get a `crypto.randomUUID()` id and a `Set-Cookie` header; `clear()` deletes the stored record.

```ts
import { Hono } from "hono";
import { getSession, session } from "beni/hono";

const app = new Hono();
app.use("*", session({ client, ttlSeconds: 86_400 }));

app.post("/login", async (c) => {
  const user = await authenticate(c);
  getSession(c).set("userId", user.id);
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

Session values are `unknown` per key; `get<T>` is a convenience assertion, not a validation. The session is a convenience bag; codec-level typing belongs to your [Beni schemas](/beni/core-concepts/defining-schemas/).

## Putting it together

```ts
import { Hono } from "hono";
import { upstash } from "beni/upstash";
import { cache, getSession, ratelimit, session } from "beni/hono";

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});

const app = new Hono();

app.use("*", ratelimit({ client, limit: 100, windowMs: 60_000 }));
app.use("*", session({ client }));

app.get("/pricing", cache({ client, ttlMs: 60_000 }), (c) =>
  c.json({ plans: ["free", "pro"] })
);

app.post("/login", (c) => {
  getSession(c).set("userId", "u1");
  return c.json({ ok: true });
});

export default app;
```

The same file deploys to a Node server, a Bun process, Deno Deploy, or a Cloudflare Worker; only the adapter changes.

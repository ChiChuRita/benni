---
title: "Next.js"
description: "Redis-backed ISR caching and edge-ready rate limiting for Next.js: a custom cache handler and a middleware limiter in one import."
---

`benni/next` connects Next.js to Redis in the two places it matters: a **custom cache handler** so ISR/App-Router cache entries survive deploys and are shared across instances, and a **rate limiter** for middleware, route handlers, and Server Actions.

Both work over every adapter. On Vercel and other edge runtimes, pair them with [`benni/upstash`](/benni/runtime/edge/): middleware has no TCP, but the Upstash adapter needs nothing beyond `fetch`.

## Cache handler

Next.js caches pages, route handler output, and `fetch` data in local files by default, so each instance has its own cache and a deploy wipes it. A custom cache handler moves that storage to Redis.

```ts
// cache-handler.mjs
import { cacheHandler } from "benni/next";
import { upstash } from "benni/upstash";

export default cacheHandler({
  client: () =>
    upstash({
      url: process.env.UPSTASH_REDIS_REST_URL as string,
      token: process.env.UPSTASH_REDIS_REST_TOKEN as string
    })
});
```

```ts
// next.config.ts
const nextConfig = {
  cacheHandler: require.resolve("./cache-handler.mjs"),
  cacheMaxMemorySize: 0 // disable the per-instance in-memory cache
};

export default nextConfig;
```

`cacheHandler(options)` returns a class; Next.js instantiates the module's default export itself. Pass `client` as a lazy factory (as above) so no connection is opened when Next.js loads the module at build time; the factory is awaited once and cached.

| Option | Default | Meaning |
| --- | --- | --- |
| `client` | - | A `RedisClient`, a promise of one, or a lazy factory (awaited once). |
| `prefix` | `"next-cache"` | Key namespace. |
| `defaultTtlSeconds` | - | Safety-cap TTL for entries without a `revalidate` period. |

The handler matches the cache-handler shape of Next.js 14.1+; the Next.js 15 `resetRequestCache()` hook is a no-op on this handler (it keeps no request-local state). Reads fail open: an entry that does not decode is treated as a miss, never an error.

### How tags map to Redis keys

Each entry is stored as JSON under `<prefix>:entry:<key>`, with `SET ... EX <revalidate>` when the page declares a numeric `revalidate`, without a TTL when it opts out (`revalidate: false`), unless `defaultTtlSeconds` caps it. Each tag keeps a set of the keys written under it:

```
next-cache:entry:/blog          -> { value, lastModified, tags }
next-cache:tag:posts            -> SMEMBERS { "/blog", "/blog/post-1" }
```

A tag set is expired alongside the entries it names: every write extends the set to the entry's TTL, never shortens it, and an entry that never expires makes the set permanent. So a tag set is reclaimed once its last member has gone, instead of growing for the life of the deployment.

`revalidateTag("posts")` is then one `SMEMBERS` per tag plus a chunked `DEL` of the matching entries, followed by an `SREM` of exactly the members it saw, with no scans. Only the tags Next.js passes on `set()` (`ctx.tags`) feed the index.

## Rate limiting

`rateLimit(options)` wraps the [`ratelimit`](/benni/primitives/ratelimit/) primitive (an exact sliding window, one atomic Lua round trip per check) in a web-standard shape: give it a `Request`, get back `null` (allowed) or a finished `429 Response`.

```ts
// middleware.ts
import { rateLimit } from "benni/next";
import { upstash } from "benni/upstash";

const limiter = rateLimit({
  client: () =>
    upstash({
      url: process.env.UPSTASH_REDIS_REST_URL as string,
      token: process.env.UPSTASH_REDIS_REST_TOKEN as string
    }),
  limit: 20,
  windowMs: 10_000,
  identify: (request) =>
    request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"
});

export async function middleware(request: Request) {
  const denied = await limiter(request);
  if (denied) return denied;
}

export const config = { matcher: "/api/:path*" };
```

The denial response carries `Retry-After` (seconds) plus `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` (epoch seconds).

`identify` is required on purpose. There is no request property a limiter can trust without knowing the deployment: on a self-hosted Next.js, or behind a proxy that appends rather than replaces, `x-forwarded-for` is attacker-controlled, so a default built on it would let a caller vary one header to bypass the limit and mint a fresh Redis key every time. The snippet above is the right form on a platform whose edge overwrites the header, such as Vercel. Better still is an identity you authenticated yourself:

```ts
const limiter = rateLimit({
  client,
  limit: 100,
  windowMs: 60_000,
  identify: (request) => request.headers.get("x-api-key") ?? "anonymous"
});
```

### Server Actions

A Server Action has no `Request`. The limiter also exposes `.check(identity)`, which returns the raw [`RatelimitResult`](/benni/primitives/ratelimit/):

```ts
"use server";

export async function submitComment(formData: FormData) {
  const { success, resetMs } = await limiter.check(await getUserId());
  if (!success) {
    return { error: "Too many comments. Try again shortly.", resetMs };
  }
  // ...
}
```

## Which adapter where

- **Edge middleware**: [`benni/upstash`](/benni/runtime/edge/). The edge runtime has no TCP sockets; the Upstash adapter speaks HTTP with zero dependencies.
- **Route handlers / Server Actions on Node, and self-hosted deploys**: [`benni/node`](/benni/runtime/node/) for pooled TCP connections; `benni/upstash` also works if you are already on Upstash.
- **The cache handler** runs wherever your Next.js server runs and only needs `send`/`pipeline`, so either adapter fits.

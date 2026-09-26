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
| `defaultTtlSeconds` | - | TTL for entries Next.js gives no lifetime (`revalidate: false`), which otherwise never expire. |

**Supported: Next.js 15 and 16.** Verified end to end on 15.5 and 16.3 with a real app, two `next start` instances sharing one Redis. 15.0 to 15.2 pass the revalidate period in a different field, which the handler also reads. Next.js 14 is not supported. Reads fail open: an entry that does not decode is treated as a miss, never an error.

### What is stored, and for how long

Each entry lives under `{<prefix>}:entry:<key>`. Next.js values carry binary data: an App Router page has its RSC payload in a `Buffer` and its prefetch segments in a `Map` of Buffers, and a route handler's body is a `Buffer`. Plain JSON turns a Buffer into `{ type, data }` and a Map into `{}`, so the handler uses a tagged JSON encoding, and every value comes back from `get()` exactly as Next.js handed it to `set()`. A binary route handler's response is byte-identical after a round trip through Redis.

The TTL comes from what Next.js passes:

| Entry | Redis TTL |
| --- | --- |
| Page or route handler, Next.js 16 | `cacheControl.expire` (one year unless you set `expireTime`) |
| Page or route handler, Next.js 15 | `revalidate` |
| `fetch` data | its `revalidate`, at most one year |
| No lifetime (`revalidate: false`) | none, or `defaultTtlSeconds` |

The entry has to outlive `revalidate`: between `revalidate` and `expire`, Next.js serves the stale entry and regenerates it in the background, which is what makes ISR fast. Next.js 15 never gives the handler `expire`, so there an entry goes at `revalidate` and the first request after that waits for a fresh render.

### How tags map to Redis keys

Next.js puts tags in two places, and the handler reads both: a `fetch` entry's `set()` carries its tags, while a page or route handler records its tags in the `x-next-cache-tags` header of the stored value. That header includes the implicit path tags (`_N_T_/blog`) that `revalidatePath` targets. Each tag keeps a set of the keys written under it:

```
{next-cache}:entry:/blog          -> the page, with its RSC payload and segments
{next-cache}:tag:posts            -> SMEMBERS { "/blog", "<fetch cache key>" }
{next-cache}:tag:_N_T_/blog       -> SMEMBERS { "/blog" }
{next-cache}:revalidated:_N_T_/blog -> when that tag was last revalidated
```

The `{next-cache}` hash tag keeps every key in one Cluster slot, so the multi-key commands below work on a cluster.

A tag set is expired alongside the entries it names: every write extends the set to the entry's TTL, never shortens it, and an entry that never expires makes the set permanent. So a tag set is reclaimed once its last member has gone, instead of growing for the life of the deployment.

`revalidateTag("posts")` and `revalidatePath("/blog")` (a tag underneath) take two round trips, with no scans: one pipeline of `SMEMBERS` per tag, then one pipeline that deletes the matching entries in chunks and `SREM`s exactly the members it saw.

A fetch entry is indexed only under its own tags, so `revalidatePath` cannot reach the fetches a page made through the tag sets. Instead, `revalidateTag` also records when each tag was revalidated (kept for a year, the longest a fetch entry lives), and a fetch lookup checks its own tags and its route's implicit tags against those records in the same round trip as the `GET`. A fetch written before its route was revalidated is then a miss on every instance, not only the one that called `revalidatePath`.

Next.js 16's `revalidateTag(tag, "max")` asks for stale-while-revalidate. This handler expires immediately either way: the next request renders fresh rather than being served the stale entry once.

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

On Next.js 16, middleware is called proxy: the file is `proxy.ts` and the function is `export async function proxy(request: Request)`. The limiter is the same.

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

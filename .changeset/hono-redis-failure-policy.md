---
"benni": minor
---

`benni/hono` states what each middleware does when Redis fails, and `rateLimitMiddleware()` takes a `failOpen` option.

The three middlewares disagreed without saying so: `cacheMiddleware()` swallowed every Redis error while `rateLimitMiddleware()` and `sessionMiddleware()` let it propagate into a 500. Each choice was right for its job, and now it is written down in the JSDoc and on the Hono page. `cacheMiddleware()` fails open because a cache is an optimization. `sessionMiddleware()` fails closed because failing open would treat a signed-in visitor as anonymous and silently drop what the handler wrote. `rateLimitMiddleware()` fails closed by default, since a limiter that cannot count should not quietly stop limiting, and `failOpen: true` now lets requests through unlimited, without the `X-RateLimit-*` headers, for deployments that prefer availability. It covers only the limiter's own Redis round trip: errors from `key` and from the handler still propagate.

```ts
app.use(
  "*",
  rateLimitMiddleware({ client, limit: 100, windowMs: 60_000, key, failOpen: true })
);
```

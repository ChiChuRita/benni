---
"beni": minor
---

The rate-limit subject is now a required option: `key` on `ratelimit` from `beni/hono`, and
`identify` on `rateLimit` from `beni/next`. Both previously defaulted to the first
`x-forwarded-for` hop (with `cf-connecting-ip` and `"anonymous"` behind it on Hono).

That default was unsafe. There is no request property a limiter can trust without knowing the
deployment: on a self-hosted app the header is set by the client, and many proxies append to it
rather than replacing it, so a caller could pick its own identity, bypass the limit by varying one
header, and mint a separate Redis key on every request. Taking the subject from the caller matches
what `@upstash/ratelimit` does, and it makes the trust boundary explicit instead of implied.

To keep the old behaviour where your platform genuinely overwrites the header, pass it yourself:

```ts
// beni/next
identify: (request) =>
  request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"

// beni/hono
key: (c) => c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ?? "anonymous"
```

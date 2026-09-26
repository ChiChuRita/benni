---
"benni": patch
---

`upstash()` checks `url` and `token` on the first request instead of at construction, so `next build` no longer fails on a module that builds the client at top level.

Now that adapters return their client synchronously, a Next.js `cache-handler.mjs` or a `lib/redis.ts` builds `upstash({ url: process.env.UPSTASH_URL, token: process.env.UPSTASH_TOKEN })` when it is imported, and `next build` imports it in environments that never set those variables and never send a command. The eager check threw there and failed the build. The first command now rejects with the same message (`upstash() requires a url (the REST endpoint)`), before any request is made and without reaching `onError`, which is for failures in transit. `timeoutMs` is still checked at construction, since it is never an environment variable.

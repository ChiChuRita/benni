---
"benni": patch
---

`benni/next`'s `cacheHandler` now works with what Next.js 15 and 16 actually send: binary values round-trip byte for byte, entries get the TTL Next.js asks for, and `revalidateTag` and `revalidatePath` reach pages and `fetch` data on every instance.

The handler was written against the Next.js 14 contract, and three things had moved underneath it. Captured from a real Next.js 16.3 app:

```text
set("/static", { kind: "APP_PAGE", html, rscData: <Buffer 3495B>,
  segmentData: Map { "/_tree" => <Buffer>, ... },
  headers: { "x-next-cache-tags": "_N_T_/layout,...,_N_T_/static,posts" } },
  { cacheControl: { revalidate: 600, expire: 31536000 }, isFallback: false })
```

- **Values were corrupted.** `JSON.stringify` turned `rscData` and a route handler's `body` into `{ type: "Buffer", data: [...] }` and `segmentData` into `{}`, and nothing restored them on `get`. Values now use a tagged JSON encoding (`{ "$b": base64 }` for bytes, `{ "$m": [...] }` for a Map), so every value comes back as it went in. A caller's own data that looks like a tag is escaped, not misread. Entries written by 0.1.0 are read as misses and rewritten on the next render, rather than handed to Next.js broken.
- **No entry got a TTL.** The revalidate period now arrives in `ctx.cacheControl` for pages and route handlers (15.3+) and in `data.revalidate` for fetch entries, and the handler read only `ctx.revalidate`. A page now expires at `cacheControl.expire`, so Next.js can keep serving it stale while it regenerates, falling back to `revalidate` on Next.js 15, which does not pass `expire`. A fetch entry expires at its `revalidate`. `defaultTtlSeconds` now applies only where its docs said it did, to entries Next.js gives no lifetime. Before, on 15.3+, it applied to every entry.
- **Revalidation missed pages.** A page's `set()` context has no tags: they are in the value's `x-next-cache-tags` header, which is also where the implicit `_N_T_/<path>` tag behind `revalidatePath` lives. The handler now indexes those. A fetch entry is only indexed under its explicit tags, so `revalidateTag` also records when each tag was revalidated. A fetch lookup then checks its own and its route's implicit tags against those records in the same round trip as its `GET`, which lets `revalidatePath("/blog")` refresh the data `/blog` fetched, even on an instance that never saw the call.

`revalidateTag` now takes two round trips however many tags and keys it touches. The chunked `DEL`s and `SREM`s go out as one pipeline, where each used to be its own round trip.

Verified end to end on Next.js 16.3.6 and 15.5.26: two `next start` instances shared one Redis, and curl drove them. Entries landed with the expected TTLs, and a binary route handler's body kept its sha256 when served from Redis by the instance that never rendered it. `revalidateTag` on one instance and `revalidatePath` on the other each refreshed the page and its fetch on the opposite instance. The unit tests use payloads captured from that app. The [Next.js page](https://chichurita.github.io/benni/integrations/nextjs/) states the supported range (15 and 16) and the TTL table, and it notes that `middleware.ts` is `proxy.ts` on Next.js 16.

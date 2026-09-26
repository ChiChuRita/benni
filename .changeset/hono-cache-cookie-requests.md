---
"benni": minor
---

**Breaking:** `benni/hono`'s `cache()` no longer reads or stores the cache for requests that carry a `Cookie` header, unless you opt in.

A route authenticated by a cookie through middleware Benni cannot see (your own auth check, a third-party session library) sends no `Set-Cookie` and never touches Benni's `session()`, so neither storage guard fired: the first visitor's page was stored under a key shared by everyone and replayed to the next visitor. Cookies are credentials like `Authorization`, which already bypassed the cache, so the safe behaviour is now the default. Two explicit ways back in:

```ts
// Before: cached and shared across visitors, cookies or not
app.get("/pricing", cache({ client, ttlMs: 60_000 }), handler);

// After: per visitor, keyed on the exact Cookie header
app.get("/account", cache({ client, ttlMs: 60_000, vary: ["cookie"] }), handler);

// After: one shared entry, when nothing behind it depends on a cookie
app.get("/pricing", cache({ client, ttlMs: 60_000, ignoreCookies: true }), handler);
```

Routes that read Benni's own `session()` stay unstorable either way. Expect a lower hit rate on sites where every visitor carries some cookie (a session id, analytics) until you opt the public routes in with `ignoreCookies`.

---
"beni": minor
---

Close a set of correctness and safety holes in the `beni/hono` middleware, and add session id rotation.

`cache()` no longer stores a response that was derived from the session identity. The touched-tracking that keeps a per-user response out of a shared cache only fired on `get`/`set`/`delete`/`clear`, so a handler that returned something built from `session.id` or `session.isNew` slipped past it. A live session id was then stored under a session-independent key and replayed to every later visitor, who could send it back as their own cookie. Reading the bag at all now counts, whether you reach it through `getSession(c)` or `c.get("session")`.

`Session` gains `regenerate()`. It mints a fresh id, carries the current data over, deletes the record under the old id, and issues a new `Set-Cookie`. There was previously no way to rotate a session id and no code path that could re-issue the cookie for an existing session, so the standard fixation defence, renew the identifier on login and on privilege change, was unavailable. Call it on login.

Sessions loaded from an existing record are now written back with `SET ... XX`. A request already in flight when a concurrent `clear()` deleted the record used to re-store its stale snapshot with a fresh full lifetime, re-authenticating the session the user had just logged out of.

`cache()` is stricter about what it will store. Only a plain `200` is storable, so a `206` built for someone else's `Range` header can no longer be replayed, with `Content-Range` stripped, to clients that sent no `Range` at all; ranged requests pass straight through. A response saying `no-store`, `no-cache`, or `private` in `Cache-Control` is respected, as is a `Vary` naming a header the key does not fold in (`Vary: *` is never stored). Stored entries now keep `cache-control`, `vary`, `etag`, and `last-modified` alongside `content-type`, so a replay stays honest to the browser and to any CDN in front of you.

The default cache key includes the request origin, so one app bound to several hostnames no longer shares a single entry per path across them. This changes the default key format, which means existing entries are missed once and rebuilt. Callers who want cross-host sharing can restore the old behaviour with `key: (c) => { const url = new URL(c.req.url); return c.req.method + ":" + url.pathname + url.search; }`.

`ratelimit()` sets `X-RateLimit-Limit`, `-Remaining`, and `-Reset` after the handler runs rather than before. Set before, they lived in Hono's prepared-header bag and were dropped whenever anything downstream returned a fresh `Response`, including `cache()` on every hit, so the documented headers vanished on exactly the cheapest requests.

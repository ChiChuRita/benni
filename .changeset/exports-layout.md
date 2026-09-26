---
"benni": minor
---

**Breaking:** the adapter-author API moves to `benni/core`, and the Hono and Next.js middleware get names that say they are middleware.

The root `benni` entry is now `benni()`, its errors, and the types an app names. The client contract and the helpers for writing an adapter moved to `benni/core`: `resolveClient`, `ClientSource`, `ClientProvider`, `redisServerError`, `createScriptRunner`, `defineScript` (with `DefineScriptOptions`, `RedisScript`, `ScriptRunner`), and the contract types `RedisClient`, `RedisCommand`, `RedisCommandArgument`, `RedisReply`, `RedisSession`, and `RedisSubscriber`. `benni/core` no longer repeats the schema builders under `define*` names (`defineHash`, `defineKeyspace`, …); declare schemas from `benni/schema` (`hash`, `kv`, …).

```ts
// Before
import { benni, defineScript, type RedisReply } from "benni";
import { defineHash } from "benni/core";

// After
import { benni } from "benni";
import { defineScript, type RedisReply } from "benni/core";
import { hash } from "benni/schema";
```

In `benni/hono`, `ratelimit`, `cache`, and `session` shared their names with the `ratelimit`/`cache` schema builders and with `redis.session()`. They are now `rateLimitMiddleware`, `cacheMiddleware`, and `sessionMiddleware`, with options types `RateLimitMiddlewareOptions`, `CacheMiddlewareOptions`, and `SessionMiddlewareOptions`. In `benni/next`, `rateLimit` is now `rateLimitMiddleware`; `cacheHandler` is unchanged.

```ts
// Before
import { cache, ratelimit, session } from "benni/hono";
import { rateLimit } from "benni/next";

// After
import { cacheMiddleware, rateLimitMiddleware, sessionMiddleware } from "benni/hono";
import { rateLimitMiddleware } from "benni/next";
```

---
title: "Rate Limiting (primitive)"
description: "A sliding-window rate limiter over Redis: one atomic round trip per check, accurate, and edge-ready."
---

`ratelimit` is a sliding-window rate limiter. Each `check(id)` is a single atomic Lua round trip that drops expired entries, counts the window, and admits the request if it is under the limit.

```ts
import { ratelimit } from "benni/primitives";

const limiter = ratelimit(client, { limit: 10, windowMs: 60_000 });

const { success, remaining, resetMs } = await limiter.check(userId);
if (!success) {
  throw new Response("Too Many Requests", {
    status: 429,
    headers: { "Retry-After": String(Math.ceil((resetMs - Date.now()) / 1000)) }
  });
}
```

`ratelimit` takes any `RedisClient`, so it runs over every adapter, including [`benni/upstash`](/benni/runtime/edge/) on the edge, which is where rate limiting is most often needed.

## The result

```ts
type RatelimitResult = {
  success: boolean;   // is this request allowed?
  limit: number;      // the configured limit
  remaining: number;  // requests left in the window (0 when denied)
  resetMs: number;    // epoch-ms when the window next frees a slot
};
```

## How it works

The window is a **log of request timestamps in one sorted set** (a single key, so it is Redis Cluster safe). That makes the limit exact (no fixed-window boundary bursts) at the cost of storing up to `limit` entries per key. For typical API limits (tens to hundreds per window) that is ideal; for very high per-key rates, prefer a counter-based limiter.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `limit` | - | Maximum requests allowed within the window. |
| `windowMs` | - | Window length in milliseconds. |
| `prefix` | `"ratelimit"` | Key namespace; keys are `<prefix>:<id>`. |

Use a stable `id` per subject: a user id, API key, or IP. Each id is limited independently.

See [Rate Limiting patterns](/benni/patterns/rate-limiting/) for the underlying Redis approach if you want to roll your own.

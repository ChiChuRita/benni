---
title: "Rate Limiting From Scratch"
description: "Cap requests per window with an atomic INCR + EXPIRE Lua script."
---

:::tip
For most apps, reach for the first-class [`ratelimit` primitive](/beni/primitives/ratelimit/), a sliding-window limiter in one atomic call. This page shows the underlying fixed-window pattern if you want to roll your own.
:::

Rate limiting caps how many actions a caller may take in a time window. The classic Redis approach is a **fixed window** counter: `INCR` a per-caller key and set its TTL to the window on the first hit. Doing both inside one `script()` keeps the increment and the expiry atomic, so a burst can never leave a counter without a TTL, the bug that turns a rate limiter into a permanent lockout.

## Define the limiter script

```ts
import { number, script } from "beni/schema";

export const rateLimit = script("rate-limit", {
  keys: ["counter"],
  args: { windowSeconds: number() },
  returns: number(),
  lua: `
    local current = redis.call("INCR", KEYS[1])
    if current == 1 then
      redis.call("EXPIRE", KEYS[1], ARGV[1])
    end
    return current
  `
});
```

Add it to your bound `{ schema }` to reach it as `redis.query.rateLimit`, or call it directly with `redis.script(rateLimit)`.

## Check a limit

```ts
const WINDOW_SECONDS = 60;
const MAX_PER_WINDOW = 100;

async function allow(userId: string): Promise<boolean> {
  const count = await redis.script(rateLimit).run({
    keys: { counter: `ratelimit:${userId}` },
    args: { windowSeconds: WINDOW_SECONDS }
  });
  return count <= MAX_PER_WINDOW;
}
```

`INCR` returns the running count for the window. The first call in a window creates the key and arms its TTL; the window resets when the key expires, so there is nothing to clean up.

## Use it in a request handler

```ts
async function handle(request: Request, userId: string): Promise<Response> {
  if (!(await allow(userId))) {
    return new Response("Too Many Requests", { status: 429 });
  }
  return serve(request);
}
```

## Report the remaining budget

`script()` decodes a single scalar, so the counter comes back as a number. Derive the rest on the client and surface it in headers:

```ts
const count = await redis.script(rateLimit).run({
  keys: { counter: `ratelimit:${userId}` },
  args: { windowSeconds: WINDOW_SECONDS }
});
const remaining = Math.max(0, MAX_PER_WINDOW - count);
const allowed = count <= MAX_PER_WINDOW;
```

## Beyond fixed windows

- **Sliding window**: key on a rolling bucket (`Math.floor(Date.now() / 1000 / WINDOW)`) and sum the current and previous buckets, weighted by how far into the window you are. Same `script()` shape, a little more Lua.
- **Token bucket**: store the token count and last-refill timestamp in a `hash`, refilling on read. Combine [per-field TTL](/beni/data-structures/hashes/#field-expiration) with a `script()` for the atomic refill-and-take.

The fixed window is the simplest correct choice and the right default; reach for the others only when smoothing bursts across the boundary actually matters.

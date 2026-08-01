---
title: "Scripts"
description: "Use the script() builder to define Lua scripts with named keys, typed args, and a typed return value."
---

Use the `script()` builder to define Lua scripts with named keys, typed arguments, and a typed return value.

## Define A Script

```ts
import { number, script } from "beni/schema";

export const rateLimit = script("rate-limit", {
  keys: ["counter"],
  args: { limit: number(), windowSeconds: number() },
  returns: number(),
  lua: `
    local current = redis.call("INCR", KEYS[1])
    if current == 1 then
      redis.call("EXPIRE", KEYS[1], ARGV[2])
    end
    if current > tonumber(ARGV[1]) then
      return 0
    end
    return current
  `
});
```

Keys are named and map to `KEYS[1..n]` in declared order. Args encode through their codecs and map to `ARGV[1..n]` in declared order.

## Run A Script

```ts
const current = await redis.script(rateLimit).run({
  keys: { counter: "rate:user:42" },
  args: { limit: 100, windowSeconds: 60 }
});
//    ^? number
```

Both `keys` and `args` are checked at compile time: missing or misspelled names are type errors.

## EVALSHA Caching

The first run loads the script with `SCRIPT LOAD` and caches its SHA per client. Later runs send only the hash via `EVALSHA`. When Redis replies with `NOSCRIPT` (after a server restart or `SCRIPT FLUSH`), Beni reloads the script and retries automatically, so callers never see the error.

A script can also return a `NOSCRIPT`-coded error of its own, and Redis passes it through byte for byte, so the message alone cannot tell the two apart. Before reloading a cached SHA, Beni asks the server with `SCRIPT EXISTS`: if the script is still there, the error came from the script and is raised as-is, rather than re-running side effects the script has already applied.

## Scalar Returns Only

`returns` decodes scalar replies (strings and numbers) through its codec. A script that returns a table or nil throws `TypeError: Expected Redis script reply to decode from scalar`.

For structured replies, drop to `defineScript` from the main entrypoint and decode the raw reply yourself:

```ts
import { createScriptRunner, defineScript } from "beni";

const topTwo = defineScript<[], string[]>({
  lua: `return redis.call("ZREVRANGE", KEYS[1], 0, 1)`,
  keyCount: 1,
  decode(reply) {
    if (!Array.isArray(reply)) {
      throw new TypeError("Expected Redis script reply to return array");
    }
    return reply as string[];
  }
});

const runner = createScriptRunner(client);
const top = await runner.run(topTwo, ["leaderboard:global"], []);
```

`defineScript` uses positional keys and args instead of named ones, but the same `EVALSHA` caching and `NOSCRIPT` recovery apply.

---
title: "Why Benni?"
description: "Why add a typed schema layer when node-redis and ioredis are already typed? Because they type the commands, not your data."
---

Redis is often used like this, here with raw `node-redis`:

```ts
await nodeRedis.hSet(`user:${id}`, {
  name: user.name,
  score: String(user.score)
});

const raw = await nodeRedis.hGetAll(`user:${id}`);
const loadedUser = {
  name: raw.name,
  score: Number(raw.score)
};
```

`node-redis` even types that `hGetAll` call, as `Record<string, string>`. The types are *present*, but they describe Redis's wire shape, not your data: `score` comes back a `string`, and you coerce it by hand. This works, but over time it creates problems:

- Key names are spread across the codebase.
- Values are manually serialized and parsed.
- Return types are not obvious.
- Data structures are implicit.
- Refactoring is risky.
- Raw Redis commands are powerful but easy to misuse.

Benni keeps Redis explicit, but adds a typed schema layer:

```ts
export const users = hash("user", {
  name: string(),
  score: number()
});

await redis.hash(users).hset(id, {
  name: "Ada",
  score: 10
});

const user = await redis.hash(users).hget(id);
```

You still use Redis. You still understand what happens. You just stop scattering strings and parsers across your app.

## Benni And Redis Clients

Benni is not a replacement for Redis. It is a typed layer on top of a Redis client: the client types the commands, Benni types your data.

| Feature | node-redis / ioredis | Benni |
| --- | --- | --- |
| Raw Redis commands | Yes | Yes |
| Command-level types | Yes (wire shape) | Yes |
| Typed schemas | Manual | Yes |
| Typed hash / JSON values | Manual | Yes |
| Key prefixes | Manual | Schema-based |
| Runtime reach | Node only | Node, Bun, Deno, edge/serverless |
| Escape hatch | Native | `redis.raw` |

Use a raw client when you want direct command access everywhere. Use Benni when your application has repeated Redis data patterns and you want your declared types to survive the round-trip, safer keys, and better refactoring, across every runtime.

## What It Measures Out To

Three apps built twice against Redis 8, once through Benni and once through raw `node-redis`, feature for feature. Lines of implementation code, blank lines excluded:

| App | Benni | Raw |
| --- | --- | --- |
| URL shortener (hash, counter, sorted set, stream, cache, rate limit) | 97 | 171 |
| AI generation service (`queue`: resumable stream, cancel, retries) | 45 | 437 |
| Realtime presence and payouts (sessions, leaderboard, pub/sub, `WATCH`, lock) | 103 | 197 |

Plain typed reads and writes come out about even. What Benni saves is the code around them: the raw column carries a sliding-window limiter, a read-through cache with single-flight, a token-fenced lock, and a queue with heartbeat leases, six hand-written Lua scripts in total. Reach for BullMQ and a limiter package instead and the counts converge again, at the price of several more dependencies that still hand your data back as `string | null`.

Nine ordinary Redis bugs planted in both versions (a typo'd hash field, a wrong value type, a missing required field, a read of an undeclared field, a nullable read treated as non-null, a counter reply used as a string, the wrong store kind, an undeclared event shape published to a typed channel, and a number member in a string-member sorted set) were **nine compile errors through Benni and four through the raw version**, which had a hand-written typed edge of its own. The five it missed were the quiet ones: the typo added a second field rather than replacing one, a date string in a numeric slot read back as `NaN`, a partial write left a partial record, and a field nobody writes read as `undefined`. Only the wrong store kind threw.

## What The Types Cost

Nothing you can measure at runtime: 2,000 sequential ops against a local Redis, seven interleaved reps, medians of 166 ms for Benni's `hset` against 162 ms for `node-redis`'s `hSet`, and 171 ms against 161 ms for the read. Three to six percent, inside the run-to-run spread, on a loopback with no real round trip to hide behind.

At compile time it is cheaper than not using it. The same three apps under `tsc --extendedDiagnostics`:

| | Types | Instantiations | Check time |
| --- | --- | --- | --- |
| Benni versions | 8,766 | 13,774 | 0.15s |
| Raw `node-redis` versions | 33,695 | 198,061 | 0.54s |

A schema layer sounds like something that slows an editor down. `node-redis`'s own command generics cost roughly 14 times the type instantiations that Benni's typed surface does, so in practice the schema layer is the cheap part.

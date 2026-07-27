---
title: "Why Beni?"
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

Beni keeps Redis explicit, but adds a typed schema layer:

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

## Beni And Redis Clients

Beni is not a replacement for Redis. It is a typed layer on top of a Redis client: the client types the commands, Beni types your data.

| Feature | node-redis / ioredis | Beni |
| --- | --- | --- |
| Raw Redis commands | Yes | Yes |
| Command-level types | Yes (wire shape) | Yes |
| Typed schemas | Manual | Yes |
| Typed hash / JSON values | Manual | Yes |
| Key prefixes | Manual | Schema-based |
| Runtime reach | Node only | Node, Bun, Deno, edge/serverless |
| Escape hatch | Native | `redis.raw` |

Use a raw client when you want direct command access everywhere. Use Beni when your application has repeated Redis data patterns and you want your declared types to survive the round-trip, safer keys, and better refactoring, across every runtime.

---
title: "Sessions"
description: "Sessions are a natural fit for JSON key-value entries with TTL."
---

Sessions are a natural fit for JSON key-value entries with TTL.

```ts
import { json, kv } from "beni/schema";

type Session = {
  userId: string;
  createdAt: string;
};

export const sessions = kv("session", json<Session>());
```

Create a session:

```ts
await redis.kv(sessions).set(
  sessionId,
  {
    userId: "42",
    createdAt: new Date().toISOString()
  },
  {
    ttlSeconds: 60 * 60 * 24 * 7
  }
);
```

Read a session:

```ts
const session = await redis.kv(sessions).get(sessionId);
```

Extend a session:

```ts
await redis.kv(sessions).expire(sessionId, 60 * 60 * 24 * 7);
```

Delete a session:

```ts
await redis.kv(sessions).del(sessionId);
```

Raw Redis equivalent:

```ts
await redis.set(`session:${sessionId}`, JSON.stringify(session), {
  EX: 60 * 60 * 24 * 7
});
```

## Field-level expiry with hashes

When parts of a session expire on different schedules — say a short-lived CSRF token alongside a week-long identity — model it as a `hash` and give each field its own TTL. Redis 8 sets the values and their expiry atomically with `HSETEX`:

```ts
import { hash, number, string } from "beni/schema";

export const sessionData = hash("session", {
  userId: string(),
  csrfToken: string(),
  lastSeen: number()
});
```

```ts
// Identity lives for a week; the CSRF token for an hour.
await redis.hash(sessionData).hsetex(
  sessionId,
  { userId: "42", lastSeen: Date.now() },
  { ttlSeconds: 60 * 60 * 24 * 7 }
);
await redis.hash(sessionData).hsetex(
  sessionId,
  { csrfToken: token },
  { ttlSeconds: 60 * 60 }
);

// Read the identity and slide its TTL in one round trip (HGETEX).
const identity = await redis.hash(sessionData).hgetex(
  sessionId,
  ["userId", "lastSeen"],
  { ttlSeconds: 60 * 60 * 24 * 7 }
);
```

The CSRF token expires on its own an hour in while the identity fields keep the session alive for a week — no separate keys, and each field carries its own clock. See [Field Expiration](/beni/data-structures/hashes/#field-expiration).

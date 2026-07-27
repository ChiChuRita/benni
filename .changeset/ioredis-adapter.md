---
"beni": minor
---

Add `beni/ioredis` — a full adapter for [ioredis](https://www.npmjs.com/package/ioredis), the most widely deployed Redis client for Node.

Until now Beni's Node story required node-redis, so trying it meant migrating your
data layer *and* your Redis client. This removes the second migration. It accepts
a URL, ioredis options, or — the point — an ioredis instance you already have:

```ts
import Redis from "ioredis";
import { ioredis } from "beni/ioredis";

const existing = new Redis(process.env.REDIS_URL); // yours, already tuned
const client = await ioredis(existing);
```

An adopted client is borrowed: `close()` reaps the sessions and subscriber
connections Beni leased from it and leaves the client itself open, because the
caller still owns its lifetime. Beni also attaches an `"error"` listener only to
clients it creates, so it never swallows errors on a client it does not own.

The adapter passes the shared client-contract suite in full — sessions with
blocking reads, `WATCH` transactions (including the abort-to-`null` signal and
per-command errors inside a committed EXEC), prompt close during a blocked read,
Pub/Sub with pattern subscriptions, and the parent-close leak backstops. Sessions
disable reconnection and the offline queue so a drop fails fast instead of
silently losing `WATCH` state, and close via `disconnect()` so an in-flight
blocking read is rejected at once rather than waiting out its timeout. ioredis
speaks RESP2, whose flat reply shapes are what the typed stores already decode, so
replies need no normalization.

`ioredis` is an optional peer dependency, so nothing changes for existing users.

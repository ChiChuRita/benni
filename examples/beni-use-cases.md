# Beni Use Cases

These examples pressure-test Beni against common application workloads. They use the schema-first API: define Redis resources as TypeScript values, bind a client once, then use `redis.query` or the explicit `redis.<kind>(schema)` accessors.

## Shared Setup

```ts
// redis.ts
import { beni } from "beni";
import { node } from "beni/node";
import * as schema from "./schema";

const client = await node({
  url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
});

export { client };
export const redis = beni(client, { schema });
```

## 1. SaaS User Profiles And Team Membership

```ts
// schema.ts
import { boolean, enumOf, hash, json, kv, set, string } from "beni/schema";

type Preferences = {
  theme: "light" | "dark";
  timezone: string;
};

export const users = hash("user", {
  email: string(),
  name: string(),
  active: boolean(),
  plan: enumOf(["free", "pro", "enterprise"])
});

export const userPreferences = kv("user:preferences", json<Preferences>());
export const teamMembers = set("team:members", string());
```

```ts
// app.ts
import { redis } from "./redis";

await redis.query.users.hset("u_42", {
  email: "ada@example.com",
  name: "Ada Lovelace",
  active: true,
  plan: "pro"
});

await redis.query.userPreferences.set("u_42", {
  theme: "dark",
  timezone: "Europe/London"
});

await redis.query.teamMembers.sadd("team_1", ["u_42"]);

const user = await redis.query.users.hget("u_42");
const preferences = await redis.query.userPreferences.get("u_42");
const teammates = await redis.query.teamMembers.smembers("team_1");
```

## 2. API Session Store With Rolling TTL

```ts
// schema.ts
import { json, kv } from "beni/schema";

export type Session = {
  userId: string;
  issuedAt: number;
  scopes: string[];
};

export const sessions = kv("session", json<Session>());
```

```ts
// auth.ts
import type { Session } from "./schema";
import { redis } from "./redis";

const ttlSeconds = 60 * 60 * 24 * 7;

export async function createSession(token: string, session: Session) {
  await redis.query.sessions.set(token, session, { ttlSeconds, nx: true });
}

export async function readSession(token: string) {
  const session = await redis.query.sessions.get(token);
  if (session) {
    await redis.query.sessions.expire(token, ttlSeconds);
  }
  return session;
}

export async function revokeSession(token: string) {
  await redis.query.sessions.del(token);
}
```

## 3. Product Analytics: Unique Visitors And Daily Activity

```ts
// schema.ts
import { bitmap, hll, string } from "beni/schema";

export const pageVisitors = hll("analytics:page-visitors", string());
export const dailyActiveUsers = bitmap("analytics:dau");
```

```ts
// analytics.ts
import { redis } from "./redis";

export async function recordPageView(pageId: string, userId: string, userOrdinal: number) {
  const day = new Date().toISOString().slice(0, 10);

  await redis.query.pageVisitors.pfadd(`${day}:${pageId}`, [userId]);
  await redis.query.dailyActiveUsers.setbit(day, userOrdinal, true);
}

export async function readDailyDashboard(pageId: string) {
  const day = new Date().toISOString().slice(0, 10);
  const uniquePageVisitors = await redis.query.pageVisitors.pfcount(`${day}:${pageId}`);
  const activeUsers = await redis.query.dailyActiveUsers.bitcount(day);

  return { activeUsers, uniquePageVisitors };
}
```

## 4. Leaderboards With Typed Profiles

```ts
// schema.ts
import { hash, number, string, zset } from "beni/schema";

export const players = hash("player", {
  displayName: string(),
  country: string(),
  level: number()
});

export const leaderboards = zset("leaderboard", string());
```

```ts
// leaderboard.ts
import { redis } from "./redis";

export async function submitScore(userId: string, score: number) {
  await redis.query.leaderboards.zadd("global", [{ member: userId, score }]);
}

export async function topPlayers() {
  const top = await redis.query.leaderboards.zrange("global", {
    start: 0,
    stop: 9,
    rev: true,
    withScores: true
  });

  return Promise.all(
    top.map(async ({ member: userId, score }) => ({
      userId,
      score,
      profile: await redis.query.players.hget(userId)
    }))
  );
}
```

## 5. Background Job Queue With A Worker Session

```ts
// schema.ts
import { json, list } from "beni/schema";

export type Job = {
  id: string;
  kind: "send-email" | "rebuild-report";
  payload: unknown;
};

export const jobs = list("jobs", json<Job>());
```

```ts
// worker.ts
import type { Job } from "./schema";
import { redis } from "./redis";

export async function enqueue(job: Job) {
  await redis.query.jobs.rpush("pending", [job]);
}

export async function runWorker() {
  await redis.session(async (session) => {
    while (true) {
      const job = await session.list(jobs).blmove(
        "pending",
        "processing",
        "left",
        "right",
        { timeoutSeconds: 5 }
      );

      if (!job) continue;

      try {
        await handleJob(job);
      } finally {
        await session.list(jobs).lrem("processing", 1, job);
      }
    }
  });
}
```

## 6. Audit Log With Consumer Groups

```ts
// schema.ts
import { number, stream, string } from "beni/schema";

export const auditEvents = stream("audit", {
  actorId: string(),
  action: string(),
  createdAt: number()
});
```

```ts
// audit.ts
import { redis } from "./redis";

export async function writeAuditEvent(actorId: string, action: string) {
  await redis.query.auditEvents.xadd("app", {
    actorId,
    action,
    createdAt: Date.now()
  });
}

export async function processAuditEvents() {
  const group = redis.query.auditEvents.group("indexers");
  await group.create("app", { from: "start", mkstream: true });

  const worker = group.consumer(`worker-${process.pid}`);
  const batch = await worker.xreadgroup("app", { count: 50 });

  for (const entry of batch) {
    await indexAuditEvent(entry.value);
    await worker.xack("app", [entry.id]);
  }
}
```

## 7. Realtime Notifications With Typed Pub/Sub

```ts
// schema.ts
import { channel, json, pattern } from "beni/schema";

type Notification = {
  id: string;
  userId: string;
  title: string;
};

export const tenantNotifications = channel("notifications:tenant:acme", json<Notification>());
export const allTenantNotifications = pattern("notifications:tenant:*", json<Notification>());
```

```ts
// notifications.ts
import { beni } from "beni";
import { node } from "beni/node";
import * as schema from "./schema";

const client = await node();
const redis = beni(client, { schema });

// The first subscribe leases one subscriber connection off the bound client and
// closes it again when the last subscription goes away.
const subscription = await redis.query.allTenantNotifications.subscribe((message, channel) => {
  console.log("received", { channel, message });
});

await redis.query.tenantNotifications.publish({
  id: "n_1",
  userId: "u_42",
  title: "Report finished"
});

await subscription.unsubscribe();
await client.close();
```

## 8. Store Locator With Geo Queries

```ts
// schema.ts
import { geo, string } from "beni/schema";

export const stores = geo("stores", string());
```

```ts
// stores.ts
import { redis } from "./redis";

await redis.query.stores.geoadd("berlin", [
  { member: "store_1", longitude: 13.405, latitude: 52.52 },
  { member: "store_2", longitude: 13.3777, latitude: 52.5163 }
]);

export async function nearbyStores(longitude: number, latitude: number) {
  return redis.query.stores.geosearch("berlin", {
    from: { longitude, latitude },
    by: { radius: 5, unit: "km" },
    sort: "ASC",
    count: 10
  });
}
```

## 9. Edge Rate Limiting With Upstash

```ts
// edge-rate-limit.ts
import { ratelimit } from "beni/primitives";
import { upstash } from "beni/upstash";

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!
});

const limiter = ratelimit(client, {
  prefix: "api:ratelimit",
  limit: 100,
  windowMs: 60_000
});

export async function guardRequest(userId: string) {
  const result = await limiter.check(userId);
  if (!result.success) {
    return new Response("Too Many Requests", {
      status: 429,
      headers: {
        "Retry-After": String(Math.ceil((result.resetMs - Date.now()) / 1000))
      }
    });
  }
}
```

## 10. Autocomplete Index With Lexicographic Sorted Sets

```ts
// schema.ts
import { zset, string } from "beni/schema";

export const nameIndex = zset("search:name", string());
```

```ts
// search.ts
import { redis } from "./redis";

export async function indexName(name: string) {
  const normalized = name.trim().toLowerCase();
  await redis.query.nameIndex.zadd("users", [{ member: normalized, score: 0 }]);
}

export async function autocomplete(prefix: string) {
  const min = prefix.toLowerCase();
  const max = `${min}\xff`;

  return redis.query.nameIndex.zrange("users", {
    byLex: true,
    min: { value: min },
    max: { value: max },
    offset: 0,
    count: 10
  });
}
```

## 11. Inventory Reservation With Optimistic Transactions

```ts
// schema.ts
import { kv, number } from "beni/schema";

export const stock = kv("inventory:stock", number());
```

```ts
// inventory.ts
import { numberReply, okReply } from "beni";
import { redis } from "./redis";
import { stock } from "./schema";

export async function reserveSku(sku: string, quantity: number) {
  return redis.watch(
    stock.key(sku),
    async (session) => {
      const available = (await session.kv(stock).get(sku)) ?? 0;
      if (available < quantity) return null;

      return session
        .multi()
        .add(["DECRBY", stock.key(sku), quantity], numberReply)
        .add(["SET", `inventory:reservation:${sku}`, String(quantity)], okReply);
    },
    { attempts: 5 }
  );
}
```

## 12. Scheduled Work With Sorted Sets And Locks

```ts
// schema.ts
import { json, zset } from "beni/schema";

export type ScheduledJob = {
  id: string;
  runAt: number;
  payload: unknown;
};

export const scheduledJobs = zset("scheduled-jobs", json<ScheduledJob>());
```

```ts
// scheduler.ts
import { lock } from "beni/primitives";
import type { ScheduledJob } from "./schema";
import { client, redis } from "./redis";

const locks = lock(client, { prefix: "scheduled-job-lock", ttlMs: 30_000 });

export async function schedule(job: ScheduledJob) {
  await redis.query.scheduledJobs.zadd("default", [{ member: job, score: job.runAt }]);
}

export async function runDueJobs() {
  const due = await redis.query.scheduledJobs.zrange("default", {
    byScore: true,
    min: "-inf",
    max: Date.now(),
    offset: 0,
    count: 25
  });

  for (const job of due) {
    await locks.run(job.id, async () => {
      await handleJob(job);
      await redis.query.scheduledJobs.zrem("default", [job]);
    });
  }
}
```

## What These Examples Have In Common

Every workload above uses the same three moves: declare the key family as a
schema, reach it through the bound handle, and let the codec carry the type
across the Redis boundary. Nothing here needed a migration, an entity class, or
a cast.

Where a workload needs one of the hard parts done right, it reaches for a
primitive instead of hand-rolling it: `cache` for read-through with stampede
protection, `lock` for mutual exclusion, `ratelimit` for a sliding window, and
for at-least-once queue processing, the consumer-group and blocking-worker
patterns in the [docs](../docs/src/content/docs/advanced/blocking-operations.md).

Anything Beni does not type yet stays reachable through `redis.raw.send([...])`,
so no use case is blocked on the typed surface catching up.

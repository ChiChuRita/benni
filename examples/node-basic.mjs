// The schema-first API from the quickstart: declare schemas once, bind a
// client, and every read comes back as your declared type.
import { beni } from "beni";
import { node } from "beni/node";
import { hash, json, kv, list, number, set, string, zset } from "beni/schema";

const redisUrl =
  process.env.BENI_REDIS_URL ??
  process.env.REDIS_URL ??
  "redis://127.0.0.1:6379";

const schema = {
  profiles: kv("example:profile", json()),
  counters: kv("example:counter", number()),
  users: hash("example:user", {
    name: string(),
    score: number()
  }),
  roles: set("example:roles", string()),
  jobs: list("example:jobs", json()),
  leaderboard: zset("example:leaderboard", string())
};

const client = await node({ url: redisUrl });
const redis = beni(client, { schema });
const id = `demo:${Date.now()}`;

try {
  await redis
    .kv(schema.profiles)
    .set(id, { name: "Ada", score: 10 }, { ttlSeconds: 60 });
  const profile = await redis.kv(schema.profiles).get(id);

  const visits = await redis.counter(schema.counters).incrby(id, 3);

  await redis.hash(schema.users).hset(id, { name: "Ada", score: 10 });
  await redis.hash(schema.users).hincrby(id, "score", 5);
  const user = await redis.hash(schema.users).hget(id);

  await redis.set(schema.roles).sadd(id, ["admin", "editor"]);
  const userRoles = await redis.set(schema.roles).smembers(id);

  await redis.list(schema.jobs).rpush(id, [
    { id: "job-1", kind: "email" },
    { id: "job-2", kind: "report" }
  ]);
  const nextJob = await redis.list(schema.jobs).lpop(id);

  await redis.zset(schema.leaderboard).zadd("daily", [
    { member: "ada", score: 15 },
    { member: "grace", score: 12 }
  ]);
  const topScores = await redis.zset(schema.leaderboard).zrange("daily", {
    start: 0,
    stop: -1,
    withScores: true
  });

  const fullKey = redis.hash(schema.users).key(id); // "example:user:demo:…"
  const pong = await redis.raw.send(["PING"]);

  console.log({
    profile,
    visits,
    user,
    userRoles,
    nextJob,
    topScores,
    fullKey,
    pong
  });
} finally {
  await Promise.allSettled([
    redis.kv(schema.profiles).del(id),
    redis.counter(schema.counters).del(id),
    redis.hash(schema.users).del(id),
    redis.set(schema.roles).del(id),
    redis.list(schema.jobs).del(id),
    redis.zset(schema.leaderboard).del("daily")
  ]);
  await client.close();
}

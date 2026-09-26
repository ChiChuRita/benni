// The schema-first API from the quickstart: declare schemas once, bind a
// client, and every read comes back as your declared type.
import { benni } from "benni";
import { node } from "benni/node";
import { hash, json, kv, list, number, set, string, zset } from "benni/schema";

const redisUrl =
  process.env.BENNI_REDIS_URL ??
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

const redis = benni({ client: node({ url: redisUrl }), schema });
const id = `demo:${Date.now()}`;

try {
  await redis.query.profiles.set(
    id,
    { name: "Ada", score: 10 },
    { ttlSeconds: 60 }
  );
  const profile = await redis.query.profiles.get(id);

  const visits = await redis.query.counters.incrby(id, 3);

  await redis.query.users.hset(id, { name: "Ada", score: 10 });
  await redis.query.users.hincrby(id, "score", 5);
  const user = await redis.query.users.hget(id);

  await redis.query.roles.sadd(id, ["admin", "editor"]);
  const userRoles = await redis.query.roles.smembers(id);

  await redis.query.jobs.rpush(id, [
    { id: "job-1", kind: "email" },
    { id: "job-2", kind: "report" }
  ]);
  const nextJob = await redis.query.jobs.lpop(id);

  await redis.query.leaderboard.zadd("daily", [
    { member: "ada", score: 15 },
    { member: "grace", score: 12 }
  ]);
  const topScores = await redis.query.leaderboard.zrange("daily", {
    start: 0,
    stop: -1,
    withScores: true
  });

  const fullKey = redis.query.users.key(id); // "example:user:demo:…"
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
    redis.query.profiles.del(id),
    redis.query.counters.del(id),
    redis.query.users.del(id),
    redis.query.roles.del(id),
    redis.query.jobs.del(id),
    redis.query.leaderboard.del("daily")
  ]);
  await redis.close();
}

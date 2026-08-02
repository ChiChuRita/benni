import { afterAll, describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { RedisServerError } from "../src/core/errors.js";
import { defineHash } from "../src/core/hash.js";
import { defineSortedSet } from "../src/core/sorted-set.js";
import type { RedisClient } from "../src/core/types.js";
import { benni } from "../src/database.js";
import { ioredis } from "../src/ioredis/index.js";
import { node } from "../src/node/index.js";

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

const runPrefix = `benni:servererr:${Date.now()}:${Math.random()
  .toString(36)
  .slice(2)}`;

/**
 * Two schemas deliberately sharing one prefix: writing the hash and then
 * issuing a ZADD against the same id is the shortest route to a real server
 * error through the typed API, no raw commands involved.
 */
const users = defineHash(`${runPrefix}:collide`, {
  name: codecs.string(),
  score: codecs.number()
});
const leaderboard = defineSortedSet(`${runPrefix}:collide`, codecs.string());

const openClients: RedisClient[] = [];

async function track(client: Promise<RedisClient>): Promise<RedisClient> {
  const opened = await client;
  openClients.push(opened);
  return opened;
}

afterAll(async () => {
  for (const client of openClients) {
    try {
      await client.send(["DEL", `${runPrefix}:collide:1`]);
    } catch {
      // Best effort; the run prefix is unique per run either way.
    }
    await client.close();
  }
});

const adapters: Array<[string, () => Promise<RedisClient>]> = [
  ["node", () => track(node({ url: redisUrl }))],
  ["ioredis", () => track(ioredis({ url: redisUrl as string }))]
];

describeRedis("normalized server errors", () => {
  for (const [label, createClient] of adapters) {
    it(`surfaces WRONGTYPE from the typed API as RedisServerError on ${label}`, async () => {
      const client = await createClient();
      const redis = benni(client);

      await redis.hash(users).hset("1", { name: "Ada", score: 10 });

      let thrown: unknown;
      try {
        await redis.zset(leaderboard).zadd("1", { score: 1, member: "ada" });
      } catch (error) {
        thrown = error;
      }

      expect(thrown).toBeInstanceOf(RedisServerError);
      const error = thrown as RedisServerError;
      // The whole point: branch on the code, not on a substring of the message,
      // and get the same answer on every adapter.
      expect(error.code).toBe("WRONGTYPE");
      expect(error.name).toBe("RedisServerError");
      expect(error.message).toBe(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
      expect(error.command).toBe("ZADD");
      // The client's own error is kept, so nothing it attached is lost.
      expect(error.cause).toBeInstanceOf(Error);
      expect(error.cause).not.toBeInstanceOf(RedisServerError);
      expect((error.cause as Error).message).toBe(error.message);
    });

    it(`keeps the committed-MULTI and WATCH-abort paths intact on ${label}`, async () => {
      const client = await createClient();
      const key = `${runPrefix}:${label}:multi`;
      const watched = `${runPrefix}:${label}:watched`;

      // A per-command failure inside a committed MULTI still reports the
      // failing command's own error (not node-redis's "N commands failed"
      // aggregate), now normalized.
      await client.send(["SET", key, "not-a-number"]);
      let thrown: unknown;
      try {
        await client.transaction?.([
          ["SET", key, "not-a-number"],
          ["INCR", key]
        ]);
      } catch (error) {
        thrown = error;
      }
      expect(thrown).toBeInstanceOf(RedisServerError);
      expect((thrown as RedisServerError).code).toBe("ERR");
      expect((thrown as RedisServerError).message).toMatch(/not an integer/);

      // WATCH abort is still the one cross-adapter null, not an error.
      const session = await client.session?.();
      expect(session).toBeDefined();
      if (!session) return;
      try {
        await client.send(["SET", watched, "before"]);
        await session.send(["WATCH", watched]);
        await client.send(["SET", watched, "clobbered"]);
        await expect(
          session.watchedTransaction([["SET", watched, "lost"]])
        ).resolves.toBeNull();

        // And a per-command failure inside a committed watched EXEC normalizes
        // the same way an unwatched one does.
        await session.send(["WATCH", watched]);
        let watchedThrown: unknown;
        try {
          await session.watchedTransaction([["INCR", watched]]);
        } catch (error) {
          watchedThrown = error;
        }
        expect(watchedThrown).toBeInstanceOf(RedisServerError);
        expect((watchedThrown as RedisServerError).code).toBe("ERR");
        // The connection survives a normalized rejection.
        await expect(session.send(["PING"])).resolves.toBe("PONG");
      } finally {
        await session.close();
      }
      await client.send(["DEL", key, watched]);
    });
  }
});

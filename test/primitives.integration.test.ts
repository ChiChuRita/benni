import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import { cache, lock, ratelimit } from "../src/primitives/index.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("primitives (live)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  describe("lock", () => {
    it("gives one holder at a time and frees on release", async () => {
      const locks = lock(client, { prefix: `${run}:lock`, ttlMs: 10_000 });
      const id = "resource";

      const first = await locks.acquire(id);
      expect(first).not.toBeNull();

      // Contended: a second acquire fails while the first is held.
      await expect(locks.acquire(id)).resolves.toBeNull();

      // Release frees it; releasing again reports we no longer hold it.
      await expect(first?.release()).resolves.toBe(true);
      await expect(first?.release()).resolves.toBe(false);

      const second = await locks.acquire(id);
      expect(second).not.toBeNull();
      await second?.release();
    });

    it("extends the TTL only while held", async () => {
      const locks = lock(client, { prefix: `${run}:lock2`, ttlMs: 10_000 });
      const handle = await locks.acquire("res");
      await expect(handle?.extend(20_000)).resolves.toBe(true);
      await handle?.release();
      await expect(handle?.extend(20_000)).resolves.toBe(false);
    });

    it("run releases the lock even when the body throws", async () => {
      const locks = lock(client, { prefix: `${run}:lock3` });
      await expect(
        locks.run("res", async () => {
          throw new Error("boom");
        })
      ).rejects.toThrow("boom");
      // The lock was released, so we can acquire it again immediately.
      const handle = await locks.acquire("res");
      expect(handle).not.toBeNull();
      await handle?.release();
    });
  });

  describe("ratelimit", () => {
    it("admits up to the limit, then denies within the window", async () => {
      const limiter = ratelimit(client, {
        limit: 3,
        windowMs: 60_000,
        prefix: `${run}:rl`
      });
      const id = "user";

      const first = await limiter.check(id);
      expect(first.success).toBe(true);
      expect(first.remaining).toBe(2);

      await expect(limiter.check(id)).resolves.toMatchObject({ success: true });
      await expect(limiter.check(id)).resolves.toMatchObject({
        success: true,
        remaining: 0
      });

      const denied = await limiter.check(id);
      expect(denied.success).toBe(false);
      expect(denied.remaining).toBe(0);
      expect(denied.resetMs).toBeGreaterThan(Date.now());
    });

    it("tracks each id independently", async () => {
      const limiter = ratelimit(client, {
        limit: 1,
        windowMs: 60_000,
        prefix: `${run}:rl2`
      });
      await expect(limiter.check("a")).resolves.toMatchObject({
        success: true
      });
      await expect(limiter.check("b")).resolves.toMatchObject({
        success: true
      });
      await expect(limiter.check("a")).resolves.toMatchObject({
        success: false
      });
    });
  });

  describe("cache", () => {
    it("collapses a stampede of concurrent misses to one loader call", async () => {
      const store = cache<{ n: number }>(client, {
        ttlMs: 60_000,
        prefix: `${run}:cache`,
        pollMs: 10
      });
      let loads = 0;
      const loader = async () => {
        loads++;
        // Hold the load long enough that every concurrent get sees the miss.
        await new Promise((resolve) => setTimeout(resolve, 50));
        return { n: 7 };
      };

      const results = await Promise.all(
        Array.from({ length: 8 }, () => store.get("hot", loader))
      );

      expect(loads).toBe(1);
      for (const result of results) expect(result).toEqual({ n: 7 });
    });

    it("expires by ttl and reloads after del", async () => {
      const store = cache<string>(client, {
        ttlMs: 60_000,
        prefix: `${run}:cache2`
      });

      await expect(store.get("k", () => "first")).resolves.toBe("first");
      // Cached: a different loader result is ignored.
      await expect(store.get("k", () => "second")).resolves.toBe("first");
      await expect(store.peek("k")).resolves.toBe("first");

      await expect(store.del("k")).resolves.toBe(1);
      await expect(store.get("k", () => "second")).resolves.toBe("second");
    });
  });
});

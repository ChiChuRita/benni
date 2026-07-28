import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import {
  budget,
  cache,
  idempotency,
  lock,
  ratelimit,
  semaphore
} from "../src/primitives/index.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("primitives (live)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  /** A fresh id per test, so nothing here depends on run order. */
  const uid = () => `${run}:${Math.random().toString(36).slice(2)}`;
  const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

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

    it("recovers immediately when the lock holder's loader throws", async () => {
      // The waiters polled only for the value, never re-tried the lock. A
      // holder whose loader threw released within milliseconds, but every
      // waiter still slept the full lockTtlMs and then all loaded at once:
      // one backend 503 became a multi-second stall plus a stampede.
      const store = cache<string>(client, {
        ttlMs: 60_000,
        prefix: `${run}:cache3`,
        lockTtlMs: 5_000,
        pollMs: 10
      });
      let loads = 0;
      const loader = async () => {
        loads++;
        await pause(30);
        if (loads === 1) throw new Error("backend is down");
        return "recovered";
      };

      const startedAt = Date.now();
      const settled = await Promise.allSettled(
        Array.from({ length: 6 }, () => store.get("hot", loader))
      );
      const elapsed = Date.now() - startedAt;

      // Well under lockTtlMs: the waiters took the freed lock instead of
      // waiting it out.
      expect(elapsed).toBeLessThan(2_000);
      // Exactly one retry after the failure — single-flight survived it.
      expect(loads).toBe(2);
      expect(settled.filter((r) => r.status === "rejected")).toHaveLength(1);
      for (const result of settled) {
        if (result.status === "fulfilled")
          expect(result.value).toBe("recovered");
      }
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

  // The three primitives added in the AI-shaped-primitives pass. Their whole
  // value is behaviour under contention, which a fake client cannot express:
  // it answers one command at a time from a scripted queue, which is exactly
  // the condition these guarantees are not about.

  it("budget: charge respects the limit", async () => {
    const b = budget(client, { limit: 100, windowMs: 60_000 });
    const id = uid();
    expect((await b.charge(id, 60)).ok).toBe(true);
    expect((await b.charge(id, 30)).ok).toBe(true);
    const denied = await b.charge(id, 30);
    expect(denied.ok).toBe(false);
    expect(denied.remaining).toBe(10);
    expect(denied.retryAfterMs).toBeGreaterThan(0);
    expect((await b.charge(id, 10)).ok).toBe(true);
  });

  it("budget: a failed settle leaves the hold usable for a retry", async () => {
    // settle()/release() flipped `settled` before awaiting Redis and never
    // reset it, so one transient error permanently disabled the handle: the
    // caller's retry and its finally-release both became silent no-ops, the
    // spend was forgotten, and the reservation blocked that headroom until
    // its TTL lapsed.
    let failNext = false;
    const flaky: RedisClient = {
      ...client,
      async send(command) {
        if (failNext && command[0] === "EVALSHA") {
          failNext = false;
          throw new Error("connection reset");
        }
        return client.send(command);
      }
    };
    const b = budget(flaky, { limit: 100, windowMs: 60_000 });
    const id = uid();
    const hold = await b.reserve(id, 10);
    expect(hold).not.toBeNull();

    failNext = true;
    await expect(hold?.settle(30)).rejects.toThrow("connection reset");

    // The retry has to actually reach Redis. Settling 30 against a 10
    // estimate is what makes this observable: a no-op retry leaves the
    // original 10 reserved (remaining 90), a real one charges 30.
    await hold?.settle(30);
    await expect(b.check(id)).resolves.toMatchObject({ remaining: 70 });
  });

  it("budget: a hold blocks others, settle charges the real cost", async () => {
    const b = budget(client, { limit: 100, windowMs: 60_000 });
    const id = uid();
    const hold = await b.reserve(id, 80);
    expect(hold).not.toBeNull();
    // 80 is held, so only 20 is left
    expect((await b.charge(id, 30)).ok).toBe(false);
    expect((await b.check(id)).remaining).toBe(20);
    await hold?.settle(10); // actually used 10, not 80
    expect((await b.check(id)).remaining).toBe(90);
    expect((await b.charge(id, 85)).ok).toBe(true);
  });

  it("budget: release charges nothing; settle is idempotent", async () => {
    const b = budget(client, { limit: 100, windowMs: 60_000 });
    const id = uid();
    const hold = await b.reserve(id, 50);
    await hold?.release();
    expect((await b.check(id)).remaining).toBe(100);
    const h2 = await b.reserve(id, 50);
    await h2?.settle(25);
    await h2?.settle(25); // second settle must not double-charge
    expect((await b.check(id)).remaining).toBe(75);
  });

  it("budget: an expired hold stops counting", async () => {
    const b = budget(client, { limit: 100, windowMs: 60_000, holdTtlMs: 300 });
    const id = uid();
    await b.reserve(id, 90);
    expect((await b.charge(id, 50)).ok).toBe(false);
    await new Promise((r) => setTimeout(r, 500));
    expect((await b.charge(id, 50)).ok).toBe(true);
  });

  it("budget: settle after the hold lapsed still charges", async () => {
    // The money was spent even though the lease is gone; a budget that
    // forgets real spend is worse than one that briefly runs over.
    const b = budget(client, { limit: 100, windowMs: 60_000, holdTtlMs: 300 });
    const id = uid();
    const hold = await b.reserve(id, 10);
    await new Promise((r) => setTimeout(r, 500));
    expect((await b.check(id)).remaining).toBe(100); // hold lapsed
    await hold?.settle(40);
    expect((await b.check(id)).remaining).toBe(60);
  });

  it("budget: extend keeps a hold alive past its lease", async () => {
    const b = budget(client, { limit: 100, windowMs: 60_000, holdTtlMs: 400 });
    const id = uid();
    const hold = await b.reserve(id, 90);
    await new Promise((r) => setTimeout(r, 250));
    expect(await hold?.extend(5_000)).toBe(true);
    await new Promise((r) => setTimeout(r, 300));
    // Past the original lease, still held because we extended.
    expect((await b.charge(id, 50)).ok).toBe(false);
  });

  it("budget: spend rolls off as the window slides", async () => {
    const b = budget(client, { limit: 100, windowMs: 1_000 });
    const id = uid();
    expect((await b.charge(id, 100)).ok).toBe(true);
    expect((await b.charge(id, 1)).ok).toBe(false);
    await new Promise((r) => setTimeout(r, 1_600));
    // A full window later the old bucket has decayed most of the way out.
    const after = await b.check(id);
    expect(after.remaining).toBeGreaterThan(50);
  });

  it("budget: 20 concurrent reserves admit exactly 12", async () => {
    const b = budget(client, { limit: 120, windowMs: 60_000 });
    const id = uid();
    const held = await Promise.all(
      Array.from({ length: 20 }, () => b.reserve(id, 10))
    );
    expect(held.filter((h) => h !== null)).toHaveLength(12);
  });

  it("budget: a zero reservation is not mistaken for a settled one", async () => {
    // The settle tombstone must not collide with a legitimate zero hold, or
    // the next settle would treat a real charge as a duplicate and skip it.
    const b = budget(client, { limit: 100, windowMs: 60_000 });
    const id = uid();
    const hold = await b.reserve(id, 0);
    expect(hold).not.toBeNull();
    await hold?.settle(40);
    expect((await b.check(id)).remaining).toBe(60);
  });

  it("budget: rejects fractional amounts instead of failing inside Lua", async () => {
    // The buckets are integer counters, so a fractional cost used to surface
    // as a raw "value is not an integer" from the script.
    const b = budget(client, { limit: 100, windowMs: 60_000 });
    await expect(b.charge(uid(), 1.5)).rejects.toThrow(
      /must be a non-negative integer/
    );
    await expect(b.reserve(uid(), 0.25)).rejects.toThrow(
      /smallest unit you meter/
    );
  });

  it("semaphore: never exceeds the limit under 50 concurrent runs", async () => {
    const s = semaphore(client, { limit: 5, leaseMs: 10_000 });
    const id = uid();
    let inFlight = 0;
    let peak = 0;
    let admitted = 0;
    await Promise.all(
      Array.from({ length: 50 }, () =>
        s.run(
          id,
          async () => {
            admitted++;
            inFlight++;
            peak = Math.max(peak, inFlight);
            await new Promise((r) => setTimeout(r, 10));
            inFlight--;
          },
          { retries: 200, retryDelayMs: 5 }
        )
      )
    );
    expect(admitted).toBe(50);
    expect(peak).toBeLessThanOrEqual(5);
    expect(peak).toBeGreaterThan(1);
    expect(await s.count(id)).toBe(0);
  });

  it("semaphore: reclaims a dead holder's slot", async () => {
    const s = semaphore(client, { limit: 1, leaseMs: 300 });
    const id = uid();
    const held = await s.acquire(id);
    expect(held).not.toBeNull();
    expect(await s.acquire(id)).toBeNull();
    await new Promise((r) => setTimeout(r, 500));
    expect(await s.acquire(id)).not.toBeNull();
    expect(await held?.extend()).toBe(false); // ours was reclaimed
  });

  it("idempotency: 10 concurrent calls run fn once", async () => {
    const once = idempotency<{ n: number }>(client, { ttlMs: 60_000 });
    const key = uid();
    let calls = 0;
    const results = await Promise.all(
      Array.from({ length: 10 }, () =>
        once.run(key, async () => {
          calls++;
          await new Promise((r) => setTimeout(r, 50));
          return { n: 42 };
        })
      )
    );
    expect(calls).toBe(1);
    expect(results.every((r) => r.value.n === 42)).toBe(true);
    expect(results.filter((r) => r.replayed)).toHaveLength(9);
  });

  it("idempotency: a throwing handler releases the key", async () => {
    const once = idempotency<string>(client);
    const key = uid();
    await expect(
      once.run(key, () => {
        throw new Error("boom");
      })
    ).rejects.toThrow("boom");
    expect(await once.peek(key)).toBeNull();
    const retry = await once.run(key, () => "ok");
    expect(retry).toEqual({ value: "ok", replayed: false });
  });

  // Regressions for the six findings from the codex review pass. Each one was
  // reproduced against a live server before being fixed; five needed a
  // specific timing or configuration that the happy-path tests never hit.

  it("F1 semaphore: a short lease must not shorten the whole set's TTL", async () => {
    const s = semaphore(client, {
      limit: 2,
      leaseMs: 5_000,
      prefix: `${uid()}`
    });
    expect(await s.acquire("x")).not.toBeNull(); // 5s holder
    expect(await s.acquire("x", { leaseMs: 100 })).not.toBeNull(); // 100ms holder
    await pause(400);
    // The 5s holder is still live, so exactly one slot must remain held.
    expect(await s.count("x")).toBe(1);
  });

  it("F2 semaphore: extend must fail once our lease elapsed", async () => {
    const s = semaphore(client, { limit: 2, leaseMs: 100, prefix: `${uid()}` });
    // A long-lived co-holder keeps the sorted set itself alive, so the expired
    // member is still physically present and ZSCORE alone cannot tell.
    await s.acquire("x", { leaseMs: 60_000 });
    const h = await s.acquire("x", { leaseMs: 100 });
    await pause(300);
    // Nothing pruned it yet, but our lease is gone: extend must not resurrect.
    expect(await h?.extend(5_000)).toBe(false);
  });

  it("F3 budget: holds must survive when holdTtlMs exceeds 2x the window", async () => {
    const b = budget(client, {
      limit: 100,
      windowMs: 100,
      holdTtlMs: 3_000,
      prefix: `${uid()}`
    });
    await b.reserve("x", 90);
    await pause(400); // past window*2, well inside holdTtlMs
    expect((await b.check("x")).remaining).toBe(10);
  });

  it("F4 budget: a duplicate settle must not double-charge after the hold was pruned", async () => {
    const b = budget(client, {
      limit: 100,
      windowMs: 60_000,
      holdTtlMs: 100,
      prefix: `${uid()}`
    });
    const h = await b.reserve("x", 10);
    await pause(200);
    await b.check("x"); // prunes the lapsed hold
    await h?.settle(40); // late settle: charges, money was spent
    await h?.settle(40); // duplicate: must charge nothing
    expect((await b.check("x")).remaining).toBe(60);
  });

  it("F5 budget: reset must clear spend even with a skewed local clock", async () => {
    const b = budget(client, {
      limit: 100,
      windowMs: 60_000,
      prefix: `${uid()}`
    });
    await b.charge("x", 40);
    const real = Date.now;
    Date.now = () => real() + 180_000; // three windows ahead
    try {
      await b.reset("x");
    } finally {
      Date.now = real;
    }
    expect((await b.check("x")).remaining).toBe(100);
  });

  it("F6 idempotency: a failed write-back must not be reported as plain success", async () => {
    const once = idempotency<string>(client, {
      prefix: `${uid()}`,
      codec: {
        encode: () => {
          throw new Error("unserializable");
        },
        decode: (s: string) => s
      }
    });
    // The effect ran but can never be replayed. Reporting success would let a
    // later duplicate re-run it with no warning.
    await expect(once.run("k", () => "did-the-work")).rejects.toThrow();
  });

  it("keeps the reservation set bounded by in-flight calls, not by throughput", async () => {
    // Settle-once used to be enforced with a server-side marker per settle,
    // which meant 200 sequential calls left 200 members for every later check
    // to scan. The bound the docs promise is concurrency, not call volume.
    const b = budget(client, { limit: 10_000_000, windowMs: 600_000 });
    const id = uid();
    for (let index = 0; index < 200; index++) {
      const hold = await b.reserve(id, 10);
      await hold?.settle(5);
    }
    const card = await client.send(["ZCARD", `budget:{${id}}:holds`]);
    expect(Number(card)).toBeLessThan(5);
  });

  it("settling twice on one handle charges once, and disarms the handle", async () => {
    const b = budget(client, { limit: 100, windowMs: 60_000 });
    const id = uid();
    const hold = await b.reserve(id, 50);
    await hold?.settle(25);
    await hold?.settle(25);
    await hold?.release();
    expect(await hold?.extend()).toBe(false);
    expect((await b.check(id)).remaining).toBe(75);
  });

  it("sets a real TTL, not a scientific-notation timestamp", async () => {
    // The lease keys are expired with PEXPIREAT on an epoch-ms value. Lua can
    // render a large number as 1.78e+12, which Redis rejects, so assert a
    // concrete TTL actually landed rather than trusting the call succeeded.
    const s = semaphore(client, { limit: 1, leaseMs: 30_000 });
    const id = uid();
    await s.acquire(id);
    const ttl = Number(await client.send(["PTTL", `semaphore:${id}`]));
    expect(ttl).toBeGreaterThan(25_000);
    expect(ttl).toBeLessThanOrEqual(30_000);
  });

  it("budget: extending past the window keeps the hold counted", async () => {
    // Deeper than the holdTtlMs > window*2 case: the extend itself has to push
    // the reservation key's own expiry out, or the hold vanishes mid-call.
    const b = budget(client, { limit: 100, windowMs: 200, holdTtlMs: 300 });
    const id = uid();
    const hold = await b.reserve(id, 90);
    expect(await hold?.extend(5_000)).toBe(true);
    await pause(600);
    expect((await b.check(id)).remaining).toBe(10);
  });
  it("semaphore: release reports false once the lease has lapsed", async () => {
    // An expired member sits in the set until some acquire prunes it, so a
    // bare ZREM answered "yes, you held it" for a slot already handed on.
    // A second, longer-lived holder is what makes this observable: it keeps
    // the ZSET key alive, so the lapsed member is still physically there.
    const slots = semaphore(client, {
      limit: 2,
      prefix: `${run}:sem-lapse`
    });
    const id = uid();
    const shortLived = await slots.acquire(id, { leaseMs: 300 });
    const longLived = await slots.acquire(id, { leaseMs: 30_000 });
    expect(shortLived).not.toBeNull();
    expect(longLived).not.toBeNull();

    await pause(500);
    // The key still exists (longLived holds it) and the lapsed member is
    // still in it, so ZREM alone would have removed it and reported true.
    await expect(shortLived?.release()).resolves.toBe(false);
    await expect(longLived?.release()).resolves.toBe(true);
  });

  it("ratelimit: resetMs on the allowed path tracks the oldest entry", async () => {
    // The allowed branch returned now + window, but in a sliding window a slot
    // frees when the OLDEST entry ages out. With requests spread through a
    // window that put X-RateLimit-Reset up to a full window late.
    const limiter = ratelimit(client, {
      limit: 5,
      windowMs: 3_000,
      prefix: `${run}:rl-reset`
    });
    const id = uid();
    const first = await limiter.check(id);
    await pause(600);
    const second = await limiter.check(id);

    expect(first.success).toBe(true);
    expect(second.success).toBe(true);
    // Both resets point at the first entry ageing out, so they agree to well
    // within the 600ms gap. The bug made the second a full 600ms later.
    expect(Math.abs(second.resetMs - first.resetMs)).toBeLessThan(250);
  });

  it("budget: check reports a real retryAfterMs when it reports not ok", async () => {
    // check's script always returns status 1, and resultOf zeroes retryAfter
    // for status 1, so a check with no headroom said "not ok, retry in 0ms".
    const b = budget(client, { limit: 10, windowMs: 60_000 });
    const id = uid();
    expect((await b.charge(id, 10)).ok).toBe(true);

    const checked = await b.check(id);
    expect(checked.ok).toBe(false);
    expect(checked.remaining).toBe(0);
    expect(checked.retryAfterMs).toBeGreaterThan(0);

    // A check with headroom still reports 0.
    const other = await b.check(uid());
    expect(other.ok).toBe(true);
    expect(other.retryAfterMs).toBe(0);
  });

  it("budget: refuses an id that would build an empty hash tag", async () => {
    // budget:{}:0 is not a smaller tag, it is no tag: Redis hashes the whole
    // key, so this id's three keys scatter and every script for it fails with
    // CROSSSLOT -- for this one id and no other.
    const b = budget(client, { limit: 10, windowMs: 60_000 });
    await expect(b.charge("", 1)).rejects.toThrow(/hash tag/);
    await expect(b.check("}oops")).rejects.toThrow(/hash tag/);
  });
});

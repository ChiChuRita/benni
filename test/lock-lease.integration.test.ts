import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import {
  LockLeaseLostError,
  LockNotAcquiredError,
  lock
} from "../src/primitives/index.js";

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

/**
 * The lease behaviour of `lock().run()`, against a real server: renewal has to
 * beat a real PX expiry, and a lost lease has to be reported rather than let a
 * body pass for exclusive. Short TTLs throughout so the suite stays quick.
 */
describeRedis("lock lease (live)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  const pause = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

  beforeAll(async () => {
    client = node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("holds the lock through a critical section far longer than ttlMs", async () => {
    const prefix = `${run}:renew`;
    const locks = lock(client, { prefix, ttlMs: 200 }); // 50ms heartbeat.
    const key = `${prefix}:res`;

    let ownerDuringBody: unknown;
    const result = await locks.run("res", async (handle) => {
      // Three whole TTLs. Without renewal the key would be long gone and a
      // second caller would already be inside the critical section.
      await pause(600);
      ownerDuringBody = await client.send(["GET", key]);
      expect(handle.signal.aborted).toBe(false);
      await expect(locks.acquire("res")).resolves.toBeNull();
      return handle.token;
    });

    expect(ownerDuringBody).toBe(result);
    // And it is released on the way out, not left on its renewed TTL.
    await expect(client.send(["EXISTS", key])).resolves.toBe(0);
  });

  it("reports a lost lease instead of resolving as if the body was exclusive", async () => {
    const prefix = `${run}:lost`;
    const locks = lock(client, { prefix, ttlMs: 200 });
    const key = `${prefix}:res`;

    let abortReason: unknown;
    const promise = locks.run(
      "res",
      async (handle) => {
        handle.signal.addEventListener("abort", () => {
          abortReason = handle.signal.reason;
        });
        // Exactly what an expiry-then-reacquire leaves behind: the key is
        // there, owned by somebody else's token. Renewal's token check fails.
        await client.send(["SET", key, "another-holder", "PX", 5_000]);
        await pause(400); // Several heartbeats.
        return "finished anyway";
      },
      { heartbeatMs: 50 }
    );

    await expect(promise).rejects.toBeInstanceOf(LockLeaseLostError);
    await expect(promise).rejects.toMatchObject({ key });
    expect(abortReason).toBeInstanceOf(LockLeaseLostError);
    // The other holder's lock is untouched: release is a check-and-delete.
    await expect(client.send(["GET", key])).resolves.toBe("another-holder");
    await client.send(["DEL", key]);
  });

  it("lets the lock lapse when renewal is switched off (the documented opt-out)", async () => {
    const prefix = `${run}:optout`;
    const locks = lock(client, { prefix, ttlMs: 150 });

    const result = await locks.run(
      "res",
      async () => {
        await pause(400);
        // No renewal, so the lock expired mid-body and another caller can take
        // it. This is the pre-renewal behaviour, kept reachable on request.
        const other = await locks.acquire("res");
        expect(other).not.toBeNull();
        await other?.release();
        return "unprotected";
      },
      { heartbeatMs: false }
    );

    expect(result).toBe("unprotected");
  });

  it("serializes concurrent callers when retries are configured", async () => {
    const prefix = `${run}:mutex`;
    const locks = lock(client, { prefix, ttlMs: 2_000 });
    const counterKey = `${prefix}:counter`;
    let inside = 0;
    let maxInside = 0;

    await Promise.all(
      Array.from({ length: 6 }, () =>
        locks.run(
          "res",
          async () => {
            inside += 1;
            maxInside = Math.max(maxInside, inside);
            // Deliberately non-atomic: only mutual exclusion makes it correct.
            const current = Number(
              (await client.send(["GET", counterKey])) ?? 0
            );
            await pause(20);
            await client.send(["SET", counterKey, String(current + 1)]);
            inside -= 1;
          },
          { retries: 200, retryDelayMs: 10 }
        )
      )
    );

    expect(maxInside).toBe(1);
    await expect(client.send(["GET", counterKey])).resolves.toBe("6");
    await client.send(["DEL", counterKey]);
  });

  it("still fails fast under contention with the default retries", async () => {
    const locks = lock(client, { prefix: `${run}:failfast`, ttlMs: 1_000 });

    const outcomes = await Promise.allSettled(
      Array.from({ length: 6 }, () => locks.run("res", () => pause(100)))
    );

    const rejected = outcomes.filter(
      (outcome) => outcome.status === "rejected"
    );
    expect(outcomes.length - rejected.length).toBe(1);
    expect(rejected).toHaveLength(5);
    for (const outcome of rejected) {
      expect(outcome.reason).toBeInstanceOf(LockNotAcquiredError);
    }
  });

  it("serializes concurrent callers with waitTimeoutMs alone", async () => {
    const locks = lock(client, { prefix: `${run}:deadline`, ttlMs: 2_000 });
    let inside = 0;
    let maxInside = 0;

    await Promise.all(
      Array.from({ length: 6 }, () =>
        locks.run(
          "res",
          async () => {
            inside += 1;
            maxInside = Math.max(maxInside, inside);
            await pause(20);
            inside -= 1;
          },
          { waitTimeoutMs: 5_000, retryDelayMs: 10 }
        )
      )
    );

    expect(maxInside).toBe(1);
  });

  it("hands out strictly increasing fences, and each run sees its own", async () => {
    // The fence is what lets a downstream store reject a paused holder that
    // wakes up after its lock was re-granted: it must rise with every grant,
    // including grants that follow an expiry rather than a release.
    const prefix = `${run}:fence`;
    const locks = lock(client, { prefix, ttlMs: 2_000 });
    const fences: number[] = [];

    await Promise.all(
      Array.from({ length: 5 }, () =>
        locks.run(
          "order",
          async (handle) => {
            fences.push(handle.fence);
            await pause(10);
          },
          { retries: 200, retryDelayMs: 5 }
        )
      )
    );
    const expired = await locks.acquire("order", { ttlMs: 50 });
    await pause(100); // Lapses without a release.
    const next = await locks.acquire("order");

    const all = [...fences, expired?.fence ?? 0, next?.fence ?? 0];
    expect(all).toEqual([...all].sort((a, b) => a - b));
    expect(new Set(all).size).toBe(all.length);
    expect(all[0]).toBeGreaterThanOrEqual(1);
    await next?.release();
    await client.send(["DEL", `{${prefix}:order}:fence`]);
  });
});

const clusterUrl = process.env.BENNI_REDIS_CLUSTER_URL;
const describeCluster = clusterUrl ? describe : describe.skip;

describeCluster("lock on a cluster-enabled node", () => {
  let client: RedisClient;
  const run = `lock-cluster:${Date.now()}`;

  beforeAll(async () => {
    client = node({ url: clusterUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("takes the lock and its fence in one script without CROSSSLOT", async () => {
    // The acquire script touches the lock and its fence counter; Redis rejects
    // it unless both share a slot, whether or not the id carries a tag.
    for (const [prefix, id] of [
      [run, "order:42"],
      [`${run}:tagged`, "{u1}"]
    ] as const) {
      const locks = lock(client, { prefix });
      const first = await locks.run(id, (handle) => handle.fence);
      const second = await locks.run(id, (handle) => handle.fence);
      expect(second).toBe(first + 1);
    }
  });
});

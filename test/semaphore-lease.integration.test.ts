import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import {
  SemaphoreLeaseLostError,
  SemaphoreNotAcquiredError,
  semaphore
} from "../src/primitives/index.js";

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

/**
 * The lease behaviour of `semaphore().run()`, against a real server: renewal has
 * to beat a real expiry-and-prune, and a lost slot has to be reported rather
 * than let a body keep running over the limit. Short leases throughout so the
 * suite stays quick.
 */
describeRedis("semaphore lease (live)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  const pause = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("holds the slot through a critical section far longer than leaseMs", async () => {
    const prefix = `${run}:renew`;
    const slots = semaphore(client, { prefix, limit: 1, leaseMs: 200 }); // 50ms.
    const key = `${prefix}:openai`;

    const result = await slots.run("openai", async (held) => {
      // Three whole leases. Without renewal the lease would be long gone, the
      // next acquire would prune it, and a second caller would be inside the
      // critical section alongside this one.
      await pause(600);
      expect(held.signal.aborted).toBe(false);
      await expect(slots.acquire("openai")).resolves.toBeNull();
      await expect(slots.count("openai")).resolves.toBe(1);
      return held.token;
    });

    expect(result).toBeTypeOf("string");
    // And the slot is given back on the way out, not left on its renewed lease.
    await expect(slots.count("openai")).resolves.toBe(0);
    await client.send(["DEL", key]);
  });

  it("reports a lost slot instead of resolving as if the body was inside the limit", async () => {
    const prefix = `${run}:lost`;
    const slots = semaphore(client, { prefix, limit: 3, leaseMs: 200 });
    const key = `${prefix}:openai`;

    let abortReason: unknown;
    const promise = slots.run(
      "openai",
      async (held) => {
        held.signal.addEventListener("abort", () => {
          abortReason = held.signal.reason;
        });
        // Exactly what an expiry-then-prune leaves behind: our member is gone
        // from the set, so the slot has been handed on. Renewal's ownership
        // check fails.
        await client.send(["ZREM", key, held.token]);
        await pause(400); // Several heartbeats.
        return "finished anyway";
      },
      { heartbeatMs: 50 }
    );

    await expect(promise).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    await expect(promise).rejects.toMatchObject({ key, limit: 3 });
    expect(abortReason).toBeInstanceOf(SemaphoreLeaseLostError);
    await client.send(["DEL", key]);
  });

  it("never admits more than the limit when bodies outlive the lease", async () => {
    const prefix = `${run}:admit`;
    // Bodies run for roughly three leases each, so only renewal keeps the count
    // honest. Without it every holder would lose its slot mid-body and the next
    // waiting caller would be let in on top of it.
    const slots = semaphore(client, { prefix, limit: 3, leaseMs: 150 });
    let inside = 0;
    let maxInside = 0;
    let maxCounted = 0;

    await Promise.all(
      Array.from({ length: 9 }, () =>
        slots.run(
          "openai",
          async () => {
            inside += 1;
            maxInside = Math.max(maxInside, inside);
            maxCounted = Math.max(maxCounted, await slots.count("openai"));
            await pause(450);
            inside -= 1;
          },
          { retries: 300, retryDelayMs: 20 }
        )
      )
    );

    expect(maxInside).toBe(3);
    expect(maxCounted).toBeLessThanOrEqual(3);
    await expect(slots.count("openai")).resolves.toBe(0);
    await client.send(["DEL", `${prefix}:openai`]);
  });

  it("lets the slot lapse when renewal is switched off (the documented opt-out)", async () => {
    const prefix = `${run}:optout`;
    const slots = semaphore(client, { prefix, limit: 1, leaseMs: 150 });

    const result = await slots.run(
      "openai",
      async () => {
        await pause(400);
        // No renewal, so our lease lapsed mid-body and the next acquire prunes
        // it and admits another caller: two callers inside a limit of one. This
        // is the pre-renewal behaviour, kept reachable on request.
        const other = await slots.acquire("openai");
        expect(other).not.toBeNull();
        await other?.release();
        return "over-admitted";
      },
      { heartbeatMs: false }
    );

    expect(result).toBe("over-admitted");
    await client.send(["DEL", `${prefix}:openai`]);
  });

  it("still fails fast under contention with the default retries", async () => {
    const prefix = `${run}:failfast`;
    const slots = semaphore(client, { prefix, limit: 1, leaseMs: 1_000 });

    const outcomes = await Promise.allSettled(
      Array.from({ length: 6 }, () => slots.run("openai", () => pause(100)))
    );

    const rejected = outcomes.filter(
      (outcome) => outcome.status === "rejected"
    );
    expect(outcomes.length - rejected.length).toBe(1);
    expect(rejected).toHaveLength(5);
    for (const outcome of rejected) {
      expect(outcome.reason).toBeInstanceOf(SemaphoreNotAcquiredError);
    }
    await client.send(["DEL", `${prefix}:openai`]);
  });
});

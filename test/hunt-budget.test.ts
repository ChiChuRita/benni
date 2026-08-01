import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import { BudgetWindowRolledError, budget } from "../src/primitives/index.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("budget (hunt regressions)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  const uid = () => `${run}:${Math.random().toString(36).slice(2)}`;
  const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));
  /** Land just past a bucket boundary, so a test's arithmetic is predictable. */
  const alignToBucket = (windowMs: number) =>
    pause(windowMs - (Date.now() % windowMs) + 15);

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("settle survives a lost reply: the retry charges nothing extra", async () => {
    // The fault that matters is not "the command never went out", it is "the
    // command applied and the reply never came back" — a socket reset, a
    // command timeout, a failover. The two are indistinguishable to the
    // caller, so settle() has to be retryable, and only Redis can know whether
    // the first attempt landed.
    let dropReply = false;
    const lossy: RedisClient = {
      ...client,
      async send(command) {
        if (dropReply && command[0] === "EVALSHA") {
          dropReply = false;
          await client.send(command);
          throw new Error("socket closed unexpectedly");
        }
        return client.send(command);
      }
    };
    const b = budget(lossy, { limit: 100, windowMs: 60_000 });
    const id = uid();
    const hold = await b.reserve(id, 10);
    expect(hold).not.toBeNull();

    dropReply = true;
    await expect(hold?.settle(40)).rejects.toThrow("socket closed");

    // Exactly what the catch block's comment invites the caller to do.
    await hold?.settle(40);
    expect((await b.check(id)).remaining).toBe(60);
  });

  it("retryAfterMs waits for units to free, not for the bucket to roll", async () => {
    // Spend sitting in the CURRENT bucket does not decay at the roll: the
    // two-bucket estimate is continuous across it. Reporting the roll sent the
    // caller back at a moment guaranteed to fail.
    const windowMs = 1_000;
    const b = budget(client, { limit: 100, windowMs });
    const id = uid();
    await alignToBucket(windowMs);
    expect((await b.charge(id, 100)).ok).toBe(true);

    const denied = await b.charge(id, 10);
    expect(denied.ok).toBe(false);
    expect(denied.retryAfterMs).toBeGreaterThan(windowMs);

    await pause(denied.retryAfterMs + 50);
    expect((await b.charge(id, 10)).ok).toBe(true);
  }, 15_000);

  it("retryAfterMs does not overstate the wait once spend is decaying", async () => {
    // The mirror image: with the spend in the previous bucket and a small
    // deficit, the units are seconds away, not a whole window.
    const windowMs = 1_000;
    const b = budget(client, { limit: 100, windowMs });
    const id = uid();
    await alignToBucket(windowMs);
    expect((await b.charge(id, 100)).ok).toBe(true);

    // Into the next bucket, where the 100 is decaying out at 0.1 units/ms.
    await alignToBucket(windowMs);
    const denied = await b.charge(id, 20);
    expect(denied.ok).toBe(false);
    expect(denied.retryAfterMs).toBeGreaterThan(0);
    expect(denied.retryAfterMs).toBeLessThan(400);

    await pause(denied.retryAfterMs + 50);
    expect((await b.charge(id, 20)).ok).toBe(true);
  }, 15_000);

  it("a stale bucket that never settles throws instead of answering", async () => {
    // A process stalled for longer than the window between building the keys
    // and the script running gets the stale-bucket sentinel. Returning it
    // meant charge() reported a spurious denial, reserve() a spurious
    // exhaustion, and settle() resolved having charged nothing at all.
    const windowMs = 300;
    let stall = false;
    const slow: RedisClient = {
      ...client,
      async send(command) {
        if (stall && command[0] === "EVALSHA") await pause(windowMs + 100);
        return client.send(command);
      }
    };
    const b = budget(slow, { limit: 100, windowMs });
    const id = uid();
    const hold = await b.reserve(id, 10);
    expect(hold).not.toBeNull();

    stall = true;
    await expect(b.charge(id, 1)).rejects.toBeInstanceOf(
      BudgetWindowRolledError
    );
    await expect(hold?.settle(30)).rejects.toBeInstanceOf(
      BudgetWindowRolledError
    );

    // Nothing was applied, so the handle is still good for a real settle.
    stall = false;
    await hold?.settle(30);
    expect((await b.check(id)).remaining).toBe(70);
  }, 20_000);

  it("extend finds its own hold for estimates past 14 significant digits", async () => {
    // Lua formats numbers with %.14g, so building the member in the script
    // stored "1e+14" where extend() went looking for the digits.
    const b = budget(client, {
      limit: 9_007_199_254_740_991,
      windowMs: 60_000
    });
    const big = await b.reserve(uid(), 100_000_000_000_000);
    expect(await big?.extend(30_000)).toBe(true);

    const biggest = await b.reserve(uid(), 9_007_199_254_740_991);
    expect(await biggest?.extend(30_000)).toBe(true);
  });

  it("caps the hold set that every charge and check has to walk", async () => {
    // A zero estimate consumes no headroom, so the limit cannot bound how many
    // holds pile up, and the preamble walks all of them on every call.
    const b = budget(client, { limit: 100, windowMs: 60_000, maxHolds: 5 });
    const id = uid();
    const holds = [];
    for (let i = 0; i < 5; i++) holds.push(await b.reserve(id, 0));
    expect(holds.every((hold) => hold !== null)).toBe(true);

    await expect(b.reserve(id, 0)).resolves.toBeNull();

    // Settling one frees a slot again: the cap is on live holds, and the
    // settle marker is a key of its own rather than a member of this set.
    await holds[0]?.settle(0);
    await expect(b.reserve(id, 0)).resolves.not.toBeNull();
  });
});

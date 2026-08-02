import { afterEach, describe, expect, it, vi } from "vitest";
import { ValidationError } from "../src/core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import {
  SemaphoreLeaseLostError,
  SemaphoreNotAcquiredError,
  semaphore
} from "../src/primitives/index.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A fake that answers by script rather than from a fixed reply queue. How many
 * renewals a timing-based test performs is not fixed, so a queued fake would
 * make every test here a race against the interval.
 *
 * The scripts are told apart by their source rather than by load order, so the
 * fake keeps answering correctly if the primitive ever loads them in a different
 * sequence.
 */
function semaphoreFake(behavior?: {
  /** Reply to the acquire script. Default `1` (slot taken). */
  acquire?: () => RedisReply;
  /** Reply to the extend script, given the 1-based call number. Default `1`. */
  extend?: (call: number) => RedisReply | Promise<RedisReply>;
  /** Reply to the release script. Default `1`. */
  release?: () => RedisReply;
}) {
  const commands: RedisCommand[] = [];
  const shas = new Map<string, "acquire" | "extend" | "release" | "count">();
  let extendCalls = 0;
  const client: RedisClient = {
    async send(command) {
      commands.push(command);
      const verb = command[0];
      if (verb === "SCRIPT") {
        const lua = String(command[2]);
        // acquire is the only one that ZADDs after a ZCARD check, extend is the
        // other ZADD, release is the bare ZREM, and what is left is count.
        const kind = lua.includes("ZADD")
          ? lua.includes("ZCARD")
            ? "acquire"
            : "extend"
          : lua.includes('"ZREM"')
            ? "release"
            : "count";
        const sha = `sha-${kind}`;
        shas.set(sha, kind);
        return sha;
      }
      if (verb === "EVALSHA") {
        const kind = shas.get(String(command[1]));
        if (kind === "acquire") {
          // `?? 1` would turn a deliberate `0` (pool full) back into a win.
          return behavior?.acquire === undefined ? 1 : behavior.acquire();
        }
        if (kind === "extend") {
          extendCalls += 1;
          return behavior?.extend?.(extendCalls) ?? 1;
        }
        return behavior?.release?.() ?? 1;
      }
      throw new Error(`Unexpected command ${String(verb)}`);
    },
    async pipeline() {
      return [];
    },
    async transaction() {
      return [];
    },
    async close() {}
  };
  return {
    client,
    commands,
    get extendCount() {
      return extendCalls;
    },
    renewals() {
      return commands.filter(
        (command) => command[0] === "EVALSHA" && command[1] === "sha-extend"
      );
    },
    evalshas() {
      return commands
        .filter((command) => command[0] === "EVALSHA")
        .map((command) => String(command[1]));
    }
  };
}

/**
 * `semaphore().run()` used to acquire with a lease and never renew it, so a
 * critical section that outlived `leaseMs` silently lost its slot: the lease
 * lapsed, the next acquire pruned it and admitted another caller, and the
 * original body kept running as though it were still inside the limit. That is
 * over-admission, the one thing a semaphore exists to prevent. These pin the
 * renewal, the loss report, and the timer hygiene that fixes it.
 */
describe("semaphore.run lease renewal", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("keeps the slot while a critical section outlives leaseMs", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 5, leaseMs: 60 });

    const result = await slots.run(
      "openai",
      async () => {
        await sleep(150); // Two and a half leases.
        return "done";
      },
      { heartbeatMs: 15 }
    );

    expect(result).toBe("done");
    const renewals = fake.renewals();
    expect(renewals.length).toBeGreaterThanOrEqual(3);
    const token = fake.commands.find(
      (command) => command[0] === "EVALSHA" && command[1] === "sha-acquire"
    )?.[6];
    // Every renewal is the token-checked extend, re-applying the same lease.
    for (const renewal of renewals) {
      expect(renewal.slice(0, 4)).toEqual([
        "EVALSHA",
        "sha-extend",
        1,
        "semaphore:openai"
      ]);
      expect(renewal[4]).toBe("60");
      expect(renewal[5]).toBe(token);
    }
  });

  it("defaults the renewal interval to a quarter of leaseMs", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 100 }); // 25ms.

    await slots.run("openai", () => sleep(120));

    // Renewed several times over the body's life, and nowhere near spinning.
    expect(fake.extendCount).toBeGreaterThanOrEqual(2);
    expect(fake.extendCount).toBeLessThanOrEqual(20);
  });

  it("adds no round trips when the body finishes inside one interval", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3 });

    await expect(slots.run("openai", async () => 7)).resolves.toBe(7);

    // Unchanged from before renewal existed: acquire, then release.
    expect(fake.evalshas()).toEqual(["sha-acquire", "sha-release"]);
  });

  it("does not renew when renewal is switched off", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3, leaseMs: 20 });

    await expect(
      slots.run("openai", async () => sleep(80).then(() => 1), {
        heartbeatMs: false
      })
    ).resolves.toBe(1);
    expect(fake.extendCount).toBe(0);
  });

  it("validates heartbeatMs before taking a slot", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3 });

    await expect(
      slots.run("openai", async () => 1, { heartbeatMs: 0 })
    ).rejects.toBeInstanceOf(ValidationError);
    // Nothing was acquired, so no slot is held until its lease lapses.
    expect(fake.commands).toHaveLength(0);
  });
});

describe("semaphore.run lost lease", () => {
  it("rejects with SemaphoreLeaseLostError even when the body resolves", async () => {
    const fake = semaphoreFake({ extend: () => 0 });
    const slots = semaphore(fake.client, { limit: 4, leaseMs: 60 });

    let abortReason: unknown;
    const promise = slots.run(
      "openai",
      async (held) => {
        held.signal.addEventListener("abort", () => {
          abortReason = held.signal.reason;
        });
        await sleep(80);
        return "finished anyway";
      },
      { heartbeatMs: 10 }
    );

    await expect(promise).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    await expect(promise).rejects.toMatchObject({
      key: "semaphore:openai",
      limit: 4
    });
    expect(abortReason).toBeInstanceOf(SemaphoreLeaseLostError);
  });

  it("aborts the handle's signal so the body can stop early", async () => {
    const fake = semaphoreFake({ extend: () => 0 });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 10_000 });

    await expect(
      slots.run(
        "openai",
        (held) =>
          new Promise<never>((_resolve, reject) => {
            held.signal.addEventListener("abort", () =>
              reject(held.signal.reason)
            );
          }),
        { heartbeatMs: 10 }
      )
    ).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    // One failed renewal was enough; nothing kept renewing a lost slot.
    expect(fake.extendCount).toBe(1);
  });

  it("aborts the signal when a manual extend finds the slot reclaimed", async () => {
    const fake = semaphoreFake({ extend: () => 0 });
    const slots = semaphore(fake.client, { limit: 2 });

    const held = await slots.acquire("openai");
    expect(held?.signal.aborted).toBe(false);
    await expect(held?.extend()).resolves.toBe(false);
    expect(held?.signal.aborted).toBe(true);
    expect(held?.signal.reason).toBeInstanceOf(SemaphoreLeaseLostError);
  });

  it("reports a failed renewal round trip without declaring the slot lost", async () => {
    const fake = semaphoreFake({
      extend: (call) => {
        if (call === 1) throw new Error("connection reset");
        return 1;
      }
    });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 200 });
    const errors: unknown[] = [];

    await expect(
      slots.run("openai", () => sleep(120).then(() => "ok"), {
        heartbeatMs: 20,
        onRenewError: (error) => errors.push(error)
      })
    ).resolves.toBe("ok");

    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("connection reset");
  });

  it("declares the slot lost once renewals keep failing past the lease", async () => {
    const fake = semaphoreFake({
      extend: () => {
        throw new Error("unreachable");
      }
    });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 40 });
    const errors: unknown[] = [];

    await expect(
      slots.run("openai", () => sleep(300).then(() => "ok"), {
        heartbeatMs: 10,
        onRenewError: (error) => errors.push(error)
      })
    ).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    expect(errors.length).toBeGreaterThanOrEqual(2);
  });

  it("declares the slot lost when a renewal hangs past the lease", async () => {
    // A round trip that never comes back is the case the one-at-a-time guard
    // hides: no further renewal is even attempted, so only the deadline can
    // notice the lease has lapsed.
    const fake = semaphoreFake({
      extend: () => new Promise<RedisReply>(() => {})
    });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 40 });

    await expect(
      slots.run("openai", () => sleep(300).then(() => "ok"), {
        heartbeatMs: 10
      })
    ).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    expect(fake.extendCount).toBe(1);
  });

  it("does not report a deliberate release as a lost lease", async () => {
    const fake = semaphoreFake({ extend: () => 0 });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 60 });

    await expect(
      slots.run(
        "openai",
        async (held) => {
          await held.release();
          // Renewals would find the slot gone; giving it up was the point.
          await sleep(80);
          return "ok";
        },
        { heartbeatMs: 10 }
      )
    ).resolves.toBe("ok");
    expect(fake.extendCount).toBe(0);
  });
});

describe("semaphore.run timer hygiene", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("unrefs the renewal timer and clears it when run settles", async () => {
    const created: NodeJS.Timeout[] = [];
    const unreffed: NodeJS.Timeout[] = [];
    const realSetInterval = globalThis.setInterval;
    vi.spyOn(globalThis, "setInterval").mockImplementation(((
      handler: () => void,
      ms?: number
    ) => {
      const timer = realSetInterval(handler, ms);
      created.push(timer);
      const unref = timer.unref.bind(timer);
      timer.unref = () => {
        unreffed.push(timer);
        return unref();
      };
      return timer;
    }) as typeof globalThis.setInterval);
    const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");

    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 80 });

    let refDuringBody: boolean | undefined;
    await slots.run(
      "openai",
      async () => {
        await sleep(60);
        // An interval that still holds a ref keeps `node script.js` alive.
        refDuringBody = created[0]?.hasRef();
      },
      { heartbeatMs: 20 }
    );

    expect(created).toHaveLength(1);
    expect(unreffed).toEqual(created);
    expect(refDuringBody).toBe(false);
    expect(clearIntervalSpy).toHaveBeenCalledWith(created[0]);
    expect(fake.extendCount).toBeGreaterThanOrEqual(1);
  });

  it("starts no timer at all when renewal is switched off", async () => {
    const setIntervalSpy = vi.spyOn(globalThis, "setInterval");
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 2 });

    await slots.run("openai", async () => 1, { heartbeatMs: false });

    expect(setIntervalSpy).not.toHaveBeenCalled();
  });
});

describe("semaphore contention defaults", () => {
  it("still fails fast when the pool is full and retries are default", async () => {
    const fake = semaphoreFake({ acquire: () => 0 });
    const slots = semaphore(fake.client, { limit: 1 });

    await expect(slots.run("full", async () => 1)).rejects.toBeInstanceOf(
      SemaphoreNotAcquiredError
    );
    // One attempt, no waiting, no renewal timer to clean up.
    expect(fake.evalshas()).toEqual(["sha-acquire"]);
  });
});

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
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

// Every test here runs on vitest's fake clock, `performance.now()` included,
// so a 150ms critical section costs no wall time and renews exactly as often on
// a loaded CI box as on a laptop.
beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Advance the fake clock a millisecond at a time until `promise` settles, then
 * hand it back. Stepping rather than jumping keeps timers firing in the order
 * real time would fire them.
 */
async function settle<T>(promise: Promise<T>): Promise<T> {
  let done = false;
  const tracked = promise.finally(() => {
    done = true;
  });
  // Observed here, so a rejection is not "unhandled" while the clock advances.
  tracked.catch(() => {});
  for (let step = 0; step < 10_000 && !done; step++) {
    await vi.advanceTimersByTimeAsync(1);
  }
  return tracked;
}

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
  it("keeps the slot while a critical section outlives leaseMs", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 5, leaseMs: 60 });

    const result = await settle(
      slots.run(
        "openai",
        async () => {
          await sleep(150); // Two and a half leases.
          return "done";
        },
        { heartbeatMs: 15 }
      )
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

    await settle(slots.run("openai", () => sleep(120)));

    // Renewed several times over the body's life, and nowhere near spinning:
    // on the fake clock, exactly at 25, 50, 75 and 100ms.
    expect(fake.extendCount).toBeGreaterThanOrEqual(2);
    expect(fake.extendCount).toBeLessThanOrEqual(20);
    expect(fake.extendCount).toBe(4);
  });

  it("adds no round trips when the body finishes inside one interval", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3 });

    await expect(settle(slots.run("openai", async () => 7))).resolves.toBe(7);

    // Unchanged from before renewal existed: acquire, then release.
    expect(fake.evalshas()).toEqual(["sha-acquire", "sha-release"]);
  });

  it("does not renew when renewal is switched off", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3, leaseMs: 20 });

    await expect(
      settle(
        slots.run("openai", async () => sleep(80).then(() => 1), {
          heartbeatMs: false
        })
      )
    ).resolves.toBe(1);
    expect(fake.extendCount).toBe(0);
  });

  it("validates heartbeatMs before taking a slot", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3 });

    await expect(
      settle(slots.run("openai", async () => 1, { heartbeatMs: 0 }))
    ).rejects.toBeInstanceOf(ValidationError);
    // Nothing was acquired, so no slot is held until its lease lapses.
    expect(fake.commands).toHaveLength(0);
  });

  it("resolves for a body many leases long while renewals keep succeeding", async () => {
    // The counterpart to every guard below: reporting a lost slot must not turn
    // into failing healthy work. The deadline moves forward with each successful
    // renewal, so five leases of body is unremarkable.
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 100 }); // 25ms.

    await expect(
      settle(slots.run("openai", () => sleep(500).then(() => "ok")))
    ).resolves.toBe("ok");
    expect(fake.extendCount).toBeGreaterThanOrEqual(4);
  });
});

/**
 * A `heartbeatMs` at or above the lease puts the first tick on or after expiry,
 * so the deadline trips before a single renewal has even been attempted, on a
 * semaphore with slots to spare. It passes for a fast body and fails for a slow
 * one, so the misconfiguration only surfaces under load. An explicit value is
 * rejected up front instead.
 */
describe("semaphore.run heartbeat bounds", () => {
  it("rejects an explicit heartbeatMs that is not meaningfully below leaseMs", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3, leaseMs: 300 });

    for (const heartbeatMs of [151, 300, 600]) {
      await expect(
        settle(slots.run("openai", async () => 1, { heartbeatMs }))
      ).rejects.toBeInstanceOf(ValidationError);
    }
    await expect(
      settle(slots.run("openai", async () => 1, { heartbeatMs: 600 }))
    ).rejects.toThrow(
      "semaphore heartbeatMs must be at most half of leaseMs (300) so a renewal lands before the slot could lapse, received 600"
    );
    // Rejected before the acquire, so no slot is held until its lease lapses.
    expect(fake.commands).toHaveLength(0);
  });

  it("accepts a heartbeatMs at exactly half of leaseMs", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3, leaseMs: 300 });

    // Half still leaves room for one renewal and one retry before expiry.
    await expect(
      settle(slots.run("openai", async () => 1, { heartbeatMs: 150 }))
    ).resolves.toBe(1);
  });

  it("checks the heartbeat against this run's leaseMs, not the store default", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3, leaseMs: 10_000 });

    await expect(
      settle(
        slots.run("openai", async () => 1, { leaseMs: 100, heartbeatMs: 80 })
      )
    ).rejects.toBeInstanceOf(ValidationError);
  });

  it("keeps deriving a default heartbeat no ratio check could satisfy", async () => {
    // `leaseMs: 1` derives a 1ms heartbeat, which *is* the whole lease. Only an
    // explicitly passed value is checked, so an absurd but working lease keeps
    // working rather than being rejected by a rule about the caller's intent.
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 3, leaseMs: 1 });

    const outcome = await slots
      .run("openai", async () => "ok")
      .catch((error: unknown) => error);

    expect(outcome).not.toBeInstanceOf(ValidationError);
    // It got as far as acquiring, which is what proves validation let it past.
    expect(fake.evalshas()).toContain("sha-acquire");
  });
});

describe("semaphore.run lost lease", () => {
  it("rejects with SemaphoreLeaseLostError even when the body resolves", async () => {
    const fake = semaphoreFake({ extend: () => 0 });
    const slots = semaphore(fake.client, { limit: 4, leaseMs: 60 });

    let abortReason: unknown;
    const promise = settle(
      slots.run(
        "openai",
        async (held) => {
          held.signal.addEventListener("abort", () => {
            abortReason = held.signal.reason;
          });
          await sleep(80);
          return "finished anyway";
        },
        { heartbeatMs: 10 }
      )
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
      settle(
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
      settle(
        slots.run("openai", () => sleep(120).then(() => "ok"), {
          heartbeatMs: 20,
          onRenewError: (error) => errors.push(error)
        })
      )
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
      settle(
        slots.run("openai", () => sleep(300).then(() => "ok"), {
          heartbeatMs: 10,
          onRenewError: (error) => errors.push(error)
        })
      )
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
      settle(
        slots.run("openai", () => sleep(300).then(() => "ok"), {
          heartbeatMs: 10
        })
      )
    ).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    expect(fake.extendCount).toBe(1);
  });

  it("does not report a deliberate release as a lost lease", async () => {
    const fake = semaphoreFake({ extend: () => 0 });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 60 });

    await expect(
      settle(
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
      )
    ).resolves.toBe("ok");
    expect(fake.extendCount).toBe(0);
  });

  it("reports a lost slot when the body blocks the event loop past the lease", async () => {
    // The case a flag set only from inside the renewal tick cannot see. A
    // synchronous stall keeps the tick (a macrotask) from ever running, and
    // `await fn(handle)` resumes on a microtask, so the completion check gets
    // there first with `lease.lost` still false. That is genuine over-admission:
    // the next acquire prunes the lapsed member and lets another caller in while
    // this body is still going.
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 60 });

    await expect(
      settle(
        slots.run("openai", () => {
          // Over three leases, fully synchronous: the monotonic clock moves on
          // and not one timer gets to run, exactly as in a real stall.
          const stalledUntil = performance.now() + 200;
          vi.spyOn(performance, "now").mockReturnValue(stalledUntil);
          return "critical section completed";
        })
      )
    ).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    // Not one renewal got to run, which is exactly why the flag was not enough.
    expect(fake.extendCount).toBe(0);
  });

  it("reports a lost slot when the final release finds it was not ours", async () => {
    // Nothing renewed and the local deadline is nowhere near: only the release
    // knows, because it runs the same ownership check `extend` does. Its answer
    // used to be discarded.
    const fake = semaphoreFake({ release: () => 0 });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 10_000 });

    await expect(
      settle(slots.run("openai", async () => "ok"))
    ).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
    expect(fake.extendCount).toBe(0);
  });

  it("survives an onRenewError hook that throws", async () => {
    // The renewal promise is deliberately discarded, so a throw from the hook
    // used to reject a promise nobody observes: an `unhandledRejection`, fatal
    // in default Node. A telemetry callback cannot be allowed to do that.
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      const fake = semaphoreFake({
        extend: () => {
          throw new Error("connection reset");
        }
      });
      const slots = semaphore(fake.client, { limit: 2, leaseMs: 200 });
      let hookCalls = 0;

      await expect(
        settle(
          slots.run("openai", () => sleep(120).then(() => "ok"), {
            heartbeatMs: 20,
            onRenewError: () => {
              hookCalls += 1;
              throw new Error("hook blew up");
            }
          })
        )
      ).resolves.toBe("ok");

      // Called, repeatedly, and swallowed every time: the body's outcome stands.
      expect(hookCalls).toBeGreaterThanOrEqual(2);
      await vi.advanceTimersByTimeAsync(20); // A turn to report anything unhandled.
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  it("does not call onRenewError for a renewal that settles after run returned", async () => {
    // A round trip still in flight when the slot is given back can fail after
    // the caller already has an answer. Reporting then would fire the hook
    // outside the lifetime of the call it belongs to, so a late failure is
    // dropped.
    const fake = semaphoreFake({
      extend: () =>
        new Promise<RedisReply>((_resolve, reject) => {
          setTimeout(() => reject(new Error("late")), 80);
        })
    });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 200 });
    const errors: unknown[] = [];

    await expect(
      settle(
        slots.run("openai", () => sleep(30).then(() => "ok"), {
          heartbeatMs: 10,
          onRenewError: (error) => errors.push(error)
        })
      )
    ).resolves.toBe("ok");

    expect(fake.extendCount).toBe(1);
    // Long enough for the in-flight renewal to reject.
    await vi.advanceTimersByTimeAsync(120);
    expect(errors).toEqual([]);
  });
});

describe("semaphore.run timer hygiene", () => {
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
    await settle(
      slots.run(
        "openai",
        async () => {
          await sleep(60);
          // An interval that still holds a ref keeps `node script.js` alive.
          refDuringBody = created[0]?.hasRef();
        },
        { heartbeatMs: 20 }
      )
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

    await settle(slots.run("openai", async () => 1, { heartbeatMs: false }));

    expect(setIntervalSpy).not.toHaveBeenCalled();
  });

  it("clears the renewal timer as soon as the lease is lost", async () => {
    // `run()`'s exit path is not the only thing that has to clear the interval:
    // a body that ignores the abort signal and never settles never reaches it,
    // and the interval used to stay armed, waking up to early-return for the
    // life of the process.
    const created: NodeJS.Timeout[] = [];
    const realSetInterval = globalThis.setInterval;
    vi.spyOn(globalThis, "setInterval").mockImplementation(((
      handler: () => void,
      ms?: number
    ) => {
      const timer = realSetInterval(handler, ms);
      created.push(timer);
      return timer;
    }) as typeof globalThis.setInterval);
    const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");

    const fake = semaphoreFake({ extend: () => 0 });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 10_000 });

    let lostReason: unknown;
    // Never settles and never checks the signal, so `run()` stays pending.
    void slots.run(
      "openai",
      (held) =>
        new Promise<never>(() => {
          held.signal.addEventListener("abort", () => {
            lostReason = held.signal.reason;
          });
        }),
      { heartbeatMs: 10 }
    );
    await vi.advanceTimersByTimeAsync(60); // Several heartbeats after the loss.

    expect(lostReason).toBeInstanceOf(SemaphoreLeaseLostError);
    expect(created).toHaveLength(1);
    expect(clearIntervalSpy).toHaveBeenCalledWith(created[0]);
    // And it really is gone: no later tick renewed again.
    expect(fake.extendCount).toBe(1);
  });
});

describe("semaphore contention defaults", () => {
  it("still fails fast when the pool is full and retries are default", async () => {
    const fake = semaphoreFake({ acquire: () => 0 });
    const slots = semaphore(fake.client, { limit: 1 });

    await expect(
      settle(slots.run("full", async () => 1))
    ).rejects.toBeInstanceOf(SemaphoreNotAcquiredError);
    // One attempt, no waiting, no renewal timer to clean up.
    expect(fake.evalshas()).toEqual(["sha-acquire"]);
  });
});

describe("semaphore lease deadlines use a monotonic clock", () => {
  it("does not call a healthy slot lost when the wall clock jumps forward", async () => {
    // `Date.now()` follows the wall clock. An NTP step of an hour mid-body
    // used to put every deadline in the past, so a healthy, renewed slot was
    // reported lost and its body's result thrown away.
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 100 });

    await expect(
      settle(
        slots.run("openai", async () => {
          await sleep(30);
          vi.setSystemTime(Date.now() + 3_600_000);
          await sleep(30);
          return "ok";
        })
      )
    ).resolves.toBe("ok");
  });

  it("still calls the slot lost when a wall-clock step backwards hides the lapse", async () => {
    const fake = semaphoreFake({
      extend: () => new Promise<RedisReply>(() => {})
    });
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 40 });

    await expect(
      settle(
        slots.run(
          "openai",
          async () => {
            vi.setSystemTime(Date.now() - 3_600_000);
            await sleep(200);
            return "ok";
          },
          { heartbeatMs: 10 }
        )
      )
    ).rejects.toBeInstanceOf(SemaphoreLeaseLostError);
  });
});

describe("semaphore waiting", () => {
  it("jitters each retry delay around retryDelayMs", async () => {
    const fake = semaphoreFake({ acquire: () => 0 });
    const slots = semaphore(fake.client, { limit: 1 });
    // The first wait draws the bottom of the range, the second the top.
    vi.spyOn(Math, "random").mockReturnValueOnce(0).mockReturnValueOnce(0.999);
    const attempts = () =>
      fake.evalshas().filter((sha) => sha === "sha-acquire").length;

    void slots.acquire("full", { retries: 2, retryDelayMs: 100 });
    await vi.advanceTimersByTimeAsync(49);
    expect(attempts()).toBe(1);
    await vi.advanceTimersByTimeAsync(1); // 0.5 × 100
    expect(attempts()).toBe(2);
    await vi.advanceTimersByTimeAsync(148);
    expect(attempts()).toBe(2);
    await vi.advanceTimersByTimeAsync(2); // ~1.5 × 100
    expect(attempts()).toBe(3);
  });

  it("bounds the whole wait with waitTimeoutMs, trying once more at the deadline", async () => {
    const fake = semaphoreFake({ acquire: () => 0 });
    const slots = semaphore(fake.client, { limit: 1 });
    vi.spyOn(Math, "random").mockReturnValue(0.5); // Exactly retryDelayMs.

    let outcome: unknown = "pending";
    void slots
      .acquire("full", { waitTimeoutMs: 250, retryDelayMs: 100 })
      .then((held) => {
        outcome = held;
      });
    await vi.advanceTimersByTimeAsync(249);
    expect(outcome).toBe("pending");
    await vi.advanceTimersByTimeAsync(1);
    expect(outcome).toBeNull();
    expect(fake.evalshas().filter((sha) => sha === "sha-acquire")).toHaveLength(
      4
    );
  });

  it("gives run a slot that frees up inside waitTimeoutMs", async () => {
    let attempt = 0;
    const fake = semaphoreFake({ acquire: () => (++attempt < 3 ? 0 : 1) });
    const slots = semaphore(fake.client, { limit: 1 });

    await expect(
      settle(slots.run("full", async () => "in", { waitTimeoutMs: 1_000 }))
    ).resolves.toBe("in");
  });

  it("rejects a negative waitTimeoutMs before taking a slot", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 1 });

    await expect(
      slots.acquire("x", { waitTimeoutMs: -5 })
    ).rejects.toBeInstanceOf(ValidationError);
    expect(fake.commands).toHaveLength(0);
  });
});

describe("semaphore extend", () => {
  it("re-applies this acquisition's leaseMs, not the store default", async () => {
    const fake = semaphoreFake();
    const slots = semaphore(fake.client, { limit: 2, leaseMs: 60_000 });

    const held = await slots.acquire("openai", { leaseMs: 5_000 });
    await expect(held?.extend()).resolves.toBe(true);
    // [EVALSHA, sha, 1, key, leaseMs, token]
    expect(fake.renewals()[0]?.[4]).toBe("5000");
  });
});

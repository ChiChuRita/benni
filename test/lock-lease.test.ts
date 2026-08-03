import { afterEach, describe, expect, it, vi } from "vitest";
import { ValidationError } from "../src/core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import {
  LockLeaseLostError,
  LockNotAcquiredError,
  lock
} from "../src/primitives/index.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A fake that answers by command rather than from a fixed reply queue. How many
 * renewals a timing-based test performs is not fixed, so a queued fake would
 * make every test here a race against the interval.
 */
function lockFake(behavior?: {
  /** Reply to the acquiring `SET`. Default `"OK"`. */
  acquire?: () => RedisReply;
  /** Reply to the extend script, given the 1-based call number. Default `1`. */
  extend?: (call: number) => RedisReply | Promise<RedisReply>;
  /** Reply to the release script. Default `1`. */
  release?: () => RedisReply;
}) {
  const commands: RedisCommand[] = [];
  let extendCalls = 0;
  const client: RedisClient = {
    async send(command) {
      commands.push(command);
      const verb = command[0];
      // `?? "OK"` would turn a deliberate `null` (lock held) back into a win.
      if (verb === "SET") {
        return behavior?.acquire === undefined ? "OK" : behavior.acquire();
      }
      if (verb === "SCRIPT") {
        // Two scripts, told apart by their source: extend PEXPIREs, release DELs.
        return String(command[2]).includes("PEXPIRE")
          ? "sha-extend"
          : "sha-release";
      }
      if (verb === "EVALSHA") {
        if (command[1] !== "sha-extend") return behavior?.release?.() ?? 1;
        extendCalls += 1;
        return behavior?.extend?.(extendCalls) ?? 1;
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
    of(verb: string) {
      return commands.filter((command) => command[0] === verb);
    },
    renewals() {
      return commands.filter(
        (command) => command[0] === "EVALSHA" && command[1] === "sha-extend"
      );
    }
  };
}

/**
 * `lock().run()` used to acquire with a TTL and never renew it, so a critical
 * section that outlived `ttlMs` silently lost the lock: the key expired, another
 * caller took it, and the body kept running as though it were still exclusive.
 * These pin the renewal, the loss report, and the timer hygiene that fixes it.
 */
describe("lock.run lease renewal", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("keeps the lock while a critical section outlives ttlMs", async () => {
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 60 });

    const result = await locks.run(
      "res",
      async () => {
        await sleep(150); // Two and a half TTLs.
        return "done";
      },
      { heartbeatMs: 15 }
    );

    expect(result).toBe("done");
    const renewals = fake.renewals();
    expect(renewals.length).toBeGreaterThanOrEqual(3);
    const token = fake.of("SET")[0]?.[2];
    // Every renewal is the token-checked extend, re-applying the same TTL.
    for (const renewal of renewals) {
      expect(renewal.slice(0, 4)).toEqual([
        "EVALSHA",
        "sha-extend",
        1,
        "lock:res"
      ]);
      expect(renewal[4]).toBe(token);
      expect(renewal[5]).toBe("60");
    }
  });

  it("defaults the renewal interval to a quarter of ttlMs", async () => {
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 100 }); // 25ms heartbeat.

    await locks.run("res", () => sleep(120));

    // Renewed several times over the body's life, and nowhere near spinning.
    expect(fake.extendCount).toBeGreaterThanOrEqual(2);
    expect(fake.extendCount).toBeLessThanOrEqual(20);
  });

  it("adds no round trips when the body finishes inside one interval", async () => {
    const fake = lockFake();
    const locks = lock(fake.client);

    await expect(locks.run("res", async () => 7)).resolves.toBe(7);

    // Unchanged from before renewal existed: acquire, load, release.
    expect(fake.commands.map((command) => command[0])).toEqual([
      "SET",
      "SCRIPT",
      "EVALSHA"
    ]);
  });

  it("does not renew when renewal is switched off", async () => {
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 20 });

    await expect(
      locks.run("res", async () => sleep(80).then(() => 1), {
        heartbeatMs: false
      })
    ).resolves.toBe(1);
    expect(fake.extendCount).toBe(0);
  });

  it("validates heartbeatMs before taking the lock", async () => {
    const fake = lockFake();
    const locks = lock(fake.client);

    await expect(
      locks.run("res", async () => 1, { heartbeatMs: 0 })
    ).rejects.toBeInstanceOf(ValidationError);
    // Nothing was acquired, so nothing is stranded until its TTL lapses.
    expect(fake.commands).toHaveLength(0);
  });

  it("resolves for a body many TTLs long while renewals keep succeeding", async () => {
    // The counterpart to every guard below: reporting a lost lock must not turn
    // into failing healthy work. The deadline moves forward with each successful
    // renewal, so five TTLs of body is unremarkable.
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 100 }); // 25ms heartbeat.

    await expect(
      locks.run("res", () => sleep(500).then(() => "ok"))
    ).resolves.toBe("ok");
    expect(fake.extendCount).toBeGreaterThanOrEqual(4);
  });
});

/**
 * A `heartbeatMs` at or above the TTL puts the first tick on or after expiry, so
 * the deadline trips before a single renewal has even been attempted, on an
 * uncontended lock. It passes for a fast body and fails for a slow one, so the
 * misconfiguration only surfaces under load. An explicit value is rejected up
 * front instead.
 */
describe("lock.run heartbeat bounds", () => {
  it("rejects an explicit heartbeatMs that is not meaningfully below ttlMs", async () => {
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 300 });

    for (const heartbeatMs of [151, 300, 600]) {
      await expect(
        locks.run("res", async () => 1, { heartbeatMs })
      ).rejects.toBeInstanceOf(ValidationError);
    }
    await expect(
      locks.run("res", async () => 1, { heartbeatMs: 600 })
    ).rejects.toThrow(
      "lock heartbeatMs must be at most half of ttlMs (300) so a renewal lands before the lock could lapse, received 600"
    );
    // Rejected before the acquire, so nothing is stranded until its TTL lapses.
    expect(fake.commands).toHaveLength(0);
  });

  it("accepts a heartbeatMs at exactly half of ttlMs", async () => {
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 300 });

    // Half still leaves room for one renewal and one retry before expiry.
    await expect(
      locks.run("res", async () => 1, { heartbeatMs: 150 })
    ).resolves.toBe(1);
  });

  it("checks the heartbeat against this run's ttlMs, not the store default", async () => {
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 10_000 });

    await expect(
      locks.run("res", async () => 1, { ttlMs: 100, heartbeatMs: 80 })
    ).rejects.toBeInstanceOf(ValidationError);
  });

  it("keeps deriving a default heartbeat no ratio check could satisfy", async () => {
    // `ttlMs: 1` derives a 1ms heartbeat, which *is* the whole TTL. Only an
    // explicitly passed value is checked, so an absurd but working TTL keeps
    // working rather than being rejected by a rule about the caller's intent.
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 1 });

    const outcome = await locks
      .run("res", async () => "ok")
      .catch((error: unknown) => error);

    expect(outcome).not.toBeInstanceOf(ValidationError);
    // It got as far as acquiring, which is what proves validation let it past.
    expect(fake.of("SET")).toHaveLength(1);
  });
});

describe("lock.run lost lease", () => {
  it("rejects with LockLeaseLostError even when the body resolves", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = lock(fake.client, { ttlMs: 60 });

    let abortReason: unknown;
    const promise = locks.run(
      "res",
      async (handle) => {
        handle.signal.addEventListener("abort", () => {
          abortReason = handle.signal.reason;
        });
        await sleep(80);
        return "finished anyway";
      },
      { heartbeatMs: 10 }
    );

    await expect(promise).rejects.toBeInstanceOf(LockLeaseLostError);
    await expect(promise).rejects.toMatchObject({ key: "lock:res" });
    expect(abortReason).toBeInstanceOf(LockLeaseLostError);
  });

  it("aborts the handle's signal so the body can stop early", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = lock(fake.client, { ttlMs: 10_000 });

    await expect(
      locks.run(
        "res",
        (handle) =>
          new Promise<never>((_resolve, reject) => {
            handle.signal.addEventListener("abort", () =>
              reject(handle.signal.reason)
            );
          }),
        { heartbeatMs: 10 }
      )
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    // One failed renewal was enough; nothing kept renewing a lost lock.
    expect(fake.extendCount).toBe(1);
  });

  it("aborts the signal when a manual extend finds the lock gone", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = lock(fake.client);

    const handle = await locks.acquire("res");
    expect(handle?.signal.aborted).toBe(false);
    await expect(handle?.extend()).resolves.toBe(false);
    expect(handle?.signal.aborted).toBe(true);
    expect(handle?.signal.reason).toBeInstanceOf(LockLeaseLostError);
  });

  it("reports a failed renewal round trip without declaring the lock lost", async () => {
    const fake = lockFake({
      extend: (call) => {
        if (call === 1) throw new Error("connection reset");
        return 1;
      }
    });
    const locks = lock(fake.client, { ttlMs: 200 });
    const errors: unknown[] = [];

    await expect(
      locks.run("res", () => sleep(120).then(() => "ok"), {
        heartbeatMs: 20,
        onRenewError: (error) => errors.push(error)
      })
    ).resolves.toBe("ok");

    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("connection reset");
  });

  it("declares the lock lost once renewals keep failing past the TTL", async () => {
    const fake = lockFake({
      extend: () => {
        throw new Error("unreachable");
      }
    });
    const locks = lock(fake.client, { ttlMs: 40 });
    const errors: unknown[] = [];

    await expect(
      locks.run("res", () => sleep(300).then(() => "ok"), {
        heartbeatMs: 10,
        onRenewError: (error) => errors.push(error)
      })
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    expect(errors.length).toBeGreaterThanOrEqual(2);
  });

  it("declares the lock lost when a renewal hangs past the TTL", async () => {
    // A round trip that never comes back is the case the one-at-a-time guard
    // hides: no further renewal is even attempted, so only the deadline can
    // notice the lock has lapsed.
    const fake = lockFake({ extend: () => new Promise<RedisReply>(() => {}) });
    const locks = lock(fake.client, { ttlMs: 40 });

    await expect(
      locks.run("res", () => sleep(300).then(() => "ok"), { heartbeatMs: 10 })
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    expect(fake.extendCount).toBe(1);
  });

  it("does not report a deliberate release as a lost lease", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = lock(fake.client, { ttlMs: 60 });

    await expect(
      locks.run(
        "res",
        async (handle) => {
          await handle.release();
          // Renewals would find the key gone; giving it up was the point.
          await sleep(80);
          return "ok";
        },
        { heartbeatMs: 10 }
      )
    ).resolves.toBe("ok");
    expect(fake.extendCount).toBe(0);
  });

  it("reports a lost lock when the body blocks the event loop past the ttl", async () => {
    // The case a flag set only from inside the renewal tick cannot see. A
    // synchronous stall keeps the tick (a macrotask) from ever running, and
    // `await fn(handle)` resumes on a microtask, so the completion check gets
    // there first with `lease.lost` still false. The deadline has to be read at
    // completion too, or `run()` reports a success for an expired lock.
    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 60 });

    await expect(
      locks.run("res", () => {
        const until = Date.now() + 200; // Over three TTLs, fully synchronous.
        while (Date.now() < until) {}
        return "critical section completed";
      })
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    // Not one renewal got to run, which is exactly why the flag was not enough.
    expect(fake.extendCount).toBe(0);
  });

  it("reports a lost lock when the final release finds it was not ours", async () => {
    // Nothing renewed and the local deadline is nowhere near: only the release
    // knows, because it runs the same token check `extend` does. Its answer used
    // to be discarded.
    const fake = lockFake({ release: () => 0 });
    const locks = lock(fake.client, { ttlMs: 10_000 });

    await expect(locks.run("res", async () => "ok")).rejects.toBeInstanceOf(
      LockLeaseLostError
    );
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
      const fake = lockFake({
        extend: () => {
          throw new Error("connection reset");
        }
      });
      const locks = lock(fake.client, { ttlMs: 200 });
      let hookCalls = 0;

      await expect(
        locks.run("res", () => sleep(120).then(() => "ok"), {
          heartbeatMs: 20,
          onRenewError: () => {
            hookCalls += 1;
            throw new Error("hook blew up");
          }
        })
      ).resolves.toBe("ok");

      // Called, repeatedly, and swallowed every time: the body's outcome stands.
      expect(hookCalls).toBeGreaterThanOrEqual(2);
      await sleep(20); // A turn for Node to report anything unhandled.
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  it("does not call onRenewError for a renewal that settles after run returned", async () => {
    // A round trip still in flight when the lock is released can fail after the
    // caller already has an answer. Reporting then would fire the hook outside
    // the lifetime of the call it belongs to, so a late failure is dropped.
    const fake = lockFake({
      extend: () =>
        new Promise<RedisReply>((_resolve, reject) => {
          setTimeout(() => reject(new Error("late")), 80);
        })
    });
    const locks = lock(fake.client, { ttlMs: 200 });
    const errors: unknown[] = [];

    await expect(
      locks.run("res", () => sleep(30).then(() => "ok"), {
        heartbeatMs: 10,
        onRenewError: (error) => errors.push(error)
      })
    ).resolves.toBe("ok");

    expect(fake.extendCount).toBe(1);
    await sleep(120); // Long enough for the in-flight renewal to reject.
    expect(errors).toEqual([]);
  });
});

describe("lock.run timer hygiene", () => {
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

    const fake = lockFake();
    const locks = lock(fake.client, { ttlMs: 80 });

    let refDuringBody: boolean | undefined;
    await locks.run(
      "res",
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
    const fake = lockFake();
    const locks = lock(fake.client);

    await locks.run("res", async () => 1, { heartbeatMs: false });

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

    const fake = lockFake({ extend: () => 0 });
    const locks = lock(fake.client, { ttlMs: 10_000 });

    let lostReason: unknown;
    // Never settles and never checks the signal, so `run()` stays pending.
    void locks.run(
      "res",
      (handle) =>
        new Promise<never>(() => {
          handle.signal.addEventListener("abort", () => {
            lostReason = handle.signal.reason;
          });
        }),
      { heartbeatMs: 10 }
    );
    await sleep(60); // Several heartbeats after the loss.

    expect(lostReason).toBeInstanceOf(LockLeaseLostError);
    expect(created).toHaveLength(1);
    expect(clearIntervalSpy).toHaveBeenCalledWith(created[0]);
    // And it really is gone: no later tick renewed again.
    expect(fake.extendCount).toBe(1);
  });
});

describe("lock contention defaults", () => {
  it("still fails fast when the lock is held and retries are default", async () => {
    const fake = lockFake({ acquire: () => null });
    const locks = lock(fake.client);

    await expect(locks.run("held", async () => 1)).rejects.toBeInstanceOf(
      LockNotAcquiredError
    );
    // One attempt, no waiting, no renewal timer to clean up.
    expect(fake.of("SET")).toHaveLength(1);
  });
});

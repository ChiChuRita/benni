import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { slotOf } from "../src/cluster.js";
import { ValidationError } from "../src/core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import {
  LockLeaseLostError,
  LockNotAcquiredError
} from "../src/primitives/errors.js";
import { createLock } from "../src/primitives/lock.js";
import { fakeClient } from "./fake-client.js";

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
 * renewals a timing-based test performs depends on the interval, so a queued
 * fake would make every test here a race against it. Scripts are told apart by
 * their source: acquire INCRs the fence, extend PEXPIREs, release DELs.
 */
function lockFake(behavior?: {
  /** Reply to the acquire script: the fence, or `0` when held. Default `1`. */
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
      if (verb === "SCRIPT") {
        const lua = String(command[2]);
        if (lua.includes("INCR")) return "sha-acquire";
        return lua.includes("PEXPIRE") ? "sha-extend" : "sha-release";
      }
      if (verb === "EVALSHA") {
        if (command[1] === "sha-acquire") {
          // `?? 1` would turn a deliberate `0` (lock held) back into a win.
          return behavior?.acquire === undefined ? 1 : behavior.acquire();
        }
        if (command[1] === "sha-release") return behavior?.release?.() ?? 1;
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
  const evalsOf = (sha: string) =>
    commands.filter(
      (command) => command[0] === "EVALSHA" && command[1] === sha
    );
  return {
    client,
    commands,
    get extendCount() {
      return extendCalls;
    },
    acquires: () => evalsOf("sha-acquire"),
    renewals: () => evalsOf("sha-extend")
  };
}

/**
 * `createLock().run()` used to acquire with a TTL and never renew it, so a critical
 * section that outlived `ttlMs` silently lost the lock: the key expired, another
 * caller took it, and the body kept running as though it were still exclusive.
 * These pin the renewal, the loss report, and the timer hygiene that fixes it.
 */
describe("lock.run lease renewal", () => {
  it("keeps the lock while a critical section outlives ttlMs", async () => {
    const fake = lockFake();
    const locks = createLock(fake.client, { ttlMs: 60 });

    const result = await settle(
      locks.run(
        "res",
        async () => {
          await sleep(150); // Two and a half TTLs.
          return "done";
        },
        { heartbeatMs: 15 }
      )
    );

    expect(result).toBe("done");
    const renewals = fake.renewals();
    expect(renewals.length).toBeGreaterThanOrEqual(3);
    // [EVALSHA, sha, 2, key, fenceKey, token, ttlMs]
    const token = fake.acquires()[0]?.[5];
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
    const locks = createLock(fake.client, { ttlMs: 100 }); // 25ms heartbeat.

    await settle(locks.run("res", () => sleep(120)));

    // Renewed several times over the body's life, and nowhere near spinning:
    // on the fake clock, exactly at 25, 50, 75 and 100ms.
    expect(fake.extendCount).toBeGreaterThanOrEqual(2);
    expect(fake.extendCount).toBeLessThanOrEqual(20);
    expect(fake.extendCount).toBe(4);
  });

  it("adds no round trips when the body finishes inside one interval", async () => {
    const fake = lockFake();
    const locks = createLock(fake.client);

    await expect(settle(locks.run("res", async () => 7))).resolves.toBe(7);

    // Acquire and release, each a script load plus its run, and no renewal.
    expect(fake.commands.map((command) => command[0])).toEqual([
      "SCRIPT",
      "EVALSHA",
      "SCRIPT",
      "EVALSHA"
    ]);
    expect(fake.extendCount).toBe(0);
  });

  it("does not renew when renewal is switched off", async () => {
    const fake = lockFake();
    const locks = createLock(fake.client, { ttlMs: 20 });

    await expect(
      settle(
        locks.run("res", async () => sleep(80).then(() => 1), {
          heartbeatMs: false
        })
      )
    ).resolves.toBe(1);
    expect(fake.extendCount).toBe(0);
  });

  it("validates heartbeatMs before taking the lock", async () => {
    const fake = lockFake();
    const locks = createLock(fake.client);

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
    const locks = createLock(fake.client, { ttlMs: 100 }); // 25ms heartbeat.

    await expect(
      settle(locks.run("res", () => sleep(500).then(() => "ok")))
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
    const locks = createLock(fake.client, { ttlMs: 300 });

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
    const locks = createLock(fake.client, { ttlMs: 300 });

    // Half still leaves room for one renewal and one retry before expiry.
    await expect(
      settle(locks.run("res", async () => 1, { heartbeatMs: 150 }))
    ).resolves.toBe(1);
  });

  it("checks the heartbeat against this run's ttlMs, not the store default", async () => {
    const fake = lockFake();
    const locks = createLock(fake.client, { ttlMs: 10_000 });

    await expect(
      locks.run("res", async () => 1, { ttlMs: 100, heartbeatMs: 80 })
    ).rejects.toBeInstanceOf(ValidationError);
  });

  it("keeps deriving a default heartbeat no ratio check could satisfy", async () => {
    // `ttlMs: 1` derives a 1ms heartbeat, which *is* the whole TTL. Only an
    // explicitly passed value is checked, so an absurd but working TTL keeps
    // working rather than being rejected by a rule about the caller's intent.
    const fake = lockFake();
    const locks = createLock(fake.client, { ttlMs: 1 });

    const outcome = await settle(locks.run("res", async () => "ok")).catch(
      (error: unknown) => error
    );

    expect(outcome).not.toBeInstanceOf(ValidationError);
    // It got as far as acquiring, which is what proves validation let it past.
    expect(fake.acquires()).toHaveLength(1);
  });
});

describe("lock.run lost lease", () => {
  it("rejects with LockLeaseLostError even when the body resolves", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = createLock(fake.client, { ttlMs: 60 });

    let abortReason: unknown;
    const promise = settle(
      locks.run(
        "res",
        async (handle) => {
          handle.signal.addEventListener("abort", () => {
            abortReason = handle.signal.reason;
          });
          await sleep(80);
          return "finished anyway";
        },
        { heartbeatMs: 10 }
      )
    );

    await expect(promise).rejects.toBeInstanceOf(LockLeaseLostError);
    await expect(promise).rejects.toMatchObject({ key: "lock:res" });
    expect(abortReason).toBeInstanceOf(LockLeaseLostError);
  });

  it("aborts the handle's signal so the body can stop early", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = createLock(fake.client, { ttlMs: 10_000 });

    await expect(
      settle(
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
      )
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    // One failed renewal was enough; nothing kept renewing a lost lock.
    expect(fake.extendCount).toBe(1);
  });

  it("aborts the signal when a manual extend finds the lock gone", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = createLock(fake.client);

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
    const locks = createLock(fake.client, { ttlMs: 200 });
    const errors: unknown[] = [];

    await expect(
      settle(
        locks.run("res", () => sleep(120).then(() => "ok"), {
          heartbeatMs: 20,
          onRenewError: (error) => errors.push(error)
        })
      )
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
    const locks = createLock(fake.client, { ttlMs: 40 });
    const errors: unknown[] = [];

    await expect(
      settle(
        locks.run("res", () => sleep(300).then(() => "ok"), {
          heartbeatMs: 10,
          onRenewError: (error) => errors.push(error)
        })
      )
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    expect(errors.length).toBeGreaterThanOrEqual(2);
  });

  it("declares the lock lost when a renewal hangs past the TTL", async () => {
    // A round trip that never comes back is the case the one-at-a-time guard
    // hides: no further renewal is even attempted, so only the deadline can
    // notice the lock has lapsed.
    const fake = lockFake({ extend: () => new Promise<RedisReply>(() => {}) });
    const locks = createLock(fake.client, { ttlMs: 40 });

    await expect(
      settle(
        locks.run("res", () => sleep(300).then(() => "ok"), { heartbeatMs: 10 })
      )
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    expect(fake.extendCount).toBe(1);
  });

  it("does not report a deliberate release as a lost lease", async () => {
    const fake = lockFake({ extend: () => 0 });
    const locks = createLock(fake.client, { ttlMs: 60 });

    await expect(
      settle(
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
    const locks = createLock(fake.client, { ttlMs: 60 });

    await expect(
      settle(
        locks.run("res", () => {
          // Over three TTLs, fully synchronous: the monotonic clock moves on
          // and not one timer gets to run, exactly as in a real stall.
          const stalledUntil = performance.now() + 200;
          vi.spyOn(performance, "now").mockReturnValue(stalledUntil);
          return "critical section completed";
        })
      )
    ).rejects.toBeInstanceOf(LockLeaseLostError);
    // Not one renewal got to run, which is exactly why the flag was not enough.
    expect(fake.extendCount).toBe(0);
  });

  it("reports a lost lock when the final release finds it was not ours", async () => {
    // Nothing renewed and the local deadline is nowhere near: only the release
    // knows, because it runs the same token check `extend` does. Its answer used
    // to be discarded.
    const fake = lockFake({ release: () => 0 });
    const locks = createLock(fake.client, { ttlMs: 10_000 });

    await expect(
      settle(locks.run("res", async () => "ok"))
    ).rejects.toBeInstanceOf(LockLeaseLostError);
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
      const locks = createLock(fake.client, { ttlMs: 200 });
      let hookCalls = 0;

      await expect(
        settle(
          locks.run("res", () => sleep(120).then(() => "ok"), {
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
    // A round trip still in flight when the lock is released can fail after the
    // caller already has an answer. Reporting then would fire the hook outside
    // the lifetime of the call it belongs to, so a late failure is dropped.
    const fake = lockFake({
      extend: () =>
        new Promise<RedisReply>((_resolve, reject) => {
          setTimeout(() => reject(new Error("late")), 80);
        })
    });
    const locks = createLock(fake.client, { ttlMs: 200 });
    const errors: unknown[] = [];

    await expect(
      settle(
        locks.run("res", () => sleep(30).then(() => "ok"), {
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

  it("does not mask fn's success when release rejects (review #7)", async () => {
    // Only the acquire's load and run are queued; the release's SCRIPT LOAD
    // hits an empty queue and rejects — run() must still resolve with fn's
    // result.
    const locks = createLock(fakeClient([], ["sha-acquire", 1]));
    await expect(locks.run("r", async () => 7)).resolves.toBe(7);
  });
});

describe("lock.run timer hygiene", () => {
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
    const locks = createLock(fake.client, { ttlMs: 80 });

    let refDuringBody: boolean | undefined;
    await settle(
      locks.run(
        "res",
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
    const fake = lockFake();
    const locks = createLock(fake.client);

    await settle(locks.run("res", async () => 1, { heartbeatMs: false }));

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
    const locks = createLock(fake.client, { ttlMs: 10_000 });

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
    await vi.advanceTimersByTimeAsync(60); // Several heartbeats after the loss.

    expect(lostReason).toBeInstanceOf(LockLeaseLostError);
    expect(created).toHaveLength(1);
    expect(clearIntervalSpy).toHaveBeenCalledWith(created[0]);
    // And it really is gone: no later tick renewed again.
    expect(fake.extendCount).toBe(1);
  });
});

describe("lock lease deadlines use a monotonic clock", () => {
  it("does not call a healthy lock lost when the wall clock jumps forward", async () => {
    // `Date.now()` follows the wall clock. An NTP step of an hour mid-body
    // used to put every deadline in the past, so a healthy, renewed lock was
    // reported lost and its body's result thrown away.
    const fake = lockFake();
    const locks = createLock(fake.client, { ttlMs: 100 });

    await expect(
      settle(
        locks.run("res", async () => {
          await sleep(30);
          vi.setSystemTime(Date.now() + 3_600_000);
          await sleep(30);
          return "ok";
        })
      )
    ).resolves.toBe("ok");
  });

  it("still calls the lock lost when a wall-clock step backwards hides the lapse", async () => {
    // The mirror image: stepping the wall clock back used to make an expired
    // lease look live for the size of the step. Renewals hang, so only the
    // deadline can notice, and it must read time that only moves forward.
    const fake = lockFake({ extend: () => new Promise<RedisReply>(() => {}) });
    const locks = createLock(fake.client, { ttlMs: 40 });

    await expect(
      settle(
        locks.run(
          "res",
          async () => {
            vi.setSystemTime(Date.now() - 3_600_000);
            await sleep(200);
            return "ok";
          },
          { heartbeatMs: 10 }
        )
      )
    ).rejects.toBeInstanceOf(LockLeaseLostError);
  });
});

describe("lock contention defaults", () => {
  it("still fails fast when the lock is held and retries are default", async () => {
    const fake = lockFake({ acquire: () => 0 });
    const locks = createLock(fake.client);

    await expect(locks.run("held", async () => 1)).rejects.toBeInstanceOf(
      LockNotAcquiredError
    );
    // One attempt, no waiting, no renewal timer to clean up.
    expect(fake.acquires()).toHaveLength(1);
  });

  it("jitters each retry delay around retryDelayMs", async () => {
    // Fixed delays kept contenders that collided once waking in lockstep and
    // colliding again. Each wait is spread over [0.5, 1.5) × retryDelayMs.
    const fake = lockFake({ acquire: () => 0 });
    const locks = createLock(fake.client);
    // The first wait draws the bottom of the range, the second the top.
    vi.spyOn(Math, "random").mockReturnValueOnce(0).mockReturnValueOnce(0.999);

    void locks.acquire("held", { retries: 2, retryDelayMs: 100 });
    await vi.advanceTimersByTimeAsync(49);
    expect(fake.acquires()).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1); // 0.5 × 100
    expect(fake.acquires()).toHaveLength(2);

    await vi.advanceTimersByTimeAsync(148);
    expect(fake.acquires()).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(2); // ~1.5 × 100
    expect(fake.acquires()).toHaveLength(3);
  });

  it("bounds the whole wait with waitTimeoutMs, trying once more at the deadline", async () => {
    const fake = lockFake({ acquire: () => 0 });
    const locks = createLock(fake.client);
    vi.spyOn(Math, "random").mockReturnValue(0.5); // Exactly retryDelayMs.

    let outcome: unknown = "pending";
    void locks
      .acquire("held", { waitTimeoutMs: 250, retryDelayMs: 100 })
      .then((handle) => {
        outcome = handle;
      });
    await vi.advanceTimersByTimeAsync(249);
    expect(outcome).toBe("pending");
    await vi.advanceTimersByTimeAsync(1);
    expect(outcome).toBeNull();
    // At 0, 100, 200, and the last sleep clipped to land on 250.
    expect(fake.acquires()).toHaveLength(4);
  });

  it("lets retries stop the wait before waitTimeoutMs does", async () => {
    const fake = lockFake({ acquire: () => 0 });
    const locks = createLock(fake.client);

    await expect(
      settle(
        locks.run("held", async () => 1, {
          retries: 1,
          retryDelayMs: 10,
          waitTimeoutMs: 60_000
        })
      )
    ).rejects.toBeInstanceOf(LockNotAcquiredError);
    expect(fake.acquires()).toHaveLength(2);
  });

  it("rejects a negative waitTimeoutMs before taking anything", async () => {
    const fake = lockFake();
    const locks = createLock(fake.client);

    await expect(
      locks.run("res", async () => 1, { waitTimeoutMs: -1 })
    ).rejects.toBeInstanceOf(ValidationError);
    expect(fake.commands).toHaveLength(0);
  });
});

describe("lock extend", () => {
  it("re-applies this acquisition's ttlMs, not the store default", async () => {
    const fake = lockFake();
    const locks = createLock(fake.client, { ttlMs: 30_000 });

    const handle = await locks.acquire("res", { ttlMs: 5_000 });
    await expect(handle?.extend()).resolves.toBe(true);
    expect(fake.renewals()[0]?.[5]).toBe("5000");
  });
});

describe("lock fencing token", () => {
  it("draws the fence in the acquiring script and exposes it on the handle", async () => {
    let fence = 41;
    const fake = lockFake({ acquire: () => ++fence });
    const locks = createLock(fake.client, { ttlMs: 10_000 });

    const first = await locks.acquire("order:42");
    expect(first?.fence).toBe(42);
    const seen = await settle(locks.run("order:42", (handle) => handle.fence));
    expect(seen).toBe(43);

    // One script: SET NX PX on the lock, INCR on the fence, both keys named.
    expect(fake.acquires()[0]?.slice(2)).toEqual([
      2,
      "lock:order:42",
      "{lock:order:42}:fence",
      first?.token,
      "10000"
    ]);
  });

  it("keeps the fence counter in the lock's Cluster slot", async () => {
    // A key with no tag hashes whole, so the fence wraps it in braces; a key
    // that already has a tag keeps it. Either way the script is one slot.
    for (const [prefix, id] of [
      ["lock", "order:42"],
      ["cache:lock", "{u1}"],
      ["{tenant}:lock", "x"]
    ] as const) {
      const fake = lockFake();
      await createLock(fake.client, { prefix }).acquire(id);
      const [, , , key, fenceKey] = fake.acquires()[0] ?? [];
      expect(slotOf(String(fenceKey))).toBe(slotOf(String(key)));
    }
  });

  it("refuses an id whose lock key cannot share a slot with its fence", async () => {
    // "a}b" hashes whole (no "{" before the "}"), and wrapping it in braces
    // would end the tag at that "}". Rejected before any round trip instead of
    // failing with CROSSSLOT on a cluster only.
    const fake = lockFake();
    await expect(createLock(fake.client).acquire("a}b")).rejects.toBeInstanceOf(
      ValidationError
    );
    expect(fake.commands).toHaveLength(0);
  });
});

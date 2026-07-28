import { describe, expect, it } from "vitest";
import { slotOf } from "../src/cluster.js";
import { ValidationError } from "../src/core/errors.js";
import type { RedisCommand } from "../src/core/types.js";
import {
  budget,
  cache,
  IdempotencyConflictError,
  IdempotencyNotRecordedError,
  IdempotencyTimeoutError,
  idempotency,
  lock,
  ratelimit,
  SemaphoreNotAcquiredError,
  semaphore
} from "../src/primitives/index.js";
import { fakeClient } from "./fake-client.js";

describe("lock", () => {
  it("acquires with SET NX PX and returns a handle", async () => {
    const commands: RedisCommand[] = [];
    const locks = lock(fakeClient(commands, ["OK"]), { ttlMs: 10_000 });

    const handle = await locks.acquire("order:42");
    expect(handle).not.toBeNull();
    expect(handle?.key).toBe("lock:order:42");
    expect(typeof handle?.token).toBe("string");
    expect(commands).toHaveLength(1);
    const [set] = commands;
    expect(set?.slice(0, 2)).toEqual(["SET", "lock:order:42"]);
    expect(set?.slice(3)).toEqual(["NX", "PX", 10_000]);
    expect(typeof set?.[2]).toBe("string"); // the random token
  });

  it("returns null when the lock is held and no retries are configured", async () => {
    const commands: RedisCommand[] = [];
    const locks = lock(fakeClient(commands, [null]));

    await expect(locks.acquire("held")).resolves.toBeNull();
    expect(commands).toHaveLength(1);
  });

  it("retries until acquired", async () => {
    const commands: RedisCommand[] = [];
    const locks = lock(fakeClient(commands, [null, null, "OK"]));

    const handle = await locks.acquire("busy", {
      retries: 5,
      retryDelayMs: 0
    });
    expect(handle).not.toBeNull();
    expect(commands).toHaveLength(3); // two misses, then success
  });

  it("releases only when the token still matches", async () => {
    const commands: RedisCommand[] = [];
    // SET -> OK, then release: SCRIPT LOAD -> sha, EVALSHA -> 1 (deleted)
    const locks = lock(fakeClient(commands, ["OK", "sha1", 1]));

    const handle = await locks.acquire("res");
    await expect(handle?.release()).resolves.toBe(true);

    const evalsha = commands.at(-1);
    expect(evalsha?.slice(0, 4)).toEqual(["EVALSHA", "sha1", 1, "lock:res"]);
    expect(evalsha?.[4]).toBe(handle?.token);
  });

  it("release resolves false when we no longer hold the lock", async () => {
    const commands: RedisCommand[] = [];
    const locks = lock(fakeClient(commands, ["OK", "sha1", 0]));

    const handle = await locks.acquire("res");
    await expect(handle?.release()).resolves.toBe(false);
  });

  it("run acquires, invokes fn, and releases", async () => {
    const commands: RedisCommand[] = [];
    const locks = lock(fakeClient(commands, ["OK", "sha1", 1]));

    const result = await locks.run("res", async (handle) => {
      expect(handle.key).toBe("lock:res");
      return 123;
    });

    expect(result).toBe(123);
    expect(commands.map((c) => c[0])).toEqual(["SET", "SCRIPT", "EVALSHA"]);
  });

  it("run throws when the lock cannot be acquired", async () => {
    const locks = lock(fakeClient([], [null]));
    await expect(locks.run("held", async () => 1)).rejects.toThrow(
      'Could not acquire lock "lock:held"'
    );
  });

  it("rejects a non-positive ttl", async () => {
    const locks = lock(fakeClient([], []));
    await expect(locks.acquire("res", { ttlMs: 0 })).rejects.toThrow(
      ValidationError
    );
  });
});

describe("ratelimit", () => {
  it("runs the sliding-window script and reports an allowed request", async () => {
    const commands: RedisCommand[] = [];
    // SCRIPT LOAD -> sha, EVALSHA -> [allowed, remaining, reset, retryAfter]
    const client = fakeClient(commands, ["sha1", [1, 9, 1_700_000_060_000, 0]]);
    const limiter = ratelimit(client, { limit: 10, windowMs: 60_000 });

    const result = await limiter.check("user:1");
    expect(result).toEqual({
      success: true,
      limit: 10,
      remaining: 9,
      resetMs: 1_700_000_060_000,
      retryAfterMs: 0
    });

    const evalsha = commands.at(-1);
    expect(evalsha?.slice(0, 4)).toEqual([
      "EVALSHA",
      "sha1",
      1,
      "ratelimit:user:1"
    ]);
    // [EVALSHA, sha, keyCount, key, windowMs, limit, member]. No timestamp is
    // sent: the script reads it from the server, so app-server clock skew
    // cannot shift the window.
    expect(evalsha?.[4]).toBe("60000"); // windowMs
    expect(evalsha?.[5]).toBe("10"); // limit
    expect(typeof evalsha?.[6]).toBe("string"); // unique member
    expect(evalsha).toHaveLength(7);
  });

  it("reports a denied request with a skew-free retry delay", async () => {
    const client = fakeClient([], ["sha1", [0, 0, 1_700_000_099_000, 850]]);
    const limiter = ratelimit(client, { limit: 5, windowMs: 1_000 });

    const result = await limiter.check("user:2");
    expect(result.success).toBe(false);
    expect(result.remaining).toBe(0);
    expect(result.resetMs).toBe(1_700_000_099_000);
    // A duration computed server-side, so `Retry-After` never has to
    // difference a Redis timestamp against a possibly-skewed local clock.
    expect(result.retryAfterMs).toBe(850);
  });

  it("rejects a non-positive limit or window", () => {
    expect(() =>
      ratelimit(fakeClient([], []), { limit: 0, windowMs: 1 })
    ).toThrow(ValidationError);
    expect(() =>
      ratelimit(fakeClient([], []), { limit: 1, windowMs: -5 })
    ).toThrow(ValidationError);
  });
});

describe("cache", () => {
  it("returns a hit without touching the loader", async () => {
    const commands: RedisCommand[] = [];
    const store = cache<{ n: number }>(fakeClient(commands, ['{"n":1}']), {
      ttlMs: 60_000
    });

    const value = await store.get("a", () => {
      throw new Error("loader must not run on a hit");
    });

    expect(value).toEqual({ n: 1 });
    expect(commands).toEqual([["GET", "cache:{a}"]]);
  });

  it("on a miss, takes the fill lock, loads once, and writes with PX", async () => {
    const commands: RedisCommand[] = [];
    // GET miss, lock SET OK, double-check GET miss, SET value, SCRIPT LOAD, EVALSHA release
    const store = cache<{ n: number }>(
      fakeClient(commands, [null, "OK", null, "OK", "sha1", 1]),
      { ttlMs: 60_000 }
    );
    let loads = 0;

    const value = await store.get("a", () => {
      loads++;
      return { n: 2 };
    });

    expect(value).toEqual({ n: 2 });
    expect(loads).toBe(1);
    expect(commands.map((c) => c[0])).toEqual([
      "GET",
      "SET", // fill lock (cache:lock:{a} NX PX)
      "GET", // double-check
      "SET", // the value
      "SCRIPT",
      "EVALSHA" // lock release
    ]);
    const lockSet = commands[1];
    expect(lockSet?.[1]).toBe("cache:lock:{a}");
    const valueSet = commands[3];
    expect(valueSet).toEqual(["SET", "cache:{a}", '{"n":2}', "PX", 60_000]);
  });

  it("skips the loader when the double-check hits, and still releases", async () => {
    const commands: RedisCommand[] = [];
    const store = cache<string>(
      fakeClient(commands, [null, "OK", '"filled"', "sha1", 1]),
      { ttlMs: 60_000 }
    );

    const value = await store.get("a", () => {
      throw new Error("loader must not run when double-check hits");
    });

    expect(value).toBe("filled");
    expect(commands.map((c) => c[0])).toEqual([
      "GET",
      "SET",
      "GET",
      "SCRIPT",
      "EVALSHA"
    ]);
  });

  it("polls for the value while another caller holds the fill lock", async () => {
    const commands: RedisCommand[] = [];
    // GET miss, lock SET denied (null), poll GET hit
    const store = cache<string>(fakeClient(commands, [null, null, '"other"']), {
      ttlMs: 60_000,
      pollMs: 1
    });

    const value = await store.get("a", () => {
      throw new Error("loader must not run while polling succeeds");
    });

    expect(value).toBe("other");
    expect(commands.map((c) => c[0])).toEqual(["GET", "SET", "GET"]);
  });

  it("fails open when the lock holder never fills", async () => {
    // Poll count is timing-dependent, so answer by command instead of a queue:
    // every GET misses, the lock SET is always denied, the value SET succeeds.
    const commands: RedisCommand[] = [];
    const client = {
      async send(command: RedisCommand) {
        commands.push(command);
        if (command[0] === "GET") return null;
        return command[1] === "cache:lock:{a}" ? null : "OK";
      },
      async pipeline() {
        return [];
      },
      async close() {}
    };
    const store = cache<string>(client, {
      ttlMs: 60_000,
      lockTtlMs: 3,
      pollMs: 1
    });

    await expect(store.get("a", () => "self-loaded")).resolves.toBe(
      "self-loaded"
    );
    expect(commands.at(-1)).toEqual([
      "SET",
      "cache:{a}",
      '"self-loaded"',
      "PX",
      60_000
    ]);
  });

  it("peek reads without loading and set/del round-trip", async () => {
    const commands: RedisCommand[] = [];
    const store = cache<string>(fakeClient(commands, ['"v"', null, "OK", 1]), {
      ttlMs: 5_000
    });

    await expect(store.peek("a")).resolves.toBe("v");
    await expect(store.peek("missing")).resolves.toBeNull();
    await store.set("a", "w");
    await expect(store.del("a")).resolves.toBe(1);

    expect(commands[2]).toEqual(["SET", "cache:{a}", '"w"', "PX", 5_000]);
    expect(commands[3]).toEqual(["DEL", "cache:{a}"]);
  });

  it("rejects non-positive ttl, lock ttl, and poll intervals", () => {
    expect(() => cache(fakeClient([], []), { ttlMs: 0 })).toThrow(
      ValidationError
    );
    expect(() =>
      cache(fakeClient([], []), { ttlMs: 1, lockTtlMs: -1 })
    ).toThrow(ValidationError);
    expect(() => cache(fakeClient([], []), { ttlMs: 1, pollMs: 0 })).toThrow(
      ValidationError
    );
  });
});

describe("budget", () => {
  it("sends three co-located keys and no client timestamp", async () => {
    const commands: RedisCommand[] = [];
    // SCRIPT LOAD -> sha, EVALSHA -> [status, bucket, remaining, retryAfter]
    const client = fakeClient(commands, ["sha1", [1, 7, 40, 0]]);
    const budgets = budget(client, { limit: 100, windowMs: 60_000 });

    const result = await budgets.charge("u1", 60);
    expect(result).toEqual({
      ok: true,
      limit: 100,
      remaining: 40,
      retryAfterMs: 0
    });

    const evalsha = commands.at(-1);
    // The two buckets and the reservation set share a `{u1}` hash tag, so one
    // Cluster node owns all three and the script can touch them together.
    expect(evalsha?.slice(0, 3)).toEqual(["EVALSHA", "sha1", 3]);
    const keys = evalsha?.slice(3, 6) as string[];
    expect(keys[2]).toBe("budget:{u1}:holds");
    expect(keys[0]).toMatch(/^budget:\{u1\}:\d+$/);
    expect(new Set(keys.map(slotOf)).size).toBe(1);
    // windowMs, bucket, limit, cost. No wall-clock reading is sent: the
    // script takes its own, so app-server skew cannot shift the window.
    expect(evalsha?.slice(6)).toEqual([
      "60000",
      keys[0].split(":").pop(),
      "100",
      "60"
    ]);
  });

  it("reports a denial with the retry delay and no hold", async () => {
    const client = fakeClient([], ["sha1", [0, 7, 5, 12_345]]);
    const budgets = budget(client, { limit: 100, windowMs: 60_000 });

    expect(await budgets.charge("u1", 60)).toEqual({
      ok: false,
      limit: 100,
      remaining: 5,
      retryAfterMs: 12_345
    });
    expect(
      await budget(fakeClient([], ["sha1", [0, 7, 5, 1]]), {
        limit: 100,
        windowMs: 60_000
      }).reserve("u1", 60)
    ).toBeNull();
  });

  it("retries once against the server's bucket when the window rolled over", async () => {
    const commands: RedisCommand[] = [];
    // First EVALSHA reports -1 (our bucket guess was stale) and names bucket
    // 999; the retry must use that bucket's keys.
    const client = fakeClient(commands, [
      "sha1",
      [-1, 999, 0, 0],
      [1, 999, 10, 0]
    ]);
    const budgets = budget(client, { limit: 100, windowMs: 60_000 });

    expect((await budgets.charge("u1", 5)).ok).toBe(true);
    const evalshas = commands.filter((c) => c[0] === "EVALSHA");
    expect(evalshas).toHaveLength(2);
    expect(evalshas[1][3]).toBe("budget:{u1}:999");
    expect(evalshas[1][4]).toBe("budget:{u1}:998");
  });

  it("rejects bad configuration and negative amounts", async () => {
    expect(() => budget(fakeClient([], []), { limit: 0, windowMs: 1 })).toThrow(
      ValidationError
    );
    expect(() =>
      budget(fakeClient([], []), { limit: 1, windowMs: -5 })
    ).toThrow(ValidationError);
    await expect(
      budget(fakeClient([], []), { limit: 10, windowMs: 10 }).charge("u", -1)
    ).rejects.toThrow(ValidationError);
  });
});

describe("semaphore", () => {
  it("acquires a slot through the script and releases through one too", async () => {
    const commands: RedisCommand[] = [];
    // acquire: SCRIPT LOAD + EVALSHA. release: its own SCRIPT LOAD + EVALSHA,
    // because a bare ZREM cannot tell a live lease from a lapsed one.
    const client = fakeClient(commands, ["sha1", 1, "sha2", 1]);
    const slots = semaphore(client, { limit: 5, leaseMs: 30_000 });

    const held = await slots.acquire("openai");
    expect(held?.key).toBe("semaphore:openai");
    const evalsha = commands.at(-1);
    expect(evalsha?.slice(0, 4)).toEqual([
      "EVALSHA",
      "sha1",
      1,
      "semaphore:openai"
    ]);
    // limit, leaseMs, token. Again no timestamp: leases are compared against
    // server time so two holders cannot disagree about who still owns a slot.
    expect(evalsha?.slice(4, 6)).toEqual(["5", "30000"]);
    expect(evalsha).toHaveLength(7);

    expect(await held?.release()).toBe(true);
    expect(commands.at(-1)?.slice(0, 4)).toEqual([
      "EVALSHA",
      "sha2",
      1,
      "semaphore:openai"
    ]);
  });

  it("returns null when full, and retries when asked", async () => {
    const full = semaphore(fakeClient([], ["sha1", 0]), { limit: 1 });
    expect(await full.acquire("x")).toBeNull();

    const commands: RedisCommand[] = [];
    const eventually = semaphore(fakeClient(commands, ["sha1", 0, 0, 1]), {
      limit: 1
    });
    expect(
      await eventually.acquire("x", { retries: 5, retryDelayMs: 1 })
    ).not.toBeNull();
    expect(commands.filter((c) => c[0] === "EVALSHA")).toHaveLength(3);
  });

  it("run releases even when the body throws", async () => {
    const commands: RedisCommand[] = [];
    const slots = semaphore(fakeClient(commands, ["sha1", 1, "sha2", 1]), {
      limit: 2
    });
    await expect(
      slots.run("x", () => {
        throw new Error("boom");
      })
    ).rejects.toThrow("boom");
    // The release script ran, so the slot is back even on the throwing path.
    expect(commands.at(-1)?.[0]).toBe("EVALSHA");
    expect(commands.at(-1)?.[1]).toBe("sha2");
  });

  it("run throws SemaphoreNotAcquiredError when no slot came free", async () => {
    const slots = semaphore(fakeClient([], ["sha1", 0]), { limit: 3 });
    await expect(slots.run("busy", () => 1)).rejects.toThrow(
      SemaphoreNotAcquiredError
    );
  });
});

describe("idempotency", () => {
  it("runs once and replays the stored result", async () => {
    const commands: RedisCommand[] = [];
    // SET NX -> OK (we win), SCRIPT LOAD -> sha, EVALSHA (complete) -> 1
    const client = fakeClient(commands, ["OK", "sha1", 1]);
    const once = idempotency<{ n: number }>(client);

    let calls = 0;
    const first = await once.run("k1", () => {
      calls++;
      return { n: 7 };
    });
    expect(first).toEqual({ value: { n: 7 }, replayed: false });
    expect(calls).toBe(1);
    expect(commands[0].slice(0, 2)).toEqual(["SET", "idem:k1"]);
    expect(commands[0].slice(4)).toEqual(["PX", 30_000]);

    // A later caller loses the SET NX and finds a completed record.
    const replayClient = fakeClient([], [null, `D${JSON.stringify({ n: 7 })}`]);
    const replayed = await idempotency<{ n: number }>(replayClient).run(
      "k1",
      () => {
        calls++;
        return { n: 999 };
      }
    );
    expect(replayed).toEqual({ value: { n: 7 }, replayed: true });
    expect(calls).toBe(1); // the handler did NOT run a second time
  });

  it("releases the key when the handler throws", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK", "sha1", 1]);
    const once = idempotency<string>(client);

    await expect(
      once.run("k2", () => {
        throw new Error("boom");
      })
    ).rejects.toThrow("boom");
    // The abandon script ran, so a retry can claim the key again.
    expect(commands.at(-1)?.[0]).toBe("EVALSHA");
  });

  it("surfaces a failed write-back instead of reporting plain success", async () => {
    // The effect happened but was not recorded, so the running marker will
    // lapse and a later call with this key will run the handler again.
    // Reporting success would hide exactly the guarantee this primitive sells.
    // Only the acquire reply is queued, so the complete script's SCRIPT LOAD
    // hits an empty queue and rejects.
    const once = idempotency<string>(fakeClient([], ["OK"]));
    const error = (await once
      .run("k8", () => "did-the-work")
      .catch((e: unknown) => e)) as IdempotencyNotRecordedError<string>;
    expect(error).toBeInstanceOf(IdempotencyNotRecordedError);
    // The work is not lost: the caller can still salvage the result.
    expect(error.value).toBe("did-the-work");
    expect(error.key).toBe("k8");
    expect(error.cause).toBeDefined();
  });

  it("surfaces a write-back the script declined, not just one that threw", async () => {
    // The complete script reports "your marker lapsed, or a later caller owns
    // this key now" as a 0 return rather than an error, so awaiting it without
    // reading the reply let the miss pass for success: nothing was stored, and
    // the next call with this key silently ran the handler a second time.
    // SET NX -> OK (claimed), SCRIPT LOAD -> sha, EVALSHA -> 0 (declined).
    const once = idempotency<string>(fakeClient([], ["OK", "sha1", 0]));
    const error = (await once
      .run("k9", () => "did-the-work")
      .catch((e: unknown) => e)) as IdempotencyNotRecordedError<string>;
    expect(error).toBeInstanceOf(IdempotencyNotRecordedError);
    expect(error.value).toBe("did-the-work");
    expect(error.key).toBe("k9");
  });

  it("throws on a concurrent holder under onConflict: throw", async () => {
    // SET NX -> null (someone holds it), GET -> a running marker.
    const client = fakeClient([], [null, "Rsomeone-elses-token"]);
    const once = idempotency<string>(client, { onConflict: "throw" });
    await expect(once.run("k3", () => "x")).rejects.toThrow(
      IdempotencyConflictError
    );
  });

  it("runs unguarded when the key is absent", async () => {
    const commands: RedisCommand[] = [];
    const once = idempotency<string>(fakeClient(commands, []));
    expect(await once.run(null, () => "v")).toEqual({
      value: "v",
      replayed: false
    });
    expect(commands).toEqual([]);
  });

  it("waits for the holder's result, then replays it", async () => {
    // SET NX -> null (lost the race), GET -> still running, poll, GET -> done.
    const client = fakeClient(
      [],
      [null, "Rother-token", null, `D${JSON.stringify("done")}`]
    );
    const once = idempotency<string>(client, { pollMs: 1 });
    expect(await once.run("k5", () => "never")).toEqual({
      value: "done",
      replayed: true
    });
  });

  it("gives up with a timeout when the holder never finishes", async () => {
    // Always lose the SET NX and always find a running marker.
    const replies = Array.from({ length: 200 }, (_, index) =>
      index % 2 === 0 ? null : "Rother-token"
    );
    const once = idempotency<string>(fakeClient([], replies), {
      pollMs: 1,
      waitTimeoutMs: 20
    });
    await expect(once.run("k6", () => "never")).rejects.toThrow(
      IdempotencyTimeoutError
    );
  });

  it("peek decodes a completed record, and forget drops it", async () => {
    const done = idempotency<{ ok: boolean }>(
      fakeClient([], [`D${JSON.stringify({ ok: true })}`, 1])
    );
    expect(await done.peek("k7")).toEqual({ ok: true });
    expect(await done.forget("k7")).toBe(true);
  });

  it("peek returns null while still running", async () => {
    const once = idempotency<string>(fakeClient([], ["Rtoken"]));
    expect(await once.peek("k4")).toBeNull();
  });
});

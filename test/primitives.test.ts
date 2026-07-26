import { describe, expect, it } from "vitest";
import { ValidationError } from "../src/core/errors.js";
import type { RedisCommand } from "../src/core/types.js";
import { cache, lock, ratelimit } from "../src/primitives/index.js";
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
    // SCRIPT LOAD -> sha, EVALSHA -> [allowed, remaining, reset]
    const client = fakeClient(commands, ["sha1", [1, 9, 1_700_000_060_000]]);
    const limiter = ratelimit(client, { limit: 10, windowMs: 60_000 });

    const result = await limiter.check("user:1");
    expect(result).toEqual({
      success: true,
      limit: 10,
      remaining: 9,
      resetMs: 1_700_000_060_000
    });

    const evalsha = commands.at(-1);
    expect(evalsha?.slice(0, 4)).toEqual([
      "EVALSHA",
      "sha1",
      1,
      "ratelimit:user:1"
    ]);
    // [EVALSHA, sha, keyCount, key, now, windowMs, limit, member]
    expect(typeof evalsha?.[4]).toBe("string"); // now
    expect(evalsha?.[5]).toBe("60000"); // windowMs
    expect(evalsha?.[6]).toBe("10"); // limit
    expect(typeof evalsha?.[7]).toBe("string"); // unique member
  });

  it("reports a denied request", async () => {
    const client = fakeClient([], ["sha1", [0, 0, 1_700_000_099_000]]);
    const limiter = ratelimit(client, { limit: 5, windowMs: 1_000 });

    const result = await limiter.check("user:2");
    expect(result.success).toBe(false);
    expect(result.remaining).toBe(0);
    expect(result.resetMs).toBe(1_700_000_099_000);
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
    expect(commands).toEqual([["GET", "cache:a"]]);
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
      "SET", // fill lock (cache:lock:a NX PX)
      "GET", // double-check
      "SET", // the value
      "SCRIPT",
      "EVALSHA" // lock release
    ]);
    const lockSet = commands[1];
    expect(lockSet?.[1]).toBe("cache:lock:a");
    const valueSet = commands[3];
    expect(valueSet).toEqual(["SET", "cache:a", '{"n":2}', "PX", 60_000]);
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
        return command[1] === "cache:lock:a" ? null : "OK";
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
      "cache:a",
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

    expect(commands[2]).toEqual(["SET", "cache:a", '"w"', "PX", 5_000]);
    expect(commands[3]).toEqual(["DEL", "cache:a"]);
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

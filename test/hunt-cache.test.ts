import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { node } from "../src/node/index.js";
import { cache } from "../src/primitives/cache.js";
import { fakeClient } from "./fake-client.js";

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A client for the waiting side of `get()`: the value misses until `fillAtMs`,
 * the fill lock is always held (every acquire is denied) and its token rolls
 * over every `leaseMs`, which is how a real handoff looks to a waiter.
 */
function contendedClient(options: {
  commands: RedisCommand[];
  fillAtMs: number;
  leaseMs: number;
}): RedisClient {
  const startedAt = Date.now();
  return {
    async send(command: RedisCommand): Promise<RedisReply> {
      options.commands.push(command);
      const elapsed = Date.now() - startedAt;
      const [name, key] = command;
      if (name === "GET" && key === "cache:{a}") {
        return elapsed >= options.fillAtMs ? '"other"' : null;
      }
      if (name === "GET") return `t${Math.floor(elapsed / options.leaseMs)}`;
      // Deny every acquire; the fail-open SET on the entry succeeds.
      return key === "cache:lock:{a}" ? null : "OK";
    },
    async pipeline() {
      return [];
    },
    async close() {}
  };
}

describe("cache fill fencing", () => {
  it("publishes under the fill lease instead of a bare SET", async () => {
    const commands: RedisCommand[] = [];
    // GET miss, lock SET OK, double-check GET miss, publish SCRIPT LOAD +
    // EVALSHA, then the lock's own release SCRIPT LOAD + EVALSHA.
    const store = cache<string>(
      fakeClient(commands, [null, "OK", null, "sha-pub", 1, "sha-rel", 1]),
      { ttlMs: 60_000 }
    );

    await expect(store.get("a", () => "v")).resolves.toBe("v");

    const token = commands[1]?.[2];
    expect(commands[1]?.[1]).toBe("cache:lock:{a}");
    // The value is written by a script that checks the token first, so a
    // del() or a lease handoff during the load drops the write.
    expect(commands[4]).toEqual([
      "EVALSHA",
      "sha-pub",
      2,
      "cache:{a}",
      "cache:lock:{a}",
      token,
      '"v"',
      "60000"
    ]);
    // Nothing writes the entry unconditionally.
    const bareSets = commands.filter(
      (c) => c[0] === "SET" && c[1] === "cache:{a}"
    );
    expect(bareSets).toEqual([]);
  });

  it("fails open with NX so a lease-less fill cannot clobber a fresher entry", async () => {
    // Every GET misses and every acquire is denied, so the caller waits out
    // the lock TTL and loads for itself.
    const commands: RedisCommand[] = [];
    const client: RedisClient = {
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
      "NX",
      "PX",
      60_000
    ]);
  });

  it("del drops the entry and the fill lease in one script", async () => {
    const commands: RedisCommand[] = [];
    const store = cache<string>(fakeClient(commands, ["sha-del", 1]), {
      ttlMs: 5_000
    });

    await expect(store.del("a")).resolves.toBe(1);
    expect(commands[1]).toEqual([
      "EVALSHA",
      "sha-del",
      2,
      "cache:{a}",
      "cache:lock:{a}"
    ]);
  });

  it("restarts the wait window when the fill lease changes hands", async () => {
    // The deadline used to be frozen at the first failed acquire, so every
    // waiter but the successor gave up at lockTtlMs and hit the backend while
    // a new holder was plainly loading.
    const commands: RedisCommand[] = [];
    const store = cache<string>(
      contendedClient({ commands, fillAtMs: 250, leaseMs: 30 }),
      { ttlMs: 60_000, lockTtlMs: 100, pollMs: 5 }
    );

    await expect(
      store.get("a", () => {
        throw new Error("loader must not run while a holder is loading");
      })
    ).resolves.toBe("other");
  });

  it("caps the total wait even while the lease keeps changing hands", async () => {
    // Following handoffs must not become an unbounded wait.
    const commands: RedisCommand[] = [];
    const store = cache<string>(
      contendedClient({
        commands,
        fillAtMs: Number.POSITIVE_INFINITY,
        leaseMs: 30
      }),
      { ttlMs: 60_000, lockTtlMs: 100, pollMs: 5 }
    );

    const startedAt = Date.now();
    await expect(store.get("a", () => "self-loaded")).resolves.toBe(
      "self-loaded"
    );
    const elapsed = Date.now() - startedAt;
    // Comfortably past one lockTtlMs (the window did restart), and still
    // inside the cap of three.
    expect(elapsed).toBeGreaterThan(200);
    expect(elapsed).toBeLessThan(900);
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("cache fill fencing (live)", () => {
  let client: RedisClient;
  const run = `hunt-cache:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("an invalidation beats a load that is already in flight", async () => {
    // The canonical write-through order (update the row, then invalidate) used
    // to lose: the loader's pre-update snapshot landed after the DEL and was
    // republished with a full TTL, so one correct invalidation served stale
    // data for the whole ttlMs.
    const store = cache<string>(client, {
      ttlMs: 60_000,
      prefix: `${run}:del`
    });
    let row = "v1";
    const inflight = store.get("u1", async () => {
      const snapshot = row;
      await pause(300);
      return snapshot;
    });

    await pause(50);
    row = "v2";
    await store.del("u1");

    // The in-flight caller still gets the value it loaded, but it is not cached.
    await expect(inflight).resolves.toBe("v1");
    await expect(store.peek("u1")).resolves.toBeNull();
    await expect(store.get("u1", () => row)).resolves.toBe("v2");
    // del still reports the entry's own deleted count, not the lock's.
    await expect(store.del("u1")).resolves.toBe(1);
    await expect(store.del("u1")).resolves.toBe(0);
  });

  it("a holder whose lease expired cannot overwrite a newer fill", async () => {
    const store = cache<string>(client, {
      ttlMs: 60_000,
      prefix: `${run}:stale`,
      lockTtlMs: 200,
      pollMs: 20
    });
    const slow = store.get("k", async () => {
      await pause(800);
      return "old";
    });

    // Once the first lease has expired, this caller takes it and publishes
    // while the original loader is still running.
    await pause(400);
    await expect(store.get("k", () => "new")).resolves.toBe("new");

    await expect(slow).resolves.toBe("old");
    await expect(store.peek("k")).resolves.toBe("new");
  });

  it("waiters re-collapse onto the holder that takes over an expired lease", async () => {
    const store = cache<string>(client, {
      ttlMs: 60_000,
      prefix: `${run}:handoff`,
      lockTtlMs: 300,
      pollMs: 20
    });
    let loads = 0;
    const loader = async () => {
      const attempt = ++loads;
      // The first caller wins the lease and then hangs well past its expiry;
      // whoever takes over is quick.
      await pause(attempt === 1 ? 2_000 : 100);
      return attempt === 1 ? "hung" : "fresh";
    };

    const results = await Promise.all(
      Array.from({ length: 10 }, () => store.get("hot", loader))
    );

    // The successor's lease is live and visible, so the waiters wait for it
    // instead of all failing open on the dead holder's clock.
    expect(loads).toBeLessThanOrEqual(3);
    expect(results.filter((value) => value === "fresh").length).toBe(9);
    // And the hung holder's stale result never lands.
    await expect(store.peek("hot")).resolves.toBe("fresh");
  });
});

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi
} from "vitest";
import { slotOf } from "../src/cluster.js";
import { codecs } from "../src/core/codecs.js";
import { ValidationError } from "../src/core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { node } from "../src/node/index.js";
import { CacheWaitTimeoutError, cache } from "../src/primitives/cache.js";
import { fakeClient } from "./fake-client.js";

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A client that answers the cache's scripts by their source rather than from a
 * queue, since how many polls or renewals a test performs depends on timing.
 * `claim` answers each claim (`[1, value]` hit, `[2]` claimed, `[0]` held).
 */
function cacheFake(behavior: {
  commands: RedisCommand[];
  get?: () => RedisReply;
  claim: (call: number) => RedisReply;
  publish?: () => RedisReply;
}): RedisClient & { calls(kind: string): number } {
  const shas = new Map<string, string>();
  const counts = new Map<string, number>();
  const bump = (kind: string) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return next;
  };
  return {
    async send(command: RedisCommand): Promise<RedisReply> {
      behavior.commands.push(command);
      if (command[0] === "GET") return behavior.get?.() ?? null;
      if (command[0] === "SCRIPT") {
        const lua = String(command[2]);
        const kind = lua.includes("NX")
          ? "claim"
          : lua.includes("PEXPIRE")
            ? "extend"
            : lua.includes("ARGV[3]")
              ? "publish"
              : lua.includes("GET")
                ? "release"
                : "other";
        shas.set(`sha-${kind}`, kind);
        return `sha-${kind}`;
      }
      if (command[0] === "EVALSHA") {
        const kind = shas.get(String(command[1])) ?? "other";
        const call = bump(kind);
        if (kind === "claim") return behavior.claim(call);
        if (kind === "publish") return behavior.publish?.() ?? 1;
        return 1;
      }
      throw new Error(`Unexpected command ${String(command[0])}`);
    },
    async pipeline() {
      return [];
    },
    async close() {},
    calls: (kind: string) => counts.get(kind) ?? 0
  };
}

describe("cache fill fencing", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("publishes under the fill lease instead of a bare SET", async () => {
    const commands: RedisCommand[] = [];
    // GET miss, claim SCRIPT LOAD + EVALSHA ({2}: lock taken), then the
    // publish SCRIPT LOAD + EVALSHA, which also frees the lock.
    const store = cache<string>(
      fakeClient(commands, [null, "sha-claim", [2], "sha-pub", 1]),
      { ttlMs: 60_000 }
    );

    await expect(store.get("a", () => "v")).resolves.toBe("v");

    const token = commands[2]?.[5];
    expect(commands[2]?.slice(3, 5)).toEqual(["cache:{a}", "cache:lock:{a}"]);
    // The value is written by a script that checks the token first, so a
    // set(), a del(), or a lease handoff during the load drops the write.
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

  it("renews the fill lock for as long as the load takes", async () => {
    // A 40s load under the 10s default lock: the lock was taken with a TTL and
    // never renewed, so a new loader started every 10s while the first was
    // still going. It is renewed every quarter of lockTtlMs instead.
    const commands: RedisCommand[] = [];
    const client = cacheFake({ commands, claim: () => [2] });
    const store = cache<string>(client, { ttlMs: 60_000 });

    const load = store.get("a", () => pause(40_000).then(() => "slow"));
    await vi.advanceTimersByTimeAsync(40_000);

    await expect(load).resolves.toBe("slow");
    // Every 2.5s over 40s, token-checked on the fill lock.
    expect(client.calls("extend")).toBeGreaterThanOrEqual(15);
    const renewal = commands.find(
      (c) => c[0] === "EVALSHA" && c[1] === "sha-extend"
    );
    expect(renewal?.slice(3)).toEqual([
      "cache:lock:{a}",
      commands.find((c) => c[1] === "sha-claim")?.[5],
      "10000"
    ]);
    expect(client.calls("publish")).toBe(1);
  });

  it("frees the fill lock at once when the loader throws", async () => {
    const commands: RedisCommand[] = [];
    const client = cacheFake({ commands, claim: () => [2] });
    const store = cache<string>(client, { ttlMs: 60_000 });

    await expect(
      store.get("a", () => {
        throw new Error("backend is down");
      })
    ).rejects.toThrow("backend is down");
    // A waiter can take it over on its next poll rather than after lockTtlMs.
    expect(client.calls("release")).toBe(1);
    expect(client.calls("publish")).toBe(0);
  });

  it("keeps waiting on a holder that is still loading, then takes over its freed lock", async () => {
    // The deadline used to be frozen at the first failed acquire, so every
    // waiter but the successor gave up at lockTtlMs and hit the backend while
    // a holder was plainly loading. Waiters now watch through the claim script:
    // held, held, …, then free, which they take in the same round trip.
    const commands: RedisCommand[] = [];
    const client = cacheFake({
      commands,
      claim: (call) => (call < 6 ? [0] : [2])
    });
    const store = cache<string>(client, {
      ttlMs: 60_000,
      lockTtlMs: 100,
      pollMs: 5
    });
    let loads = 0;

    const load = store.get("a", () => {
      loads++;
      return "took-over";
    });
    await vi.advanceTimersByTimeAsync(1_000);

    await expect(load).resolves.toBe("took-over");
    expect(loads).toBe(1);
  });

  it("throws CacheWaitTimeoutError at the deadline instead of loading", async () => {
    // At the old 3 × lockTtlMs hard deadline every waiter called the loader
    // unfenced, all at once, against a backend already too slow to answer in
    // time: the stampede the cache exists to prevent, plus a SET NX that lost
    // any del() issued meanwhile.
    const commands: RedisCommand[] = [];
    const client = cacheFake({ commands, claim: () => [0] });
    const store = cache<string>(client, {
      ttlMs: 60_000,
      lockTtlMs: 100,
      pollMs: 5
    });
    const loader = vi.fn(() => "must not run");

    const waiting = store.get("a", loader).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(299);
    await expect(
      Promise.race([waiting, Promise.resolve("still waiting")])
    ).resolves.toBe("still waiting");
    await vi.advanceTimersByTimeAsync(1);

    const error = await waiting;
    expect(error).toBeInstanceOf(CacheWaitTimeoutError);
    expect(error).toMatchObject({ key: "cache:{a}" });
    expect(loader).not.toHaveBeenCalled();
    // Nothing was written by the caller that gave up.
    expect(commands.filter((c) => c[0] === "SET")).toEqual([]);
    expect(client.calls("publish")).toBe(0);
  });

  it("backs off its polls, one round trip each", async () => {
    // Two GETs every 50ms per waiter was 40 round trips a second each; a 40s
    // load with 500 waiters cost 800,000. Polls now double from pollMs to
    // eight times it, jittered, and each is a single script.
    vi.spyOn(Math, "random").mockReturnValue(0.5); // No jitter: exact delays.
    const commands: RedisCommand[] = [];
    const client = cacheFake({ commands, claim: () => [0] });
    const store = cache<string>(client, {
      ttlMs: 60_000,
      waitTimeoutMs: 2_000
    });

    const waiting = store.get("a", () => "x").catch(() => undefined);
    await vi.advanceTimersByTimeAsync(2_000);
    await waiting;

    // 0, 50, 150, 350, 750, then every 400ms: 1150, 1550, 1950, and 2000.
    expect(client.calls("claim")).toBe(9);
    expect(commands.filter((c) => c[0] === "GET")).toHaveLength(1);
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
});

describe("cache keys", () => {
  it("keeps a cache entry and its own fill lock in one slot", async () => {
    // The fill lock used to be `cache:lock:<id>` next to `cache:<id>`, which
    // are different slots: two nodes per miss, and a single-flight guarantee
    // spread across them. Tagging the id co-locates the pair while the cache
    // itself still spreads, which is the property a cache must keep.
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ['"value"']);
    const entries = cache<string>(client, {
      ttlMs: 1000,
      codec: codecs.json()
    });
    await entries.peek("42");
    const entryKey = commands[0][1] as string;
    expect(entryKey).toBe("cache:{42}");
    expect(slotOf(entryKey)).toBe(slotOf("cache:lock:{42}"));
    // Different ids still land on different slots: the cache stays spread.
    expect(slotOf("cache:{42}")).not.toBe(slotOf("cache:{43}"));
  });

  it("refuses an id that would split the entry and its fill lock across slots", async () => {
    // An empty id or one starting with "}" yields an empty tag, which Redis
    // ignores and hashes the whole key instead: the entry and its lock then
    // land on different slots and every script for that id is CROSSSLOT.
    const commands: RedisCommand[] = [];
    const store = cache<string>(fakeClient(commands, []), { ttlMs: 1_000 });
    for (const id of ["", "}x"]) {
      await expect(store.get(id, () => "v")).rejects.toBeInstanceOf(
        ValidationError
      );
      await expect(store.peek(id)).rejects.toBeInstanceOf(ValidationError);
      await expect(store.set(id, "v")).rejects.toBeInstanceOf(ValidationError);
      await expect(store.del(id)).rejects.toBeInstanceOf(ValidationError);
    }
    // So does a prefix whose "{" steals the tag from the id.
    const stolen = cache<string>(fakeClient(commands, []), {
      ttlMs: 1_000,
      prefix: "a{"
    });
    await expect(stolen.peek("u1")).rejects.toBeInstanceOf(ValidationError);
    expect(commands).toEqual([]);
  });

  it("accepts braces that still co-locate the pair", async () => {
    const commands: RedisCommand[] = [];
    const store = cache<string>(fakeClient(commands, [null, null]), {
      ttlMs: 1_000,
      prefix: "{app}:cache"
    });
    await store.peek("a}b"); // tag "a" for both keys
    await store.peek("u1"); // tag "app" for both keys
    expect(commands.map((c) => c[1])).toEqual([
      "{app}:cache:{a}b}",
      "{app}:cache:{u1}"
    ]);
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("cache fill fencing (live)", () => {
  let client: RedisClient;
  const run = `cache:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  beforeAll(async () => {
    client = node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  /**
   * The client, except that fill-lock renewals fail: what a loader sees when
   * its process stalls or its connection to Redis drops mid-load, so its lease
   * genuinely lapses after lockTtlMs while the loader keeps going.
   */
  function withoutRenewal(inner: RedisClient): RedisClient {
    let extendSha: string | undefined;
    return {
      ...inner,
      async send(command) {
        if (command[0] === "EVALSHA" && command[1] === extendSha) {
          throw new Error("connection reset");
        }
        const reply = await inner.send(command);
        if (command[0] === "SCRIPT" && String(command[2]).includes("PEXPIRE")) {
          extendSha = String(reply);
        }
        return reply;
      }
    };
  }

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

  it("a direct set() beats a load that is already in flight", async () => {
    // set() neither checked for nor broke the fill lock, so a slower loader
    // holding v1 published over a fresh set(v2) and served v1 for ttlMs.
    const store = cache<string>(client, {
      ttlMs: 60_000,
      prefix: `${run}:set`
    });
    const inflight = store.get("u1", async () => {
      await pause(300);
      return "v1";
    });

    await pause(50);
    await store.set("u1", "v2");

    await expect(inflight).resolves.toBe("v1");
    await expect(store.peek("u1")).resolves.toBe("v2");
  });

  it("a slow load keeps its fill lock, so nobody else loads", async () => {
    // A 40s load under a 10s lock, scaled down: the lock used to lapse under
    // the loader and hand the load to a new caller every lockTtlMs.
    const store = cache<string>(client, {
      ttlMs: 60_000,
      prefix: `${run}:slow`,
      lockTtlMs: 200,
      waitTimeoutMs: 5_000,
      pollMs: 20
    });
    let loads = 0;
    const loader = async () => {
      loads++;
      await pause(1_000); // Five lock lifetimes.
      return "slow";
    };

    const results = await Promise.all(
      Array.from({ length: 10 }, () => store.get("hot", loader))
    );

    expect(loads).toBe(1);
    expect(results).toEqual(Array.from({ length: 10 }, () => "slow"));
  });

  it("a holder whose lease lapsed cannot overwrite a newer fill", async () => {
    const stalled = cache<string>(withoutRenewal(client), {
      ttlMs: 60_000,
      prefix: `${run}:stale`,
      lockTtlMs: 200
    });
    const healthy = cache<string>(client, {
      ttlMs: 60_000,
      prefix: `${run}:stale`,
      lockTtlMs: 200,
      pollMs: 20
    });
    const slow = stalled.get("k", async () => {
      await pause(800);
      return "old";
    });

    // Once the first lease has lapsed, this caller takes it and publishes
    // while the original loader is still running.
    await pause(400);
    await expect(healthy.get("k", () => "new")).resolves.toBe("new");

    await expect(slow).resolves.toBe("old");
    await expect(healthy.peek("k")).resolves.toBe("new");
  });

  it("waiters re-collapse onto the holder that takes over a lapsed lease", async () => {
    const stalled = cache<string>(withoutRenewal(client), {
      ttlMs: 60_000,
      prefix: `${run}:handoff`,
      lockTtlMs: 300
    });
    const healthy = cache<string>(client, {
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

    const first = stalled.get("hot", loader);
    await pause(20);
    const results = await Promise.all(
      Array.from({ length: 9 }, () => healthy.get("hot", loader))
    );

    // The successor's lease is live and visible, so the waiters wait for it
    // instead of all failing open on the dead holder's clock.
    expect(loads).toBeLessThanOrEqual(3);
    expect(loads).toBe(2);
    expect(results.filter((value) => value === "fresh").length).toBe(9);
    // And the hung holder's stale result never lands.
    await expect(first).resolves.toBe("hung");
    await expect(healthy.peek("hot")).resolves.toBe("fresh");
  });

  it("waiters give up at waitTimeoutMs instead of stampeding a slow backend", async () => {
    const store = cache<string>(client, {
      ttlMs: 60_000,
      prefix: `${run}:deadline`,
      lockTtlMs: 200,
      waitTimeoutMs: 300,
      pollMs: 20
    });
    let loads = 0;
    const loader = async () => {
      loads++;
      await pause(1_000);
      return "late";
    };

    const settled = await Promise.allSettled(
      Array.from({ length: 5 }, () => store.get("hot", loader))
    );

    expect(loads).toBe(1);
    expect(settled.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    for (const result of settled) {
      if (result.status === "rejected") {
        expect(result.reason).toBeInstanceOf(CacheWaitTimeoutError);
      }
    }
    await expect(store.peek("hot")).resolves.toBe("late");
  });
});

const clusterUrl = process.env.BENNI_REDIS_CLUSTER_URL;
const describeCluster = clusterUrl ? describe : describe.skip;

describeCluster("cache on a cluster-enabled node", () => {
  let client: RedisClient;
  const run = `cache-cluster:${Date.now()}`;

  beforeAll(async () => {
    client = node({ url: clusterUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("runs every script for an id without CROSSSLOT", async () => {
    const store = cache<string>(client, { ttlMs: 60_000, prefix: run });
    for (const id of ["u1", "a}b", "{nested}"]) {
      await expect(store.get(id, () => `v:${id}`)).resolves.toBe(`v:${id}`);
      await store.set(id, "w");
      await expect(store.peek(id)).resolves.toBe("w");
      await expect(store.del(id)).resolves.toBe(1);
    }
  });
});

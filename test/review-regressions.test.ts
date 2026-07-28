import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  codecs,
  createHashStore,
  createSortedSetStore,
  createStringStore,
  defineHash,
  defineKeyspace,
  defineSortedSet,
  ValidationError
} from "../src/core/index.js";
import { slotOf } from "../src/core/slot.js";
import type { RedisCommand } from "../src/core/types.js";
import { node } from "../src/node/index.js";
import { cache, lock } from "../src/primitives/index.js";
import { upstash } from "../src/upstash/index.js";
import { fakeClient } from "./fake-client.js";

// Regressions for the 2026-07-11 adversarial review. Each block pins one
// confirmed finding so it cannot quietly return.

describe("infinite sorted-set scores (review #1)", () => {
  const board = defineSortedSet("board", codecs.string());

  it("decodes RESP2 inf/-inf score strings", async () => {
    const store = createSortedSetStore(fakeClient([], ["inf", "-inf"]), board);
    await expect(store.zscore("d", "a")).resolves.toBe(
      Number.POSITIVE_INFINITY
    );
    await expect(store.zscore("d", "b")).resolves.toBe(
      Number.NEGATIVE_INFINITY
    );
  });

  it("encodes Infinity scores as +inf/-inf and rejects NaN", async () => {
    const commands: import("../src/core/index.js").RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, [1]), board);

    await store.zadd("d", [{ score: Number.POSITIVE_INFINITY, member: "top" }]);
    expect(commands[0]).toEqual(["ZADD", "board:d", "+inf", "top"]);

    await expect(
      store.zadd("d", [{ score: Number.NaN, member: "x" }])
    ).rejects.toThrow(ValidationError);
    await expect(store.zincrby("d", Number.NaN, "x")).rejects.toThrow(
      ValidationError
    );
  });

  it("zrandmember rejects withScores without count instead of dropping it", async () => {
    const store = createSortedSetStore(fakeClient([], []), board);
    await expect(store.zrandmember("d", { withScores: true })).rejects.toThrow(
      "withScores requires count"
    );
  });
});

describe("getrange validation (review #3)", () => {
  it("rejects non-integer bounds before sending", async () => {
    const store = createStringStore(
      fakeClient([], []),
      defineKeyspace("text", codecs.string())
    );
    await expect(store.getrange("k", Number.NaN, 4)).rejects.toThrow(
      ValidationError
    );
    await expect(store.getrange("k", 0, 1.5)).rejects.toThrow(ValidationError);
  });
});

describe("whole-record hash reads/writes (review #4)", () => {
  const users = defineHash("user", {
    name: codecs.string(),
    score: codecs.number()
  });

  it("names the missing fields on a partial hash", async () => {
    const store = createHashStore(fakeClient([], [["Ada", null]]), users);
    await expect(store.hget("42")).rejects.toThrow(
      "missing declared field(s): score"
    );
  });
});

describe("upstash top-level transaction error (review #5)", () => {
  it("surfaces the Redis error text from a failed /multi-exec", async () => {
    const fn = (async () => ({
      ok: true,
      status: 200,
      json: async () => ({ error: "ERR EXEC without MULTI" })
    })) as unknown as typeof fetch;
    const client = upstash({ url: "https://x", token: "t", fetch: fn });
    await expect(client.transaction?.([["SET", "k", "v"]])).rejects.toThrow(
      "EXEC without MULTI"
    );
  });
});

describe("lock.run release failures (review #7)", () => {
  it("does not mask fn's success when release rejects", async () => {
    // Only the acquire reply is queued; the release's SCRIPT LOAD hits an
    // empty queue and rejects — run() must still resolve with fn's result.
    const locks = lock(fakeClient([], ["OK"]));
    await expect(locks.run("r", async () => 7)).resolves.toBe(7);
  });
});

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("infinite scores against real Redis", () => {
  let client: import("../src/core/index.js").RedisClient;
  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("round-trips +inf/-inf scores", async () => {
    const board = defineSortedSet(
      `review:${Date.now()}:${Math.random().toString(36).slice(2)}`,
      codecs.string()
    );
    const store = createSortedSetStore(client, board);

    await store.zadd("d", [
      { score: Number.POSITIVE_INFINITY, member: "top" },
      { score: Number.NEGATIVE_INFINITY, member: "bottom" },
      { score: 1, member: "mid" }
    ]);
    await expect(store.zscore("d", "top")).resolves.toBe(
      Number.POSITIVE_INFINITY
    );
    await expect(
      store.zrange("d", { start: 0, stop: -1, withScores: true })
    ).resolves.toEqual([
      { member: "bottom", score: Number.NEGATIVE_INFINITY },
      { member: "mid", score: 1 },
      { member: "top", score: Number.POSITIVE_INFINITY }
    ]);
    await store.del("d");
  });
});

// Regressions for the cluster-safe-keys pass. Both were real breakage on a
// Redis Cluster that a single-node test suite could never surface.

describe("cluster-safe primitive and adapter keys", () => {
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

  it("keeps every budget key for one id in a single slot", () => {
    // budget touches three keys per id in one script (two window buckets and
    // the reservation set), so they must co-locate or the script is illegal
    // on a cluster. Different ids still spread.
    const bucket = 29_753;
    const keys = [
      `budget:{u1}:${bucket}`,
      `budget:{u1}:${bucket - 1}`,
      "budget:{u1}:holds"
    ];
    expect(new Set(keys.map(slotOf)).size).toBe(1);
    expect(slotOf("budget:{u1}:holds")).not.toBe(slotOf("budget:{u2}:holds"));
  });

  it("keeps every Next.js cache key in one slot", () => {
    // revalidateTag DELs entries and tag sets in one command, so they must
    // share a slot or the handler is simply broken on a cluster.
    expect(slotOf("{next-cache}:entry:/blog")).toBe(
      slotOf("{next-cache}:tag:posts")
    );
  });
});

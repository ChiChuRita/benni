import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import { assertSameSlot, slotOf } from "../src/cluster.js";
import { codecs } from "../src/core/index.js";
import type {
  Keyspace,
  RedisClient,
  SetSchema,
  SortedSetSchema
} from "../src/core/types.js";
import { beni } from "../src/index.js";
import { node } from "../src/node/index.js";
import { kv, set as setSchema, zset } from "../src/schema.js";

/**
 * Runs against a cluster-ENABLED single node (see Dockerfile.cluster), which
 * is all this feature needs: Redis rejects a cross-slot multi-key command
 * before it considers slot ownership, and it answers CLUSTER KEYSLOT, which a
 * plain instance refuses.
 *
 *   pnpm redis:cluster:build && pnpm redis:cluster:run
 *   BENI_REDIS_CLUSTER_URL=redis://127.0.0.1:6381 pnpm test
 */
const clusterUrl = process.env.BENI_REDIS_CLUSTER_URL;
const describeCluster = clusterUrl ? describe : describe.skip;

describeCluster("redis cluster", () => {
  let client: RedisClient;

  beforeAll(async () => {
    client = await node({ url: clusterUrl });
    // Idempotent: against an already-configured container this just errors.
    await client
      .send(["CLUSTER", "ADDSLOTSRANGE", 0, 16383])
      .catch(() => undefined);
    // The node takes a few seconds to move from `fail` to `ok` after the slots
    // land. Measured at ~5s on a cold container, so poll well past that: a
    // budget near the real convergence time makes this suite flaky, which is
    // worse than not having it.
    let info = "";
    for (let attempt = 0; attempt < 300; attempt++) {
      info = String(await client.send(["CLUSTER", "INFO"]));
      if (info.includes("cluster_state:ok")) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(info).toContain("cluster_state:ok");
  }, 60_000);

  it("computes the same slot the server does, for any key", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(
          fc.string(),
          fc.string({ unit: "grapheme" }),
          fc.constantFrom(
            "",
            "{}",
            "{",
            "}",
            "a{}b",
            "a{}{b}",
            "a{b{c}d}",
            "a}b{c}",
            "{user1000}.following",
            "café",
            "キー",
            "x".repeat(300)
          )
        ),
        async (key) => {
          const reply = await client.send(["CLUSTER", "KEYSLOT", key]);
          expect(slotOf(key)).toBe(Number(reply));
        }
      ),
      { numRuns: 200 }
    );
  });

  /**
   * Drives the multi-key inventory against the real server. The untagged pass
   * proves each row genuinely is CROSSSLOT (so the guard's inventory is the
   * right set), and the tagged pass proves the layouts actually fix it rather
   * than merely quieting our own check.
   */
  const untagged = {
    kv: kv("ikv", codecs.string()),
    set: setSchema("iset", codecs.string()),
    zset: zset("izset", codecs.string())
  };
  const tagged = {
    kv: kv("tkv", codecs.string(), { hashTag: "prefix" }),
    set: setSchema("tset", codecs.string(), { hashTag: "prefix" }),
    zset: zset("tzset", codecs.string(), { hashTag: "prefix" })
  };

  // Widened so both the untagged and the tagged bundle satisfy it: the cases
  // only ever call methods, never inspect the key type.
  type Schemas = {
    readonly kv: Keyspace<string, string>;
    readonly set: SetSchema<string, string>;
    readonly zset: SortedSetSchema<string, string>;
  };
  const cases: ReadonlyArray<{
    readonly command: string;
    readonly run: (redis: ReturnType<typeof beni>, s: Schemas) => Promise<void>;
  }> = [
    {
      command: "MGET",
      run: async (r, s) => void (await r.kv(s.kv).mget(["a", "b"]))
    },
    {
      command: "MSET",
      run: async (r, s) =>
        void (await r.kv(s.kv).mset([
          ["a", "1"],
          ["b", "2"]
        ]))
    },
    {
      command: "SUNIONSTORE",
      run: async (r, s) =>
        void (await r.set(s.set).sunionstore("d", "a", ["b"]))
    },
    {
      command: "SINTER",
      run: async (r, s) => void (await r.set(s.set).sinter("a", ["b"]))
    },
    {
      command: "SMOVE",
      run: async (r, s) => void (await r.set(s.set).smove("a", "b", "m"))
    },
    {
      command: "ZUNIONSTORE",
      run: async (r, s) =>
        void (await r.zset(s.zset).zunionstore("d", "a", ["b"]))
    },
    {
      command: "ZRANGESTORE",
      run: async (r, s) =>
        void (await r
          .zset(s.zset)
          .zrangestore("d", "a", { start: 0, stop: -1 }))
    }
  ];

  it.each(cases)("$command really is CROSSSLOT without a hash tag", async ({
    run
  }) => {
    // Guard OFF, so the command reaches the server and Redis is the judge.
    const redis = beni(client, {});
    await expect(run(redis, untagged)).rejects.toThrow(/CROSSSLOT/);
  });

  it.each(cases)('$command succeeds under hashTag: "prefix"', async ({
    run
  }) => {
    const redis = beni(client, { cluster: assertSameSlot });
    await expect(run(redis, tagged)).resolves.toBeUndefined();
  });
});

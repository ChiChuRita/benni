import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";
import { assertSameSlot, CrossSlotError, slotOf } from "../src/cluster.js";
import { codecs } from "../src/core/codecs.js";
import type { RedisCommand } from "../src/core/types.js";
import { beni } from "../src/database.js";
import {
  bitmap,
  geo,
  hll,
  kv,
  list,
  script,
  set as setSchema,
  zset
} from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

type Beni = ReturnType<typeof beni>;

const tags = setSchema("tag", codecs.string());
const tagsPinned = setSchema("tag", codecs.string(), { hashTag: "prefix" });
const profiles = kv("profile", codecs.string());
const carts = kv("cart", codecs.string(), { hashTag: "id" });

const twoKeyScript = script("twoKey", {
  keys: ["a", "b"],
  args: {},
  returns: codecs.number(),
  lua: "return 1"
});

// Paired untagged / prefix-tagged schemas, so every row below can be driven
// through both layouts without restating the shape.
const pin = { hashTag: "prefix" } as const;
const kvPlain = kv("kv", codecs.string());
const kvTagged = kv("kv", codecs.string(), pin);
const setPlain = setSchema("s", codecs.string());
const setTagged = setSchema("s", codecs.string(), pin);
const zsetPlain = zset("z", codecs.string());
const zsetTagged = zset("z", codecs.string(), pin);
const listPlain = list("l", codecs.string());
const listTagged = list("l", codecs.string(), pin);
const hllPlain = hll("h", codecs.string());
const hllTagged = hll("h", codecs.string(), pin);
const bitmapPlain = bitmap("b");
const bitmapTagged = bitmap("b", pin);
const geoPlain = geo("g", codecs.string());
const geoTagged = geo("g", codecs.string(), pin);

const kvOf = (r: Beni, t: boolean) => (t ? r.kv(kvTagged) : r.kv(kvPlain));
const setOf = (r: Beni, t: boolean) => (t ? r.set(setTagged) : r.set(setPlain));
const zsetOf = (r: Beni, t: boolean) =>
  t ? r.zset(zsetTagged) : r.zset(zsetPlain);
const listOf = (r: Beni, t: boolean) =>
  t ? r.list(listTagged) : r.list(listPlain);
const hllOf = (r: Beni, t: boolean) => (t ? r.hll(hllTagged) : r.hll(hllPlain));
const bitmapOf = (r: Beni, t: boolean) =>
  t ? r.bitmap(bitmapTagged) : r.bitmap(bitmapPlain);
const geoOf = (r: Beni, t: boolean) => (t ? r.geo(geoTagged) : r.geo(geoPlain));

/** Two keys with different hash tags that nonetheless land on one slot. */
function findSlotCollision(): readonly [string, string] {
  const seen = new Map<number, string>();
  for (let index = 0; index < 20_000; index++) {
    const key = `k:{t${index}}`;
    const slot = slotOf(key);
    const previous = seen.get(slot);
    if (previous !== undefined) return [previous, key];
    seen.set(slot, key);
  }
  throw new Error("no collision found");
}

describe("cluster guard", () => {
  it("is off by default: cross-slot mget still sends", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [[null, null]]);
    await beni(client).kv(profiles).mget(["a", "b"]);
    expect(commands).toEqual([["MGET", "profile:a", "profile:b"]]);
  });

  it("throws before sending once the guard is installed", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, []);
    const redis = beni(client, { cluster: assertSameSlot });
    await expect(redis.kv(profiles).mget(["a", "b"])).rejects.toThrow(
      CrossSlotError
    );
    expect(commands).toEqual([]);
  });

  it("carries both keys and both slots on the error", async () => {
    const client = fakeClient([], []);
    const redis = beni(client, { cluster: assertSameSlot });
    const error = (await redis
      .set(tags)
      .sunion("a1", ["b7"])
      .catch((e: unknown) => e)) as CrossSlotError;
    expect(error).toBeInstanceOf(CrossSlotError);
    expect(error.command).toBe("SUNION");
    expect(error.keys).toEqual(["tag:a1", "tag:b7"]);
    expect(error.slots).toEqual([slotOf("tag:a1"), slotOf("tag:b7")]);
    expect(error.message).toContain('hashTag: "prefix"');
  });

  it('hashTag: "prefix" makes the same call legal', async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["x"]]);
    const redis = beni(client, { cluster: assertSameSlot });
    await redis.set(tagsPinned).sunion("a1", ["b7"]);
    expect(commands).toEqual([["SUNION", "{tag}:a1", "{tag}:b7"]]);
  });

  it('hashTag: "id" co-locates one id across schemas', () => {
    expect(carts.key("u42")).toBe("cart:{u42}");
    expect(slotOf(carts.key("u42"))).toBe(slotOf("orders:{u42}"));
  });

  it("multi().keys() declares what exec checks", async () => {
    const client = fakeClient([], []);
    const redis = beni(client, { cluster: assertSameSlot });
    // Built at runtime, so the compile-time check cannot see the tags. This is
    // exactly the case the runtime guard exists for.
    const declared: string[] = ["cart:{a}", "cart:{b}"];
    await expect(
      redis
        .multi()
        .keys(declared)
        .add(["INCR", declared[0]], (r) => r)
        .exec()
    ).rejects.toThrow(CrossSlotError);
  });

  it("checks script keys, which Lua cannot span either", async () => {
    const client = fakeClient([], []);
    const redis = beni(client, { cluster: assertSameSlot });
    const keys: Record<string, string> = { a: "x:{1}", b: "y:{2}" };
    await expect(
      redis.script(twoKeyScript).run({
        keys: keys as { readonly a: string; readonly b: string },
        args: {}
      })
    ).rejects.toThrow(CrossSlotError);
  });

  it("keys that collide onto one slot are allowed", () => {
    // Distinct tags, same slot: legal on a cluster, so the guard must not fire.
    const collide = findSlotCollision();
    expect(slotOf(collide[0])).toBe(slotOf(collide[1]));
    expect(() =>
      assertSameSlot("MGET", [collide[0], collide[1]])
    ).not.toThrow();
  });

  it("single-key and empty calls never reach the CRC", () => {
    expect(() => assertSameSlot("GET", ["only"])).not.toThrow();
    expect(() => assertSameSlot("DEL", [])).not.toThrow();
  });

  // The fix hint depends on the layout the schema already uses; a caller who
  // has adopted one needs to be told the next step, not the first one.
  it.each([
    [
      undefined,
      'hashTag: "prefix"',
      'so its keys become "{p}:<id>" and the whole keyspace shares one slot'
    ],
    [
      "id" as const,
      'This schema uses hashTag: "id"',
      "or call this command with keys that share one id"
    ],
    [
      "prefix" as const,
      'This schema already uses hashTag: "prefix"',
      'Give both schemas hashTag: "id"'
    ]
  ])("suggests the right fix for layout %s", (hashTag, opening, detail) => {
    const error = (() => {
      try {
        assertSameSlot("MGET", ["a:{1}", "b:{2}"], { prefix: "p", hashTag });
        return null;
      } catch (thrown) {
        return thrown as CrossSlotError;
      }
    })();
    expect(error?.message).toContain(opening);
    expect(error?.message).toContain(detail);
  });
});

/**
 * Every multi-key method, driven twice: once with an untagged schema (must
 * throw, nothing sent) and once with `hashTag: "prefix"` (must send). If a new
 * multi-key method is added without a guard, its row here fails.
 */
describe("every multi-key method is guarded", () => {
  const cases: ReadonlyArray<{
    readonly command: string;
    readonly run: (redis: Beni, tagged: boolean) => Promise<unknown>;
  }> = [
    { command: "MGET", run: (r, t) => kvOf(r, t).mget(["a", "b"]) },
    {
      command: "MSET",
      run: (r, t) =>
        kvOf(r, t).mset([
          ["a", "1"],
          ["b", "2"]
        ])
    },
    {
      command: "MSETNX",
      run: (r, t) =>
        kvOf(r, t).msetnx([
          ["a", "1"],
          ["b", "2"]
        ])
    },
    { command: "SUNION", run: (r, t) => setOf(r, t).sunion("a", ["b"]) },
    { command: "SINTER", run: (r, t) => setOf(r, t).sinter("a", ["b"]) },
    { command: "SDIFF", run: (r, t) => setOf(r, t).sdiff("a", ["b"]) },
    {
      command: "SINTERCARD",
      run: (r, t) => setOf(r, t).sintercard("a", ["b"])
    },
    {
      command: "SUNIONSTORE",
      run: (r, t) => setOf(r, t).sunionstore("d", "a", ["b"])
    },
    {
      command: "SINTERSTORE",
      run: (r, t) => setOf(r, t).sinterstore("d", "a", ["b"])
    },
    {
      command: "SDIFFSTORE",
      run: (r, t) => setOf(r, t).sdiffstore("d", "a", ["b"])
    },
    { command: "SMOVE", run: (r, t) => setOf(r, t).smove("a", "b", "m") },
    { command: "ZUNION", run: (r, t) => zsetOf(r, t).zunion("a", ["b"]) },
    { command: "ZINTER", run: (r, t) => zsetOf(r, t).zinter("a", ["b"]) },
    { command: "ZDIFF", run: (r, t) => zsetOf(r, t).zdiff("a", ["b"]) },
    {
      command: "ZUNIONSTORE",
      run: (r, t) => zsetOf(r, t).zunionstore("d", "a", ["b"])
    },
    {
      command: "ZINTERSTORE",
      run: (r, t) => zsetOf(r, t).zinterstore("d", "a", ["b"])
    },
    {
      command: "ZDIFFSTORE",
      run: (r, t) => zsetOf(r, t).zdiffstore("d", "a", ["b"])
    },
    {
      command: "ZINTERCARD",
      run: (r, t) => zsetOf(r, t).zintercard("a", ["b"])
    },
    {
      command: "ZRANGESTORE",
      run: (r, t) => zsetOf(r, t).zrangestore("d", "a", { start: 0, stop: -1 })
    },
    {
      command: "ZMPOP",
      run: (r, t) => zsetOf(r, t).zmpop(["a", "b"], { min: true })
    },
    {
      command: "LMPOP",
      run: (r, t) => listOf(r, t).lmpop(["a", "b"], { direction: "left" })
    },
    {
      command: "LMOVE",
      run: (r, t) => listOf(r, t).lmove("a", "b", "left", "right")
    },
    { command: "PFCOUNT", run: (r, t) => hllOf(r, t).pfcount(["a", "b"]) },
    { command: "PFMERGE", run: (r, t) => hllOf(r, t).pfmerge("d", ["a"]) },
    {
      command: "BITOP",
      run: (r, t) => bitmapOf(r, t).bitop("d", "AND", ["a", "b"])
    },
    {
      command: "GEOSEARCHSTORE",
      run: (r, t) =>
        geoOf(r, t).geosearchstore("d", "a", {
          from: { longitude: 0, latitude: 0 },
          by: { radius: 1, unit: "km" }
        })
    }
  ];

  it.each(cases)("$command throws and sends nothing", async ({ run }) => {
    const commands: RedisCommand[] = [];
    const redis = beni(fakeClient(commands, []), { cluster: assertSameSlot });
    await expect(run(redis, false)).rejects.toThrow(CrossSlotError);
    expect(commands).toEqual([]);
  });

  it.each(cases)('$command passes under hashTag: "prefix"', async ({ run }) => {
    const commands: RedisCommand[] = [];
    // Replies are generous and untyped; we only care that nothing threw before
    // the send and that every key carries the tag.
    const redis = beni(
      fakeClient(
        commands,
        Array.from({ length: 8 }, () => 0)
      ),
      { cluster: assertSameSlot }
    );
    await run(redis, true).catch(() => {
      // Decode failures are fine: the guard ran and let the command through,
      // which is the whole assertion.
    });
    expect(commands.length).toBeGreaterThan(0);
    for (const command of commands) {
      for (const arg of command.slice(1)) {
        if (typeof arg === "string" && arg.includes(":")) {
          expect(arg.startsWith("{")).toBe(true);
        }
      }
    }
  });
});

/**
 * The whole point of taking the guard as a value is that `beni()` never names
 * it, so `core/slot` stays out of the root entry's module graph. That is a
 * one-line regression away: turning the `import type { SlotGuard }` in
 * database.ts back into a value import would silently re-pin the CRC16 table
 * and the error prose into every bundle. Walk the built graph and prove it.
 */
describe("beni/cluster stays out of the root baseline", () => {
  const built = existsSync("dist/index.mjs");
  const test = built ? it : it.skip;

  test("core/slot is unreachable from dist/index.mjs", () => {
    const seen = new Set<string>();
    const stack = ["dist/index.mjs"];
    while (stack.length > 0) {
      const file = stack.pop() as string;
      if (seen.has(file) || !existsSync(file)) continue;
      seen.add(file);
      const source = readFileSync(file, "utf8");
      for (const match of source.matchAll(/from\s+"(\.[^"]+)"/g)) {
        stack.push(join(dirname(file), match[1]));
      }
    }
    expect([...seen].filter((file) => file.includes("slot"))).toEqual([]);
    // ...but it is of course reachable from the entry that exists to carry it.
    expect(existsSync("dist/cluster.mjs")).toBe(true);
    expect(readFileSync("dist/cluster.mjs", "utf8")).toContain("slot.mjs");
  });
});

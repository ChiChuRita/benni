import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { createCounterStore } from "../src/core/counter.js";
import {
  createKeyValueStore,
  type KeyValueSetOptions
} from "../src/core/key-value.js";
import { defineKeyspace } from "../src/core/schemas.js";
import {
  createStringStore,
  type LcsIdxResult,
  type StringGetExOptions
} from "../src/core/string.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

const users = defineKeyspace("user", codecs.string());
const texts = defineKeyspace("text", codecs.string());
const counters = defineKeyspace("counter", codecs.number());

describe("createKeyValueStore conditional writes", () => {
  it("emits SET NX and maps OK/null replies to booleans", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(
      fakeClient(commands, ["OK", null]),
      users
    );

    await expect(store.set("42", "beni", { nx: true })).resolves.toBe(true);
    await expect(
      store.set("42", "beni", { nx: true, ttlSeconds: 60 })
    ).resolves.toBe(false);

    expect(commands).toEqual([
      ["SET", "user:42", "beni", "NX"],
      ["SET", "user:42", "beni", "NX", "EX", 60]
    ]);
  });

  it("emits SET XX and maps OK/null replies to booleans", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(
      fakeClient(commands, [null, "OK"]),
      users
    );

    await expect(store.set("42", "beni", { xx: true })).resolves.toBe(false);
    await expect(
      store.set("42", "beni", { xx: true, ttlSeconds: 30 })
    ).resolves.toBe(true);

    expect(commands).toEqual([
      ["SET", "user:42", "beni", "XX"],
      ["SET", "user:42", "beni", "XX", "EX", 30]
    ]);
  });

  it("throws TypeError on unexpected conditional SET replies", async () => {
    await expect(
      createKeyValueStore(fakeClient([], [1]), users).set("42", "beni", {
        nx: true
      })
    ).rejects.toThrow(TypeError);
    await expect(
      createKeyValueStore(fakeClient([], [1]), users).set("42", "beni", {
        xx: true
      })
    ).rejects.toThrow(TypeError);
  });

  it("emits KEEPTTL and ignores keepTtl: false", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(
      fakeClient(commands, ["OK", "OK"]),
      users
    );

    await store.set("42", "beni", { keepTtl: true });
    await store.set("42", "beni", { ttlSeconds: 60 });

    expect(commands).toEqual([
      ["SET", "user:42", "beni", "KEEPTTL"],
      ["SET", "user:42", "beni", "EX", 60]
    ]);
  });

  it("rejects combining keepTtl with ttlSeconds before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(fakeClient(commands, []), users);

    await expect(
      // @ts-expect-error keepTtl+ttlSeconds no longer compiles; pin the runtime guard
      store.set("42", "beni", { keepTtl: true, ttlSeconds: 60 })
    ).rejects.toThrow(TypeError);

    expect(commands).toEqual([]);
  });

  it("emits MSETNX for array and map inputs", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(fakeClient(commands, [1, 0]), users);

    await expect(
      store.msetnx([
        ["1", "one"],
        ["2", "two"]
      ])
    ).resolves.toBe(true);
    await expect(store.msetnx(new Map([["3", "three"]]))).resolves.toBe(false);

    expect(commands).toEqual([
      ["MSETNX", "user:1", "one", "user:2", "two"],
      ["MSETNX", "user:3", "three"]
    ]);
  });

  it("returns true for empty mSetIfAbsent inputs without a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(fakeClient(commands, []), users);

    await expect(store.msetnx([])).resolves.toBe(true);
    await expect(store.msetnx(new Map())).resolves.toBe(true);

    expect(commands).toEqual([]);
  });
});

describe("createStringStore getEx modes", () => {
  it("keeps accepting plain seconds", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(fakeClient(commands, ["hello"]), texts);

    await expect(strings.getex("greeting", 60)).resolves.toBe("hello");

    expect(commands).toEqual([["GETEX", "text:greeting", "EX", 60]]);
  });

  it("emits EX, PX, EXAT, PXAT, and PERSIST from option objects", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(
      fakeClient(commands, ["a", "b", "c", "d", "e"]),
      texts
    );

    await expect(strings.getex("greeting", { ttlSeconds: 60 })).resolves.toBe(
      "a"
    );
    await expect(
      strings.getex("greeting", { ttlMilliseconds: 1500 })
    ).resolves.toBe("b");
    await expect(
      strings.getex("greeting", { expireAtSeconds: 1735689600 })
    ).resolves.toBe("c");
    await expect(
      strings.getex("greeting", { expireAtMilliseconds: 1735689600000 })
    ).resolves.toBe("d");
    await expect(strings.getex("greeting", { persist: true })).resolves.toBe(
      "e"
    );

    expect(commands).toEqual([
      ["GETEX", "text:greeting", "EX", 60],
      ["GETEX", "text:greeting", "PX", 1500],
      ["GETEX", "text:greeting", "EXAT", 1735689600],
      ["GETEX", "text:greeting", "PXAT", 1735689600000],
      ["GETEX", "text:greeting", "PERSIST"]
    ]);
  });

  it("returns null for missing keys in every mode", async () => {
    const strings = createStringStore(fakeClient([], [null, null]), texts);

    await expect(strings.getex("missing", 60)).resolves.toBeNull();
    await expect(
      strings.getex("missing", { persist: true })
    ).resolves.toBeNull();
  });

  it("throws TypeError on unexpected GETEX replies", async () => {
    await expect(
      createStringStore(fakeClient([], [1]), texts).getex("greeting", {
        persist: true
      })
    ).rejects.toThrow(TypeError);
  });

  it("validates every numeric getEx mode before sending", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(fakeClient(commands, []), texts);

    await expect(strings.getex("greeting", 0)).rejects.toThrow(TypeError);
    await expect(strings.getex("greeting", { ttlSeconds: 0 })).rejects.toThrow(
      TypeError
    );
    await expect(
      strings.getex("greeting", { ttlMilliseconds: 1.5 })
    ).rejects.toThrow(TypeError);
    await expect(
      strings.getex("greeting", { expireAtSeconds: 0 })
    ).rejects.toThrow(TypeError);
    await expect(
      strings.getex("greeting", { expireAtMilliseconds: -1 })
    ).rejects.toThrow(TypeError);

    expect(commands).toEqual([]);
  });

  it("requires exactly one getEx mode", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(fakeClient(commands, []), texts);

    await expect(
      strings.getex("greeting", {} as StringGetExOptions)
    ).rejects.toThrow(TypeError);
    await expect(
      strings.getex("greeting", {
        ttlSeconds: 1,
        ttlMilliseconds: 1
      } as StringGetExOptions)
    ).rejects.toThrow(TypeError);
    await expect(
      strings.getex("greeting", {
        persist: false
      } as unknown as StringGetExOptions)
    ).rejects.toThrow(TypeError);

    expect(commands).toEqual([]);
  });
});

describe("createStringStore lcs", () => {
  it("returns the subsequence string by default", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(fakeClient(commands, ["mytext"]), texts);

    await expect(strings.lcs("k1", "k2")).resolves.toBe("mytext");
    expect(commands).toEqual([["LCS", "text:k1", "text:k2"]]);
  });

  it("returns the length with LEN", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(fakeClient(commands, [6]), texts);

    await expect(strings.lcs("k1", "k2", { len: true })).resolves.toBe(6);
    expect(commands).toEqual([["LCS", "text:k1", "text:k2", "LEN"]]);
  });

  it("decodes IDX match ranges from a flat-array reply (RESP2)", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(
      fakeClient(commands, [
        [
          "matches",
          [
            [
              [4, 7],
              [5, 8]
            ],
            [
              [2, 3],
              [0, 1]
            ]
          ],
          "len",
          6
        ]
      ]),
      texts
    );

    await expect(strings.lcs("k1", "k2", { idx: true })).resolves.toEqual({
      matches: [
        { a: [4, 7], b: [5, 8] },
        { a: [2, 3], b: [0, 1] }
      ],
      length: 6
    });
    expect(commands).toEqual([["LCS", "text:k1", "text:k2", "IDX"]]);
  });

  it("decodes IDX from a RESP3 map and passes MINMATCHLEN + WITHMATCHLEN", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(
      fakeClient(commands, [
        new Map<RedisReply, RedisReply>([
          ["matches", [[[4, 7], [5, 8], 4]]],
          ["len", 6]
        ])
      ]),
      texts
    );

    await expect(
      strings.lcs("k1", "k2", { idx: true, minMatchLen: 4, withMatchLen: true })
    ).resolves.toEqual({
      matches: [{ a: [4, 7], b: [5, 8], length: 4 }],
      length: 6
    });
    expect(commands).toEqual([
      ["LCS", "text:k1", "text:k2", "IDX", "MINMATCHLEN", 4, "WITHMATCHLEN"]
    ]);
  });

  it("validates minMatchLen and rejects malformed replies", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(
      fakeClient(commands, [1, [1, 2, 3]]),
      texts
    );

    await expect(
      strings.lcs("k1", "k2", { idx: true, minMatchLen: -1 })
    ).rejects.toThrow("minMatchLen must be a non-negative safe integer");
    await expect(strings.lcs("k1", "k2")).rejects.toThrow(
      "Expected Redis LCS to return string"
    );
    await expect(strings.lcs("k1", "k2", { idx: true })).rejects.toThrow(
      "Expected Redis LCS IDX to return an array or map"
    );
    expect(commands).toEqual([
      ["LCS", "text:k1", "text:k2"],
      ["LCS", "text:k1", "text:k2", "IDX"]
    ]);
  });

  it("rejects malformed IDX matches, matches items, and ranges", async () => {
    const strings = createStringStore(
      fakeClient(
        [],
        [
          ["matches", "nope", "len", 6],
          ["matches", [[[4, 7]]], "len", 6],
          ["matches", [[["x"], [5, 8]]], "len", 6]
        ]
      ),
      texts
    );

    await expect(strings.lcs("k1", "k2", { idx: true })).rejects.toThrow(
      "Expected Redis LCS IDX matches to return an array"
    );
    await expect(strings.lcs("k1", "k2", { idx: true })).rejects.toThrow(
      "Expected Redis LCS IDX match to return range pairs"
    );
    await expect(strings.lcs("k1", "k2", { idx: true })).rejects.toThrow(
      "Expected Redis LCS IDX range to return a start/end pair"
    );
  });
});

describe("createCounterStore incrByFloat", () => {
  it("emits INCRBYFLOAT and parses the bulk string reply", async () => {
    const commands: RedisCommand[] = [];
    const store = createCounterStore(fakeClient(commands, ["3.7"]), counters);

    await expect(store.incrbyfloat("hits", 2.5)).resolves.toBe(3.7);

    expect(commands).toEqual([["INCRBYFLOAT", "counter:hits", 2.5]]);
  });

  it("accepts negative and integer amounts", async () => {
    const commands: RedisCommand[] = [];
    const store = createCounterStore(
      fakeClient(commands, ["-0.5", "2"]),
      counters
    );

    await expect(store.incrbyfloat("hits", -3)).resolves.toBe(-0.5);
    await expect(store.incrbyfloat("hits", 0.25)).resolves.toBe(2);

    expect(commands).toEqual([
      ["INCRBYFLOAT", "counter:hits", -3],
      ["INCRBYFLOAT", "counter:hits", 0.25]
    ]);
  });

  it("validates the amount before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createCounterStore(fakeClient(commands, []), counters);

    await expect(store.incrbyfloat("hits", Number.NaN)).rejects.toThrow(
      TypeError
    );
    await expect(store.incrbyfloat("hits", Infinity)).rejects.toThrow(
      TypeError
    );

    expect(commands).toEqual([]);
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typeClient = null as unknown as RedisClient;

const profileStore = createKeyValueStore(
  typeClient,
  defineKeyspace("profile", codecs.json<{ name: string }>())
);
const stringStore = createStringStore(
  typeClient,
  defineKeyspace("text", codecs.string())
);
const counterStore = createCounterStore(
  typeClient,
  defineKeyspace("counter", codecs.number())
);

type SetOptionsParam = Parameters<typeof profileStore.set>[2];
type SetValue = Parameters<typeof profileStore.set>[1];
type MSetIfAbsentResult = Awaited<ReturnType<typeof profileStore.msetnx>>;
type GetExParam = Parameters<typeof stringStore.getex>[1];
type GetExResult = Awaited<ReturnType<typeof stringStore.getex>>;
type IncrByFloatResult = Awaited<ReturnType<typeof counterStore.incrbyfloat>>;

type _SetOptionsParam = Expect<
  Equal<SetOptionsParam, KeyValueSetOptions | undefined>
>;
type _SetValue = Expect<Equal<SetValue, { name: string }>>;
type _MSetIfAbsentResult = Expect<Equal<MSetIfAbsentResult, boolean>>;
type _GetExParam = Expect<Equal<GetExParam, number | StringGetExOptions>>;
type _GetExResult = Expect<Equal<GetExResult, string | null>>;
type _IncrByFloatResult = Expect<Equal<IncrByFloatResult, number>>;

async function conditionalSetTypeProbes() {
  // nx/xx select the conditional overload that resolves to boolean.
  const nx = await profileStore.set("42", { name: "beni" }, { nx: true });
  type _Nx = Expect<Equal<typeof nx, boolean>>;
  const xx = await profileStore.set("42", { name: "beni" }, { xx: true });
  type _Xx = Expect<Equal<typeof xx, boolean>>;
  // plain set resolves to void.
  const plain = await profileStore.set("42", { name: "beni" });
  type _Plain = Expect<Equal<typeof plain, void>>;
}
void conditionalSetTypeProbes;

async function lcsTypeProbes() {
  const plain = await stringStore.lcs("a", "b");
  type _Plain = Expect<Equal<typeof plain, string>>;
  const len = await stringStore.lcs("a", "b", { len: true });
  type _Len = Expect<Equal<typeof len, number>>;
  const idx = await stringStore.lcs("a", "b", {
    idx: true,
    withMatchLen: true
  });
  type _Idx = Expect<Equal<typeof idx, LcsIdxResult>>;
}
void lcsTypeProbes;

function expectTypeErrorsOnly() {
  // @ts-expect-error lcs LEN and IDX options are mutually exclusive.
  void stringStore.lcs("a", "b", { len: true, idx: true });

  // @ts-expect-error keepTtl must be a boolean.
  void profileStore.set("42", { name: "beni" }, { keepTtl: "yes" });

  // @ts-expect-error set value must match the codec input type.
  void profileStore.set("42", { name: 1 }, { nx: true });

  // @ts-expect-error ttlSeconds must be a number.
  void profileStore.set("k", { name: "a" }, { xx: true, ttlSeconds: "60" });

  // @ts-expect-error msetnx values must match the codec input type.
  void profileStore.msetnx([["42", { name: 1 }]]);

  // @ts-expect-error getex options must pick a known expiry mode.
  void stringStore.getex("a", {});

  // @ts-expect-error persist mode only accepts true.
  void stringStore.getex("a", { persist: false });

  // @ts-expect-error incrbyfloat amounts must be numbers.
  void counterStore.incrbyfloat("hits", "1.5");
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

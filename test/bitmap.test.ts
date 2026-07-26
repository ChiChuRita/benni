import { describe, expect, it } from "vitest";
import {
  type BitfieldOffset,
  type BitfieldType,
  createBitmapStore,
  defineBitmap
} from "../src/core/bitmap.js";
import { ValidationError } from "../src/core/errors.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

describe("defineBitmap", () => {
  it("formats keys with string, number, and bigint ids", () => {
    const flags = defineBitmap("flags");

    expect(flags.prefix).toBe("flags");
    expect(flags.key("42")).toBe("flags:42");
    expect(flags.key(42)).toBe("flags:42");
    expect(flags.key(42n)).toBe("flags:42");
  });
});

describe("createBitmapStore", () => {
  it("emits SETBIT and decodes the previous bit", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [0, 1]),
      defineBitmap("flags")
    );

    await expect(store.setbit("42", 7, true)).resolves.toBe(false);
    await expect(store.setbit("42", 7, false)).resolves.toBe(true);

    expect(commands).toEqual([
      ["SETBIT", "flags:42", 7, 1],
      ["SETBIT", "flags:42", 7, 0]
    ]);
  });

  it("emits GETBIT and decodes the bit", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [1, 0]),
      defineBitmap("flags")
    );

    await expect(store.getbit("42", 0)).resolves.toBe(true);
    await expect(store.getbit("42", 8)).resolves.toBe(false);

    expect(commands).toEqual([
      ["GETBIT", "flags:42", 0],
      ["GETBIT", "flags:42", 8]
    ]);
  });

  it("emits BITCOUNT with optional range and unit", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [12, 4, 2]),
      defineBitmap("flags")
    );

    await expect(store.bitcount("42")).resolves.toBe(12);
    await expect(store.bitcount("42", { start: 0, end: -1 })).resolves.toBe(4);
    await expect(
      store.bitcount("42", { start: 5, end: 30, unit: "BIT" })
    ).resolves.toBe(2);

    expect(commands).toEqual([
      ["BITCOUNT", "flags:42"],
      ["BITCOUNT", "flags:42", 0, -1],
      ["BITCOUNT", "flags:42", 5, 30, "BIT"]
    ]);
  });

  it("emits BITPOS with optional start, end, and unit", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [3, 16, 9, 21]),
      defineBitmap("flags")
    );

    await expect(store.bitpos("42", true)).resolves.toBe(3);
    await expect(store.bitpos("42", false, { start: 2 })).resolves.toBe(16);
    await expect(store.bitpos("42", true, { start: 2, end: -1 })).resolves.toBe(
      9
    );
    await expect(
      store.bitpos("42", true, { start: 2, end: -1, unit: "BIT" })
    ).resolves.toBe(21);

    expect(commands).toEqual([
      ["BITPOS", "flags:42", 1],
      ["BITPOS", "flags:42", 0, 2],
      ["BITPOS", "flags:42", 1, 2, -1],
      ["BITPOS", "flags:42", 1, 2, -1, "BIT"]
    ]);
  });

  it("maps a BITPOS -1 reply to null", async () => {
    const store = createBitmapStore(
      fakeClient([], [-1]),
      defineBitmap("flags")
    );

    await expect(store.bitpos("42", true)).resolves.toBeNull();
  });

  it("emits BITOP for AND, OR, XOR, and NOT", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [4, 4, 4, 4]),
      defineBitmap("flags")
    );

    await expect(store.bitop("dest", "AND", ["a", "b"])).resolves.toBe(4);
    await expect(store.bitop("dest", "OR", ["a", "b"])).resolves.toBe(4);
    await expect(store.bitop("dest", "XOR", ["a", "b"])).resolves.toBe(4);
    await expect(store.bitop("dest", "NOT", ["a"])).resolves.toBe(4);

    expect(commands).toEqual([
      ["BITOP", "AND", "flags:dest", "flags:a", "flags:b"],
      ["BITOP", "OR", "flags:dest", "flags:a", "flags:b"],
      ["BITOP", "XOR", "flags:dest", "flags:a", "flags:b"],
      ["BITOP", "NOT", "flags:dest", "flags:a"]
    ]);
  });

  it("emits DEL", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [1]),
      defineBitmap("flags")
    );

    await expect(store.del("42")).resolves.toBe(1);

    expect(commands).toEqual([["DEL", "flags:42"]]);
  });

  it("rejects invalid bit offsets before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, []),
      defineBitmap("flags")
    );

    await expect(store.setbit("42", -1, true)).rejects.toThrow(
      "offset must be a non-negative safe integer"
    );
    await expect(store.setbit("42", 1.5, true)).rejects.toThrow(TypeError);
    await expect(
      store.setbit("42", Number.MAX_SAFE_INTEGER + 1, true)
    ).rejects.toThrow(TypeError);
    await expect(store.getbit("42", -1)).rejects.toThrow(
      "offset must be a non-negative safe integer"
    );
    await expect(store.getbit("42", Number.NaN)).rejects.toThrow(TypeError);

    expect(commands).toEqual([]);
  });

  it("rejects non-integer count ranges before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, []),
      defineBitmap("flags")
    );

    await expect(store.bitcount("42", { start: 0.5, end: 1 })).rejects.toThrow(
      "start must be a safe integer"
    );
    await expect(
      store.bitcount("42", { start: 0, end: Number.NaN })
    ).rejects.toThrow("end must be a safe integer");

    expect(commands).toEqual([]);
  });

  it("rejects invalid position options before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, []),
      defineBitmap("flags")
    );

    await expect(store.bitpos("42", true, { end: 3 })).rejects.toThrow(
      "bitpos end requires start"
    );
    await expect(store.bitpos("42", true, { unit: "BIT" })).rejects.toThrow(
      "bitpos unit requires start and end"
    );
    await expect(
      store.bitpos("42", true, { start: 0, unit: "BIT" })
    ).rejects.toThrow("bitpos unit requires start and end");
    await expect(
      store.bitpos("42", true, { start: 0.5, end: 1 })
    ).rejects.toThrow("start must be a safe integer");
    await expect(
      store.bitpos("42", true, { start: 0, end: Number.POSITIVE_INFINITY })
    ).rejects.toThrow("end must be a safe integer");

    expect(commands).toEqual([]);
  });

  it("rejects invalid combine source ids before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, []),
      defineBitmap("flags")
    );

    await expect(store.bitop("dest", "OR", [])).rejects.toThrow(
      "bitop requires at least one source id"
    );
    await expect(store.bitop("dest", "NOT", ["a", "b"])).rejects.toThrow(
      "bitop with NOT requires exactly one source id"
    );

    expect(commands).toEqual([]);
  });

  it("builds a BITFIELD chain and decodes the result tuple", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [[100, 42, null]]),
      defineBitmap("metrics")
    );

    const result = await store
      .bitfield("42")
      .get("u32", 0)
      .set("u32", 0, 100)
      .overflow("fail")
      .incrby("u8", "#8", 1)
      .exec();

    expect(result).toEqual([100, 42, null]);
    expect(commands).toEqual([
      [
        "BITFIELD",
        "metrics:42",
        "GET",
        "u32",
        0,
        "SET",
        "u32",
        0,
        100,
        "OVERFLOW",
        "FAIL",
        "INCRBY",
        "u8",
        "#8",
        1
      ]
    ]);
  });

  it("maps overflow modes to WRAP and SAT tokens", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [[0, 0]]),
      defineBitmap("metrics")
    );

    await store
      .bitfield("42")
      .overflow("wrap")
      .incrby("u8", 0, 1)
      .overflow("sat")
      .incrby("u8", 0, 1)
      .exec();

    expect(commands).toEqual([
      [
        "BITFIELD",
        "metrics:42",
        "OVERFLOW",
        "WRAP",
        "INCRBY",
        "u8",
        0,
        1,
        "OVERFLOW",
        "SAT",
        "INCRBY",
        "u8",
        0,
        1
      ]
    ]);
  });

  it("sends BITFIELD with no operations for an empty chain", async () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, [[]]),
      defineBitmap("metrics")
    );

    await expect(store.bitfield("42").exec()).resolves.toEqual([]);
    expect(commands).toEqual([["BITFIELD", "metrics:42"]]);
  });

  it("rejects invalid bitfield operations before sending", () => {
    const commands: RedisCommand[] = [];
    const store = createBitmapStore(
      fakeClient(commands, []),
      defineBitmap("metrics")
    );

    expect(() => store.bitfield("42").get("u99", 0)).toThrow(
      "unsigned width must be 1-63"
    );
    expect(() => store.bitfield("42").incrby("i65", 0, 1)).toThrow(
      "signed width must be 1-64"
    );
    expect(() => store.bitfield("42").get("nope" as BitfieldType, 0)).toThrow(
      ValidationError
    );
    expect(() => store.bitfield("42").get("u8", -1)).toThrow(
      "offset must be a non-negative safe integer"
    );
    expect(() =>
      store.bitfield("42").get("u8", "#x" as BitfieldOffset)
    ).toThrow('bitfield offset must be a non-negative integer or "#<n>"');
    expect(() => store.bitfield("42").set("u8", 0, 1.5)).toThrow(
      "value must be a safe integer"
    );
    expect(() => store.bitfield("42").incrby("u8", 0, Number.NaN)).toThrow(
      "increment must be a safe integer"
    );

    expect(commands).toEqual([]);
  });

  it("throws when BITFIELD does not return an array", async () => {
    const store = createBitmapStore(
      fakeClient([], ["12"]),
      defineBitmap("metrics")
    );

    await expect(store.bitfield("42").get("u8", 0).exec()).rejects.toThrow(
      "Expected Redis BITFIELD to return an array"
    );
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typeClient = null as unknown as RedisClient;

const flags = defineBitmap("flags");
const flagStore = createBitmapStore(typeClient, flags);

type FlagKey = ReturnType<typeof flags.key<"42">>;
type FlagPrefix = typeof flags.prefix;
type SetBitResult = Awaited<ReturnType<typeof flagStore.setbit>>;
type GetBitResult = Awaited<ReturnType<typeof flagStore.getbit>>;
type CountResult = Awaited<ReturnType<typeof flagStore.bitcount>>;
type PositionResult = Awaited<ReturnType<typeof flagStore.bitpos>>;
type CombineResult = Awaited<ReturnType<typeof flagStore.bitop>>;
type CombineOperation = Parameters<typeof flagStore.bitop>[1];

type _FlagKey = Expect<Equal<FlagKey, "flags:42">>;
type _FlagPrefix = Expect<Equal<FlagPrefix, "flags">>;
type _SetBitResult = Expect<Equal<SetBitResult, boolean>>;
type _GetBitResult = Expect<Equal<GetBitResult, boolean>>;
type _CountResult = Expect<Equal<CountResult, number>>;
type _PositionResult = Expect<Equal<PositionResult, number | null>>;
type _CombineResult = Expect<Equal<CombineResult, number>>;
type _CombineOperation = Expect<
  Equal<CombineOperation, "AND" | "OR" | "XOR" | "NOT">
>;

const bitfieldChain = flagStore
  .bitfield("42")
  .get("u8", 0)
  .set("u8", 0, 1)
  .incrby("i16", "#1", 2);
type BitfieldResult = Awaited<ReturnType<typeof bitfieldChain.exec>>;
type EmptyBitfieldResult = Awaited<
  ReturnType<ReturnType<typeof flagStore.bitfield>["exec"]>
>;
type _BitfieldResult = Expect<
  Equal<BitfieldResult, [number, number | null, number | null]>
>;
type _EmptyBitfieldResult = Expect<Equal<EmptyBitfieldResult, []>>;

const knownFlags = defineBitmap("known", { ids: ["one", "two"] });
const knownFlagStore = createBitmapStore(typeClient, knownFlags);
type KnownFlagKey = ReturnType<typeof knownFlags.key<"one">>;
type KnownFlagId = Parameters<typeof knownFlagStore.getbit>[0];
type _KnownFlagKey = Expect<Equal<KnownFlagKey, "known:one">>;
type _KnownFlagId = Expect<Equal<KnownFlagId, "one" | "two">>;

function expectTypeErrorsOnly() {
  // @ts-expect-error setBit values must be booleans, not raw 0/1.
  void flagStore.setbit("42", 0, 1);

  // @ts-expect-error bit offsets must be numbers.
  void flagStore.getbit("42", "0");

  // @ts-expect-error count ranges require both start and end.
  void flagStore.bitcount("42", { start: 0 });

  // @ts-expect-error range units are limited to BYTE or BIT.
  void flagStore.bitcount("42", { start: 0, end: -1, unit: "NIBBLE" });

  // @ts-expect-error position bits must be booleans.
  void flagStore.bitpos("42", 1);

  // @ts-expect-error combine operations are limited to AND, OR, XOR, NOT.
  void flagStore.bitop("NAND", "dest", ["a"]);

  // @ts-expect-error known bitmap keyspaces only accept declared ids.
  void knownFlagStore.setbit("three", 0, true);

  // @ts-expect-error combine source ids must match the declared ids.
  void knownFlagStore.bitop("one", "OR", ["three"]);
}

void expectTypeErrorsOnly;

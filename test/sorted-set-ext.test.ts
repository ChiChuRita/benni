import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import type {
  SortedSetLexBound,
  SortedSetRangeByScoreOptions,
  SortedSetScoreBound
} from "../src/core/sorted-set.js";
import {
  createSortedSetStore,
  defineSortedSet
} from "../src/core/sorted-set.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  SortedSetEntry
} from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

const board = defineSortedSet("board", codecs.string());

function storeWithReply(reply: RedisReply) {
  return createSortedSetStore(fakeClient([], [reply]), board);
}

describe("createSortedSetStore extensions", () => {
  it("supports typed ZREVRANK, ZMSCORE, and ZRANDMEMBER", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [
        0,
        ["10", null],
        "alice",
        ["alice", "bob"],
        ["alice", "alice", "bob"]
      ]),
      board
    );

    await expect(store.zrevrank("game", "alice")).resolves.toBe(0);
    await expect(store.zmscore("game", ["alice", "bob"])).resolves.toEqual([
      10,
      null
    ]);
    await expect(store.zrandmember("game")).resolves.toBe("alice");
    await expect(store.zrandmember("game", { count: 2 })).resolves.toEqual([
      "alice",
      "bob"
    ]);
    await expect(store.zrandmember("game", { count: -3 })).resolves.toEqual([
      "alice",
      "alice",
      "bob"
    ]);

    expect(commands).toEqual([
      ["ZREVRANK", "board:game", "alice"],
      ["ZMSCORE", "board:game", "alice", "bob"],
      ["ZRANDMEMBER", "board:game"],
      ["ZRANDMEMBER", "board:game", 2],
      ["ZRANDMEMBER", "board:game", -3]
    ]);
  });

  it("supports REV and BYSCORE range reads", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [
        ["bob", "alice"],
        ["alice", "bob"],
        ["bob", "alice"],
        ["alice"],
        [["alice", "10"]]
      ]),
      board
    );

    await expect(
      store.zrange("game", { start: 0, stop: -1, rev: true })
    ).resolves.toEqual(["bob", "alice"]);
    await expect(
      store.zrange("game", { byScore: true, min: 0, max: "+inf" })
    ).resolves.toEqual(["alice", "bob"]);
    await expect(
      store.zrange("game", { byScore: true, min: "-inf", max: 100, rev: true })
    ).resolves.toEqual(["bob", "alice"]);
    await expect(
      store.zrange("game", {
        byScore: true,
        min: "(1",
        max: 100,
        offset: 1,
        count: 2
      })
    ).resolves.toEqual(["alice"]);
    await expect(
      store.zrange("game", {
        byScore: true,
        withScores: true,
        min: 0,
        max: 20,
        rev: true,
        offset: 0,
        count: 1
      })
    ).resolves.toEqual([{ member: "alice", score: 10 }]);

    expect(commands).toEqual([
      ["ZRANGE", "board:game", 0, -1, "REV"],
      ["ZRANGE", "board:game", 0, "+inf", "BYSCORE"],
      ["ZRANGE", "board:game", 100, "-inf", "BYSCORE", "REV"],
      ["ZRANGE", "board:game", "(1", 100, "BYSCORE", "LIMIT", 1, 2],
      [
        "ZRANGE",
        "board:game",
        20,
        0,
        "BYSCORE",
        "REV",
        "LIMIT",
        0,
        1,
        "WITHSCORES"
      ]
    ]);
  });

  it("supports ZRANGESTORE for rank and score ranges", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, [2, 3]), board);

    await expect(
      store.zrangestore("dest", "src", { start: 0, stop: -1 })
    ).resolves.toBe(2);
    await expect(
      store.zrangestore("dest", "src", {
        byScore: true,
        min: "-inf",
        max: "(5",
        rev: true,
        offset: 0,
        count: 3
      })
    ).resolves.toBe(3);

    expect(commands).toEqual([
      ["ZRANGESTORE", "board:dest", "board:src", 0, -1],
      [
        "ZRANGESTORE",
        "board:dest",
        "board:src",
        "(5",
        "-inf",
        "BYSCORE",
        "REV",
        "LIMIT",
        0,
        3
      ]
    ]);
  });

  it("supports range removals and bulk pops", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [
        2,
        1,
        ["alice", "1", "bob", "2"],
        [["carol", "9"]]
      ]),
      board
    );

    await expect(store.zremrangebyrank("game", 0, 1)).resolves.toBe(2);
    await expect(store.zremrangebyscore("game", "(0", "+inf")).resolves.toBe(1);
    await expect(store.zpopmin("game", { count: 2 })).resolves.toEqual([
      { member: "alice", score: 1 },
      { member: "bob", score: 2 }
    ]);
    await expect(store.zpopmax("game", { count: 1 })).resolves.toEqual([
      { member: "carol", score: 9 }
    ]);

    expect(commands).toEqual([
      ["ZREMRANGEBYRANK", "board:game", 0, 1],
      ["ZREMRANGEBYSCORE", "board:game", "(0", "+inf"],
      ["ZPOPMIN", "board:game", 2],
      ["ZPOPMAX", "board:game", 1]
    ]);
  });

  it("supports sorted-set algebra commands", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [
        ["alice", "bob"],
        ["alice", "10"],
        ["alice"],
        [["alice", "3"]],
        ["bob"],
        ["bob", "2"],
        3,
        1,
        2,
        1,
        1
      ]),
      board
    );

    await expect(store.zunion("a", ["b"])).resolves.toEqual(["alice", "bob"]);
    await expect(
      store.zunion("a", ["b"], {
        withScores: true,
        weights: [1, 2],
        aggregate: "max"
      })
    ).resolves.toEqual([{ member: "alice", score: 10 }]);
    await expect(
      store.zinter("a", ["b"], { aggregate: "min" })
    ).resolves.toEqual(["alice"]);
    await expect(
      store.zinter("a", ["b"], { withScores: true })
    ).resolves.toEqual([{ member: "alice", score: 3 }]);
    await expect(store.zdiff("a", ["b"])).resolves.toEqual(["bob"]);
    await expect(
      store.zdiff("a", ["b"], { withScores: true })
    ).resolves.toEqual([{ member: "bob", score: 2 }]);
    await expect(
      store.zunionstore("dest", "a", ["b"], { weights: [2, 3] })
    ).resolves.toBe(3);
    await expect(
      store.zinterstore("dest", "a", ["b"], { aggregate: "sum" })
    ).resolves.toBe(1);
    await expect(store.zdiffstore("dest", "a", ["b"])).resolves.toBe(2);
    await expect(store.zintercard("a", ["b"])).resolves.toBe(1);
    await expect(store.zintercard("a", ["b"], { limit: 10 })).resolves.toBe(1);

    expect(commands).toEqual([
      ["ZUNION", 2, "board:a", "board:b"],
      [
        "ZUNION",
        2,
        "board:a",
        "board:b",
        "WEIGHTS",
        1,
        2,
        "AGGREGATE",
        "MAX",
        "WITHSCORES"
      ],
      ["ZINTER", 2, "board:a", "board:b", "AGGREGATE", "MIN"],
      ["ZINTER", 2, "board:a", "board:b", "WITHSCORES"],
      ["ZDIFF", 2, "board:a", "board:b"],
      ["ZDIFF", 2, "board:a", "board:b", "WITHSCORES"],
      ["ZUNIONSTORE", "board:dest", 2, "board:a", "board:b", "WEIGHTS", 2, 3],
      [
        "ZINTERSTORE",
        "board:dest",
        2,
        "board:a",
        "board:b",
        "AGGREGATE",
        "SUM"
      ],
      ["ZDIFFSTORE", "board:dest", 2, "board:a", "board:b"],
      ["ZINTERCARD", 2, "board:a", "board:b"],
      ["ZINTERCARD", 2, "board:a", "board:b", "LIMIT", 10]
    ]);
  });

  it("skips empty inputs and returns null for missing values", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [null, null]),
      board
    );

    await expect(store.zmscore("game", [])).resolves.toEqual([]);
    await expect(store.zpopmin("game", { count: 0 })).resolves.toEqual([]);
    await expect(store.zpopmax("game", { count: 0 })).resolves.toEqual([]);
    await expect(store.zrevrank("game", "alice")).resolves.toBeNull();
    await expect(store.zrandmember("game")).resolves.toBeNull();

    expect(commands).toEqual([
      ["ZREVRANK", "board:game", "alice"],
      ["ZRANDMEMBER", "board:game"]
    ]);
  });

  it("validates numeric inputs before sending commands", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, []), board);
    const offsetOnly = {
      min: 0,
      max: 10,
      offset: 0
    } as unknown as SortedSetRangeByScoreOptions;
    const countOnly = {
      min: 0,
      max: 10,
      count: 2
    } as unknown as SortedSetRangeByScoreOptions;

    await expect(store.zrandmember("game", { count: 0 })).rejects.toThrow(
      "count must be a nonzero safe integer"
    );
    await expect(store.zrandmember("game", { count: 1.5 })).rejects.toThrow(
      TypeError
    );
    await expect(
      store.zrange("game", { start: 0.5, stop: 1, rev: true })
    ).rejects.toThrow("start must be a safe integer");
    await expect(
      store.zrange("game", { start: 0, stop: 1.5, rev: true })
    ).rejects.toThrow("stop must be a safe integer");
    await expect(
      store.zrange("game", {
        byScore: true,
        min: Number.POSITIVE_INFINITY,
        max: 1
      })
    ).rejects.toThrow("min must be a finite number");
    await expect(
      store.zrange("game", { byScore: true, min: 0, max: Number.NaN })
    ).rejects.toThrow("max must be a finite number");
    await expect(
      store.zrange("game", { byScore: true, ...offsetOnly })
    ).rejects.toThrow("offset and count must be provided together");
    await expect(
      store.zrange("game", { byScore: true, ...countOnly })
    ).rejects.toThrow("offset and count must be provided together");
    await expect(
      store.zrange("game", {
        byScore: true,
        min: 0,
        max: 1,
        offset: -1,
        count: 2
      })
    ).rejects.toThrow("offset must be a nonnegative safe integer");
    await expect(
      store.zrange("game", {
        byScore: true,
        min: 0,
        max: 1,
        offset: 0.5,
        count: 2
      })
    ).rejects.toThrow(TypeError);
    await expect(
      store.zrange("game", {
        byScore: true,
        min: 0,
        max: 1,
        offset: 0,
        count: 1.5
      })
    ).rejects.toThrow("count must be a safe integer");
    await expect(
      store.zrangestore("dest", "src", { start: 0.5, stop: 1 })
    ).rejects.toThrow(TypeError);
    await expect(store.zremrangebyrank("game", 0, 1.5)).rejects.toThrow(
      TypeError
    );
    await expect(
      store.zremrangebyscore("game", Number.NEGATIVE_INFINITY, 1)
    ).rejects.toThrow(TypeError);
    await expect(store.zpopmin("game", { count: -1 })).rejects.toThrow(
      "count must be a nonnegative safe integer"
    );
    await expect(store.zpopmin("game", { count: 1.5 })).rejects.toThrow(
      TypeError
    );
    await expect(store.zpopmax("game", { count: -2 })).rejects.toThrow(
      TypeError
    );
    await expect(store.zunion("a", ["b"], { weights: [1] })).rejects.toThrow(
      "weights length must match the number of keys"
    );
    await expect(
      store.zunion("a", ["b"], { weights: [1, Number.POSITIVE_INFINITY] })
    ).rejects.toThrow("weights must be finite numbers");
    await expect(
      store.zunionstore("dest", "a", ["b"], { weights: [1, 2, 3] })
    ).rejects.toThrow(TypeError);
    await expect(
      store.zinterstore("dest", "a", ["b"], { weights: [1] })
    ).rejects.toThrow(TypeError);
    await expect(store.zintercard("a", ["b"], { limit: -1 })).rejects.toThrow(
      "limit must be a nonnegative safe integer"
    );
    await expect(store.zintercard("a", ["b"], { limit: 0.5 })).rejects.toThrow(
      TypeError
    );

    expect(commands).toEqual([]);
  });

  it("throws on unexpected replies", async () => {
    await expect(storeWithReply("0").zrevrank("game", "alice")).rejects.toThrow(
      TypeError
    );
    await expect(
      storeWithReply("nope").zmscore("game", ["alice"])
    ).rejects.toThrow("Expected Redis ZMSCORE to return array");
    await expect(
      storeWithReply([true]).zmscore("game", ["alice"])
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply(["nope"]).zmscore("game", ["alice"])
    ).rejects.toThrow(TypeError);
    await expect(storeWithReply(1).zrandmember("game")).rejects.toThrow(
      "Expected Redis ZRANDMEMBER to return string or null"
    );
    await expect(
      storeWithReply("alice").zrandmember("game", { count: 1 })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply([1]).zrandmember("game", { count: 1 })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply("alice").zrange("game", { start: 0, stop: -1, rev: true })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply(null).zrange("game", { byScore: true, min: 0, max: 1 })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply([["alice"]]).zrange("game", {
        byScore: true,
        withScores: true,
        min: 0,
        max: 1
      })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply(["alice"]).zrange("game", {
        byScore: true,
        withScores: true,
        min: 0,
        max: 1
      })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply("1").zrangestore("dest", "src", { start: 0, stop: -1 })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply("1").zremrangebyrank("game", 0, 1)
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply("1").zremrangebyscore("game", 0, 1)
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply(["alice"]).zpopmin("game", { count: 1 })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply([["alice"]]).zpopmax("game", { count: 1 })
    ).rejects.toThrow(TypeError);
    await expect(storeWithReply("alice").zunion("a", ["b"])).rejects.toThrow(
      TypeError
    );
    await expect(
      storeWithReply(["alice"]).zunion("a", ["b"], { withScores: true })
    ).rejects.toThrow(TypeError);
    await expect(storeWithReply(42).zinter("a", ["b"])).rejects.toThrow(
      TypeError
    );
    await expect(
      storeWithReply([["alice", "nope"]]).zinter("a", ["b"], {
        withScores: true
      })
    ).rejects.toThrow(TypeError);
    await expect(storeWithReply(null).zdiff("a", ["b"])).rejects.toThrow(
      TypeError
    );
    await expect(
      storeWithReply([[1, "1"]]).zdiff("a", ["b"], { withScores: true })
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply("3").zunionstore("dest", "a", ["b"])
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply("1").zinterstore("dest", "a", ["b"])
    ).rejects.toThrow(TypeError);
    await expect(
      storeWithReply("2").zdiffstore("dest", "a", ["b"])
    ).rejects.toThrow(TypeError);
    await expect(storeWithReply("2").zintercard("a", ["b"])).rejects.toThrow(
      TypeError
    );
  });
});

describe("createSortedSetStore lexicographic ranges", () => {
  it("emits ZRANGE BYLEX with inclusive, exclusive, and sentinel bounds", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [
        ["a", "b"],
        ["a", "b", "c"]
      ]),
      board
    );

    await expect(
      store.zrange("game", {
        byLex: true,
        min: { value: "a" },
        max: { value: "c", inclusive: false }
      })
    ).resolves.toEqual(["a", "b"]);
    await expect(
      store.zrange("game", { byLex: true, min: "-", max: "+" })
    ).resolves.toEqual(["a", "b", "c"]);

    expect(commands).toEqual([
      ["ZRANGE", "board:game", "[a", "(c", "BYLEX"],
      ["ZRANGE", "board:game", "-", "+", "BYLEX"]
    ]);
  });

  it("swaps bounds for reverse and appends LIMIT", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [["c", "b"]]),
      board
    );

    await expect(
      store.zrange("game", {
        byLex: true,
        min: "-",
        max: "+",
        rev: true,
        offset: 1,
        count: 2
      })
    ).resolves.toEqual(["c", "b"]);

    expect(commands).toEqual([
      ["ZRANGE", "board:game", "+", "-", "BYLEX", "REV", "LIMIT", 1, 2]
    ]);
  });

  it("emits ZLEXCOUNT and ZREMRANGEBYLEX", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, [3, 2]), board);

    await expect(
      store.zlexcount("game", { value: "a" }, { value: "z" })
    ).resolves.toBe(3);
    await expect(
      store.zremrangebylex("game", "-", { value: "c", inclusive: false })
    ).resolves.toBe(2);

    expect(commands).toEqual([
      ["ZLEXCOUNT", "board:game", "[a", "[z"],
      ["ZREMRANGEBYLEX", "board:game", "-", "(c"]
    ]);
  });

  it("stores a lexicographic range with ZRANGESTORE BYLEX", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, [2]), board);

    await expect(
      store.zrangestore("dest", "src", {
        byLex: true,
        min: { value: "a" },
        max: { value: "m" }
      })
    ).resolves.toBe(2);

    expect(commands).toEqual([
      ["ZRANGESTORE", "board:dest", "board:src", "[a", "[m", "BYLEX"]
    ]);
  });

  it("encodes non-string members into lex bounds through the codec", async () => {
    const commands: RedisCommand[] = [];
    const jsonBoard = defineSortedSet("tags", codecs.json<{ tag: string }>());
    const store = createSortedSetStore(fakeClient(commands, [1]), jsonBoard);

    await store.zremrangebylex(
      "game",
      { value: { tag: "a" } },
      { value: { tag: "z" } }
    );

    expect(commands).toEqual([
      ["ZREMRANGEBYLEX", "tags:game", '[{"tag":"a"}', '[{"tag":"z"}']
    ]);
  });

  it("throws on malformed lex bounds and unexpected replies", async () => {
    const store = createSortedSetStore(fakeClient([], []), board);

    await expect(
      store.zrange("game", {
        byLex: true,
        min: {} as unknown as { value: string },
        max: "+"
      })
    ).rejects.toThrow('min must be "-", "+", or { value }');
    await expect(
      storeWithReply("nope").zrange("game", { byLex: true, min: "-", max: "+" })
    ).rejects.toThrow("Expected Redis ZRANGE to return array");
    await expect(
      storeWithReply("1").zlexcount("game", "-", "+")
    ).rejects.toThrow("Expected Redis ZLEXCOUNT to return number");
    await expect(
      storeWithReply("1").zremrangebylex("game", "-", "+")
    ).rejects.toThrow("Expected Redis ZREMRANGEBYLEX to return number");
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typeClient = null as unknown as RedisClient;

const typedBoard = createSortedSetStore(
  typeClient,
  defineSortedSet("typed", codecs.json<{ name: string }>())
);
const knownBoard = createSortedSetStore(
  typeClient,
  defineSortedSet("known", codecs.string(), { ids: ["a", "b"] })
);

// Overloaded methods (zrange, zrandmember, zunion, zpopmin) resolve their
// return type through an actual call so the matching overload is selected,
// not the trailing signature that a bare ReturnType<> would pick.
type TypedScores = Awaited<ReturnType<typeof typedBoard.zmscore>>;
type TypedRevRank = Awaited<ReturnType<typeof typedBoard.zrevrank>>;
type TypedRandomMember = Awaited<ReturnType<typeof typedBoard.zrandmember>>;
type TypedRandomMembers = Awaited<ReturnType<typeof zrandmemberManyProbe>>;
type TypedRangeByScore = Awaited<ReturnType<typeof rangeByScoreProbe>>;
type TypedRangeByScoreWithScores = Awaited<
  ReturnType<typeof rangeByScoreWithScoresProbe>
>;
type TypedPopMinMany = Awaited<ReturnType<typeof popMinManyProbe>>;
type TypedUnion = Awaited<ReturnType<typeof typedBoard.zunion>>;
type TypedRangeStore = Awaited<ReturnType<typeof typedBoard.zrangestore>>;
type TypedScoreBoundParam = Parameters<typeof typedBoard.zremrangebyscore>[1];
type KnownUnionOthers = Parameters<typeof knownBoard.zunion>[1];

const zrandmemberManyProbe = () => typedBoard.zrandmember("game", { count: 2 });
const rangeByScoreProbe = () =>
  typedBoard.zrange("game", { byScore: true, min: 0, max: 10 });
const rangeByScoreWithScoresProbe = () =>
  typedBoard.zrange("game", {
    byScore: true,
    withScores: true,
    min: 0,
    max: 10
  });
const popMinManyProbe = () => typedBoard.zpopmin("game", { count: 2 });

type _TypedScores = Expect<Equal<TypedScores, Array<number | null>>>;
type _TypedRevRank = Expect<Equal<TypedRevRank, number | null>>;
type _TypedRandomMember = Expect<
  Equal<TypedRandomMember, { name: string } | null>
>;
type _TypedRandomMembers = Expect<
  Equal<TypedRandomMembers, Array<{ name: string }>>
>;
type _TypedRangeByScore = Expect<
  Equal<TypedRangeByScore, Array<{ name: string }>>
>;
type _TypedRangeByScoreWithScores = Expect<
  Equal<TypedRangeByScoreWithScores, Array<SortedSetEntry<{ name: string }>>>
>;
// zpopmin(id, { count }) resolves to the array overload; a countless call
// (zpopmin(id) / zpopmin(id, {})) resolves to the single-or-null overload.
type _TypedPopMinMany = Expect<
  Equal<TypedPopMinMany, Array<SortedSetEntry<{ name: string }>>>
>;
type _TypedUnion = Expect<Equal<TypedUnion, Array<{ name: string }>>>;
type _TypedRangeStore = Expect<Equal<TypedRangeStore, number>>;
type _TypedScoreBoundParam = Expect<
  Equal<TypedScoreBoundParam, SortedSetScoreBound>
>;
const rangeByLexProbe = () =>
  typedBoard.zrange("game", { byLex: true, min: "-", max: "+" });
type TypedRangeByLex = Awaited<ReturnType<typeof rangeByLexProbe>>;
type _TypedRangeByLex = Expect<Equal<TypedRangeByLex, Array<{ name: string }>>>;
type TypedLexBoundParam = Parameters<typeof typedBoard.zremrangebylex>[1];
type _TypedLexBoundParam = Expect<
  Equal<TypedLexBoundParam, SortedSetLexBound<{ name: string }>>
>;
type _KnownUnionOthers = Expect<
  Equal<KnownUnionOthers, readonly ("a" | "b")[]>
>;

function expectTypeErrorsOnly() {
  // @ts-expect-error score bounds must be numbers, infinities, or exclusive "(n" strings.
  void typedBoard.zrange("game", { byScore: true, min: "abc", max: 10 });

  // @ts-expect-error offset requires count.
  void typedBoard.zrange("game", { byScore: true, min: 0, max: 10, offset: 0 });

  // @ts-expect-error count requires offset.
  void typedBoard.zrange("game", { byScore: true, min: 0, max: 10, count: 5 });

  // @ts-expect-error by-score range stores require max.
  void typedBoard.zrangestore("dest", "src", { byScore: true, min: 0 });

  // @ts-expect-error aggregate must be sum, min, or max.
  void typedBoard.zunion("a", ["b"], { aggregate: "avg" });

  // @ts-expect-error random member counts must be numbers.
  void typedBoard.zrandmember("game", { count: "5" });

  // @ts-expect-error scored members must match the member codec type.
  void typedBoard.zmscore("game", ["alice"]);

  // @ts-expect-error ZDIFF takes no weights or aggregate options.
  void typedBoard.zdiff("a", ["b"], { weights: [1, 2] });

  // @ts-expect-error known sorted sets only accept declared ids.
  void knownBoard.zunion("a", ["c"]);

  // @ts-expect-error removal score bounds must be score bounds.
  void typedBoard.zremrangebyscore("game", "alice", 10);

  // @ts-expect-error lex bound values must match the member codec type.
  const badLexMin: SortedSetLexBound<{ name: string }> = { value: "alice" };
  void typedBoard.zrange("game", { byLex: true, min: badLexMin, max: "+" });

  // @ts-expect-error raw "[x" strings are not valid lex bounds; use { value }.
  void typedBoard.zrange("game", { byLex: true, min: "[a", max: "+" });
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

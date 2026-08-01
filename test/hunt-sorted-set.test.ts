import { afterAll, describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { ValidationError } from "../src/core/errors.js";
import type {
  SortedSetRandomMemberOptions,
  SortedSetRangeOptions
} from "../src/core/sorted-set.js";
import {
  createSortedSetStore,
  defineSortedSet
} from "../src/core/sorted-set.js";
import type {
  RedisClient,
  RedisCommand,
  SortedSetEntry
} from "../src/core/types.js";
import { node } from "../src/node/index.js";
import { fakeClient } from "./fake-client.js";

const board = defineSortedSet("hunt-zset", codecs.string());

describe("score bounds accept the infinities the score encoder produces", () => {
  it("translates +/-Infinity bounds to the Redis spellings", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [["ada"], 1, 1, 2]),
      board
    );

    await expect(
      store.zrange("game", {
        byScore: true,
        min: Number.NEGATIVE_INFINITY,
        max: Number.POSITIVE_INFINITY
      })
    ).resolves.toEqual(["ada"]);
    await expect(
      store.zremrangebyscore("game", Number.NEGATIVE_INFINITY, 1)
    ).resolves.toBe(1);
    await expect(
      store.zrangestore("dest", "game", {
        byScore: true,
        min: 1,
        max: Number.POSITIVE_INFINITY
      })
    ).resolves.toBe(1);
    await expect(
      store.zcount("game", Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY)
    ).resolves.toBe(2);

    expect(commands).toEqual([
      ["ZRANGE", "hunt-zset:game", "-inf", "+inf", "BYSCORE"],
      ["ZREMRANGEBYSCORE", "hunt-zset:game", "-inf", 1],
      ["ZRANGESTORE", "hunt-zset:dest", "hunt-zset:game", 1, "+inf", "BYSCORE"],
      ["ZCOUNT", "hunt-zset:game", "-inf", "+inf"]
    ]);
  });

  it("still rejects NaN bounds, including on zcount", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, []), board);

    await expect(
      store.zrange("game", { byScore: true, min: 0, max: Number.NaN })
    ).rejects.toThrow(ValidationError);
    await expect(store.zcount("game", Number.NaN, 1)).rejects.toThrow(
      "min must not be NaN"
    );
    await expect(store.zcount("game", 0, Number.NaN)).rejects.toThrow(
      "max must not be NaN"
    );
    expect(commands).toEqual([]);
  });

  it("leaves string bounds alone", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, [0]), board);

    await expect(store.zcount("game", "(1", "+inf")).resolves.toBe(0);
    expect(commands).toEqual([["ZCOUNT", "hunt-zset:game", "(1", "+inf"]]);
  });
});

describe("zrandmember with a computed count of zero", () => {
  it("returns [] without a round trip, matching zpopmin", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, []), board);

    await expect(store.zrandmember("game", { count: 0 })).resolves.toEqual([]);
    await expect(
      store.zrandmember("game", { count: 0, withScores: true })
    ).resolves.toEqual([]);
    expect(commands).toEqual([]);
  });

  it("still rejects a count that is not a safe integer", async () => {
    const store = createSortedSetStore(fakeClient([], []), board);

    await expect(store.zrandmember("game", { count: 1.5 })).rejects.toThrow(
      "count must be a safe integer"
    );
  });
});

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("infinite scores survive the full round trip", () => {
  const live = defineSortedSet(
    `beni:hunt:zset:${Date.now()}:${Math.random().toString(36).slice(2)}`,
    codecs.string()
  );
  let client: RedisClient;

  afterAll(async () => {
    if (client === undefined) return;
    try {
      await client.send(["DEL", live.key("ranked")]);
    } finally {
      await client.close();
    }
  });

  it("reads back a score it wrote and ranges over it", async () => {
    client = await node({ url: redisUrl });
    const store = createSortedSetStore(client, live);

    await store.zadd("ranked", [
      { score: Number.NEGATIVE_INFINITY, member: "banned" },
      { score: 10, member: "ada" },
      { score: Number.POSITIVE_INFINITY, member: "vip" }
    ]);

    const top = await store.zscore("ranked", "vip");
    expect(top).toBe(Number.POSITIVE_INFINITY);

    // The value zscore just produced has to be usable as a bound.
    await expect(
      store.zrange("ranked", { byScore: true, min: 10, max: top as number })
    ).resolves.toEqual(["ada", "vip"]);
    await expect(
      store.zcount("ranked", Number.NEGATIVE_INFINITY, top as number)
    ).resolves.toBe(3);
    await expect(
      store.zremrangebyscore(
        "ranked",
        Number.NEGATIVE_INFINITY,
        Number.NEGATIVE_INFINITY
      )
    ).resolves.toBe(1);
  });

  it("accepts a zero count for ZRANDMEMBER, as the server does", async () => {
    const store = createSortedSetStore(client, live);

    await expect(store.zrandmember("ranked", { count: 0 })).resolves.toEqual(
      []
    );
  });
});

// Type-level regressions. These assert at compile time; tsc is the runner.
// A `withScores` that is only known to be `boolean` makes the reply shape
// unknowable, so it has to be a compile error rather than a silent match on
// the members-only overload. Excess-property checking does not help: the flag
// is a declared member of every one of these option types.
type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typeClient = null as unknown as RedisClient;
const typedBoard = createSortedSetStore(typeClient, board);
const flag = null as unknown as boolean;

function ambiguousWithScoresIsARejection() {
  // @ts-expect-error a boolean withScores cannot pick a reply shape.
  void typedBoard.zrange("game", { start: 0, stop: -1, withScores: flag });

  void typedBoard.zrange("game", {
    byScore: true,
    min: 0,
    max: 1,
    // @ts-expect-error the byScore variant declares withScores too.
    withScores: flag
  });

  const rangeOptions: SortedSetRangeOptions<string> = {
    start: 0,
    stop: -1,
    withScores: true
  };
  // @ts-expect-error the exported union leaves withScores optional.
  void typedBoard.zrange("game", rangeOptions);

  // @ts-expect-error zdiff's members overload used to declare withScores?: boolean.
  void typedBoard.zdiff("a", ["b"], { withScores: flag });

  // An options object that shares a property with SortedSetCombineOptions
  // passes the weak-type check, so only the flag's type can catch it.
  const combineOptions = { aggregate: "sum" as const, withScores: flag };
  // @ts-expect-error zunion cannot promise members for an unknown flag.
  void typedBoard.zunion("a", ["b"], combineOptions);
  // @ts-expect-error zinter cannot either.
  void typedBoard.zinter("a", ["b"], combineOptions);

  // @ts-expect-error a counted draw's reply shape depends on the flag as well.
  void typedBoard.zrandmember("game", { count: 2, withScores: flag });

  const randomOptions: SortedSetRandomMemberOptions = { count: 3 };
  // @ts-expect-error count is optional on the alias, so the reply shape is unknowable.
  void typedBoard.zrandmember("game", randomOptions);
}

void ambiguousWithScoresIsARejection;

// The honest calls still resolve to the overload they always did.
const rangeMembersProbe = () =>
  typedBoard.zrange("game", { start: 0, stop: -1 });
const rangeFalseProbe = () =>
  typedBoard.zrange("game", { start: 0, stop: -1, withScores: false });
const rangeEntriesProbe = () =>
  typedBoard.zrange("game", { start: 0, stop: -1, withScores: true });
const diffMembersProbe = () => typedBoard.zdiff("a", ["b"]);
const unionEntriesProbe = () =>
  typedBoard.zunion("a", ["b"], { aggregate: "sum", withScores: true });
const randomOneProbe = () => typedBoard.zrandmember("game");
const randomManyProbe = () => typedBoard.zrandmember("game", { count: 2 });
const randomEntriesProbe = () =>
  typedBoard.zrandmember("game", { count: 2, withScores: true });

type _RangeMembers = Expect<
  Equal<Awaited<ReturnType<typeof rangeMembersProbe>>, string[]>
>;
type _RangeFalse = Expect<
  Equal<Awaited<ReturnType<typeof rangeFalseProbe>>, string[]>
>;
type _RangeEntries = Expect<
  Equal<
    Awaited<ReturnType<typeof rangeEntriesProbe>>,
    Array<SortedSetEntry<string>>
  >
>;
type _DiffMembers = Expect<
  Equal<Awaited<ReturnType<typeof diffMembersProbe>>, string[]>
>;
type _UnionEntries = Expect<
  Equal<
    Awaited<ReturnType<typeof unionEntriesProbe>>,
    Array<SortedSetEntry<string>>
  >
>;
type _RandomOne = Expect<
  Equal<Awaited<ReturnType<typeof randomOneProbe>>, string | null>
>;
type _RandomMany = Expect<
  Equal<Awaited<ReturnType<typeof randomManyProbe>>, string[]>
>;
type _RandomEntries = Expect<
  Equal<
    Awaited<ReturnType<typeof randomEntriesProbe>>,
    Array<SortedSetEntry<string>>
  >
>;

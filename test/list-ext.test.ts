import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { createListStore } from "../src/core/list.js";
import { defineList } from "../src/core/schemas.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

describe("createListStore push-if-exists", () => {
  it("emits LPUSHX and RPUSHX with encoded values", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [2, 3]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(store.lpushx("a", [{ task: "one" }])).resolves.toBe(2);
    await expect(
      store.rpushx("a", [{ task: "two" }, { task: "three" }])
    ).resolves.toBe(3);

    expect(commands).toEqual([
      ["LPUSHX", "jobs:a", '{"task":"one"}'],
      ["RPUSHX", "jobs:a", '{"task":"two"}', '{"task":"three"}']
    ]);
  });

  it("returns 0 for missing keys", async () => {
    const store = createListStore(
      fakeClient([], [0, 0]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpushx("missing", ["x"])).resolves.toBe(0);
    await expect(store.rpushx("missing", ["x"])).resolves.toBe(0);
  });

  it("short-circuits empty values to 0 without a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, []),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpushx("a", [])).resolves.toBe(0);
    await expect(store.rpushx("a", [])).resolves.toBe(0);
    expect(commands).toEqual([]);
  });
});

describe("createListStore insert", () => {
  it("emits LINSERT BEFORE and AFTER with encoded pivot and value", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [3, 4]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(
      store.linsert(
        "a",
        { task: "pivot" },
        { task: "new" },
        {
          position: "before"
        }
      )
    ).resolves.toBe(3);
    await expect(
      store.linsert(
        "a",
        { task: "pivot" },
        { task: "new" },
        {
          position: "after"
        }
      )
    ).resolves.toBe(4);

    expect(commands).toEqual([
      ["LINSERT", "jobs:a", "BEFORE", '{"task":"pivot"}', '{"task":"new"}'],
      ["LINSERT", "jobs:a", "AFTER", '{"task":"pivot"}', '{"task":"new"}']
    ]);
  });

  it("passes through -1 for missing pivots and 0 for missing keys", async () => {
    const store = createListStore(
      fakeClient([], [-1, 0]),
      defineList("jobs", codecs.string())
    );

    await expect(
      store.linsert("a", "missing", "x", { position: "before" })
    ).resolves.toBe(-1);
    await expect(
      store.linsert("missing", "pivot", "x", { position: "after" })
    ).resolves.toBe(0);
  });
});

describe("createListStore position", () => {
  it("emits LPOS with the encoded value", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [2]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(store.lpos("a", { task: "x" })).resolves.toBe(2);

    expect(commands).toEqual([["LPOS", "jobs:a", '{"task":"x"}']]);
  });

  it("appends RANK when provided", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [5]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpos("a", "x", { rank: -1 })).resolves.toBe(5);

    expect(commands).toEqual([["LPOS", "jobs:a", "x", "RANK", -1]]);
  });

  it("returns null when the value is not found", async () => {
    const store = createListStore(
      fakeClient([], [null]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpos("a", "missing")).resolves.toBeNull();
  });

  it("rejects invalid ranks without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, []),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpos("a", "x", { rank: 0 })).rejects.toThrow(
      "rank must be a nonzero safe integer"
    );
    await expect(store.lpos("a", "x", { rank: 1.5 })).rejects.toThrow(
      "rank must be a nonzero safe integer"
    );
    expect(commands).toEqual([]);
  });
});

describe("createListStore positions", () => {
  it("emits LPOS with COUNT and returns all indexes", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [[0, 3, 7]]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(store.lpos("a", { task: "x" }, { count: 0 })).resolves.toEqual(
      [0, 3, 7]
    );

    expect(commands).toEqual([["LPOS", "jobs:a", '{"task":"x"}', "COUNT", 0]]);
  });

  it("appends RANK when provided", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [[7, 3]]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpos("a", "x", { count: 2, rank: -1 })).resolves.toEqual(
      [7, 3]
    );

    expect(commands).toEqual([["LPOS", "jobs:a", "x", "COUNT", 2, "RANK", -1]]);
  });

  it("returns an empty array when nothing matches", async () => {
    const store = createListStore(
      fakeClient([], [[]]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpos("a", "missing", { count: 0 })).resolves.toEqual([]);
  });

  it("rejects invalid counts and ranks without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, []),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpos("a", "x", { count: -1 })).rejects.toThrow(
      "count must be a non-negative safe integer"
    );
    await expect(store.lpos("a", "x", { count: 1.5 })).rejects.toThrow(
      "count must be a non-negative safe integer"
    );
    await expect(store.lpos("a", "x", { count: 1, rank: 0 })).rejects.toThrow(
      "rank must be a nonzero safe integer"
    );
    expect(commands).toEqual([]);
  });

  it("rejects non-array replies and non-number items", async () => {
    const store = createListStore(
      fakeClient([], [null, ["2"]]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpos("a", "x", { count: 0 })).rejects.toThrow(
      "Expected Redis LPOS to return array"
    );
    await expect(store.lpos("a", "x", { count: 0 })).rejects.toThrow(
      "Expected Redis LPOS item to return number"
    );
  });
});

describe("createListStore pop-many", () => {
  it("emits LPOP and RPOP with a count and decodes replies", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [
        ['{"task":"one"}', '{"task":"two"}'],
        ['{"task":"three"}']
      ]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(store.lpop("a", { count: 2 })).resolves.toEqual([
      { task: "one" },
      { task: "two" }
    ]);
    await expect(store.rpop("a", { count: 1 })).resolves.toEqual([
      { task: "three" }
    ]);

    expect(commands).toEqual([
      ["LPOP", "jobs:a", 2],
      ["RPOP", "jobs:a", 1]
    ]);
  });

  it("returns an empty array for missing keys", async () => {
    const store = createListStore(
      fakeClient([], [null, null]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lpop("missing", { count: 2 })).resolves.toEqual([]);
    await expect(store.rpop("missing", { count: 2 })).resolves.toEqual([]);
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const client = null as unknown as RedisClient;

const jobs = createListStore(
  client,
  defineList("jobs", codecs.json<{ task: string }>())
);

type PushIfExistsValue = Parameters<typeof jobs.lpushx>[1][number];
type PushIfExistsResult = Awaited<ReturnType<typeof jobs.rpushx>>;
type InsertPivot = Parameters<typeof jobs.linsert>[1];
type InsertResult = Awaited<ReturnType<typeof jobs.linsert>>;
// lpos is overloaded; ReturnType/Parameters resolve to the last (count)
// overload — scalar-vs-array is exercised through direct calls below.
type PositionResult = Awaited<ReturnType<typeof jobs.lpos>>;
type PositionOptions = Parameters<typeof jobs.lpos>[2];
type PopManyResult = Awaited<ReturnType<typeof jobs.lpop>>;

type _PushIfExistsValue = Expect<Equal<PushIfExistsValue, { task: string }>>;
type _PushIfExistsResult = Expect<Equal<PushIfExistsResult, number>>;
type _InsertPivot = Expect<Equal<InsertPivot, { task: string }>>;
type _InsertResult = Expect<Equal<InsertResult, number>>;
type _PositionResult = Expect<Equal<PositionResult, number[]>>;
type _PositionOptions = Expect<
  Equal<PositionOptions, { count: number; rank?: number }>
>;
type _PopManyResult = Expect<Equal<PopManyResult, Array<{ task: string }>>>;

const knownJobs = createListStore(
  client,
  defineList("known", codecs.string(), { ids: ["one", "two"] })
);
type KnownJobId = Parameters<typeof knownJobs.lpop>[0];
type _KnownJobId = Expect<Equal<KnownJobId, "one" | "two">>;

function expectTypeErrorsOnly() {
  // @ts-expect-error pushed values must match the list codec type.
  void jobs.lpushx("a", [{ task: 1 }]);

  // @ts-expect-error pushed values must match the list codec type.
  void jobs.rpushx("a", ["x"]);

  // @ts-expect-error insert pivot must match the list codec type.
  void jobs.linsert("a", "pivot", { task: "x" }, { position: "before" });

  // @ts-expect-error inserted value must match the list codec type.
  void jobs.linsert("a", { task: "pivot" }, 1, { position: "after" });

  // @ts-expect-error searched value must match the list codec type.
  void jobs.lpos("a", "x");

  // @ts-expect-error rank must be a number.
  void jobs.lpos("a", { task: "x" }, { rank: "1" });

  // @ts-expect-error match count must be a number.
  void jobs.lpos("a", { task: "x" }, { count: "2" });

  // @ts-expect-error pop count must be a number.
  void jobs.lpop("a", { count: "2" });

  // @ts-expect-error known list keyspaces only accept declared ids.
  void knownJobs.rpop("three", { count: 1 });
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

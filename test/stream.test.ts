import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import {
  createStreamStore,
  defineStream,
  type StreamEntry
} from "../src/core/stream.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

const events = defineStream("events", {
  type: codecs.string(),
  amount: codecs.number()
});

describe("defineStream", () => {
  it("formats keys with string, number, and bigint ids", () => {
    expect(events.prefix).toBe("events");
    expect(events.key("42")).toBe("events:42");
    expect(events.key(42)).toBe("events:42");
    expect(events.key(42n)).toBe("events:42");
  });

  it("keeps the declared field codecs on the schema", () => {
    expect(Object.keys(events.fields)).toEqual(["type", "amount"]);
  });
});

describe("createStreamStore", () => {
  it("emits XADD with the declared fields encoded in order", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, ["1-1"]), events);

    await expect(store.xadd("42", { type: "credit", amount: 5 })).resolves.toBe(
      "1-1"
    );

    expect(commands).toEqual([
      ["XADD", "events:42", "*", "type", "credit", "amount", "5"]
    ]);
  });

  it("emits XADD with an explicit entry id", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, ["5-1"]), events);

    await expect(
      store.xadd("42", { type: "credit", amount: 5 }, { entryId: "5-1" })
    ).resolves.toBe("5-1");

    expect(commands).toEqual([
      ["XADD", "events:42", "5-1", "type", "credit", "amount", "5"]
    ]);
  });

  it("emits XADD with NOMKSTREAM and MAXLEN options", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(
      fakeClient(commands, ["1-1", "1-2", "1-3"]),
      events
    );

    await store.xadd("42", { type: "credit", amount: 5 }, { nomkstream: true });
    await store.xadd(
      "42",
      { type: "credit", amount: 5 },
      { maxLen: { count: 100 } }
    );
    await store.xadd(
      "42",
      { type: "credit", amount: 5 },
      {
        nomkstream: true,
        maxLen: { count: 100, approximate: true },
        entryId: "9-9"
      }
    );

    expect(commands).toEqual([
      ["XADD", "events:42", "NOMKSTREAM", "*", "type", "credit", "amount", "5"],
      [
        "XADD",
        "events:42",
        "MAXLEN",
        100,
        "*",
        "type",
        "credit",
        "amount",
        "5"
      ],
      [
        "XADD",
        "events:42",
        "NOMKSTREAM",
        "MAXLEN",
        "~",
        100,
        "9-9",
        "type",
        "credit",
        "amount",
        "5"
      ]
    ]);
  });

  it("maps a null XADD reply to null only when noCreate is set", async () => {
    const store = createStreamStore(fakeClient([], [null, null]), events);

    await expect(
      store.xadd("42", { type: "credit", amount: 5 }, { nomkstream: true })
    ).resolves.toBeNull();
    await expect(
      store.xadd("42", { type: "credit", amount: 5 })
    ).rejects.toThrow("Expected Redis XADD to return string");
  });

  it("rejects invalid maxLength counts before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, []), events);
    const value = { type: "credit", amount: 5 };

    await expect(
      store.xadd("42", value, { maxLen: { count: 0 } })
    ).rejects.toThrow("maxLen.count must be a positive safe integer");
    await expect(
      store.xadd("42", value, { maxLen: { count: 1.5 } })
    ).rejects.toThrow(TypeError);
    await expect(
      store.xadd("42", value, { maxLen: { count: Number.NaN } })
    ).rejects.toThrow(TypeError);

    expect(commands).toEqual([]);
  });

  it("emits XLEN and decodes the length", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, [3]), events);

    await expect(store.xlen("42")).resolves.toBe(3);

    expect(commands).toEqual([["XLEN", "events:42"]]);
  });

  it("emits XRANGE with default bounds and decodes entries", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(
      fakeClient(commands, [
        [
          ["1-1", ["type", "credit", "amount", "5"]],
          ["1-2", ["type", "debit"]]
        ]
      ]),
      events
    );

    await expect(store.xrange("42")).resolves.toEqual([
      { id: "1-1", value: { type: "credit", amount: 5 } },
      { id: "1-2", value: { type: "debit" } }
    ]);

    expect(commands).toEqual([["XRANGE", "events:42", "-", "+"]]);
  });

  it("emits XRANGE with explicit bounds and COUNT", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, [[]]), events);

    await expect(
      store.xrange("42", { start: "1-1", end: "9-9", count: 10 })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XRANGE", "events:42", "1-1", "9-9", "COUNT", 10]
    ]);
  });

  it("skips undeclared entry fields silently", async () => {
    const store = createStreamStore(
      fakeClient([], [[["1-1", ["extra", "x", "type", "credit"]]]]),
      events
    );

    await expect(store.xrange("42")).resolves.toEqual([
      { id: "1-1", value: { type: "credit" } }
    ]);
  });

  it("emits XREVRANGE with reversed default bounds", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(
      fakeClient(commands, [[["1-2", ["amount", "7"]]], []]),
      events
    );

    await expect(store.xrevrange("42")).resolves.toEqual([
      { id: "1-2", value: { amount: 7 } }
    ]);
    await expect(
      store.xrevrange("42", { start: "9-9", end: "1-1", count: 2 })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XREVRANGE", "events:42", "+", "-"],
      ["XREVRANGE", "events:42", "9-9", "1-1", "COUNT", 2]
    ]);
  });

  it("rejects invalid range counts before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, []), events);

    await expect(store.xrange("42", { count: 0 })).rejects.toThrow(
      "count must be a positive safe integer"
    );
    await expect(store.xrevrange("42", { count: -1 })).rejects.toThrow(
      TypeError
    );

    expect(commands).toEqual([]);
  });

  it("emits XDEL and short-circuits empty entry id lists", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, [2]), events);

    await expect(store.xdel("42", [])).resolves.toBe(0);
    await expect(store.xdel("42", ["1-1", "1-2"])).resolves.toBe(2);

    expect(commands).toEqual([["XDEL", "events:42", "1-1", "1-2"]]);
  });

  it("emits XTRIM MAXLEN with optional approximate trimming", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, [5, 3]), events);

    await expect(store.xtrim("42", { maxLen: { count: 100 } })).resolves.toBe(
      5
    );
    await expect(
      store.xtrim("42", { maxLen: { count: 100, approximate: true } })
    ).resolves.toBe(3);

    expect(commands).toEqual([
      ["XTRIM", "events:42", "MAXLEN", 100],
      ["XTRIM", "events:42", "MAXLEN", "~", 100]
    ]);
  });

  it("emits XTRIM MINID with optional approximate trimming", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, [5, 3]), events);

    await expect(store.xtrim("42", { minId: { value: "1-1" } })).resolves.toBe(
      5
    );
    await expect(
      store.xtrim("42", { minId: { value: "1-1", approximate: true } })
    ).resolves.toBe(3);

    expect(commands).toEqual([
      ["XTRIM", "events:42", "MINID", "1-1"],
      ["XTRIM", "events:42", "MINID", "~", "1-1"]
    ]);
  });

  it("rejects invalid trim counts before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, []), events);

    await expect(store.xtrim("42", { maxLen: { count: 0 } })).rejects.toThrow(
      "count must be a positive safe integer"
    );
    await expect(store.xtrim("42", { maxLen: { count: 1.5 } })).rejects.toThrow(
      TypeError
    );

    expect(commands).toEqual([]);
  });

  it("emits XREAD and unwraps the single stream reply", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(
      fakeClient(commands, [
        [["events:42", [["1-1", ["type", "credit", "amount", "5"]]]]],
        [["events:42", []]]
      ]),
      events
    );

    await expect(store.xread("42", "0")).resolves.toEqual([
      { id: "1-1", value: { type: "credit", amount: 5 } }
    ]);
    await expect(store.xread("42", "1-1", { count: 5 })).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XREAD", "STREAMS", "events:42", "0"],
      ["XREAD", "COUNT", 5, "STREAMS", "events:42", "1-1"]
    ]);
  });

  it("maps a null XREAD reply to an empty array", async () => {
    const store = createStreamStore(fakeClient([], [null]), events);

    await expect(store.xread("42", "$")).resolves.toEqual([]);
  });

  it("unwraps map-shaped XREAD replies", async () => {
    const store = createStreamStore(
      fakeClient(
        [],
        [
          new Map<RedisReply, RedisReply>([
            ["events:42", [["1-1", ["amount", "7"]]]]
          ])
        ]
      ),
      events
    );

    await expect(store.xread("42", "0")).resolves.toEqual([
      { id: "1-1", value: { amount: 7 } }
    ]);
  });

  it("rejects invalid readAfter counts before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, []), events);

    await expect(store.xread("42", "0", { count: 0 })).rejects.toThrow(
      "count must be a positive safe integer"
    );

    expect(commands).toEqual([]);
  });

  it("emits DEL", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, [1]), events);

    await expect(store.del("42")).resolves.toBe(1);

    expect(commands).toEqual([["DEL", "events:42"]]);
  });

  it("throws on unexpected XRANGE reply shapes", async () => {
    const withReply = (reply: RedisReply) =>
      createStreamStore(fakeClient([], [reply]), events);

    await expect(withReply("nope").xrange("42")).rejects.toThrow(
      "Expected Redis XRANGE to return array"
    );
    await expect(withReply(["1-1"]).xrange("42")).rejects.toThrow(
      "Expected Redis XRANGE to return id/fields pairs"
    );
    await expect(withReply([["1-1"]]).xrange("42")).rejects.toThrow(
      "Expected Redis XRANGE to return id/fields pairs"
    );
    await expect(withReply([[1, []]]).xrange("42")).rejects.toThrow(
      "Expected Redis XRANGE to return id/fields pairs"
    );
    await expect(withReply([["1-1", "fields"]]).xrange("42")).rejects.toThrow(
      "Expected Redis XRANGE to return field/value pairs"
    );
    await expect(withReply([["1-1", ["type"]]]).xrange("42")).rejects.toThrow(
      "Expected Redis XRANGE to return field/value pairs"
    );
    await expect(
      withReply([["1-1", ["type", 5]]]).xrange("42")
    ).rejects.toThrow("Expected Redis XRANGE to return field/value pairs");
    await expect(withReply("nope").xrevrange("42")).rejects.toThrow(
      "Expected Redis XREVRANGE to return array"
    );
  });

  it("throws on unexpected XREAD reply shapes", async () => {
    const withReply = (reply: RedisReply) =>
      createStreamStore(fakeClient([], [reply]), events);

    await expect(withReply("nope").xread("42", "0")).rejects.toThrow(
      "Expected Redis XREAD to return array or null"
    );
    await expect(withReply([]).xread("42", "0")).rejects.toThrow(
      "Expected Redis XREAD to return one stream"
    );
    await expect(
      withReply([
        ["events:42", []],
        ["events:43", []]
      ]).xread("42", "0")
    ).rejects.toThrow("Expected Redis XREAD to return one stream");
    await expect(withReply(["events:42"]).xread("42", "0")).rejects.toThrow(
      "Expected Redis XREAD to return key/entries pairs"
    );
    await expect(withReply([["events:42"]]).xread("42", "0")).rejects.toThrow(
      "Expected Redis XREAD to return key/entries pairs"
    );
    await expect(withReply([[1, []]]).xread("42", "0")).rejects.toThrow(
      "Expected Redis XREAD to return key/entries pairs"
    );
    await expect(
      withReply([["events:42", "entries"]]).xread("42", "0")
    ).rejects.toThrow("Expected Redis XREAD to return array");
    await expect(
      withReply([["events:42", [["1-1", ["type"]]]]]).xread("42", "0")
    ).rejects.toThrow("Expected Redis XREAD to return field/value pairs");
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typeClient = null as unknown as RedisClient;

const typedEvents = defineStream("events", {
  type: codecs.string(),
  amount: codecs.number()
});
const typedStore = createStreamStore(typeClient, typedEvents);

type EventKey = ReturnType<typeof typedEvents.key<"42">>;
type EventPrefix = typeof typedEvents.prefix;
type AddValue = Parameters<typeof typedStore.xadd>[1];
type AddResult = Awaited<ReturnType<typeof typedStore.xadd>>;
type LengthResult = Awaited<ReturnType<typeof typedStore.xlen>>;
type RangeResult = Awaited<ReturnType<typeof typedStore.xrange>>;
type ReadAfterResult = Awaited<ReturnType<typeof typedStore.xread>>;
type EntryValue = RangeResult[number]["value"];
type RemoveResult = Awaited<ReturnType<typeof typedStore.xdel>>;
type TrimResult = Awaited<ReturnType<typeof typedStore.xtrim>>;

type _EventKey = Expect<Equal<EventKey, "events:42">>;
type _EventPrefix = Expect<Equal<EventPrefix, "events">>;
type _AddValue = Expect<Equal<AddValue, { type: string; amount: number }>>;
type _AddResult = Expect<Equal<AddResult, string>>;
type _LengthResult = Expect<Equal<LengthResult, number>>;
type _RangeResult = Expect<
  Equal<RangeResult, Array<StreamEntry<typeof typedEvents.fields>>>
>;
type _ReadAfterResult = Expect<Equal<ReadAfterResult, RangeResult>>;
type _EntryValue = Expect<
  Equal<EntryValue, { type?: string; amount?: number }>
>;
type _RemoveResult = Expect<Equal<RemoveResult, number>>;
type _TrimResult = Expect<Equal<TrimResult, number>>;

const knownEvents = defineStream(
  "known",
  { type: codecs.string() },
  { ids: ["one", "two"] }
);
const knownStore = createStreamStore(typeClient, knownEvents);
type KnownEventKey = ReturnType<typeof knownEvents.key<"one">>;
type KnownEventId = Parameters<typeof knownStore.xlen>[0];
type _KnownEventKey = Expect<Equal<KnownEventKey, "known:one">>;
type _KnownEventId = Expect<Equal<KnownEventId, "one" | "two">>;

function expectTypeErrorsOnly() {
  // @ts-expect-error add requires every declared field.
  void typedStore.xadd("42", { type: "credit" });

  // @ts-expect-error field values must match the declared field codec.
  void typedStore.xadd("42", { type: "credit", amount: "5" });

  // @ts-expect-error undeclared fields are rejected at compile time.
  void typedStore.xadd("42", { type: "credit", amount: 5, extra: true });

  // @ts-expect-error entry ids must be strings.
  void typedStore.xadd("42", { type: "credit", amount: 5 }, { entryId: 5 });

  const badMaxLength = { maxLen: { count: "10" } };
  // @ts-expect-error maxLength counts must be numbers.
  void typedStore.xadd("42", { type: "credit", amount: 5 }, badMaxLength);

  // @ts-expect-error range counts must be numbers.
  void typedStore.xrange("42", { count: "5" });

  // @ts-expect-error removed entry ids must be strings.
  void typedStore.xdel("42", [1]);

  // @ts-expect-error minimum entry ids must be strings.
  void typedStore.xtrim("42", { minId: { value: 5 } });

  // @ts-expect-error readAfter entry ids must be strings.
  void typedStore.xread("42", 0);

  // @ts-expect-error known stream keyspaces only accept declared ids.
  void knownStore.xlen("three");
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

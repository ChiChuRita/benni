import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { ValidationError } from "../src/core/errors.js";
import {
  createBlockingListOps,
  createListStore,
  defineList
} from "../src/core/list.js";
import {
  createBlockingSortedSetOps,
  createSortedSetStore,
  defineSortedSet
} from "../src/core/sorted-set.js";
import {
  createBlockingStreamOps,
  decodeStreamEntries,
  decodeStreamEntry,
  xreadStreamPairs
} from "../src/core/stream.js";
import { defineStream } from "../src/core/stream-resource.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

const invalidTimeoutMessage =
  'Blocking timeoutSeconds must be a positive finite number of seconds or "forever"';

const events = defineStream("events", {
  type: codecs.string(),
  amount: codecs.number()
});

describe("createBlockingListOps single-key pops", () => {
  it("emits BLPOP and BRPOP with the timeout as a string and drops the key", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, [
        ["jobs:a", '{"task":"one"}'],
        ["jobs:a", '{"task":"two"}']
      ]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(ops.blpop("a", { timeoutSeconds: 5 })).resolves.toEqual({
      task: "one"
    });
    await expect(ops.brpop("a", { timeoutSeconds: 5 })).resolves.toEqual({
      task: "two"
    });

    expect(commands).toEqual([
      ["BLPOP", "jobs:a", "5"],
      ["BRPOP", "jobs:a", "5"]
    ]);
  });

  it("returns null on a server-side timeout", async () => {
    const ops = createBlockingListOps(
      fakeClient([], [null]),
      defineList("jobs", codecs.string())
    );

    await expect(ops.blpop("a", { timeoutSeconds: 1 })).resolves.toBeNull();
  });

  it("rejects zero, negative, NaN, and Infinity timeouts without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, []),
      defineList("jobs", codecs.string())
    );

    for (const timeoutSeconds of [
      0,
      -1,
      Number.NaN,
      Number.POSITIVE_INFINITY
    ]) {
      await expect(ops.blpop("a", { timeoutSeconds })).rejects.toThrow(
        new ValidationError(invalidTimeoutMessage)
      );
      await expect(ops.brpop("a", { timeoutSeconds })).rejects.toThrow(
        new ValidationError(invalidTimeoutMessage)
      );
    }
    expect(commands).toEqual([]);
  });

  it("rejects malformed replies", async () => {
    const ops = createBlockingListOps(
      fakeClient([], ["x", ["jobs:a"], ["jobs:a", 1]]),
      defineList("jobs", codecs.string())
    );

    await expect(ops.blpop("a", { timeoutSeconds: 1 })).rejects.toThrow(
      "Expected Redis BLPOP to return key/value pair or null"
    );
    await expect(ops.blpop("a", { timeoutSeconds: 1 })).rejects.toThrow(
      "Expected Redis BLPOP to return key/value pair or null"
    );
    await expect(ops.brpop("a", { timeoutSeconds: 1 })).rejects.toThrow(
      "Expected Redis BRPOP to return key/value pair or null"
    );
  });
});

describe("createBlockingListOps multi-key pops", () => {
  it("emits BLPOP with every key before the timeout and attributes the answer", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, [["jobs:b", '{"task":"two"}']]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(ops.blpop(["a", "b"], { timeoutSeconds: 5 })).resolves.toEqual(
      {
        id: "b",
        value: { task: "two" }
      }
    );

    expect(commands).toEqual([["BLPOP", "jobs:a", "jobs:b", "5"]]);
  });

  it("emits BRPOP for the right-side variant", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, [["jobs:a", "x"]]),
      defineList("jobs", codecs.string())
    );

    await expect(
      ops.brpop(["a", "b"], { timeoutSeconds: 0.25 })
    ).resolves.toEqual({
      id: "a",
      value: "x"
    });

    expect(commands).toEqual([["BRPOP", "jobs:a", "jobs:b", "0.25"]]);
  });

  it("preserves numeric ids in the attribution", async () => {
    const ops = createBlockingListOps(
      fakeClient([], [["jobs:2", "x"]]),
      defineList("jobs", codecs.string())
    );

    const hit = await ops.blpop([1, 2], { timeoutSeconds: 1 });
    expect(hit).not.toBeNull();
    expect(hit?.id).toBe(2);
  });

  it("attributes by exact key equality, not prefix parsing", async () => {
    const ops = createBlockingListOps(
      fakeClient(
        [],
        [
          ["jobs:1:x", "a"],
          ["jobs:1", "b"]
        ]
      ),
      defineList("jobs", codecs.string())
    );

    await expect(
      ops.blpop(["1", "1:x"], { timeoutSeconds: 1 })
    ).resolves.toEqual({
      id: "1:x",
      value: "a"
    });
    await expect(
      ops.blpop(["1", "1:x"], { timeoutSeconds: 1 })
    ).resolves.toEqual({
      id: "1",
      value: "b"
    });
  });

  it("rejects keys that were not requested", async () => {
    const ops = createBlockingListOps(
      fakeClient([], [["jobs:1:x", "a"]]),
      defineList("jobs", codecs.string())
    );

    await expect(ops.blpop(["1"], { timeoutSeconds: 1 })).rejects.toThrow(
      "Expected Redis BLPOP to return one of the requested keys"
    );
  });

  it("returns null on a server-side timeout", async () => {
    const ops = createBlockingListOps(
      fakeClient([], [null]),
      defineList("jobs", codecs.string())
    );

    await expect(
      ops.blpop(["a", "b"], { timeoutSeconds: 1 })
    ).resolves.toBeNull();
  });

  it("rejects empty ids without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, []),
      defineList("jobs", codecs.string())
    );

    await expect(ops.blpop([], { timeoutSeconds: 1 })).rejects.toThrow(
      "ids must contain at least one id"
    );
    await expect(ops.brpop([], { timeoutSeconds: 1 })).rejects.toThrow(
      "ids must contain at least one id"
    );
    expect(commands).toEqual([]);
  });
});

describe("createBlockingListOps blmove", () => {
  it("emits BLMOVE with sides and the timeout string", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, ['{"task":"one"}']),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(
      ops.blmove("pending", "working", "left", "right", {
        timeoutSeconds: 0.25
      })
    ).resolves.toEqual({ task: "one" });

    expect(commands).toEqual([
      ["BLMOVE", "jobs:pending", "jobs:working", "LEFT", "RIGHT", "0.25"]
    ]);
  });

  it("returns null on a server-side timeout", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, [null]),
      defineList("jobs", codecs.string())
    );

    await expect(
      ops.blmove("a", "b", "right", "left", { timeoutSeconds: 1 })
    ).resolves.toBeNull();

    expect(commands).toEqual([
      ["BLMOVE", "jobs:a", "jobs:b", "RIGHT", "LEFT", "1"]
    ]);
  });
});

describe("createBlockingListOps BLMPOP", () => {
  it("emits BLMPOP with timeout, numkeys, keys, side and attributes the answer", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, [["jobs:b", ['{"task":"two"}']]]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(
      ops.blmpop(["a", "b"], { direction: "left", timeoutSeconds: 5 })
    ).resolves.toEqual({ id: "b", values: [{ task: "two" }] });

    expect(commands).toEqual([["BLMPOP", "5", 2, "jobs:a", "jobs:b", "LEFT"]]);
  });

  it("appends COUNT and supports the right side with a forever timeout", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, [["jobs:a", ["x", "y"]]]),
      defineList("jobs", codecs.string())
    );

    await expect(
      ops.blmpop(["a"], {
        direction: "right",
        timeoutSeconds: "forever",
        count: 2
      })
    ).resolves.toEqual({ id: "a", values: ["x", "y"] });

    expect(commands).toEqual([
      ["BLMPOP", "0", 1, "jobs:a", "RIGHT", "COUNT", 2]
    ]);
  });

  it("returns null on timeout and rejects empty ids / bad counts", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingListOps(
      fakeClient(commands, [null]),
      defineList("jobs", codecs.string())
    );

    await expect(
      ops.blmpop(["a", "b"], { direction: "left", timeoutSeconds: 1 })
    ).resolves.toBeNull();
    await expect(
      ops.blmpop([], { direction: "left", timeoutSeconds: 1 })
    ).rejects.toThrow("ids must contain at least one id");
    await expect(
      ops.blmpop(["a"], { direction: "left", timeoutSeconds: 1, count: 0 })
    ).rejects.toThrow("count must be a positive safe integer");
  });

  it("rejects malformed replies and unknown keys", async () => {
    const ops = createBlockingListOps(
      fakeClient([], [["jobs:a"], ["nope:a", ["x"]]]),
      defineList("jobs", codecs.string())
    );

    await expect(
      ops.blmpop(["a"], { direction: "left", timeoutSeconds: 1 })
    ).rejects.toThrow(
      "Expected Redis BLMPOP to return key/values pair or null"
    );
    await expect(
      ops.blmpop(["a"], { direction: "left", timeoutSeconds: 1 })
    ).rejects.toThrow(
      "Expected Redis BLMPOP to return one of the requested keys"
    );
  });
});

describe("createListStore lmpop", () => {
  it("emits LMPOP with numkeys, keys, and the direction", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [["jobs:b", ['{"task":"one"}', '{"task":"two"}']]]),
      defineList("jobs", codecs.json<{ task: string }>())
    );

    await expect(
      store.lmpop(["a", "b"], { direction: "left" })
    ).resolves.toEqual({
      id: "b",
      values: [{ task: "one" }, { task: "two" }]
    });

    expect(commands).toEqual([["LMPOP", 2, "jobs:a", "jobs:b", "LEFT"]]);
  });

  it("appends COUNT when provided", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, [["jobs:a", ["x"]]]),
      defineList("jobs", codecs.string())
    );

    await expect(
      store.lmpop(["a", "b", "c"], { direction: "right", count: 5 })
    ).resolves.toEqual({
      id: "a",
      values: ["x"]
    });

    expect(commands).toEqual([
      ["LMPOP", 3, "jobs:a", "jobs:b", "jobs:c", "RIGHT", "COUNT", 5]
    ]);
  });

  it("returns null when every list is empty", async () => {
    const store = createListStore(
      fakeClient([], [null]),
      defineList("jobs", codecs.string())
    );

    await expect(
      store.lmpop(["a", "b"], { direction: "left" })
    ).resolves.toBeNull();
  });

  it("rejects invalid counts and empty ids without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(
      fakeClient(commands, []),
      defineList("jobs", codecs.string())
    );

    await expect(
      store.lmpop(["a"], { direction: "left", count: 0 })
    ).rejects.toThrow("count must be a positive safe integer");
    await expect(
      store.lmpop(["a"], { direction: "left", count: 1.5 })
    ).rejects.toThrow("count must be a positive safe integer");
    await expect(store.lmpop([], { direction: "right" })).rejects.toThrow(
      "ids must contain at least one id"
    );
    expect(commands).toEqual([]);
  });

  it("rejects malformed replies and unknown keys", async () => {
    const store = createListStore(
      fakeClient([], ["x", ["jobs:zzz", ["a"]]]),
      defineList("jobs", codecs.string())
    );

    await expect(store.lmpop(["a"], { direction: "left" })).rejects.toThrow(
      "Expected Redis LMPOP to return key/values pair or null"
    );
    await expect(store.lmpop(["a"], { direction: "left" })).rejects.toThrow(
      "Expected Redis LMPOP to return one of the requested keys"
    );
  });
});

describe("createBlockingSortedSetOps", () => {
  it("emits BZPOPMIN/BZPOPMAX and decodes string scores", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingSortedSetOps(
      fakeClient(commands, [
        ["board:a", "alice", "3"],
        ["board:a", "zoe", "9.5"]
      ]),
      defineSortedSet("board", codecs.string())
    );

    await expect(ops.bzpopmin("a", { timeoutSeconds: 5 })).resolves.toEqual({
      member: "alice",
      score: 3
    });
    await expect(ops.bzpopmax("a", { timeoutSeconds: 0.1 })).resolves.toEqual({
      member: "zoe",
      score: 9.5
    });

    expect(commands).toEqual([
      ["BZPOPMIN", "board:a", "5"],
      ["BZPOPMAX", "board:a", "0.1"]
    ]);
  });

  it("decodes RESP3 numeric scores", async () => {
    const ops = createBlockingSortedSetOps(
      fakeClient([], [["board:a", "alice", 1.5]]),
      defineSortedSet("board", codecs.string())
    );

    await expect(ops.bzpopmin("a", { timeoutSeconds: 1 })).resolves.toEqual({
      member: "alice",
      score: 1.5
    });
  });

  it("attributes multi-key pops back to typed ids", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingSortedSetOps(
      fakeClient(commands, [["board:2", "bob", "2"]]),
      defineSortedSet("board", codecs.string())
    );

    const hit = await ops.bzpopmin([1, 2], { timeoutSeconds: "forever" });
    expect(hit).toEqual({ id: 2, entry: { member: "bob", score: 2 } });
    expect(hit.id).toBe(2);

    expect(commands).toEqual([["BZPOPMIN", "board:1", "board:2", "0"]]);
  });

  it("emits BZPOPMAX for the max variant and returns null on timeout", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingSortedSetOps(
      fakeClient(commands, [null]),
      defineSortedSet("board", codecs.string())
    );

    await expect(
      ops.bzpopmax(["a", "b"], { timeoutSeconds: 0.25 })
    ).resolves.toBeNull();

    expect(commands).toEqual([["BZPOPMAX", "board:a", "board:b", "0.25"]]);
  });

  it("rejects malformed replies and unknown keys", async () => {
    const ops = createBlockingSortedSetOps(
      fakeClient(
        [],
        [
          ["board:a", "alice"],
          ["other:a", "alice", "1"]
        ]
      ),
      defineSortedSet("board", codecs.string())
    );

    await expect(ops.bzpopmin("a", { timeoutSeconds: 1 })).rejects.toThrow(
      "Expected Redis BZPOPMIN to return key/member/score triple or null"
    );
    await expect(ops.bzpopmax(["a"], { timeoutSeconds: 1 })).rejects.toThrow(
      "Expected Redis BZPOPMAX to return one of the requested keys"
    );
  });
});

describe("createBlockingSortedSetOps BZMPOP", () => {
  it("emits BZMPOP with timeout, numkeys, keys, end and attributes the answer", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingSortedSetOps(
      fakeClient(commands, [
        [
          "board:b",
          [
            ["alice", "1"],
            ["bob", "2.5"]
          ]
        ]
      ]),
      defineSortedSet("board", codecs.string())
    );

    await expect(
      ops.bzmpop(["a", "b"], { min: true }, { timeoutSeconds: 5 })
    ).resolves.toEqual({
      id: "b",
      entries: [
        { member: "alice", score: 1 },
        { member: "bob", score: 2.5 }
      ]
    });

    expect(commands).toEqual([["BZMPOP", "5", 2, "board:a", "board:b", "MIN"]]);
  });

  it("appends COUNT and supports MAX with a forever timeout", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingSortedSetOps(
      fakeClient(commands, [["board:a", [["zoe", "9"]]]]),
      defineSortedSet("board", codecs.string())
    );

    await expect(
      ops.bzmpop(["a"], { max: true, count: 2 }, { timeoutSeconds: "forever" })
    ).resolves.toEqual({ id: "a", entries: [{ member: "zoe", score: 9 }] });

    expect(commands).toEqual([
      ["BZMPOP", "0", 1, "board:a", "MAX", "COUNT", 2]
    ]);
  });

  it("returns null on timeout and rejects empty ids / bad counts", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingSortedSetOps(
      fakeClient(commands, [null]),
      defineSortedSet("board", codecs.string())
    );

    await expect(
      ops.bzmpop(["a", "b"], { min: true }, { timeoutSeconds: 1 })
    ).resolves.toBeNull();
    await expect(
      ops.bzmpop([], { min: true }, { timeoutSeconds: 1 })
    ).rejects.toThrow("ids must contain at least one id");
    await expect(
      ops.bzmpop(["a"], { min: true, count: 0 }, { timeoutSeconds: 1 })
    ).rejects.toThrow("count must be a positive safe integer");
  });

  it("rejects malformed replies and unknown keys", async () => {
    const ops = createBlockingSortedSetOps(
      fakeClient([], [["board:a"], ["nope:a", [["m", "1"]]]]),
      defineSortedSet("board", codecs.string())
    );

    await expect(
      ops.bzmpop(["a"], { min: true }, { timeoutSeconds: 1 })
    ).rejects.toThrow(
      "Expected Redis BZMPOP to return key/entries pair or null"
    );
    await expect(
      ops.bzmpop(["a"], { min: true }, { timeoutSeconds: 1 })
    ).rejects.toThrow(
      "Expected Redis BZMPOP to return one of the requested keys"
    );
  });
});

describe("createSortedSetStore zmpop", () => {
  it("emits ZMPOP with numkeys, keys, and the end", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [
        [
          "board:b",
          [
            ["alice", "1"],
            ["bob", "2.5"]
          ]
        ]
      ]),
      defineSortedSet("board", codecs.string())
    );

    await expect(store.zmpop(["a", "b"], { min: true })).resolves.toEqual({
      id: "b",
      entries: [
        { member: "alice", score: 1 },
        { member: "bob", score: 2.5 }
      ]
    });

    expect(commands).toEqual([["ZMPOP", 2, "board:a", "board:b", "MIN"]]);
  });

  it("appends COUNT when provided and supports MAX", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [["board:a", [["zoe", "9"]]]]),
      defineSortedSet("board", codecs.string())
    );

    await expect(store.zmpop(["a"], { max: true, count: 2 })).resolves.toEqual({
      id: "a",
      entries: [{ member: "zoe", score: 9 }]
    });

    expect(commands).toEqual([["ZMPOP", 1, "board:a", "MAX", "COUNT", 2]]);
  });

  it("returns null when every sorted set is empty", async () => {
    const store = createSortedSetStore(
      fakeClient([], [null]),
      defineSortedSet("board", codecs.string())
    );

    await expect(store.zmpop(["a", "b"], { min: true })).resolves.toBeNull();
  });

  it("rejects invalid counts and empty ids without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, []),
      defineSortedSet("board", codecs.string())
    );

    await expect(store.zmpop(["a"], { min: true, count: 0 })).rejects.toThrow(
      "count must be a positive safe integer"
    );
    await expect(store.zmpop(["a"], { max: true, count: -1 })).rejects.toThrow(
      "count must be a positive safe integer"
    );
    await expect(store.zmpop([], { min: true })).rejects.toThrow(
      "ids must contain at least one id"
    );
    expect(commands).toEqual([]);
  });

  it("rejects malformed replies and unknown keys", async () => {
    const store = createSortedSetStore(
      fakeClient([], [["board:a"], ["nope:a", [["m", "1"]]]]),
      defineSortedSet("board", codecs.string())
    );

    await expect(store.zmpop(["a"], { min: true })).rejects.toThrow(
      "Expected Redis ZMPOP to return key/entries pair or null"
    );
    await expect(store.zmpop(["a"], { min: true })).rejects.toThrow(
      "Expected Redis ZMPOP to return one of the requested keys"
    );
  });
});

describe("createBlockingStreamOps xread", () => {
  it("emits XREAD with COUNT before BLOCK and converts seconds to milliseconds", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingStreamOps(
      fakeClient(commands, [
        [["events:42", [["1-1", ["type", "credit", "amount", "5"]]]]]
      ]),
      events
    );

    await expect(
      ops.xread("42", "$", { timeoutSeconds: 0.25, count: 10 })
    ).resolves.toEqual([{ id: "1-1", value: { type: "credit", amount: 5 } }]);

    expect(commands).toEqual([
      ["XREAD", "COUNT", 10, "BLOCK", "250", "STREAMS", "events:42", "$"]
    ]);
  });

  it("reads without BLOCK when no timeout is given", async () => {
    // The session accessor spreads these ops *over* the base store, so this
    // method shadows the non-blocking xread. Requiring a timeout made a
    // two-argument call throw `Cannot read properties of undefined`, which
    // left the plain read unreachable on a session.
    const commands: RedisCommand[] = [];
    const ops = createBlockingStreamOps(
      fakeClient(commands, [null, null]),
      events
    );

    await expect(ops.xread("42", "0")).resolves.toEqual([]);
    await expect(ops.xread("42", "0", { count: 3 })).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XREAD", "STREAMS", "events:42", "0"],
      ["XREAD", "COUNT", 3, "STREAMS", "events:42", "0"]
    ]);
  });

  it("omits COUNT when not provided and converts whole seconds", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingStreamOps(fakeClient(commands, [null]), events);

    await expect(
      ops.xread("42", "1-1", { timeoutSeconds: 5 })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XREAD", "BLOCK", "5000", "STREAMS", "events:42", "1-1"]
    ]);
  });

  it("spells block-forever as BLOCK 0 and never rounds a positive timeout to 0", async () => {
    const commands: RedisCommand[] = [];
    const ops = createBlockingStreamOps(
      fakeClient(commands, [null, null]),
      events
    );

    await expect(
      ops.xread("42", "$", { timeoutSeconds: "forever" })
    ).resolves.toEqual([]);
    await expect(
      ops.xread("42", "$", { timeoutSeconds: 0.0004 })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XREAD", "BLOCK", "0", "STREAMS", "events:42", "$"],
      ["XREAD", "BLOCK", "1", "STREAMS", "events:42", "$"]
    ]);
  });
});

describe("exported stream decoders", () => {
  it("decodeStreamEntry decodes declared fields and skips undeclared ones", () => {
    expect(
      decodeStreamEntry(
        ["1-1", ["type", "credit", "amount", "5", "extra", "x"]],
        "XRANGE",
        events.fields
      )
    ).toEqual({ id: "1-1", value: { type: "credit", amount: 5 } });
  });

  it("decodeStreamEntries decodes arrays of entries", () => {
    expect(
      decodeStreamEntries(
        [
          ["1-1", ["type", "credit"]],
          ["1-2", ["amount", "2"]]
        ],
        "XRANGE",
        events.fields
      )
    ).toEqual([
      { id: "1-1", value: { type: "credit" } },
      { id: "1-2", value: { amount: 2 } }
    ]);
  });

  it("xreadStreamPairs unwraps array and map replies", () => {
    expect(xreadStreamPairs([["k", ["e"]]])).toEqual([["k", ["e"]]]);
    expect(
      xreadStreamPairs(new Map<RedisReply, RedisReply>([["k", ["e"]]]))
    ).toEqual([["k", ["e"]]]);
    expect(() => xreadStreamPairs("nope")).toThrow(
      "Expected Redis XREAD to return array or null"
    );
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const client = null as unknown as RedisClient;

const jobsSchema = defineList("jobs", codecs.json<{ task: string }>());
const sharedJobs = createListStore(client, jobsSchema);
const blockingJobs = createBlockingListOps(client, jobsSchema);
const boardSchema = defineSortedSet("board", codecs.string());
const sharedBoard = createSortedSetStore(client, boardSchema);
const blockingBoard = createBlockingSortedSetOps(client, boardSchema);
const blockingEvents = createBlockingStreamOps(client, events);

async function typeProbes() {
  const foreverPop = await blockingJobs.blpop("a", {
    timeoutSeconds: "forever"
  });
  type _ForeverPop = Expect<Equal<typeof foreverPop, { task: string }>>;

  const timedPop = await blockingJobs.brpop("a", { timeoutSeconds: 5 });
  type _TimedPop = Expect<Equal<typeof timedPop, { task: string } | null>>;

  const foreverFrom = await blockingJobs.blpop(["urgent", "pending"], {
    timeoutSeconds: "forever"
  });
  type _ForeverFromId = Expect<
    Equal<typeof foreverFrom.id, "urgent" | "pending">
  >;
  type _ForeverFromValue = Expect<
    Equal<typeof foreverFrom.value, { task: string }>
  >;

  const timedFrom = await blockingJobs.brpop(["a"], {
    timeoutSeconds: 0.5
  });
  type _TimedFrom = Expect<
    Equal<typeof timedFrom, { id: "a"; value: { task: string } } | null>
  >;

  const foreverMove = await blockingJobs.blmove("a", "b", "left", "right", {
    timeoutSeconds: "forever"
  });
  type _ForeverMove = Expect<Equal<typeof foreverMove, { task: string }>>;

  const timedMove = await blockingJobs.blmove("a", "b", "left", "right", {
    timeoutSeconds: 1
  });
  type _TimedMove = Expect<Equal<typeof timedMove, { task: string } | null>>;

  const foreverMin = await blockingBoard.bzpopmin("a", {
    timeoutSeconds: "forever"
  });
  type _ForeverMin = Expect<
    Equal<
      typeof foreverMin,
      { readonly member: string; readonly score: number }
    >
  >;

  const timedMax = await blockingBoard.bzpopmax("a", { timeoutSeconds: 1 });
  type _TimedMax = Expect<
    Equal<
      typeof timedMax,
      { readonly member: string; readonly score: number } | null
    >
  >;

  const foreverMinFrom = await blockingBoard.bzpopmin([1, 2], {
    timeoutSeconds: "forever"
  });
  type _ForeverMinFromId = Expect<Equal<typeof foreverMinFrom.id, 1 | 2>>;

  const lmpop = await sharedJobs.lmpop(["a", "b"], { direction: "left" });
  type _Lmpop = Expect<
    Equal<
      typeof lmpop,
      { id: "a" | "b"; values: Array<{ task: string }> } | null
    >
  >;

  const zmpop = await sharedBoard.zmpop(["x"], { max: true, count: 2 });
  type _Zmpop = Expect<
    Equal<
      typeof zmpop,
      {
        id: "x";
        entries: Array<{ readonly member: string; readonly score: number }>;
      } | null
    >
  >;

  const foreverLmpop = await blockingJobs.blmpop(["a", "b"], {
    direction: "left",
    timeoutSeconds: "forever"
  });
  type _ForeverLmpopId = Expect<Equal<typeof foreverLmpop.id, "a" | "b">>;
  type _ForeverLmpopValues = Expect<
    Equal<typeof foreverLmpop.values, Array<{ task: string }>>
  >;

  const timedLmpop = await blockingJobs.blmpop(["a"], {
    direction: "right",
    timeoutSeconds: 1,
    count: 2
  });
  type _TimedLmpop = Expect<
    Equal<
      typeof timedLmpop,
      { id: "a"; values: Array<{ task: string }> } | null
    >
  >;

  const foreverBzmpop = await blockingBoard.bzmpop(
    [1, 2],
    { min: true },
    {
      timeoutSeconds: "forever"
    }
  );
  type _ForeverBzmpopId = Expect<Equal<typeof foreverBzmpop.id, 1 | 2>>;

  const timedBzmpop = await blockingBoard.bzmpop(
    ["x"],
    { max: true },
    {
      timeoutSeconds: 1
    }
  );
  type _TimedBzmpop = Expect<
    Equal<
      typeof timedBzmpop,
      {
        id: "x";
        entries: Array<{ readonly member: string; readonly score: number }>;
      } | null
    >
  >;

  const blockingRead = await blockingEvents.xread("42", "$", {
    timeoutSeconds: "forever",
    count: 1
  });
  type _BlockingRead = Expect<
    Equal<
      typeof blockingRead,
      Array<{ id: string; value: Partial<{ type: string; amount: number }> }>
    >
  >;
}

void typeProbes;

function expectTypeErrorsOnly() {
  // @ts-expect-error blocking pops are structurally absent from shared stores.
  void sharedJobs.blpop;

  // @ts-expect-error blocking pops are structurally absent from shared stores.
  void sharedBoard.bzpopmin;

  // @ts-expect-error the timeout option is required on blocking pops.
  void blockingJobs.blpop("a");

  // @ts-expect-error the timeout option is required on blocking pops.
  void blockingJobs.blpop("a", {});

  // A session's xread is spread over the base store's non-blocking one and so
  // answers for both: omitting the timeout is the plain read, not an error.
  // Requiring it here made the non-blocking form unreachable on a session.
  void blockingEvents.xread("42", "$");
  void blockingEvents.xread("42", "$", { count: 1 });
  void blockingEvents.xread("42", "$", { count: 1, timeoutSeconds: 5 });

  // @ts-expect-error only "forever" is a valid non-numeric timeout.
  void blockingJobs.blpop("a", { timeoutSeconds: "never" });
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

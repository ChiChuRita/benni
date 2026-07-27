import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { ValidationError } from "../src/core/errors.js";
import type { StreamEntry } from "../src/core/stream.js";
import {
  createBlockingStreamGroupOps,
  createStreamGroupOps,
  type PendingStreamEntry,
  type StreamAutoClaimResult,
  type StreamPendingEntry,
  type StreamPendingSummary
} from "../src/core/stream-group.js";
import { defineStream } from "../src/core/stream-resource.js";
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

function groupOf(commands: RedisCommand[], replies: RedisReply[]) {
  return createStreamGroupOps(fakeClient(commands, replies), events).group(
    "workers"
  );
}

function rejectingClient(commands: RedisCommand[], error: Error): RedisClient {
  return {
    async send(command) {
      commands.push(command);
      throw error;
    },
    async pipeline() {
      return [];
    },
    async close() {}
  };
}

describe("group create", () => {
  it("emits XGROUP CREATE with MKSTREAM by default and maps OK to true", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, ["OK", "OK", "OK"]);

    await expect(group.create("42", { from: "start" })).resolves.toBe(true);
    await expect(group.create("42", { from: "end" })).resolves.toBe(true);
    await expect(
      group.create("42", { from: { entryId: "5-1" } })
    ).resolves.toBe(true);

    expect(commands).toEqual([
      ["XGROUP", "CREATE", "events:42", "workers", "0", "MKSTREAM"],
      ["XGROUP", "CREATE", "events:42", "workers", "$", "MKSTREAM"],
      ["XGROUP", "CREATE", "events:42", "workers", "5-1", "MKSTREAM"]
    ]);
  });

  it("omits MKSTREAM when mkstream is false", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, ["OK"]);

    await expect(
      group.create("42", { from: "start", mkstream: false })
    ).resolves.toBe(true);

    expect(commands).toEqual([
      ["XGROUP", "CREATE", "events:42", "workers", "0"]
    ]);
  });

  it("maps a BUSYGROUP rejection to false", async () => {
    const commands: RedisCommand[] = [];
    const group = createStreamGroupOps(
      rejectingClient(
        commands,
        new Error("BUSYGROUP Consumer Group name already exists")
      ),
      events
    ).group("workers");

    await expect(group.create("42", { from: "start" })).resolves.toBe(false);

    expect(commands).toEqual([
      ["XGROUP", "CREATE", "events:42", "workers", "0", "MKSTREAM"]
    ]);
  });

  it("rethrows non-BUSYGROUP errors", async () => {
    const group = createStreamGroupOps(
      rejectingClient(
        [],
        new Error("ERR The XGROUP subcommand requires the key to exist")
      ),
      events
    ).group("workers");

    await expect(group.create("42", { from: "start" })).rejects.toThrow(
      "ERR The XGROUP subcommand requires the key to exist"
    );
  });

  it("throws on non-OK create replies", async () => {
    const group = groupOf([], [1]);

    await expect(group.create("42", { from: "start" })).rejects.toThrow(
      "Expected Redis XGROUP to return OK"
    );
  });

  it("rejects invalid from options before sending", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, []);
    const fromMessage = 'from must be "start", "end", or { entryId }';

    await expect(
      group.create("42", { from: "now" as unknown as "start" })
    ).rejects.toThrow(new ValidationError(fromMessage));
    await expect(group.create("42", { from: { entryId: "" } })).rejects.toThrow(
      new ValidationError(fromMessage)
    );
    await expect(
      group.create("42", {} as unknown as { from: "start" })
    ).rejects.toThrow(new ValidationError(fromMessage));

    expect(commands).toEqual([]);
  });
});

describe("group destroy / deleteConsumer", () => {
  it("emits XGROUP DESTROY and maps 1/0 to booleans", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, [1, 0]);

    await expect(group.destroy("42")).resolves.toBe(true);
    await expect(group.destroy("42")).resolves.toBe(false);

    expect(commands).toEqual([
      ["XGROUP", "DESTROY", "events:42", "workers"],
      ["XGROUP", "DESTROY", "events:42", "workers"]
    ]);
  });

  it("emits XGROUP DELCONSUMER and returns the destroyed pending count", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, [3]);

    await expect(group.deleteConsumer("42", "w-1")).resolves.toBe(3);

    expect(commands).toEqual([
      ["XGROUP", "DELCONSUMER", "events:42", "workers", "w-1"]
    ]);
  });
});

describe("group ack", () => {
  it("emits XACK and short-circuits empty entry id lists", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, [2]);

    await expect(group.xack("42", [])).resolves.toBe(0);
    await expect(group.xack("42", ["1-1", "1-2"])).resolves.toBe(2);

    expect(commands).toEqual([["XACK", "events:42", "workers", "1-1", "1-2"]]);
  });
});

describe("group pending summary", () => {
  it("emits the XPENDING summary form and decodes string consumer counts", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, [
      [
        3,
        "1-1",
        "9-9",
        [
          ["w-1", "2"],
          ["w-2", "1"]
        ]
      ]
    ]);

    await expect(group.xpending("42")).resolves.toEqual({
      count: 3,
      minEntryId: "1-1",
      maxEntryId: "9-9",
      consumers: [
        { consumer: "w-1", count: 2 },
        { consumer: "w-2", count: 1 }
      ]
    });

    expect(commands).toEqual([["XPENDING", "events:42", "workers"]]);
  });

  it("decodes the empty summary with nil min/max/consumers", async () => {
    const group = groupOf([], [[0, null, null, null]]);

    await expect(group.xpending("42")).resolves.toEqual({
      count: 0,
      minEntryId: null,
      maxEntryId: null,
      consumers: []
    });
  });

  it("throws on unexpected XPENDING summary shapes", async () => {
    const summaryMessage =
      "Expected Redis XPENDING to return count/min/max/consumers summary";

    await expect(groupOf([], ["nope"]).xpending("42")).rejects.toThrow(
      summaryMessage
    );
    await expect(
      groupOf([], [[3, "1-1", "9-9"]]).xpending("42")
    ).rejects.toThrow(summaryMessage);
    await expect(
      groupOf([], [["3", "1-1", "9-9", null]]).xpending("42")
    ).rejects.toThrow("Expected Redis XPENDING to return number");
    await expect(
      groupOf([], [[3, 1, "9-9", null]]).xpending("42")
    ).rejects.toThrow("Expected Redis XPENDING to return entry id or null");
    await expect(
      groupOf([], [[3, "1-1", "9-9", "w-1"]]).xpending("42")
    ).rejects.toThrow("Expected Redis XPENDING to return consumer/count pairs");
    await expect(
      groupOf([], [[3, "1-1", "9-9", [["w-1"]]]]).xpending("42")
    ).rejects.toThrow("Expected Redis XPENDING to return consumer/count pairs");
  });
});

describe("group pending range", () => {
  it("emits the extended XPENDING form with default bounds", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, [
      [
        ["1-1", "w-1", 5000, 2],
        ["1-2", "w-2", 100, 1]
      ]
    ]);

    await expect(group.xpending("42", { count: 10 })).resolves.toEqual([
      { entryId: "1-1", consumer: "w-1", idleMs: 5000, deliveries: 2 },
      { entryId: "1-2", consumer: "w-2", idleMs: 100, deliveries: 1 }
    ]);

    expect(commands).toEqual([
      ["XPENDING", "events:42", "workers", "-", "+", 10]
    ]);
  });

  it("places IDLE before start/end and the consumer filter last", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, [[], []]);

    await expect(
      group.xpending("42", { minIdleMs: 60_000, count: 5 })
    ).resolves.toEqual([]);
    await expect(
      group.xpending("42", {
        start: "1-1",
        end: "9-9",
        count: 5,
        consumer: "w-1",
        minIdleMs: 0
      })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XPENDING", "events:42", "workers", "IDLE", 60000, "-", "+", 5],
      ["XPENDING", "events:42", "workers", "IDLE", 0, "1-1", "9-9", 5, "w-1"]
    ]);
  });

  it("rejects invalid counts and idle thresholds before sending", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, []);

    await expect(group.xpending("42", { count: 0 })).rejects.toThrow(
      "count must be a positive safe integer"
    );
    await expect(group.xpending("42", { count: 1.5 })).rejects.toThrow(
      TypeError
    );
    await expect(
      group.xpending("42", { count: 5, minIdleMs: -1 })
    ).rejects.toThrow("minIdleMs must be a non-negative safe integer");
    await expect(
      group.xpending("42", { count: 5, minIdleMs: 0.5 })
    ).rejects.toThrow(TypeError);

    expect(commands).toEqual([]);
  });

  it("throws on unexpected XPENDING row shapes", async () => {
    const rowMessage =
      "Expected Redis XPENDING to return id/consumer/idle/deliveries rows";

    await expect(
      groupOf([], ["nope"]).xpending("42", { count: 1 })
    ).rejects.toThrow("Expected Redis XPENDING to return array");
    await expect(
      groupOf([], [[["1-1", "w-1", 5000]]]).xpending("42", { count: 1 })
    ).rejects.toThrow(rowMessage);
    await expect(
      groupOf([], [[[1, "w-1", 5000, 2]]]).xpending("42", { count: 1 })
    ).rejects.toThrow(rowMessage);
    await expect(
      groupOf([], [[["1-1", 1, 5000, 2]]]).xpending("42", { count: 1 })
    ).rejects.toThrow(rowMessage);
  });
});

describe("group and consumer names", () => {
  it("rejects empty names before sending", () => {
    const ops = createStreamGroupOps(fakeClient([], []), events);

    expect(() => ops.group("")).toThrow(
      "group name must be a non-empty string"
    );
    expect(() => ops.group("workers").consumer("")).toThrow(
      "consumer name must be a non-empty string"
    );
    expect(() =>
      createBlockingStreamGroupOps(fakeClient([], []), events).group("")
    ).toThrow("group name must be a non-empty string");
  });

  it("rejects empty consumer arguments on deleteConsumer and pendingRange", async () => {
    const commands: RedisCommand[] = [];
    const group = groupOf(commands, []);

    await expect(group.deleteConsumer("42", "")).rejects.toThrow(
      "consumer name must be a non-empty string"
    );
    await expect(
      group.xpending("42", { count: 1, consumer: "" })
    ).rejects.toThrow("consumer name must be a non-empty string");

    expect(commands).toEqual([]);
  });
});

describe("consumer read", () => {
  it("emits XREADGROUP with > and unwraps the single stream reply", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, [
      [["events:42", [["1-1", ["type", "credit", "amount", "5"]]]]],
      [["events:42", []]]
    ]).consumer("w-1");

    await expect(me.xreadgroup("42")).resolves.toEqual([
      { id: "1-1", value: { type: "credit", amount: 5 } }
    ]);
    await expect(me.xreadgroup("42", { count: 5 })).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XREADGROUP", "GROUP", "workers", "w-1", "STREAMS", "events:42", ">"],
      [
        "XREADGROUP",
        "GROUP",
        "workers",
        "w-1",
        "COUNT",
        5,
        "STREAMS",
        "events:42",
        ">"
      ]
    ]);
  });

  it("maps a null reply to an empty array", async () => {
    await expect(
      groupOf([], [null]).consumer("w-1").xreadgroup("42")
    ).resolves.toEqual([]);
  });

  it("unwraps map-shaped RESP3 replies", async () => {
    const me = groupOf(
      [],
      [
        new Map<RedisReply, RedisReply>([
          ["events:42", [["1-1", ["amount", "7"]]]]
        ])
      ]
    ).consumer("w-1");

    await expect(me.xreadgroup("42")).resolves.toEqual([
      { id: "1-1", value: { amount: 7 } }
    ]);
  });

  it("rejects invalid counts before sending", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, []).consumer("w-1");

    await expect(me.xreadgroup("42", { count: 0 })).rejects.toThrow(
      "count must be a positive safe integer"
    );

    expect(commands).toEqual([]);
  });

  it("throws XREADGROUP-labelled errors on malformed replies", async () => {
    const withReply = (reply: RedisReply) =>
      groupOf([], [reply]).consumer("w-1");

    await expect(withReply("nope").xreadgroup("42")).rejects.toThrow(
      "Expected Redis XREADGROUP to return array or null"
    );
    await expect(withReply([]).xreadgroup("42")).rejects.toThrow(
      "Expected Redis XREADGROUP to return one stream"
    );
    await expect(
      withReply([
        ["events:42", []],
        ["events:43", []]
      ]).xreadgroup("42")
    ).rejects.toThrow("Expected Redis XREADGROUP to return one stream");
    await expect(withReply([["events:42"]]).xreadgroup("42")).rejects.toThrow(
      "Expected Redis XREADGROUP to return key/entries pairs"
    );
    await expect(withReply([[1, []]]).xreadgroup("42")).rejects.toThrow(
      "Expected Redis XREADGROUP to return key/entries pairs"
    );
    await expect(
      withReply([["events:42", [["1-1", ["type"]]]]]).xreadgroup("42")
    ).rejects.toThrow("Expected Redis XREADGROUP to return field/value pairs");
  });
});

describe("consumer readPending", () => {
  it("reads history from 0 by default and honors after/count", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, [
      [["events:42", [["1-1", ["type", "credit", "amount", "5"]]]]],
      [["events:42", []]]
    ]).consumer("w-1");

    await expect(me.xreadgroup("42", { after: "0" })).resolves.toEqual([
      { id: "1-1", value: { type: "credit", amount: 5 } }
    ]);
    await expect(
      me.xreadgroup("42", { after: "1-1", count: 100 })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XREADGROUP", "GROUP", "workers", "w-1", "STREAMS", "events:42", "0"],
      [
        "XREADGROUP",
        "GROUP",
        "workers",
        "w-1",
        "COUNT",
        100,
        "STREAMS",
        "events:42",
        "1-1"
      ]
    ]);
  });

  it("decodes tombstones as value null and keeps them ackable", async () => {
    const me = groupOf(
      [],
      [
        [
          [
            "events:42",
            [
              ["1-1", null],
              ["1-2", ["type", "credit", "amount", "5"]]
            ]
          ]
        ]
      ]
    ).consumer("w-1");

    const entries = await me.xreadgroup("42", { after: "0" });

    expect(entries).toEqual([
      { id: "1-1", value: null },
      { id: "1-2", value: { type: "credit", amount: 5 } }
    ]);
  });

  it("rejects the live-read sentinel before sending", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, []).consumer("w-1");
    const afterMessage =
      "after must be an entry id; new deliveries come from xreadgroup() without { after }";

    await expect(me.xreadgroup("42", { after: ">" })).rejects.toThrow(
      new ValidationError(afterMessage)
    );
    await expect(me.xreadgroup("42", { after: "" })).rejects.toThrow(
      new ValidationError(afterMessage)
    );

    expect(commands).toEqual([]);
  });
});

describe("consumer ack", () => {
  it("mirrors group.ack including the empty short-circuit", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, [1]).consumer("w-1");

    await expect(me.xack("42", [])).resolves.toBe(0);
    await expect(me.xack("42", ["1-1"])).resolves.toBe(1);

    expect(commands).toEqual([["XACK", "events:42", "workers", "1-1"]]);
  });
});

describe("consumer claim", () => {
  it("emits XCLAIM with the idle threshold before the entry ids", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, [
      [
        ["1-1", ["type", "credit", "amount", "5"]],
        ["1-2", null]
      ]
    ]).consumer("w-1");

    await expect(
      me.xclaim("42", ["1-1", "1-2"], { minIdleMs: 60_000 })
    ).resolves.toEqual([
      { id: "1-1", value: { type: "credit", amount: 5 } },
      { id: "1-2", value: null }
    ]);

    expect(commands).toEqual([
      ["XCLAIM", "events:42", "workers", "w-1", 60000, "1-1", "1-2"]
    ]);
  });

  it("short-circuits empty entry id lists", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, []).consumer("w-1");

    await expect(me.xclaim("42", [], { minIdleMs: 1000 })).resolves.toEqual([]);

    expect(commands).toEqual([]);
  });

  it("rejects invalid idle thresholds before sending", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, []).consumer("w-1");

    await expect(me.xclaim("42", ["1-1"], { minIdleMs: -1 })).rejects.toThrow(
      "minIdleMs must be a non-negative safe integer"
    );
    await expect(me.xclaim("42", ["1-1"], { minIdleMs: 0.5 })).rejects.toThrow(
      TypeError
    );

    expect(commands).toEqual([]);
  });

  it("throws on unexpected XCLAIM reply shapes", async () => {
    const me = (reply: RedisReply) => groupOf([], [reply]).consumer("w-1");

    await expect(
      me("nope").xclaim("42", ["1-1"], { minIdleMs: 0 })
    ).rejects.toThrow("Expected Redis XCLAIM to return array");
    await expect(
      me([["1-1"]]).xclaim("42", ["1-1"], { minIdleMs: 0 })
    ).rejects.toThrow("Expected Redis XCLAIM to return id/fields pairs");
  });
});

describe("consumer autoClaim", () => {
  it("emits XAUTOCLAIM from the default cursor and decodes the 3-tuple", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, [
      [
        "0-0",
        [
          ["1-1", ["type", "credit", "amount", "5"]],
          ["1-2", null]
        ],
        ["1-3", "1-4"]
      ]
    ]).consumer("w-1");

    await expect(me.xautoclaim("42", { minIdleMs: 60_000 })).resolves.toEqual({
      cursor: "0-0",
      entries: [
        { id: "1-1", value: { type: "credit", amount: 5 } },
        { id: "1-2", value: null }
      ],
      deletedIds: ["1-3", "1-4"]
    });

    expect(commands).toEqual([
      ["XAUTOCLAIM", "events:42", "workers", "w-1", 60000, "0-0"]
    ]);
  });

  it("passes start and COUNT through", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, [["3-0", [], []]]).consumer("w-1");

    await expect(
      me.xautoclaim("42", { minIdleMs: 0, start: "2-2", count: 50 })
    ).resolves.toEqual({ cursor: "3-0", entries: [], deletedIds: [] });

    expect(commands).toEqual([
      ["XAUTOCLAIM", "events:42", "workers", "w-1", 0, "2-2", "COUNT", 50]
    ]);
  });

  it("falls back to empty deletedIds on the 2-element Redis 6.2 reply", async () => {
    const me = groupOf([], [["0-0", [["1-1", ["amount", "7"]]]]]).consumer(
      "w-1"
    );

    await expect(me.xautoclaim("42", { minIdleMs: 1000 })).resolves.toEqual({
      cursor: "0-0",
      entries: [{ id: "1-1", value: { amount: 7 } }],
      deletedIds: []
    });
  });

  it("rejects invalid options before sending", async () => {
    const commands: RedisCommand[] = [];
    const me = groupOf(commands, []).consumer("w-1");

    await expect(me.xautoclaim("42", { minIdleMs: -1 })).rejects.toThrow(
      "minIdleMs must be a non-negative safe integer"
    );
    await expect(
      me.xautoclaim("42", { minIdleMs: 0, count: 0 })
    ).rejects.toThrow("count must be a positive safe integer");

    expect(commands).toEqual([]);
  });

  it("throws on unexpected XAUTOCLAIM reply shapes", async () => {
    const me = (reply: RedisReply) => groupOf([], [reply]).consumer("w-1");
    const tupleMessage =
      "Expected Redis XAUTOCLAIM to return cursor/entries reply";

    await expect(me("nope").xautoclaim("42", { minIdleMs: 0 })).rejects.toThrow(
      tupleMessage
    );
    await expect(
      me(["0-0"]).xautoclaim("42", { minIdleMs: 0 })
    ).rejects.toThrow(tupleMessage);
    await expect(
      me([1, [], []]).xautoclaim("42", { minIdleMs: 0 })
    ).rejects.toThrow(tupleMessage);
    await expect(
      me(["0-0", "entries", []]).xautoclaim("42", { minIdleMs: 0 })
    ).rejects.toThrow("Expected Redis XAUTOCLAIM to return array");
    await expect(
      me(["0-0", [], [1]]).xautoclaim("42", { minIdleMs: 0 })
    ).rejects.toThrow("Expected Redis XAUTOCLAIM to return deleted entry ids");
    await expect(
      me(["0-0", [], "deleted"]).xautoclaim("42", { minIdleMs: 0 })
    ).rejects.toThrow("Expected Redis XAUTOCLAIM to return deleted entry ids");
  });
});

describe("blocking consumer readBlocking", () => {
  function blockingConsumer(commands: RedisCommand[], replies: RedisReply[]) {
    return createBlockingStreamGroupOps(fakeClient(commands, replies), events)
      .group("workers")
      .consumer("w-1");
  }

  it("emits GROUP g c COUNT n BLOCK ms STREAMS key > in order", async () => {
    const commands: RedisCommand[] = [];
    const me = blockingConsumer(commands, [
      [["events:42", [["1-1", ["type", "credit", "amount", "5"]]]]]
    ]);

    await expect(
      me.xreadgroup("42", { timeoutSeconds: 5, count: 20 })
    ).resolves.toEqual([{ id: "1-1", value: { type: "credit", amount: 5 } }]);

    expect(commands).toEqual([
      [
        "XREADGROUP",
        "GROUP",
        "workers",
        "w-1",
        "COUNT",
        20,
        "BLOCK",
        "5000",
        "STREAMS",
        "events:42",
        ">"
      ]
    ]);
  });

  it("converts fractional seconds to integer milliseconds and forever to 0", async () => {
    const commands: RedisCommand[] = [];
    const me = blockingConsumer(commands, [null, null]);

    await expect(
      me.xreadgroup("42", { timeoutSeconds: 0.25 })
    ).resolves.toEqual([]);
    await expect(
      me.xreadgroup("42", { timeoutSeconds: "forever" })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      [
        "XREADGROUP",
        "GROUP",
        "workers",
        "w-1",
        "BLOCK",
        "250",
        "STREAMS",
        "events:42",
        ">"
      ],
      [
        "XREADGROUP",
        "GROUP",
        "workers",
        "w-1",
        "BLOCK",
        "0",
        "STREAMS",
        "events:42",
        ">"
      ]
    ]);
  });

  it("keeps the full non-blocking group and consumer surface", async () => {
    const commands: RedisCommand[] = [];
    const group = createBlockingStreamGroupOps(
      fakeClient(commands, ["OK", 1, [["events:42", []]]]),
      events
    ).group("workers");

    await expect(group.create("42", { from: "end" })).resolves.toBe(true);
    await expect(group.consumer("w-1").xack("42", ["1-1"])).resolves.toBe(1);
    await expect(group.consumer("w-1").xreadgroup("42")).resolves.toEqual([]);

    expect(commands).toEqual([
      ["XGROUP", "CREATE", "events:42", "workers", "$", "MKSTREAM"],
      ["XACK", "events:42", "workers", "1-1"],
      ["XREADGROUP", "GROUP", "workers", "w-1", "STREAMS", "events:42", ">"]
    ]);
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typeClient = null as unknown as RedisClient;

const typedEvents = defineStream(
  "events",
  { type: codecs.string(), amount: codecs.number() },
  { ids: ["login", "logout"] }
);
const sharedGroup = createStreamGroupOps(typeClient, typedEvents).group("g");
const sharedConsumer = sharedGroup.consumer("c");
const blockingGroup = createBlockingStreamGroupOps(
  typeClient,
  typedEvents
).group("g");
const blockingConsumerOps = blockingGroup.consumer("c");

async function typeProbes() {
  const created = await sharedGroup.create("login", { from: "start" });
  type _Created = Expect<Equal<typeof created, boolean>>;

  const summary = await sharedGroup.xpending("login");
  type _Summary = Expect<Equal<typeof summary, StreamPendingSummary>>;
  type _SummaryMin = Expect<Equal<typeof summary.minEntryId, string | null>>;

  const rows = await sharedGroup.xpending("login", { count: 10 });
  type _Rows = Expect<Equal<typeof rows, StreamPendingEntry[]>>;

  const live = await sharedConsumer.xreadgroup("login", { count: 5 });
  type _Live = Expect<
    Equal<typeof live, Array<StreamEntry<typeof typedEvents.fields>>>
  >;
  type _LiveValue = Expect<
    Equal<
      (typeof live)[number]["value"],
      Partial<{ type: string; amount: number }>
    >
  >;

  const history = await sharedConsumer.xreadgroup("login", { after: "0" });
  type _History = Expect<
    Equal<typeof history, Array<PendingStreamEntry<typeof typedEvents.fields>>>
  >;
  const first = history[0];
  type _HistoryValue = Expect<
    Equal<typeof first.value, Partial<{ type: string; amount: number }> | null>
  >;
  if (first.value !== null) {
    type _Narrowed = Expect<
      Equal<typeof first.value, Partial<{ type: string; amount: number }>>
    >;
  }

  const claimed = await sharedConsumer.xautoclaim("login", { minIdleMs: 1000 });
  type _Claimed = Expect<
    Equal<typeof claimed, StreamAutoClaimResult<typeof typedEvents.fields>>
  >;
  type _Cursor = Expect<Equal<typeof claimed.cursor, string>>;
  type _Deleted = Expect<Equal<typeof claimed.deletedIds, string[]>>;

  const blocked = await blockingConsumerOps.xreadgroup("login", {
    timeoutSeconds: "forever",
    count: 1
  });
  type _Blocked = Expect<
    Equal<typeof blocked, Array<StreamEntry<typeof typedEvents.fields>>>
  >;
}

void typeProbes;

function expectTypeErrorsOnly() {
  // @ts-expect-error the blocking { timeoutSeconds } overload is structurally absent from shared consumers.
  void sharedConsumer.xreadgroup("login", { timeoutSeconds: "forever" });

  // @ts-expect-error the non-blocking xreadgroup has no timeout option.
  void sharedConsumer.xreadgroup("login", { timeoutSeconds: 5 });

  // @ts-expect-error xreadgroup rejects unknown options.
  void sharedConsumer.xreadgroup("login", { bogus: 1 });

  // @ts-expect-error create requires from.
  void sharedGroup.create("login", {});

  // @ts-expect-error create requires the options argument.
  void sharedGroup.create("login");

  // @ts-expect-error the extended xpending form requires count.
  void sharedGroup.xpending("login", {});

  // @ts-expect-error known stream keyspaces only accept declared ids.
  void sharedGroup.xpending("signup");

  // @ts-expect-error xclaim requires minIdleMs.
  void sharedConsumer.xclaim("login", ["1-1"], {});

  // @ts-expect-error the blocking { timeoutSeconds } read only reads new deliveries; no { after }.
  void blockingConsumerOps.xreadgroup("login", {
    timeoutSeconds: 1,
    after: "0"
  });
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import {
  createStreamStore,
  type StreamAddOptions,
  type StreamEntry
} from "../src/core/stream.js";
import { createStreamGroupOps } from "../src/core/stream-group.js";
import { defineStream } from "../src/core/stream-resource.js";
import type {
  Codec,
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { node } from "../src/node/index.js";
import { fakeClient } from "./fake-client.js";

const events = defineStream("events", {
  kind: codecs.string()
});

// Built with fromEntries, not a literal: `{ __proto__: ... }` in an object
// literal sets the prototype instead of declaring a field, so the only way to
// reach this shape is a fields object assembled from data.
const protoFields = Object.fromEntries([
  ["__proto__", codecs.string()],
  ["ok", codecs.string()]
]) as Record<"__proto__" | "ok", Codec<string>>;
const protoEvents = defineStream("proto", protoFields);

// Reading `value.__proto__` would go through the deprecated accessor, which is
// the very thing under test; go straight to the own descriptor.
function ownValue(target: object, field: string): unknown {
  return Object.getOwnPropertyDescriptor(target, field)?.value;
}

const protoJsonFields = Object.fromEntries([
  ["__proto__", codecs.json<{ evil: boolean }>()]
]) as Record<"__proto__", Codec<{ evil: boolean }>>;
const protoJsonEvents = defineStream("protojson", protoJsonFields);

describe("xtrim MAXLEN 0", () => {
  it("sends MAXLEN 0 instead of rejecting it", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, [3]), events);

    await expect(store.xtrim("42", { maxLen: { count: 0 } })).resolves.toBe(3);

    expect(commands).toEqual([["XTRIM", "events:42", "MAXLEN", 0]]);
  });

  it("still rejects negative and fractional maxLen counts", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(fakeClient(commands, []), events);

    await expect(store.xtrim("42", { maxLen: { count: -1 } })).rejects.toThrow(
      "maxLen.count must be a nonnegative safe integer"
    );
    await expect(store.xtrim("42", { maxLen: { count: 1.5 } })).rejects.toThrow(
      "maxLen.count must be a nonnegative safe integer"
    );

    expect(commands).toEqual([]);
  });
});

describe("a stream field named __proto__", () => {
  it("goes on the wire and comes back as an own property", async () => {
    const commands: RedisCommand[] = [];
    const store = createStreamStore(
      fakeClient(commands, [
        "1-1",
        [["1-1", ["__proto__", "written", "ok", "yes"]]]
      ]),
      protoEvents
    );

    await store.xadd("1", { ["__proto__"]: "written", ok: "yes" });
    const [entry] = await store.xrange("1");

    expect(commands[0]).toEqual([
      "XADD",
      "proto:1",
      "*",
      "__proto__",
      "written",
      "ok",
      "yes"
    ]);
    expect(Object.hasOwn(entry.value, "__proto__")).toBe(true);
    expect(Object.keys(entry.value)).toEqual(["__proto__", "ok"]);
    expect(ownValue(entry.value, "__proto__")).toBe("written");
  });

  it("keeps the decoded entry's prototype when the field decodes to an object", async () => {
    const store = createStreamStore(
      fakeClient([], [[["1-1", ["__proto__", '{"evil":true}']]]]),
      protoJsonEvents
    );

    const [entry] = await store.xrange("1");

    expect(Object.getPrototypeOf(entry.value)).toBe(Object.prototype);
    expect(Object.hasOwn(entry.value, "__proto__")).toBe(true);
  });
});

describe("xreadgroup with an explicitly undefined after", () => {
  function consumerOf(commands: RedisCommand[], replies: RedisReply[]) {
    return createStreamGroupOps(fakeClient(commands, replies), events)
      .group("workers")
      .consumer("w-1");
  }

  it("reads new deliveries, which is what its type promises", async () => {
    const commands: RedisCommand[] = [];
    const me = consumerOf(commands, [[["events:42", []]]]);

    await expect(
      me.xreadgroup("42", { after: undefined, count: 10 })
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      [
        "XREADGROUP",
        "GROUP",
        "workers",
        "w-1",
        "COUNT",
        10,
        "STREAMS",
        "events:42",
        ">"
      ]
    ]);
  });

  it("still reads history for an id that is present", async () => {
    const commands: RedisCommand[] = [];
    const me = consumerOf(commands, [[["events:42", [["1-1", null]]]]]);

    await expect(me.xreadgroup("42", { after: "0" })).resolves.toEqual([
      { id: "1-1", value: null }
    ]);

    expect(commands).toEqual([
      ["XREADGROUP", "GROUP", "workers", "w-1", "STREAMS", "events:42", "0"]
    ]);
  });

  it("rejects a non-string after instead of silently reading history", async () => {
    const commands: RedisCommand[] = [];
    const me = consumerOf(commands, []);

    await expect(
      me.xreadgroup("42", { after: null as unknown as string })
    ).rejects.toThrow("after must be an entry id");

    expect(commands).toEqual([]);
  });
});

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;
const runPrefix = `beni:hunt-stream:${Date.now()}:${Math.random()
  .toString(36)
  .slice(2)}`;

describeRedis("stream fixes against real Redis", () => {
  const liveEvents = defineStream(`${runPrefix}:events`, {
    kind: codecs.string()
  });
  const liveProtoEvents = defineStream(`${runPrefix}:proto`, protoFields);
  let client: RedisClient;

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });

  afterAll(async () => {
    try {
      await client.send([
        "DEL",
        liveEvents.key("trim"),
        liveEvents.key("live"),
        liveProtoEvents.key("round")
      ]);
    } finally {
      await client.close();
    }
  });

  it("empties a stream with MAXLEN 0 and keeps the consumer group", async () => {
    const store = createStreamStore(client, liveEvents);
    const group = createStreamGroupOps(client, liveEvents).group("workers");
    await group.create("trim", { from: "start" });
    await store.xadd("trim", { kind: "a" }, { entryId: "1-1" });
    await store.xadd("trim", { kind: "b" }, { entryId: "2-1" });

    await expect(store.xtrim("trim", { maxLen: { count: 0 } })).resolves.toBe(
      2
    );
    await expect(store.xlen("trim")).resolves.toBe(0);

    // The group survived the trim, so the next read is a plain empty read
    // rather than a NOGROUP failure.
    const me = group.consumer("w-1");
    await expect(me.xreadgroup("trim")).resolves.toEqual([]);
    await expect(
      store.xadd("trim", { kind: "c" }, { entryId: "3-1" })
    ).resolves.toBe("3-1");
    await expect(me.xreadgroup("trim")).resolves.toEqual([
      { id: "3-1", value: { kind: "c" } }
    ]);
  });

  it("round-trips a field named __proto__", async () => {
    const store = createStreamStore(client, liveProtoEvents);

    const entryId = await store.xadd("round", {
      ["__proto__"]: "written",
      ok: "yes"
    });
    const [entry] = await store.xrange("round");

    expect(entry.id).toBe(entryId);
    expect(Object.hasOwn(entry.value, "__proto__")).toBe(true);
    expect(ownValue(entry.value, "__proto__")).toBe("written");
  });

  it("skips tombstones on an explicitly undefined after", async () => {
    const store = createStreamStore(client, liveEvents);
    const group = createStreamGroupOps(client, liveEvents).group("workers");
    await group.create("live", { from: "start" });
    const me = group.consumer("w-1");
    const entryId = await store.xadd("live", { kind: "gone" });
    await expect(me.xreadgroup("live")).resolves.toHaveLength(1);
    await expect(store.xdel("live", [entryId])).resolves.toBe(1);

    // The history read still sees the deleted entry as a tombstone.
    await expect(me.xreadgroup("live", { after: "0" })).resolves.toEqual([
      { id: entryId, value: null }
    ]);
    // The live read must not, or the non-nullable value it promises is a lie.
    await expect(me.xreadgroup("live", { after: undefined })).resolves.toEqual(
      []
    );
  });
});

const typeClient = null as unknown as RedisClient;
const typedStore = createStreamStore(typeClient, events);

async function xaddTypeProbes(flag: boolean, options: StreamAddOptions) {
  const value = { kind: "click" };
  const plain: string = await typedStore.xadd("42", value);
  const off: string = await typedStore.xadd("42", value, {
    entryId: "1-1"
  });
  const nullable: string | null = await typedStore.xadd("42", value, {
    nomkstream: flag
  });
  // @ts-expect-error a computed nomkstream can still make Redis reply nil.
  const computed: string = await typedStore.xadd("42", value, {
    nomkstream: flag
  });
  // @ts-expect-error an options value whose nomkstream is optional picks no overload.
  const fromValue = await typedStore.xadd("42", value, options);
  return [plain, off, nullable, computed, fromValue];
}
void xaddTypeProbes;

async function xreadgroupTypeProbes() {
  const me = createStreamGroupOps(typeClient, events)
    .group("workers")
    .consumer("w-1");
  // The live overload keeps its non-nullable value, which the runtime now
  // honors for this exact call shape.
  const live: Array<StreamEntry<typeof events.fields>> = await me.xreadgroup(
    "42",
    { after: undefined }
  );
  return live;
}
void xreadgroupTypeProbes;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

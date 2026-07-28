import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  codecs,
  createHashStore,
  createSortedSetStore,
  createStreamStore,
  createStringStore,
  defineHash,
  defineKeyspace,
  defineSortedSet,
  defineStream,
  ValidationError
} from "../src/core/index.js";
import { createScriptRunner, defineScript } from "../src/core/script.js";
import { assertSameSlot, CrossSlotError, slotOf } from "../src/core/slot.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";

/** A client whose queued replies may be Errors, so a rejection can be scripted. */
function rejecting(
  commands: RedisCommand[],
  replies: Array<RedisReply | Error>
): RedisClient {
  return {
    async send(command) {
      commands.push(command);
      const reply = replies.shift();
      if (reply === undefined) throw new Error("No fake Redis reply queued");
      if (reply instanceof Error) throw reply;
      return reply;
    },
    async pipeline() {
      throw new Error("pipeline is not used here");
    },
    async close() {}
  };
}

import { node } from "../src/node/index.js";
import { cache, lock } from "../src/primitives/index.js";
import { upstash } from "../src/upstash/index.js";
import { fakeClient } from "./fake-client.js";

// Regressions for the 2026-07-11 adversarial review. Each block pins one
// confirmed finding so it cannot quietly return.

describe("infinite sorted-set scores (review #1)", () => {
  const board = defineSortedSet("board", codecs.string());

  it("decodes RESP2 inf/-inf score strings", async () => {
    const store = createSortedSetStore(fakeClient([], ["inf", "-inf"]), board);
    await expect(store.zscore("d", "a")).resolves.toBe(
      Number.POSITIVE_INFINITY
    );
    await expect(store.zscore("d", "b")).resolves.toBe(
      Number.NEGATIVE_INFINITY
    );
  });

  it("encodes Infinity scores as +inf/-inf and rejects NaN", async () => {
    const commands: import("../src/core/index.js").RedisCommand[] = [];
    const store = createSortedSetStore(fakeClient(commands, [1]), board);

    await store.zadd("d", [{ score: Number.POSITIVE_INFINITY, member: "top" }]);
    expect(commands[0]).toEqual(["ZADD", "board:d", "+inf", "top"]);

    await expect(
      store.zadd("d", [{ score: Number.NaN, member: "x" }])
    ).rejects.toThrow(ValidationError);
    await expect(store.zincrby("d", Number.NaN, "x")).rejects.toThrow(
      ValidationError
    );
  });

  it("zrandmember rejects withScores without count instead of dropping it", async () => {
    const store = createSortedSetStore(fakeClient([], []), board);
    await expect(store.zrandmember("d", { withScores: true })).rejects.toThrow(
      "withScores requires count"
    );
  });
});

describe("getrange validation (review #3)", () => {
  it("rejects non-integer bounds before sending", async () => {
    const store = createStringStore(
      fakeClient([], []),
      defineKeyspace("text", codecs.string())
    );
    await expect(store.getrange("k", Number.NaN, 4)).rejects.toThrow(
      ValidationError
    );
    await expect(store.getrange("k", 0, 1.5)).rejects.toThrow(ValidationError);
  });
});

describe("whole-record hash reads/writes (review #4)", () => {
  const users = defineHash("user", {
    name: codecs.string(),
    score: codecs.number()
  });

  it("names the missing fields on a partial hash", async () => {
    const store = createHashStore(fakeClient([], [["Ada", null]]), users);
    await expect(store.hget("42")).rejects.toThrow(
      "missing declared field(s): score"
    );
  });
});

describe("upstash top-level transaction error (review #5)", () => {
  it("surfaces the Redis error text from a failed /multi-exec", async () => {
    const fn = (async () => ({
      ok: true,
      status: 200,
      json: async () => ({ error: "ERR EXEC without MULTI" })
    })) as unknown as typeof fetch;
    const client = upstash({ url: "https://x", token: "t", fetch: fn });
    await expect(client.transaction?.([["SET", "k", "v"]])).rejects.toThrow(
      "EXEC without MULTI"
    );
  });
});

describe("lock.run release failures (review #7)", () => {
  it("does not mask fn's success when release rejects", async () => {
    // Only the acquire reply is queued; the release's SCRIPT LOAD hits an
    // empty queue and rejects — run() must still resolve with fn's result.
    const locks = lock(fakeClient([], ["OK"]));
    await expect(locks.run("r", async () => 7)).resolves.toBe(7);
  });
});

describe("field names that collide with Object.prototype (review #6)", () => {
  // Field names arrive from Redis, so `schema.fields[field]` walked the
  // prototype chain: an undeclared field called "toString" resolved to
  // Object.prototype.toString, passed the truthiness check, and threw
  // "codec.decode is not a function". One such field made the whole record —
  // or the whole stream — unreadable through the typed API.
  const user = defineHash("user", {
    name: codecs.string(),
    score: codecs.number()
  });

  it("hgetall ignores an undeclared prototype-named field", async () => {
    const store = createHashStore(
      fakeClient(
        [],
        [["name", "Ada", "score", "10", "toString", "x", "__proto__", "y"]]
      ),
      user
    );
    await expect(store.hgetall("1")).resolves.toEqual({
      name: "Ada",
      score: 10
    });
  });

  it("hgetall does not pollute the prototype through a __proto__ field", async () => {
    const store = createHashStore(
      fakeClient([], [["name", "Ada", "score", "1", "__proto__", '{"bad":1}']]),
      user
    );
    await store.hgetall("1");
    expect(({} as Record<string, unknown>).bad).toBeUndefined();
  });

  it("rejects a prototype-named field as unknown rather than crashing", async () => {
    const store = createHashStore(fakeClient([], []), user);
    await expect(store.hget("1", "toString" as "name")).rejects.toBeInstanceOf(
      ValidationError
    );
  });

  it("xrange skips an undeclared prototype-named stream field", async () => {
    const events = defineStream("events", { action: codecs.string() });
    const store = createStreamStore(
      fakeClient([], [[["1-0", ["action", "login", "constructor", "x"]]]]),
      events
    );
    await expect(store.xrange("42")).resolves.toEqual([
      { id: "1-0", value: { action: "login" } }
    ]);
  });
});

describe("LCS is a multi-key command (review #8)", () => {
  it("runs the cross-slot guard on its two keys", async () => {
    // createStringStore never received the guard, so LCS — the one two-key
    // command in the string store — was sent unchecked even with the cluster
    // guard installed. On a single node it just works; on a real cluster the
    // server rejects it with a raw CROSSSLOT.
    const store = createStringStore(
      fakeClient([], ["mytext"]),
      defineKeyspace("doc", codecs.string()),
      assertSameSlot
    );
    await expect(store.lcs("a", "b")).rejects.toBeInstanceOf(CrossSlotError);
  });

  it("allows LCS when a hash tag co-locates the two keys", async () => {
    const commands: RedisCommand[] = [];
    const store = createStringStore(
      fakeClient(commands, ["mytext"]),
      defineKeyspace("doc", codecs.string(), { hashTag: "prefix" }),
      assertSameSlot
    );
    await expect(store.lcs("a", "b")).resolves.toBe("mytext");
    expect(slotOf(commands[0][1] as string)).toBe(
      slotOf(commands[0][2] as string)
    );
  });
});

describe("json codec and non-finite numbers (review #9)", () => {
  it("rejects NaN/Infinity instead of silently storing null", async () => {
    // JSON.stringify writes them as the literal `null`, which reads back
    // indistinguishable from "the key does not exist" — the sentinel kv.get()
    // returns for a missing key. The number() codec already refused them.
    const codec = codecs.json<number>();
    expect(() => codec.encode(Number.NaN)).toThrow(ValidationError);
    expect(() => codec.encode(Number.POSITIVE_INFINITY)).toThrow(
      ValidationError
    );
    expect(() => codec.encode(Number.NEGATIVE_INFINITY)).toThrow(
      ValidationError
    );
  });

  it("rejects a non-finite number nested in an object", async () => {
    const codec = codecs.json<{ score: number }>();
    expect(() => codec.encode({ score: Number.NaN })).toThrow(ValidationError);
  });

  it("still encodes ordinary values", () => {
    const codec = codecs.json<unknown>();
    expect(codec.encode({ a: 1, b: [null, "x"] })).toBe(
      '{"a":1,"b":[null,"x"]}'
    );
    expect(codec.encode(null)).toBe("null");
    expect(codec.encode(0)).toBe("0");
  });

  it("reports a BigInt as a ValidationError, not a raw TypeError", () => {
    const codec = codecs.json<unknown>();
    expect(() => codec.encode({ n: 1n })).toThrow(ValidationError);
  });
});

describe("medium-severity sweep (review #10)", () => {
  it("does not treat a script's own error text as NOSCRIPT", async () => {
    // isNoScriptError was a substring test, so a script whose failure merely
    // mentions NOSCRIPT triggered a reload and a re-run — side effects twice.
    // Redis wraps script failures as "ERR Error running script ...".
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejecting(commands, [
        "sha-1",
        new Error(
          "ERR Error running script (call to f_x): @user_script:2: NOSCRIPT is not a valid mode"
        )
      ])
    );
    const noop = defineScript<[], number>({
      lua: "return 1",
      keyCount: 0,
      decode: (reply) => Number(reply)
    });

    await expect(runner.run(noop, [], [])).rejects.toThrow("Error running");
    // SCRIPT LOAD + one EVALSHA. A retry would make four.
    expect(commands).toHaveLength(2);
  });

  it("still retries a genuine NOSCRIPT", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejecting(commands, [
        "sha-1",
        new Error("NOSCRIPT No matching script. Please use EVAL."),
        "sha-2",
        7
      ])
    );
    const noop = defineScript<[], number>({
      lua: "return 1",
      keyCount: 0,
      decode: (reply) => Number(reply)
    });

    await expect(runner.run(noop, [], [])).resolves.toBe(7);
    expect(commands).toHaveLength(4);
  });

  it("hset with a ttl is one transaction, not a pipeline", async () => {
    // A pipeline only batches. Between the HSET and the EXPIRE another client
    // sees a record with no expiry, and a connection lost in that window
    // leaves one that never expires.
    const batched: RedisCommand[] = [];
    const transacted: RedisCommand[] = [];
    const client: RedisClient = {
      async send() {
        return 1;
      },
      async pipeline(commands) {
        batched.push(...commands);
        return commands.map(() => 1);
      },
      async transaction(commands) {
        transacted.push(...commands);
        return commands.map(() => 1);
      },
      async close() {}
    };
    const users = defineHash("user", { name: codecs.string() });

    await createHashStore(client, users).hset(
      "42",
      { name: "Ada" },
      { ttlSeconds: 120 }
    );
    expect(transacted.map((command) => command[0])).toEqual(["HSET", "EXPIRE"]);
    expect(batched).toEqual([]);
  });

  it("hset without a ttl stays a single-command pipeline", async () => {
    const commands: RedisCommand[] = [];
    const users = defineHash("user", { name: codecs.string() });
    await createHashStore(fakeClient(commands, [1]), users).hset("42", {
      name: "Ada"
    });
    expect(commands).toEqual([["HSET", "user:42", "name", "Ada"]]);
  });

  it("json(schema) claims an async validator's promise", async () => {
    // decode() throws for an async validator, but nothing awaited the promise
    // it had already started, so a rejecting one became an unhandled
    // rejection — fatal under --unhandled-rejections=strict.
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => rejections.push(reason);
    process.on("unhandledRejection", onRejection);
    try {
      const codec = codecs.json({
        "~standard": {
          version: 1,
          vendor: "test",
          validate: () => Promise.reject(new Error("validator blew up"))
        }
      } as never);
      expect(() => codec.decode('{"a":1}')).toThrow(ValidationError);
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(rejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", onRejection);
    }
  });
});

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("infinite scores against real Redis", () => {
  let client: import("../src/core/index.js").RedisClient;
  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("round-trips +inf/-inf scores", async () => {
    const board = defineSortedSet(
      `review:${Date.now()}:${Math.random().toString(36).slice(2)}`,
      codecs.string()
    );
    const store = createSortedSetStore(client, board);

    await store.zadd("d", [
      { score: Number.POSITIVE_INFINITY, member: "top" },
      { score: Number.NEGATIVE_INFINITY, member: "bottom" },
      { score: 1, member: "mid" }
    ]);
    await expect(store.zscore("d", "top")).resolves.toBe(
      Number.POSITIVE_INFINITY
    );
    await expect(
      store.zrange("d", { start: 0, stop: -1, withScores: true })
    ).resolves.toEqual([
      { member: "bottom", score: Number.NEGATIVE_INFINITY },
      { member: "mid", score: 1 },
      { member: "top", score: Number.POSITIVE_INFINITY }
    ]);
    await store.del("d");
  });
});

// Regressions for the cluster-safe-keys pass. Both were real breakage on a
// Redis Cluster that a single-node test suite could never surface.

describe("cluster-safe primitive and adapter keys", () => {
  it("keeps a cache entry and its own fill lock in one slot", async () => {
    // The fill lock used to be `cache:lock:<id>` next to `cache:<id>`, which
    // are different slots: two nodes per miss, and a single-flight guarantee
    // spread across them. Tagging the id co-locates the pair while the cache
    // itself still spreads, which is the property a cache must keep.
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ['"value"']);
    const entries = cache<string>(client, {
      ttlMs: 1000,
      codec: codecs.json()
    });
    await entries.peek("42");
    const entryKey = commands[0][1] as string;
    expect(entryKey).toBe("cache:{42}");
    expect(slotOf(entryKey)).toBe(slotOf("cache:lock:{42}"));
    // Different ids still land on different slots: the cache stays spread.
    expect(slotOf("cache:{42}")).not.toBe(slotOf("cache:{43}"));
  });

  it("keeps every budget key for one id in a single slot", () => {
    // budget touches three keys per id in one script (two window buckets and
    // the reservation set), so they must co-locate or the script is illegal
    // on a cluster. Different ids still spread.
    const bucket = 29_753;
    const keys = [
      `budget:{u1}:${bucket}`,
      `budget:{u1}:${bucket - 1}`,
      "budget:{u1}:holds"
    ];
    expect(new Set(keys.map(slotOf)).size).toBe(1);
    expect(slotOf("budget:{u1}:holds")).not.toBe(slotOf("budget:{u2}:holds"));
  });

  it("keeps every Next.js cache key in one slot", () => {
    // revalidateTag DELs entries and tag sets in one command, so they must
    // share a slot or the handler is simply broken on a cluster.
    expect(slotOf("{next-cache}:entry:/blog")).toBe(
      slotOf("{next-cache}:tag:posts")
    );
  });
});

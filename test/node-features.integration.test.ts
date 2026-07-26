import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createBitmapStore, defineBitmap } from "../src/core/bitmap.js";
import { codecs } from "../src/core/codecs.js";
import { createCounterStore } from "../src/core/counter.js";
import { createGeoStore, defineGeoSet } from "../src/core/geo.js";
import { createHashStore } from "../src/core/hash.js";
import {
  createHyperLogLogStore,
  defineHyperLogLog
} from "../src/core/hyperloglog.js";
import { createKeyValueStore } from "../src/core/key-value.js";
import { createListStore } from "../src/core/list.js";
import {
  scanHash,
  scanKeys,
  scanKeyspace,
  scanSet,
  scanSortedSet
} from "../src/core/scan.js";
import {
  defineHash,
  defineKeyspace,
  defineList,
  defineSet,
  defineSortedSet
} from "../src/core/schemas.js";
import { createScriptRunner, defineScript } from "../src/core/script.js";
import { createSetStore } from "../src/core/set.js";
import { createSortedSetStore } from "../src/core/sorted-set.js";
import { createStreamStore, defineStream } from "../src/core/stream.js";
import { createStringStore } from "../src/core/string.js";
import {
  booleanNumberReply,
  createTransaction,
  numberReply,
  okReply,
  stringOrNullReply,
  stringReply
} from "../src/core/transaction.js";
import type { RedisClient } from "../src/core/types.js";
import { type BeniSession, beni } from "../src/database.js";
import { WatchRetriesExceededError } from "../src/index.js";
import { node } from "../src/node/index.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

const runPrefix = `beni:feat:${Date.now()}:${Math.random()
  .toString(36)
  .slice(2)}`;

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) values.push(value);
  return values;
}

async function deleteMatching(
  client: RedisClient,
  pattern: string
): Promise<void> {
  let cursor = "0";
  do {
    const reply = await client.send([
      "SCAN",
      cursor,
      "MATCH",
      pattern,
      "COUNT",
      200
    ]);
    if (!Array.isArray(reply) || reply.length !== 2) {
      throw new TypeError("Expected Redis SCAN to return [cursor, keys]");
    }
    cursor = String(reply[0]);
    const keys = reply[1];
    if (Array.isArray(keys) && keys.length > 0) {
      await client.send(["DEL", ...keys.map((key) => String(key))]);
    }
  } while (cursor !== "0");
}

describeRedis("node feature modules against real Redis", () => {
  let client: RedisClient;
  let db: ReturnType<typeof beni>;

  beforeAll(async () => {
    expect(redisUrl).toBeDefined();
    client = await node({ url: redisUrl });
    db = beni(client);
  });

  afterAll(async () => {
    try {
      await deleteMatching(client, `${runPrefix}:*`);
    } finally {
      await client.close();
    }
  });

  describe("hash store extensions", () => {
    const users = defineHash(`${runPrefix}:user`, {
      name: codecs.string(),
      score: codecs.number()
    });

    afterAll(async () => {
      await client.send([
        "DEL",
        users.key("main"),
        users.key("fresh"),
        users.key("modes"),
        users.key("lifecycle")
      ]);
    });

    it("reads, writes, and expires hash fields", async () => {
      const userStore = createHashStore(client, users);

      await expect(userStore.hgetall("main")).resolves.toBeNull();

      await userStore.hset("main", { name: "beni", score: 5 });
      await client.send(["HSET", users.key("main"), "undeclared", "ignored"]);

      await expect(userStore.hgetall("main")).resolves.toEqual({
        name: "beni",
        score: 5
      });
      await expect(userStore.hmget("main", ["name", "score"])).resolves.toEqual(
        {
          name: "beni",
          score: 5
        }
      );
      await expect(userStore.hmget("main", [])).resolves.toEqual({});

      const names = await userStore.hkeys("main");
      expect(names).toHaveLength(3);
      expect(names).toEqual(
        expect.arrayContaining(["name", "score", "undeclared"])
      );
      await expect(userStore.hlen("main")).resolves.toBe(3);

      const singleField = await userStore.hrandfield("main");
      expect(names).toContain(singleField);
      await expect(userStore.hrandfield("absent")).resolves.toBeNull();
      const distinctFields = await userStore.hrandfield("main", { count: 3 });
      expect(new Set(distinctFields).size).toBe(3);
      const repeatedFields = await userStore.hrandfield("main", { count: -5 });
      expect(repeatedFields).toHaveLength(5);

      await expect(userStore.hsetnx("main", "name", "other")).resolves.toBe(
        false
      );
      await expect(userStore.hget("main", "name")).resolves.toBe("beni");

      await expect(userStore.hstrlen("main", "name")).resolves.toBe(4);

      await expect(userStore.hincrbyfloat("main", "score", 2.5)).resolves.toBe(
        7.5
      );
      await expect(userStore.hget("main", "score")).resolves.toBe(7.5);

      await expect(userStore.hexpire("main", ["name"], 120)).resolves.toEqual([
        1
      ]);
      const nameTtl = await userStore.httl("main", "name");
      expect(nameTtl).toBeGreaterThan(0);
      expect(nameTtl).toBeLessThanOrEqual(120);
      await expect(userStore.httl("main", "score")).resolves.toBe(-1);

      await expect(
        userStore.hpersist("main", ["name", "score"])
      ).resolves.toEqual([1, -1]);
      await expect(userStore.httl("main", "name")).resolves.toBe(-1);
      await expect(userStore.hpersist("main", [])).resolves.toEqual([]);

      await expect(userStore.hexpire("main", [], 60)).resolves.toEqual([]);

      await expect(userStore.hsetnx("fresh", "name", "first")).resolves.toBe(
        true
      );
      await expect(
        userStore.hmget("fresh", ["name", "score"])
      ).resolves.toEqual({
        name: "first",
        score: null
      });
    });

    it("reads, extends, and clears a whole-record TTL via the lifecycle ops", async () => {
      const userStore = createHashStore(client, users);

      await userStore.hset(
        "lifecycle",
        { name: "beni", score: 1 },
        { ttlSeconds: 120 }
      );

      const ttl = await userStore.ttl("lifecycle");
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(120);

      await expect(userStore.expire("lifecycle", 300)).resolves.toBe(true);
      await expect(userStore.persist("lifecycle")).resolves.toBe(true);
      await expect(userStore.ttl("lifecycle")).resolves.toBe(-1);
      await expect(userStore.exists("lifecycle")).resolves.toBe(true);
    });

    it("sets, reads, and expires fields via HSETEX/HGETEX/HGETDEL and TTL modes", async () => {
      const userStore = createHashStore(client, users);

      await expect(
        userStore.hsetex(
          "modes",
          { name: "ada", score: 1 },
          { ttlSeconds: 120 }
        )
      ).resolves.toBe(true);
      // FNX: nothing set because the fields already exist.
      await expect(
        userStore.hsetex("modes", { name: "grace" }, { fnx: true })
      ).resolves.toBe(false);

      const ttlMs = await userStore.httl("modes", "name", {
        milliseconds: true
      });
      expect(ttlMs).toBeGreaterThan(0);
      expect(ttlMs).toBeLessThanOrEqual(120_000);
      await expect(
        userStore.hexpiretime("modes", "name")
      ).resolves.toBeGreaterThan(0);

      // HGETEX + PERSIST reads the values and clears the field TTL.
      await expect(
        userStore.hgetex("modes", ["name", "score"], { persist: true })
      ).resolves.toEqual({ name: "ada", score: 1 });
      await expect(userStore.httl("modes", "name")).resolves.toBe(-1);

      // HPEXPIRE relative-milliseconds sets a fresh TTL.
      await expect(
        userStore.hexpire("modes", ["name"], { ttlMilliseconds: 60_000 })
      ).resolves.toEqual([1]);
      expect(await userStore.httl("modes", "name")).toBeGreaterThan(0);

      // HGETDEL reads and removes the fields.
      await expect(
        userStore.hgetdel("modes", ["name", "score"])
      ).resolves.toEqual({ name: "ada", score: 1 });
      await expect(userStore.hgetall("modes")).resolves.toBeNull();
      await expect(userStore.hgetdel("modes", ["name"])).resolves.toEqual({
        name: null
      });
    });
  });

  describe("key-value, string, and counter extensions", () => {
    const profiles = defineKeyspace(
      `${runPrefix}:profile`,
      codecs.json<{ name: string; hits: number }>()
    );
    const texts = defineKeyspace(`${runPrefix}:text`, codecs.string());
    const counters = defineKeyspace(`${runPrefix}:counter`, codecs.number());

    afterAll(async () => {
      await client.send([
        "DEL",
        profiles.key("nx"),
        profiles.key("xx"),
        profiles.key("keep"),
        profiles.key("m:a"),
        profiles.key("m:b"),
        profiles.key("m:c"),
        profiles.key("m:d"),
        texts.key("greeting"),
        texts.key("absent"),
        texts.key("lcs:a"),
        texts.key("lcs:b"),
        counters.key("float")
      ]);
    });

    it("sets conditionally and keeps TTLs", async () => {
      const kv = createKeyValueStore(client, profiles);

      await expect(
        kv.set("nx", { name: "first", hits: 1 }, { nx: true, ttlSeconds: 90 })
      ).resolves.toBe(true);
      await expect(
        kv.set("nx", { name: "second", hits: 2 }, { nx: true })
      ).resolves.toBe(false);
      await expect(kv.get("nx")).resolves.toEqual({ name: "first", hits: 1 });
      await expect(kv.ttl("nx")).resolves.toBeGreaterThan(0);

      await expect(
        kv.set("xx", { name: "ghost", hits: 0 }, { xx: true })
      ).resolves.toBe(false);
      await expect(kv.exists("xx")).resolves.toBe(false);
      await kv.set("xx", { name: "base", hits: 1 });
      await expect(
        kv.set(
          "xx",
          { name: "replaced", hits: 2 },
          { xx: true, ttlSeconds: 90 }
        )
      ).resolves.toBe(true);
      await expect(kv.get("xx")).resolves.toEqual({
        name: "replaced",
        hits: 2
      });
      await expect(kv.ttl("xx")).resolves.toBeGreaterThan(0);

      await kv.set("keep", { name: "keep", hits: 1 }, { ttlSeconds: 120 });
      await kv.set("keep", { name: "kept", hits: 2 }, { keepTtl: true });
      await expect(kv.get("keep")).resolves.toEqual({ name: "kept", hits: 2 });
      const keptTtl = await kv.ttl("keep");
      expect(keptTtl).toBeGreaterThan(0);
      expect(keptTtl).toBeLessThanOrEqual(120);
      await kv.set("keep", { name: "reset", hits: 3 });
      await expect(kv.ttl("keep")).resolves.toBe(-1);
    });

    it("writes many keys only when all are absent", async () => {
      const kv = createKeyValueStore(client, profiles);

      await expect(
        kv.msetnx([
          ["m:a", { name: "a", hits: 1 }],
          ["m:b", { name: "b", hits: 2 }]
        ])
      ).resolves.toBe(true);
      await expect(
        kv.msetnx([
          ["m:b", { name: "clobbered", hits: 9 }],
          ["m:c", { name: "c", hits: 3 }]
        ])
      ).resolves.toBe(false);
      await expect(kv.mget(["m:a", "m:b", "m:c"])).resolves.toEqual([
        { name: "a", hits: 1 },
        { name: "b", hits: 2 },
        null
      ]);
      await expect(
        kv.msetnx(new Map([["m:d", { name: "d", hits: 4 }]]))
      ).resolves.toBe(true);
      await expect(kv.get("m:d")).resolves.toEqual({ name: "d", hits: 4 });
      await expect(kv.msetnx([])).resolves.toBe(true);
    });

    it("supports every GETEX expiry mode", async () => {
      const strings = createStringStore(client, texts);
      const key = texts.key("greeting");
      await client.send(["SET", key, "hello"]);

      await expect(strings.getex("greeting", 90)).resolves.toBe("hello");
      await expect(client.send(["TTL", key])).resolves.toBeGreaterThan(0);

      await expect(strings.getex("greeting", { persist: true })).resolves.toBe(
        "hello"
      );
      await expect(client.send(["TTL", key])).resolves.toBe(-1);

      await expect(strings.getex("greeting", { ttlSeconds: 90 })).resolves.toBe(
        "hello"
      );
      const ttl = await client.send(["TTL", key]);
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(90);

      await expect(
        strings.getex("greeting", { ttlMilliseconds: 90_000 })
      ).resolves.toBe("hello");
      const pttl = await client.send(["PTTL", key]);
      expect(pttl).toBeGreaterThan(0);
      expect(pttl).toBeLessThanOrEqual(90_000);

      await expect(
        strings.getex("greeting", {
          expireAtSeconds: Math.floor(Date.now() / 1000) + 300
        })
      ).resolves.toBe("hello");
      const atTtl = await client.send(["TTL", key]);
      expect(atTtl).toBeGreaterThan(0);
      // Allow one second of clock skew between this host and the server.
      expect(atTtl).toBeLessThanOrEqual(301);

      await expect(
        strings.getex("greeting", {
          expireAtMilliseconds: Date.now() + 300_000
        })
      ).resolves.toBe("hello");
      const atPttl = await client.send(["PTTL", key]);
      expect(atPttl).toBeGreaterThan(0);
      // Allow one second of clock skew between this host and the server.
      expect(atPttl).toBeLessThanOrEqual(301_000);

      await expect(
        strings.getex("absent", { ttlSeconds: 60 })
      ).resolves.toBeNull();
    });

    it("computes the longest common subsequence", async () => {
      const strings = createStringStore(client, texts);
      await client.send(["SET", texts.key("lcs:a"), "ohmytext"]);
      await client.send(["SET", texts.key("lcs:b"), "mynewtext"]);

      await expect(strings.lcs("lcs:a", "lcs:b")).resolves.toBe("mytext");
      await expect(strings.lcs("lcs:a", "lcs:b", { len: true })).resolves.toBe(
        6
      );

      const idx = await strings.lcs("lcs:a", "lcs:b", {
        idx: true,
        minMatchLen: 4,
        withMatchLen: true
      });
      expect(idx.length).toBe(6);
      expect(idx.matches).toEqual([{ a: [4, 7], b: [5, 8], length: 4 }]);
    });

    it("increments counters by float amounts", async () => {
      const counterStore = createCounterStore(client, counters);

      await expect(counterStore.incrbyfloat("float", 1.5)).resolves.toBe(1.5);
      await expect(counterStore.incrbyfloat("float", 2.25)).resolves.toBe(3.75);
      await expect(counterStore.incrbyfloat("float", -0.75)).resolves.toBe(3);
    });
  });

  describe("sorted set extensions", () => {
    const boards = defineSortedSet(`${runPrefix}:z`, codecs.string());
    const allMembers = ["alice", "bob", "carol", "dave"];

    afterAll(async () => {
      await client.send([
        "DEL",
        boards.key("lead"),
        boards.key("empty"),
        boards.key("src"),
        boards.key("dest-rank"),
        boards.key("dest-score"),
        boards.key("dest-rev"),
        boards.key("pop"),
        boards.key("x"),
        boards.key("y"),
        boards.key("dest-union"),
        boards.key("dest-inter"),
        boards.key("dest-diff"),
        boards.key("lex"),
        boards.key("lex-store")
      ]);
    });

    it("reads ranks, scores, random members, and score ranges", async () => {
      const zStore = createSortedSetStore(client, boards);
      await zStore.zadd("lead", [
        { member: "alice", score: 10 },
        { member: "bob", score: 20 },
        { member: "carol", score: 30 },
        { member: "dave", score: 40 }
      ]);

      await expect(zStore.zrevrank("lead", "alice")).resolves.toBe(3);
      await expect(zStore.zrevrank("lead", "zoe")).resolves.toBeNull();

      await expect(
        zStore.zmscore("lead", ["alice", "zoe", "carol"])
      ).resolves.toEqual([10, null, 30]);
      await expect(zStore.zmscore("lead", [])).resolves.toEqual([]);

      const single = await zStore.zrandmember("lead");
      expect(allMembers).toContain(single);
      await expect(zStore.zrandmember("empty")).resolves.toBeNull();

      const distinct = await zStore.zrandmember("lead", { count: 3 });
      expect(distinct).toHaveLength(3);
      expect(new Set(distinct).size).toBe(3);
      for (const member of distinct) expect(allMembers).toContain(member);

      const repeatable = await zStore.zrandmember("lead", { count: -6 });
      expect(repeatable).toHaveLength(6);
      for (const member of repeatable) expect(allMembers).toContain(member);

      await expect(
        zStore.zrange("lead", { start: 0, stop: 1, rev: true })
      ).resolves.toEqual(["dave", "carol"]);

      await expect(
        zStore.zrange("lead", { byScore: true, min: 15, max: 35 })
      ).resolves.toEqual(["bob", "carol"]);
      await expect(
        zStore.zrange("lead", { byScore: true, min: 15, max: 35, rev: true })
      ).resolves.toEqual(["carol", "bob"]);
      await expect(
        zStore.zrange("lead", {
          byScore: true,
          min: "-inf",
          max: "+inf",
          offset: 1,
          count: 2
        })
      ).resolves.toEqual(["bob", "carol"]);
      await expect(
        zStore.zrange("lead", { byScore: true, min: "(10", max: 30 })
      ).resolves.toEqual(["bob", "carol"]);
      await expect(
        zStore.zrange("lead", {
          byScore: true,
          withScores: true,
          min: 15,
          max: "+inf"
        })
      ).resolves.toEqual([
        { member: "bob", score: 20 },
        { member: "carol", score: 30 },
        { member: "dave", score: 40 }
      ]);
    });

    it("stores ranges and removes by rank and score", async () => {
      const zStore = createSortedSetStore(client, boards);
      await zStore.zadd("src", [
        { member: "alice", score: 10 },
        { member: "bob", score: 20 },
        { member: "carol", score: 30 },
        { member: "dave", score: 40 }
      ]);

      await expect(
        zStore.zrangestore("dest-rank", "src", { start: 0, stop: 1 })
      ).resolves.toBe(2);
      await expect(
        zStore.zrange("dest-rank", { start: 0, stop: -1 })
      ).resolves.toEqual(["alice", "bob"]);

      await expect(
        zStore.zrangestore("dest-score", "src", {
          byScore: true,
          min: "(20",
          max: "+inf"
        })
      ).resolves.toBe(2);
      await expect(
        zStore.zrange("dest-score", { start: 0, stop: -1 })
      ).resolves.toEqual(["carol", "dave"]);

      await expect(
        zStore.zrangestore("dest-rev", "src", {
          byScore: true,
          min: "-inf",
          max: "+inf",
          rev: true,
          offset: 0,
          count: 2
        })
      ).resolves.toBe(2);
      await expect(
        zStore.zrange("dest-rev", { start: 0, stop: -1 })
      ).resolves.toEqual(["carol", "dave"]);

      await expect(zStore.zremrangebyrank("src", 0, 1)).resolves.toBe(2);
      await expect(
        zStore.zrange("src", { start: 0, stop: -1 })
      ).resolves.toEqual(["carol", "dave"]);
      await expect(zStore.zremrangebyscore("src", "(30", "+inf")).resolves.toBe(
        1
      );
      await expect(
        zStore.zrange("src", { start: 0, stop: -1 })
      ).resolves.toEqual(["carol"]);
    });

    it("pops many entries from both ends", async () => {
      const zStore = createSortedSetStore(client, boards);
      await zStore.zadd("pop", [
        { member: "a", score: 1 },
        { member: "b", score: 2 },
        { member: "c", score: 3 },
        { member: "d", score: 4 }
      ]);

      await expect(zStore.zpopmin("pop", { count: 0 })).resolves.toEqual([]);
      await expect(zStore.zpopmax("pop", { count: 0 })).resolves.toEqual([]);
      await expect(zStore.zpopmin("pop", { count: 2 })).resolves.toEqual([
        { member: "a", score: 1 },
        { member: "b", score: 2 }
      ]);
      await expect(zStore.zpopmax("pop", { count: 2 })).resolves.toEqual([
        { member: "d", score: 4 },
        { member: "c", score: 3 }
      ]);
      await expect(zStore.zcard("pop")).resolves.toBe(0);
    });

    it("combines sorted sets with union, intersection, and difference", async () => {
      const zStore = createSortedSetStore(client, boards);
      await zStore.zadd("x", [
        { member: "a", score: 1 },
        { member: "b", score: 2 },
        { member: "c", score: 3 }
      ]);
      await zStore.zadd("y", [
        { member: "b", score: 10 },
        { member: "c", score: 20 },
        { member: "d", score: 30 }
      ]);

      await expect(zStore.zunion("x", ["y"])).resolves.toEqual([
        "a",
        "b",
        "c",
        "d"
      ]);
      await expect(
        zStore.zunion("x", ["y"], { withScores: true, aggregate: "max" })
      ).resolves.toEqual([
        { member: "a", score: 1 },
        { member: "b", score: 10 },
        { member: "c", score: 20 },
        { member: "d", score: 30 }
      ]);
      await expect(
        zStore.zunion("x", ["y"], { withScores: true, weights: [2, 1] })
      ).resolves.toEqual([
        { member: "a", score: 2 },
        { member: "b", score: 14 },
        { member: "c", score: 26 },
        { member: "d", score: 30 }
      ]);

      await expect(zStore.zinter("x", ["y"])).resolves.toEqual(["b", "c"]);
      await expect(
        zStore.zinter("x", ["y"], { withScores: true, aggregate: "min" })
      ).resolves.toEqual([
        { member: "b", score: 2 },
        { member: "c", score: 3 }
      ]);

      await expect(zStore.zdiff("x", ["y"])).resolves.toEqual(["a"]);
      await expect(
        zStore.zdiff("x", ["y"], { withScores: true })
      ).resolves.toEqual([{ member: "a", score: 1 }]);

      await expect(zStore.zunionstore("dest-union", "x", ["y"])).resolves.toBe(
        4
      );
      await expect(
        zStore.zrange("dest-union", { start: 0, stop: -1 })
      ).resolves.toEqual(["a", "b", "c", "d"]);
      await expect(
        zStore.zinterstore("dest-inter", "x", ["y"], {
          aggregate: "max"
        })
      ).resolves.toBe(2);
      await expect(zStore.zscore("dest-inter", "b")).resolves.toBe(10);
      await expect(zStore.zdiffstore("dest-diff", "x", ["y"])).resolves.toBe(1);
      await expect(
        zStore.zrange("dest-diff", { start: 0, stop: -1 })
      ).resolves.toEqual(["a"]);

      await expect(zStore.zintercard("x", ["y"])).resolves.toBe(2);
      await expect(zStore.zintercard("x", ["y"], { limit: 1 })).resolves.toBe(
        1
      );
    });

    it("reads, counts, stores, and removes lexicographic ranges", async () => {
      const zStore = createSortedSetStore(client, boards);
      await zStore.zadd("lex", [
        { member: "apple", score: 0 },
        { member: "banana", score: 0 },
        { member: "cherry", score: 0 },
        { member: "date", score: 0 }
      ]);

      await expect(
        zStore.zrange("lex", {
          byLex: true,
          min: { value: "apple" },
          max: { value: "cherry", inclusive: false }
        })
      ).resolves.toEqual(["apple", "banana"]);
      await expect(
        zStore.zrange("lex", { byLex: true, min: "-", max: "+", rev: true })
      ).resolves.toEqual(["date", "cherry", "banana", "apple"]);
      await expect(
        zStore.zrange("lex", {
          byLex: true,
          min: "-",
          max: "+",
          offset: 1,
          count: 2
        })
      ).resolves.toEqual(["banana", "cherry"]);

      await expect(zStore.zlexcount("lex", "-", "+")).resolves.toBe(4);
      await expect(
        zStore.zlexcount("lex", { value: "b" }, { value: "d" })
      ).resolves.toBe(2);

      await expect(
        zStore.zrangestore("lex-store", "lex", {
          byLex: true,
          min: "-",
          max: { value: "cherry" }
        })
      ).resolves.toBe(3);
      await expect(
        zStore.zrange("lex-store", { start: 0, stop: -1 })
      ).resolves.toEqual(["apple", "banana", "cherry"]);

      await expect(
        zStore.zremrangebylex("lex", { value: "date" }, "+")
      ).resolves.toBe(1);
      await expect(
        zStore.zrange("lex", { start: 0, stop: -1 })
      ).resolves.toEqual(["apple", "banana", "cherry"]);
    });
  });

  describe("list extensions", () => {
    const tasks = defineList(`${runPrefix}:list`, codecs.string());

    afterAll(async () => {
      await client.send(["DEL", tasks.key("main"), tasks.key("missing")]);
    });

    it("inserts, locates, and pops many list values", async () => {
      const listStore = createListStore(client, tasks);

      await expect(listStore.lpushx("main", ["nope"])).resolves.toBe(0);
      await expect(listStore.rpushx("main", ["nope"])).resolves.toBe(0);
      await expect(client.send(["EXISTS", tasks.key("main")])).resolves.toBe(0);

      await expect(listStore.rpush("main", ["a", "b", "c", "b"])).resolves.toBe(
        4
      );

      await expect(
        listStore.linsert("main", "b", "x", { position: "before" })
      ).resolves.toBe(5);
      await expect(
        listStore.linsert("main", "c", "y", { position: "after" })
      ).resolves.toBe(6);
      await expect(listStore.lrange("main", 0, -1)).resolves.toEqual([
        "a",
        "x",
        "b",
        "c",
        "y",
        "b"
      ]);
      await expect(
        listStore.linsert("main", "zz", "q", { position: "before" })
      ).resolves.toBe(-1);
      await expect(
        listStore.linsert("missing", "a", "q", { position: "after" })
      ).resolves.toBe(0);

      await expect(listStore.lpos("main", "b")).resolves.toBe(2);
      await expect(listStore.lpos("main", "b", { rank: -1 })).resolves.toBe(5);
      await expect(listStore.lpos("main", "zz")).resolves.toBeNull();
      await expect(listStore.lpos("main", "b", { count: 0 })).resolves.toEqual([
        2, 5
      ]);
      await expect(
        listStore.lpos("main", "b", { count: 1, rank: -1 })
      ).resolves.toEqual([5]);
      await expect(listStore.lpos("main", "zz", { count: 0 })).resolves.toEqual(
        []
      );

      await expect(listStore.lpop("main", { count: 2 })).resolves.toEqual([
        "a",
        "x"
      ]);
      await expect(listStore.rpop("main", { count: 2 })).resolves.toEqual([
        "b",
        "y"
      ]);
      await expect(listStore.lpop("missing", { count: 2 })).resolves.toEqual(
        []
      );
      await expect(listStore.rpop("missing", { count: 2 })).resolves.toEqual(
        []
      );

      await expect(listStore.lpushx("main", ["z"])).resolves.toBe(3);
      await expect(listStore.rpushx("main", ["w"])).resolves.toBe(4);
      await expect(listStore.lrange("main", 0, -1)).resolves.toEqual([
        "z",
        "b",
        "c",
        "w"
      ]);
      await expect(listStore.lpushx("main", [])).resolves.toBe(0);
      await expect(listStore.rpushx("main", [])).resolves.toBe(0);
    });
  });

  describe("scan iterators", () => {
    const scanSpace = defineKeyspace(`${runPrefix}:scan:key`, codecs.string());
    const tags = defineSet(`${runPrefix}:scan:set`, codecs.number());
    const meta = defineHash(`${runPrefix}:scan:hash`, {
      name: codecs.string(),
      score: codecs.number()
    });
    const board = defineSortedSet(`${runPrefix}:scan:zset`, codecs.string());
    const keyIds = Array.from({ length: 25 }, (_, index) => `k${index}`);

    afterAll(async () => {
      await client.send([
        "DEL",
        ...keyIds.map((id) => scanSpace.key(id)),
        tags.key("members"),
        meta.key("main"),
        board.key("all")
      ]);
    });

    it("scans keys and keyspaces page by page", async () => {
      const kv = createKeyValueStore(client, scanSpace);
      await kv.mset(keyIds.map((id): [string, string] => [id, `value-${id}`]));
      const expectedKeys = keyIds.map((id) => String(scanSpace.key(id))).sort();

      const scanned = await collect(
        scanKeys(client, { match: `${scanSpace.prefix}:*`, count: 5 })
      );
      expect([...scanned].sort()).toEqual(expectedKeys);

      const typed = await collect(
        scanKeys(client, {
          match: `${scanSpace.prefix}:*`,
          count: 5,
          type: "string"
        })
      );
      expect([...typed].sort()).toEqual(expectedKeys);

      const viaKeyspace = await collect(
        scanKeyspace(client, scanSpace, { count: 7 })
      );
      expect([...viaKeyspace].sort()).toEqual(expectedKeys);
    });

    it("scans set members and decodes them", async () => {
      const setStore = createSetStore(client, tags);
      const numbers = Array.from({ length: 30 }, (_, index) => index);
      await setStore.sadd("members", numbers);

      const scanned = await collect(
        scanSet(client, tags, "members", { count: 5 })
      );
      expect([...scanned].sort((a, b) => a - b)).toEqual(numbers);

      const matched = await collect(
        scanSet(client, tags, "members", { match: "1*", count: 5 })
      );
      expect([...matched].sort((a, b) => a - b)).toEqual([
        1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
      ]);
    });

    it("scans hash entries and skips undeclared fields", async () => {
      const hashStore = createHashStore(client, meta);
      await hashStore.hset("main", { name: "beni", score: 7 });
      await client.send(["HSET", meta.key("main"), "undeclared", "ignored"]);

      const entries = await collect(
        scanHash(client, meta, "main", { count: 5 })
      );
      expect(entries).toHaveLength(2);
      const byField = new Map(
        entries.map((entry) => [entry.field, entry.value] as const)
      );
      expect(byField.get("name")).toBe("beni");
      expect(byField.get("score")).toBe(7);
    });

    it("scans sorted set entries with scores", async () => {
      const zStore = createSortedSetStore(client, board);
      const entries = Array.from({ length: 30 }, (_, index) => ({
        member: `m${index}`,
        score: index
      }));
      await zStore.zadd("all", entries);

      const scanned = await collect(
        scanSortedSet(client, board, "all", { count: 5 })
      );
      expect(scanned).toHaveLength(30);
      expect([...scanned].sort((a, b) => a.score - b.score)).toEqual(entries);
    });
  });

  describe("typed transactions", () => {
    const valueKey = `${runPrefix}:tx:value`;
    const counterKey = `${runPrefix}:tx:counter`;
    const missingKey = `${runPrefix}:tx:missing`;
    const immutableKey = `${runPrefix}:tx:immutable`;

    afterAll(async () => {
      await client.send([
        "DEL",
        valueKey,
        counterKey,
        missingKey,
        immutableKey
      ]);
    });

    it("runs queued commands atomically with typed decoders", async () => {
      const results = await createTransaction(client)
        .add(["SET", valueKey, "hello"], okReply)
        .add(["GET", valueKey], stringReply)
        .add(["INCR", counterKey], numberReply)
        .add(["EXISTS", valueKey], booleanNumberReply)
        .add(["EXISTS", missingKey], booleanNumberReply)
        .add(["GET", missingKey], stringOrNullReply)
        .exec();

      expect(results).toEqual([undefined, "hello", 1, true, false, null]);
      await expect(client.send(["GET", valueKey])).resolves.toBe("hello");
    });

    it("keeps builders immutable and resolves empty transactions locally", async () => {
      await expect(createTransaction(client).exec()).resolves.toEqual([]);

      const base = createTransaction(client).add(
        ["INCR", immutableKey],
        numberReply
      );
      const extended = base.add(["INCR", immutableKey], numberReply);

      await expect(base.exec()).resolves.toEqual([1]);
      await expect(extended.exec()).resolves.toEqual([2, 3]);
      await expect(base.exec()).resolves.toEqual([4]);
    });
  });

  describe("script runner", () => {
    const scriptKey = `${runPrefix}:script:counter`;

    afterAll(async () => {
      await client.send(["DEL", scriptKey]);
    });

    it("loads scripts once and reuses the cached EVALSHA digest", async () => {
      const incrementBy = defineScript<[number], number>({
        lua: "return redis.call('INCRBY', KEYS[1], ARGV[1])",
        keyCount: 1,
        decode(reply) {
          if (typeof reply !== "number") {
            throw new TypeError("Expected Lua INCRBY to return number");
          }
          return reply;
        }
      });
      const runner = createScriptRunner(client);

      await expect(runner.run(incrementBy, [scriptKey], [5])).resolves.toBe(5);
      await expect(runner.run(incrementBy, [scriptKey], [7])).resolves.toBe(12);

      await expect(runner.run(incrementBy, [], [1])).rejects.toThrow(
        "Expected 1 script keys but received 0"
      );

      await client.send(["SCRIPT", "FLUSH"]);
      await expect(runner.run(incrementBy, [scriptKey], [3])).resolves.toBe(15);
    });
  });

  describe("bitmap store", () => {
    const flags = defineBitmap(`${runPrefix}:bitmap`);

    afterAll(async () => {
      await client.send([
        "DEL",
        flags.key("flags"),
        flags.key("a"),
        flags.key("b"),
        flags.key("and"),
        flags.key("or"),
        flags.key("xor"),
        flags.key("not")
      ]);
    });

    it("sets, reads, counts, and locates bits", async () => {
      const bitmapStore = createBitmapStore(client, flags);

      await expect(bitmapStore.setbit("flags", 7, true)).resolves.toBe(false);
      await expect(bitmapStore.setbit("flags", 7, true)).resolves.toBe(true);
      await expect(bitmapStore.getbit("flags", 7)).resolves.toBe(true);
      await expect(bitmapStore.getbit("flags", 3)).resolves.toBe(false);

      for (const offset of [0, 1, 2]) {
        await bitmapStore.setbit("flags", offset, true);
      }

      await expect(bitmapStore.bitcount("flags")).resolves.toBe(4);
      await expect(
        bitmapStore.bitcount("flags", { start: 0, end: 0 })
      ).resolves.toBe(4);
      await expect(
        bitmapStore.bitcount("flags", { start: 0, end: 2, unit: "BIT" })
      ).resolves.toBe(3);

      await expect(bitmapStore.bitpos("flags", true)).resolves.toBe(0);
      await expect(bitmapStore.bitpos("flags", false)).resolves.toBe(3);
      await expect(
        bitmapStore.bitpos("flags", true, { start: 1 })
      ).resolves.toBeNull();
      await expect(
        bitmapStore.bitpos("flags", true, { start: 0, end: 0 })
      ).resolves.toBe(0);
      await expect(
        bitmapStore.bitpos("flags", false, { start: 0, end: 2, unit: "BIT" })
      ).resolves.toBeNull();
    });

    it("combines bitmaps with BITOP", async () => {
      const bitmapStore = createBitmapStore(client, flags);
      await bitmapStore.setbit("a", 0, true);
      await bitmapStore.setbit("a", 1, true);
      await bitmapStore.setbit("b", 1, true);
      await bitmapStore.setbit("b", 2, true);

      await expect(bitmapStore.bitop("and", "AND", ["a", "b"])).resolves.toBe(
        1
      );
      await expect(bitmapStore.bitcount("and")).resolves.toBe(1);
      await expect(bitmapStore.getbit("and", 1)).resolves.toBe(true);

      await expect(bitmapStore.bitop("or", "OR", ["a", "b"])).resolves.toBe(1);
      await expect(bitmapStore.bitcount("or")).resolves.toBe(3);

      await expect(bitmapStore.bitop("xor", "XOR", ["a", "b"])).resolves.toBe(
        1
      );
      await expect(bitmapStore.bitcount("xor")).resolves.toBe(2);
      await expect(bitmapStore.getbit("xor", 0)).resolves.toBe(true);
      await expect(bitmapStore.getbit("xor", 1)).resolves.toBe(false);

      await expect(bitmapStore.bitop("not", "NOT", ["a"])).resolves.toBe(1);
      await expect(bitmapStore.bitcount("not")).resolves.toBe(6);

      await expect(bitmapStore.del("a")).resolves.toBe(1);
    });

    it("reads and writes packed fields with BITFIELD", async () => {
      const bitmapStore = createBitmapStore(client, flags);

      // SET returns the previous value (0); GET reads the stored one back.
      const [previous, current] = await bitmapStore
        .bitfield("packed")
        .set("u8", 0, 200)
        .get("u8", 0)
        .exec();
      expect(previous).toBe(0);
      expect(current).toBe(200);

      // SAT overflow clamps at the unsigned 8-bit max instead of wrapping.
      const [saturated] = await bitmapStore
        .bitfield("packed")
        .overflow("sat")
        .incrby("u8", 0, 100)
        .exec();
      expect(saturated).toBe(255);

      // FAIL overflow returns null and leaves the field unchanged.
      const [failed] = await bitmapStore
        .bitfield("packed")
        .overflow("fail")
        .incrby("u8", 0, 100)
        .exec();
      expect(failed).toBeNull();

      // A positionally-addressed field (#1 = bit offset 8) is independent.
      const [second] = await bitmapStore
        .bitfield("packed")
        .incrby("u8", "#1", 5)
        .exec();
      expect(second).toBe(5);

      await expect(bitmapStore.del("packed")).resolves.toBe(1);
    });
  });

  describe("hyperloglog store", () => {
    const visitors = defineHyperLogLog(`${runPrefix}:hll`, codecs.string());

    afterAll(async () => {
      await client.send([
        "DEL",
        visitors.key("page-a"),
        visitors.key("page-b"),
        visitors.key("merged"),
        visitors.key("empty")
      ]);
    });

    it("adds, counts, merges, and deletes", async () => {
      const hll = createHyperLogLogStore(client, visitors);

      await expect(hll.pfadd("page-a", ["u1", "u2", "u3"])).resolves.toBe(true);
      await expect(hll.pfadd("page-a", ["u1"])).resolves.toBe(false);
      await expect(hll.pfcount("page-a")).resolves.toBe(3);

      await expect(hll.pfadd("page-b", ["u3", "u4"])).resolves.toBe(true);
      await expect(hll.pfcount(["page-a", "page-b"])).resolves.toBe(4);

      await hll.pfmerge("merged", ["page-a", "page-b"]);
      await expect(hll.pfcount("merged")).resolves.toBe(4);

      await expect(hll.pfadd("empty", [])).resolves.toBe(false);
      await expect(hll.pfcount("empty")).resolves.toBe(0);

      await expect(hll.del("merged")).resolves.toBe(1);
    });
  });

  describe("geo store", () => {
    const places = defineGeoSet(`${runPrefix}:geo`, codecs.string());
    const palermo = { longitude: 13.361389, latitude: 38.115556 };
    const catania = { longitude: 15.087269, latitude: 37.502669 };

    afterAll(async () => {
      await client.send([
        "DEL",
        places.key("sicily"),
        places.key("nearby"),
        places.key("nearby-dist")
      ]);
    });

    it("adds members and measures positions and distances", async () => {
      const geoStore = createGeoStore(client, places);

      await expect(
        geoStore.geoadd("sicily", [
          { member: "Palermo", ...palermo },
          { member: "Catania", ...catania }
        ])
      ).resolves.toBe(2);
      await expect(
        geoStore.geoadd(
          "sicily",
          [{ member: "Palermo", longitude: 13.4, latitude: 38.2 }],
          { nx: true }
        )
      ).resolves.toBe(0);
      await expect(geoStore.geoadd("sicily", [])).resolves.toBe(0);

      const positions = await geoStore.geopos("sicily", ["Palermo", "Ghost"]);
      expect(positions).toHaveLength(2);
      expect(positions[0]?.longitude).toBeCloseTo(palermo.longitude, 3);
      expect(positions[0]?.latitude).toBeCloseTo(palermo.latitude, 3);
      expect(positions[1]).toBeNull();

      const km = await geoStore.geodist("sicily", "Palermo", "Catania", "km");
      expect(km).toBeCloseTo(166.2742, 0);
      const meters = await geoStore.geodist("sicily", "Palermo", "Catania");
      expect(meters).toBeCloseTo(166_274.15, -3);
      await expect(
        geoStore.geodist("sicily", "Palermo", "Ghost")
      ).resolves.toBeNull();

      const hashes = await geoStore.geohash("sicily", ["Palermo", "Ghost"]);
      expect(hashes[0]).toMatch(/^sqc8b49/);
      expect(hashes[1]).toBeNull();
    });

    it("searches by radius and box, and stores results", async () => {
      const geoStore = createGeoStore(client, places);

      const results = await geoStore.geosearch("sicily", {
        from: { longitude: 15, latitude: 37 },
        by: { radius: 200, unit: "km" },
        order: "asc",
        withDistance: true,
        withCoordinates: true
      });
      expect(results.map((result) => result.member)).toEqual([
        "Catania",
        "Palermo"
      ]);
      expect(results[0].distance).toBeCloseTo(56.4413, 0);
      expect(results[1].distance).toBeCloseTo(190.4424, 0);
      expect(results[0].coordinates?.longitude).toBeCloseTo(
        catania.longitude,
        3
      );
      expect(results[0].coordinates?.latitude).toBeCloseTo(catania.latitude, 3);

      const nearest = await geoStore.geosearch("sicily", {
        from: { member: "Palermo" },
        by: { width: 400, height: 400, unit: "km" },
        order: "asc",
        count: { count: 1 }
      });
      expect(nearest).toEqual([{ member: "Palermo" }]);

      await expect(
        geoStore.geosearchstore("nearby", "sicily", {
          from: { member: "Palermo" },
          by: { radius: 200, unit: "km" }
        })
      ).resolves.toBe(2);
      const stored = await geoStore.geopos("nearby", ["Catania"]);
      expect(stored[0]?.longitude).toBeCloseTo(catania.longitude, 3);

      await expect(
        geoStore.geosearchstore(
          "nearby-dist",
          "sicily",
          {
            from: { member: "Palermo" },
            by: { radius: 200, unit: "km" }
          },
          { storeDistance: true }
        )
      ).resolves.toBe(2);
      const score = await client.send([
        "ZSCORE",
        places.key("nearby-dist"),
        "Catania"
      ]);
      expect(Number(score)).toBeCloseTo(166.2742, 0);

      await expect(
        geoStore.geoadd(
          "sicily",
          [{ member: "Palermo", longitude: 13.5, latitude: 38.2 }],
          { xx: true, ch: true }
        )
      ).resolves.toBe(1);

      await expect(geoStore.del("sicily")).resolves.toBe(1);
    });
  });

  describe("stream store", () => {
    const events = defineStream(`${runPrefix}:stream`, {
      kind: codecs.string(),
      value: codecs.number()
    });

    afterAll(async () => {
      await client.send([
        "DEL",
        events.key("main"),
        events.key("trim"),
        events.key("missing"),
        events.key("capped")
      ]);
    });

    it("adds, ranges, reads, and removes entries", async () => {
      const streamStore = createStreamStore(client, events);

      const firstId = await streamStore.xadd("main", {
        kind: "created",
        value: 1
      });
      const secondId = await streamStore.xadd("main", {
        kind: "updated",
        value: 2
      });
      const thirdId = await streamStore.xadd("main", {
        kind: "deleted",
        value: 3
      });
      if (firstId === null || secondId === null || thirdId === null) {
        throw new Error("Expected XADD to return an entry id");
      }
      expect(firstId).toMatch(/^\d+-\d+$/);
      await expect(streamStore.xlen("main")).resolves.toBe(3);

      const all = await streamStore.xrange("main");
      expect(all.map((entry) => entry.id)).toEqual([
        firstId,
        secondId,
        thirdId
      ]);
      expect(all[0].value).toEqual({ kind: "created", value: 1 });

      await expect(streamStore.xrange("main", { count: 1 })).resolves.toEqual([
        { id: firstId, value: { kind: "created", value: 1 } }
      ]);
      const fromSecond = await streamStore.xrange("main", {
        start: secondId,
        end: "+"
      });
      expect(fromSecond.map((entry) => entry.id)).toEqual([secondId, thirdId]);

      const reversed = await streamStore.xrevrange("main");
      expect(reversed.map((entry) => entry.id)).toEqual([
        thirdId,
        secondId,
        firstId
      ]);
      await expect(
        streamStore.xrevrange("main", { count: 1 })
      ).resolves.toEqual([
        { id: thirdId, value: { kind: "deleted", value: 3 } }
      ]);

      await expect(streamStore.xread("main", "0")).resolves.toHaveLength(3);
      await expect(
        streamStore.xread("main", firstId, { count: 1 })
      ).resolves.toEqual([
        { id: secondId, value: { kind: "updated", value: 2 } }
      ]);
      await expect(streamStore.xread("main", thirdId)).resolves.toEqual([]);

      await expect(streamStore.xdel("main", [secondId])).resolves.toBe(1);
      await expect(streamStore.xdel("main", [])).resolves.toBe(0);
      await expect(streamStore.xlen("main")).resolves.toBe(2);

      await expect(streamStore.del("main")).resolves.toBe(1);
    });

    it("trims by length and minimum id", async () => {
      const streamStore = createStreamStore(client, events);
      const seeded = [
        ["1-1", 1],
        ["2-1", 2],
        ["3-1", 3],
        ["4-1", 4]
      ] as const;
      for (const [entryId, value] of seeded) {
        await expect(
          streamStore.xadd("trim", { kind: "seed", value }, { entryId })
        ).resolves.toBe(entryId);
      }

      await expect(
        streamStore.xtrim("trim", { maxLen: { count: 3 } })
      ).resolves.toBe(1);
      await expect(
        streamStore.xtrim("trim", { minId: { value: "3-1" } })
      ).resolves.toBe(1);
      const remaining = await streamStore.xrange("trim");
      expect(remaining.map((entry) => entry.id)).toEqual(["3-1", "4-1"]);
      await expect(
        streamStore.xtrim("trim", { maxLen: { count: 1, approximate: true } })
      ).resolves.toBeGreaterThanOrEqual(0);
    });

    it("honors NOMKSTREAM and MAXLEN on add", async () => {
      const streamStore = createStreamStore(client, events);

      await expect(
        streamStore.xadd(
          "missing",
          { kind: "ghost", value: 0 },
          { nomkstream: true }
        )
      ).resolves.toBeNull();
      await expect(streamStore.xlen("missing")).resolves.toBe(0);

      await streamStore.xadd("capped", { kind: "a", value: 1 });
      const keptId = await streamStore.xadd("capped", { kind: "b", value: 2 });
      const lastId = await streamStore.xadd(
        "capped",
        { kind: "c", value: 3 },
        { maxLen: { count: 2 } }
      );
      await expect(streamStore.xlen("capped")).resolves.toBe(2);
      const kept = await streamStore.xrange("capped");
      expect(kept.map((entry) => entry.id)).toEqual([keptId, lastId]);
    });
  });

  describe("session worker queue", () => {
    const jobs = defineList(`${runPrefix}:queue`, codecs.string());
    const scores = defineSortedSet(`${runPrefix}:queue:z`, codecs.string());

    afterAll(async () => {
      await client.send([
        "DEL",
        jobs.key("urgent"),
        jobs.key("pending"),
        jobs.key("timeout"),
        jobs.key("src"),
        jobs.key("processing"),
        jobs.key("mb-a"),
        jobs.key("mb-b"),
        scores.key("live"),
        scores.key("zb-a"),
        scores.key("zb-b")
      ]);
    });

    it("blocks on two keys and attributes the answering key to its typed id", async () => {
      await db.session(async (s) => {
        const queue = s.list(jobs);
        // Arm the blocking pop across two empty keys, then push from the
        // shared client so Redis serves the reply onto the session.
        const popped = queue.blpop(["urgent", "pending"], {
          timeoutSeconds: "forever"
        });
        await sleep(50);
        await expect(db.list(jobs).rpush("pending", ["email-1"])).resolves.toBe(
          1
        );

        const hit = await popped;
        // { timeoutSeconds: "forever" } removes null from the type — hit is non-null.
        expect(hit).toEqual({ id: "pending", value: "email-1" });
        // hit.id is the typed union member "urgent" | "pending".
        const answered: "urgent" | "pending" = hit.id;
        expect(answered).toBe("pending");
      });
    });

    it("returns null when a bounded blocking pop times out", async () => {
      await db.session(async (s) => {
        const started = Date.now();
        const result = await s
          .list(jobs)
          .blpop("timeout", { timeoutSeconds: 0.2 });
        const elapsed = Date.now() - started;
        expect(result).toBeNull();
        // Blocked on the server for roughly the requested window, not instantly
        // and not indefinitely.
        expect(elapsed).toBeGreaterThanOrEqual(150);
        expect(elapsed).toBeLessThan(1500);
      });
    });

    it("round-trips a job through BLMOVE into a processing list", async () => {
      await db.list(jobs).rpush("src", ["job-a", "job-b"]);
      await db.session(async (s) => {
        const moved = await s
          .list(jobs)
          .blmove("src", "processing", "left", "right", {
            timeoutSeconds: 0.3
          });
        expect(moved).toBe("job-a");
      });
      // The job now lives in the processing list, recoverable after a crash.
      await expect(db.list(jobs).lrange("processing", 0, -1)).resolves.toEqual([
        "job-a"
      ]);
      await expect(db.list(jobs).lrange("src", 0, -1)).resolves.toEqual([
        "job-b"
      ]);
    });

    it("blocks on a sorted set and pops the minimum-score member", async () => {
      await db.session(async (s) => {
        const board = s.zset(scores);
        const popped = board.bzpopmin("live", { timeoutSeconds: "forever" });
        await sleep(50);
        await db.zset(scores).zadd("live", [
          { member: "low", score: 1 },
          { member: "high", score: 9 }
        ]);

        const entry = await popped;
        expect(entry).toEqual({ member: "low", score: 1 });
      });
      // The higher-score member is left behind.
      await expect(
        db.zset(scores).zrange("live", { start: 0, stop: -1 })
      ).resolves.toEqual(["high"]);
    });

    it("blocks with BLMPOP and pops a counted batch from the first ready key", async () => {
      await db.session(async (s) => {
        const popped = s.list(jobs).blmpop(["mb-a", "mb-b"], {
          direction: "left",
          timeoutSeconds: "forever",
          count: 2
        });
        await sleep(50);
        await db.list(jobs).rpush("mb-b", ["a", "b", "c"]);

        const hit = await popped;
        expect(hit).toEqual({ id: "mb-b", values: ["a", "b"] });
        const answered: "mb-a" | "mb-b" = hit.id;
        expect(answered).toBe("mb-b");
      });
      // The uncounted remainder stays in the list.
      await expect(db.list(jobs).lrange("mb-b", 0, -1)).resolves.toEqual(["c"]);
    });

    it("blocks with BZMPOP and pops a counted batch of min-score members", async () => {
      await db.session(async (s) => {
        const popped = s.zset(scores).bzmpop(
          ["zb-a", "zb-b"],
          { min: true, count: 2 },
          {
            timeoutSeconds: "forever"
          }
        );
        await sleep(50);
        await db.zset(scores).zadd("zb-b", [
          { member: "low", score: 1 },
          { member: "mid", score: 5 },
          { member: "high", score: 9 }
        ]);

        const hit = await popped;
        expect(hit).toEqual({
          id: "zb-b",
          entries: [
            { member: "low", score: 1 },
            { member: "mid", score: 5 }
          ]
        });
      });
      await expect(
        db.zset(scores).zrange("zb-b", { start: 0, stop: -1 })
      ).resolves.toEqual(["high"]);
    });
  });

  describe("shared multi-key pops (LMPOP/ZMPOP)", () => {
    const jobs = defineList(`${runPrefix}:mpop:list`, codecs.string());
    const scores = defineSortedSet(`${runPrefix}:mpop:z`, codecs.string());

    afterAll(async () => {
      await client.send([
        "DEL",
        jobs.key("a"),
        jobs.key("b"),
        scores.key("a"),
        scores.key("b")
      ]);
    });

    it("pops from the first non-empty list key with typed attribution", async () => {
      await db.list(jobs).rpush("b", ["one", "two", "three"]);

      await expect(
        db.list(jobs).lmpop(["a", "b"], { direction: "left" })
      ).resolves.toEqual({
        id: "b",
        values: ["one"]
      });
      const hit = await db
        .list(jobs)
        .lmpop(["a", "b"], { direction: "left", count: 5 });
      expect(hit).toEqual({ id: "b", values: ["two", "three"] });
      const answered: "a" | "b" | undefined = hit?.id;
      expect(answered).toBe("b");

      // Every key empty -> null.
      await expect(
        db.list(jobs).lmpop(["a", "b"], { direction: "right" })
      ).resolves.toBeNull();
    });

    it("pops from the first non-empty sorted set key with typed attribution", async () => {
      await db.zset(scores).zadd("b", [
        { member: "alice", score: 10 },
        { member: "bob", score: 20 },
        { member: "carol", score: 30 }
      ]);

      await expect(
        db.zset(scores).zmpop(["a", "b"], { min: true })
      ).resolves.toEqual({
        id: "b",
        entries: [{ member: "alice", score: 10 }]
      });
      await expect(
        db.zset(scores).zmpop(["a", "b"], { max: true, count: 2 })
      ).resolves.toEqual({
        id: "b",
        entries: [
          { member: "carol", score: 30 },
          { member: "bob", score: 20 }
        ]
      });

      await expect(
        db.zset(scores).zmpop(["a", "b"], { min: true })
      ).resolves.toBeNull();
    });
  });

  describe("optimistic transactions (db.watch)", () => {
    const views = defineKeyspace(`${runPrefix}:watch:views`, codecs.number());
    const parallel = defineKeyspace(
      `${runPrefix}:watch:parallel`,
      codecs.number()
    );

    afterAll(async () => {
      await client.send([
        "DEL",
        views.key("home"),
        `${views.key("home")}:writes`,
        parallel.key("counter")
      ]);
    });

    it("retries exactly once after a deterministic single conflict", async () => {
      await db.kv(views).set("home", 0);

      // Clobber the watched key from the shared client exactly once, on the
      // first attempt only — after the body read, before exec — so the first
      // EXEC aborts (null) and the second commits deterministically.
      let clobbered = false;
      const attempts: number[] = [];
      const result = await db.watch(
        views.key("home"),
        async (s) => {
          const current = (await s.kv(views).get("home")) ?? 0;
          if (!clobbered) {
            clobbered = true;
            await db.counter(views).incr("home");
          }
          return s
            .multi()
            .add(["SET", views.key("home"), current + 1], okReply)
            .add(["INCR", `${views.key("home")}:writes`], numberReply);
        },
        { onAbort: ({ attempt }) => attempts.push(attempt) }
      );

      // Exactly one conflict was observed, then the retry committed.
      expect(attempts).toEqual([1]);
      // Second attempt read 1 (the clobbering INCR) and set it to 2.
      expect(result).toEqual([undefined, 1]);
      await expect(db.kv(views).get("home")).resolves.toBe(2);
    });

    it("N=5 parallel increments converge to exactly 5", async () => {
      await db.kv(parallel).set("counter", 0);

      const runs = Array.from({ length: 5 }, () =>
        db.watch(parallel.key("counter"), async (s) => {
          const current = (await s.kv(parallel).get("counter")) ?? 0;
          return s
            .multi()
            .add(["SET", parallel.key("counter"), current + 1], okReply);
        })
      );

      const results = await Promise.all(runs);
      // Every run committed (no exhaustion) under contention.
      for (const result of results) expect(result).toEqual([undefined]);
      await expect(db.kv(parallel).get("counter")).resolves.toBe(5);
    });

    it("throws WatchRetriesExceededError under sustained conflict", async () => {
      const key = views.key("home");
      await db.kv(views).set("home", 0);

      const attempts: number[] = [];
      await expect(
        db.watch(
          key,
          async (s) => {
            const current = (await s.kv(views).get("home")) ?? 0;
            // Clobber the watched key from the shared client after the read but
            // before exec, guaranteeing every attempt aborts.
            await db.counter(views).incr("home");
            return s.multi().add(["SET", key, current + 1], okReply);
          },
          {
            attempts: 3,
            onAbort: ({ attempt }) => attempts.push(attempt)
          }
        )
      ).rejects.toBeInstanceOf(WatchRetriesExceededError);
      expect(attempts).toEqual([1, 2, 3]);
    });

    it("opts out with a null body and leaves the key untouched", async () => {
      await db.kv(views).set("home", 100);

      const result = await db.watch(views.key("home"), async (s) => {
        const current = (await s.kv(views).get("home")) ?? 0;
        if (current >= 100) return null;
        return s.multi().add(["SET", views.key("home"), current + 1], okReply);
      });

      expect(result).toBeNull();
      await expect(db.kv(views).get("home")).resolves.toBe(100);
    });
  });

  describe("consumer group lifecycle", () => {
    const auditEvents = defineStream(`${runPrefix}:cg`, {
      type: codecs.string(),
      userId: codecs.string()
    });
    const groupName = "processors";

    afterAll(async () => {
      await client.send(["DEL", auditEvents.key("login")]);
    });

    it("creates, consumes, acks, claims, and reports pending state", async () => {
      const group = db.stream(auditEvents).group(groupName);
      const worker = group.consumer("w-1");

      // create() -> true on first call, false (BUSYGROUP) on the second.
      await expect(group.create("login", { from: "start" })).resolves.toBe(
        true
      );
      await expect(group.create("login", { from: "start" })).resolves.toBe(
        false
      );

      const stream = db.stream(auditEvents);
      const firstId = await stream.xadd("login", {
        type: "click",
        userId: "u1"
      });
      const secondId = await stream.xadd("login", {
        type: "view",
        userId: "u2"
      });
      const thirdId = await stream.xadd("login", {
        type: "logout",
        userId: "u3"
      });
      if (firstId === null || secondId === null || thirdId === null) {
        throw new Error("Expected XADD to return an entry id");
      }

      // Live read via ">" — new deliveries, never tombstones.
      const delivered = await worker.xreadgroup("login", { count: 10 });
      expect(delivered.map((entry) => entry.id)).toEqual([
        firstId,
        secondId,
        thirdId
      ]);
      expect(delivered[0].value).toEqual({ type: "click", userId: "u1" });

      // Pending summary reflects three unacked entries owned by w-1.
      const summary = await group.xpending("login");
      expect(summary.count).toBe(3);
      expect(summary.minEntryId).toBe(firstId);
      expect(summary.maxEntryId).toBe(thirdId);
      expect(summary.consumers).toEqual([{ consumer: "w-1", count: 3 }]);

      // Extended range fields: id/consumer/idle/deliveries.
      const rows = await group.xpending("login", { count: 10 });
      expect(rows.map((row) => row.entryId)).toEqual([
        firstId,
        secondId,
        thirdId
      ]);
      for (const row of rows) {
        expect(row.consumer).toBe("w-1");
        expect(row.deliveries).toBe(1);
        expect(row.idleMs).toBeGreaterThanOrEqual(0);
      }

      // Ack the first entry via the consumer mirror; pending count drops.
      await expect(worker.xack("login", [firstId])).resolves.toBe(1);
      await expect((await group.xpending("login")).count).toBe(2);
    });

    it("surfaces an XDEL tombstone through readPending and still acks it", async () => {
      const group = db.stream(auditEvents).group("tombstones");
      const worker = group.consumer("t-1");
      const stream = db.stream(auditEvents);

      await group.create("login", { from: "end" });
      const liveId = await stream.xadd("login", {
        type: "click",
        userId: "u1"
      });
      const doomedId = await stream.xadd("login", {
        type: "view",
        userId: "u2"
      });
      if (liveId === null || doomedId === null) {
        throw new Error("Expected XADD to return an entry id");
      }

      // Deliver both into t-1's PEL, then XDEL one entry upstream.
      await worker.xreadgroup("login", { count: 10 });
      await expect(stream.xdel("login", [doomedId])).resolves.toBe(1);

      // readPending replays the consumer's unacked history: the deleted entry
      // decodes as a tombstone (value null) but is still present in the PEL.
      const pending = await worker.xreadgroup("login", { after: "0" });
      const tombstone = pending.find((entry) => entry.id === doomedId);
      const live = pending.find((entry) => entry.id === liveId);
      expect(tombstone).toBeDefined();
      expect(tombstone?.value).toBeNull();
      expect(live?.value).toEqual({ type: "click", userId: "u1" });

      // The tombstone is still ackable — that is how the PEL is cleared.
      await expect(group.xack("login", [doomedId])).resolves.toBe(1);
      const afterAck = await worker.xreadgroup("login", { after: "0" });
      expect(afterAck.some((entry) => entry.id === doomedId)).toBe(false);
    });

    it("moves ownership with autoClaim and drops deleted ids from the PEL", async () => {
      const group = db.stream(auditEvents).group("claim");
      const dead = group.consumer("dead");
      const live = group.consumer("live");
      const stream = db.stream(auditEvents);

      await group.create("login", { from: "end" });
      const keepId = await stream.xadd("login", {
        type: "click",
        userId: "u1"
      });
      const gapId = await stream.xadd("login", { type: "view", userId: "u2" });
      if (keepId === null || gapId === null) {
        throw new Error("Expected XADD to return an entry id");
      }

      // "dead" consumer takes delivery of both entries into its PEL.
      const claimedByDead = await dead.xreadgroup("login", { count: 10 });
      expect(claimedByDead).toHaveLength(2);
      const pendingBefore = await group.xpending("login");
      expect(pendingBefore.count).toBe(2);
      expect(pendingBefore.consumers).toEqual([{ consumer: "dead", count: 2 }]);

      // XDEL one of dead's pending entries upstream, then autoClaim with
      // minIdleMs 0 so "live" steals everything still idle.
      await expect(stream.xdel("login", [gapId])).resolves.toBe(1);
      const result = await live.xautoclaim("login", {
        minIdleMs: 0,
        count: 10
      });

      // Redis 7+ removes the deleted id from the PEL itself and reports it.
      expect(result.deletedIds).toEqual([gapId]);
      expect(result.entries.map((entry) => entry.id)).toEqual([keepId]);
      expect(result.entries[0].value).toEqual({ type: "click", userId: "u1" });

      // Ownership moved to "live" and the deleted id is gone from the PEL.
      const pendingAfter = await group.xpending("login");
      expect(pendingAfter.count).toBe(1);
      expect(pendingAfter.consumers).toEqual([{ consumer: "live", count: 1 }]);

      // deleteConsumer drops the (now empty) dead consumer and its PEL.
      await expect(group.deleteConsumer("login", "dead")).resolves.toBe(0);
    });

    it("receives a late add through a session blocking group read", async () => {
      const group = db.stream(auditEvents).group("blocking");
      await group.create("login", { from: "end" });
      const stream = db.stream(auditEvents);

      await db.session(async (s) => {
        const live = s.stream(auditEvents).group("blocking").consumer("b-1");
        const batch = live.xreadgroup("login", {
          timeoutSeconds: "forever",
          count: 5
        });
        await sleep(50);
        const lateId = await stream.xadd("login", {
          type: "late",
          userId: "u9"
        });
        if (lateId === null)
          throw new Error("Expected XADD to return an entry id");

        const received = await batch;
        expect(received.map((entry) => entry.id)).toEqual([lateId]);
        expect(received[0].value).toEqual({ type: "late", userId: "u9" });
      });
    });
  });

  describe("session lifecycle", () => {
    const jobs = defineList(`${runPrefix}:life:list`, codecs.string());

    afterAll(async () => {
      await client.send(["DEL", jobs.key("blocked")]);
    });

    it("await using disposes a session and rejects a forever-block fast", async () => {
      let disposed: BeniSession;
      const started = Date.now();
      const rejection = await (async () => {
        // eslint-disable-next-line no-lone-blocks
        {
          await using session = await db.session();
          disposed = session;
          // A forever-block that only close()/dispose can unblock. Capture the
          // rejection without awaiting so the block leaves scope and dispose
          // fires, rejecting it promptly.
          const blocked = session
            .list(jobs)
            .blpop("blocked", { timeoutSeconds: "forever" })
            .then(
              () => ({ rejected: false as const }),
              (error: unknown) => ({ rejected: true as const, error })
            );
          await sleep(50);
          // Leaving this block triggers [Symbol.asyncDispose] -> close().
          return blocked;
        }
      })();

      const elapsed = Date.now() - started;
      expect(rejection.rejected).toBe(true);
      // destroy() aborts the in-flight block immediately rather than waiting
      // out the server-side timeout (which is "forever" here).
      expect(elapsed).toBeLessThan(1500);
      expect(disposed!.closed).toBe(true);
    });

    it("scoped db.session(fn) closes the session after the body resolves", async () => {
      let leased: BeniSession | undefined;
      const value = await db.session(async (s) => {
        leased = s;
        expect(s.closed).toBe(false);
        await db.list(jobs).rpush("blocked", ["scoped"]);
        return s.list(jobs).blpop("blocked", { timeoutSeconds: 0.3 });
      });

      expect(value).toBe("scoped");
      expect(leased?.closed).toBe(true);
    });
  });
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

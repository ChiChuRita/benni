import { describe, expect, it } from "vitest";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/index.js";
import {
  codecs,
  createCounterStore,
  createHashStore,
  createKeyValueStore,
  createListStore,
  createPubSubPublisher,
  createSetStore,
  createSortedSetStore,
  createStringStore,
  defineHash,
  defineKeyspace,
  defineList,
  definePubSubChannel,
  definePubSubPattern,
  defineSet,
  defineSortedSet,
  describeReply,
  ReplyShapeError,
  ValidationError
} from "../src/core/index.js";

describe("codec soundness and typed errors", () => {
  it("number() decode surfaces a ReplyShapeError carrying the value", () => {
    try {
      codecs.number().decode("not-a-number");
      expect.unreachable();
    } catch (error) {
      expect(error).toBeInstanceOf(ReplyShapeError);
      expect(error).toBeInstanceOf(TypeError);
      expect((error as ReplyShapeError).reply).toBe("not-a-number");
      expect((error as Error).message).toContain("not-a-number");
    }
  });

  it("json() decode wraps parse failures with context", () => {
    const j = codecs.json<{ a: number }>();
    expect(() => j.decode("{ broken")).toThrow(ReplyShapeError);
    expect(() => j.decode("{ broken")).toThrow(
      "JSON codec failed to decode stored value"
    );
  });

  it("enumOf() stores plain strings and validates on decode", () => {
    const status = codecs.enumOf(["pending", "active", "done"]);
    // Inferred as Codec<"pending" | "active" | "done">.
    const _typed: import("../src/core/index.js").Codec<
      "pending" | "active" | "done"
    > = status;
    void _typed;

    expect(status.encode("active")).toBe("active");
    expect(status.decode("done")).toBe("done");
    expect(() => status.decode("unknown")).toThrow(ReplyShapeError);
    expect(() => status.decode("unknown")).toThrow(
      "enum codec expected one of pending, active, done"
    );
  });

  it("store decode errors are ReplyShapeError and name the reply", async () => {
    const counters = defineKeyspace("counter", codecs.number());
    const store = createCounterStore(fakeClient([], ["x"]), counters);
    const error = await store.incr("42").catch((caught) => caught);
    expect(error).toBeInstanceOf(ReplyShapeError);
    expect((error as Error).message).toContain(
      "Expected Redis INCR to return number"
    );
    expect((error as Error).message).toContain("got");
    expect((error as ReplyShapeError).reply).toBe("x");
  });

  it("boolean() decodes both truthy and falsy string forms", () => {
    const b = codecs.boolean();
    expect(b.decode("1")).toBe(true);
    expect(b.decode("true")).toBe(true);
    expect(b.decode("0")).toBe(false);
    expect(b.decode("false")).toBe(false);
  });

  it("describeReply renders each reply shape compactly", () => {
    expect(describeReply(null)).toBe("null");
    expect(describeReply(undefined)).toBe("undefined");
    expect(describeReply(42)).toContain("number");
    expect(describeReply(true)).toContain("boolean");
    expect(describeReply([1, 2, 3])).toContain("array(length 3)");
    expect(describeReply(new Map([["a", 1]]))).toContain("map(size 1)");
    expect(describeReply(new Set([1, 2]))).toContain("set(size 2)");
    expect(describeReply(new Uint8Array([1, 2]))).toContain("bytes(length 2)");
    expect(describeReply("x".repeat(100))).toContain("…");
  });
});

describe("createKeyValueStore", () => {
  it("emits SET, GET, and DEL commands", async () => {
    const commands: RedisCommand[] = [];
    const replies: RedisReply[] = ["OK", '{"name":"beni"}', 1];
    const client = fakeClient(commands, replies);
    const users = defineKeyspace("user", codecs.json<{ name: string }>());
    const store = createKeyValueStore(client, users);

    await store.set("42", { name: "beni" });
    await expect(store.get("42")).resolves.toEqual({ name: "beni" });
    await expect(store.del("42")).resolves.toBe(1);

    expect(commands).toEqual([
      ["SET", "user:42", '{"name":"beni"}'],
      ["GET", "user:42"],
      ["DEL", "user:42"]
    ]);
  });

  it("adds EX ttl to SET commands", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(
      fakeClient(commands, ["OK"]),
      defineKeyspace("user", codecs.string())
    );

    await store.set("42", "beni", { ttlSeconds: 60 });

    expect(commands).toEqual([["SET", "user:42", "beni", "EX", 60]]);
  });

  it("returns null for missing values", async () => {
    const store = createKeyValueStore(
      fakeClient([], [null]),
      defineKeyspace("user", codecs.string())
    );

    await expect(store.get("missing")).resolves.toBeNull();
  });

  it("supports typed GETDEL and GETSET", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(
      fakeClient(commands, ["old", "older"]),
      defineKeyspace("user", codecs.string())
    );

    await expect(store.getdel("42")).resolves.toBe("old");
    await expect(store.getset("42", "new")).resolves.toBe("older");

    expect(commands).toEqual([
      ["GETDEL", "user:42"],
      ["GETSET", "user:42", "new"]
    ]);
  });

  it("supports typed MGET and MSET", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(
      fakeClient(commands, [["one", null, "three"], "OK"]),
      defineKeyspace("user", codecs.string())
    );

    await expect(store.mget(["1", "2", "3"])).resolves.toEqual([
      "one",
      null,
      "three"
    ]);
    await expect(
      store.mset([
        ["1", "one"],
        ["2", "two"]
      ])
    ).resolves.toBeUndefined();

    expect(commands).toEqual([
      ["MGET", "user:1", "user:2", "user:3"],
      ["MSET", "user:1", "one", "user:2", "two"]
    ]);
  });

  it("skips empty MGET and MSET inputs", async () => {
    const commands: RedisCommand[] = [];
    const store = createKeyValueStore(
      fakeClient(commands, []),
      defineKeyspace("user", codecs.string())
    );

    await expect(store.mget([])).resolves.toEqual([]);
    await expect(store.mset([])).resolves.toBeUndefined();
    expect(commands).toEqual([]);
  });

  it("throws on unexpected SET, GET, and DEL replies", async () => {
    await expect(
      createKeyValueStore(
        fakeClient([], [0]),
        defineKeyspace("user", codecs.string())
      ).set("42", "beni")
    ).rejects.toThrow(TypeError);

    await expect(
      createKeyValueStore(
        fakeClient([], [1]),
        defineKeyspace("user", codecs.string())
      ).get("42")
    ).rejects.toThrow(TypeError);

    await expect(
      createKeyValueStore(
        fakeClient([], ["1"]),
        defineKeyspace("user", codecs.string())
      ).del("42")
    ).rejects.toThrow(TypeError);
  });

  it("throws on unexpected typed string command replies", async () => {
    const keyspace = defineKeyspace("user", codecs.string());

    await expect(
      createKeyValueStore(fakeClient([], [1]), keyspace).getdel("42")
    ).rejects.toThrow(TypeError);
    await expect(
      createKeyValueStore(fakeClient([], [1]), keyspace).getset("42", "new")
    ).rejects.toThrow(TypeError);
    await expect(
      createKeyValueStore(fakeClient([], ["one"]), keyspace).mget(["1"])
    ).rejects.toThrow(TypeError);
    await expect(
      createKeyValueStore(fakeClient([], [[1]]), keyspace).mget(["1"])
    ).rejects.toThrow(TypeError);
    await expect(
      createKeyValueStore(fakeClient([], ["NO"]), keyspace).mset([["1", "one"]])
    ).rejects.toThrow(TypeError);
  });
});

describe("createCounterStore", () => {
  it("supports typed INCR, INCRBY, DECR, and DECRBY", async () => {
    const commands: RedisCommand[] = [];
    const counters = createCounterStore(
      fakeClient(commands, [1, 6, 5, 3]),
      defineKeyspace("counter", codecs.number())
    );

    await expect(counters.incr("hits")).resolves.toBe(1);
    await expect(counters.incrby("hits", 5)).resolves.toBe(6);
    await expect(counters.decr("hits")).resolves.toBe(5);
    await expect(counters.decrby("hits", 2)).resolves.toBe(3);

    expect(commands).toEqual([
      ["INCR", "counter:hits"],
      ["INCRBY", "counter:hits", 5],
      ["DECR", "counter:hits"],
      ["DECRBY", "counter:hits", 2]
    ]);
  });

  it("throws on invalid counter inputs and replies", async () => {
    const keyspace = defineKeyspace("counter", codecs.number());

    await expect(
      createCounterStore(fakeClient([], ["1"]), keyspace).incr("hits")
    ).rejects.toThrow(TypeError);
    await expect(
      createCounterStore(fakeClient([], [1]), keyspace).incrby("hits", 1.5)
    ).rejects.toThrow(TypeError);
    await expect(
      createCounterStore(fakeClient([], [1]), keyspace).decrby("hits", 1.5)
    ).rejects.toThrow(TypeError);
  });
});

describe("createStringStore", () => {
  it("supports typed APPEND, GETRANGE, SETRANGE, STRLEN, and GETEX", async () => {
    const commands: RedisCommand[] = [];
    const strings = createStringStore(
      fakeClient(commands, [5, "hello", 11, 11, "hello world"]),
      defineKeyspace("text", codecs.string())
    );

    await expect(strings.append("greeting", "hello")).resolves.toBe(5);
    await expect(strings.getrange("greeting", 0, 4)).resolves.toBe("hello");
    await expect(strings.setrange("greeting", 5, " world")).resolves.toBe(11);
    await expect(strings.strlen("greeting")).resolves.toBe(11);
    await expect(strings.getex("greeting", 60)).resolves.toBe("hello world");

    expect(commands).toEqual([
      ["APPEND", "text:greeting", "hello"],
      ["GETRANGE", "text:greeting", 0, 4],
      ["SETRANGE", "text:greeting", 5, " world"],
      ["STRLEN", "text:greeting"],
      ["GETEX", "text:greeting", "EX", 60]
    ]);
  });

  it("returns null for missing GETEX values", async () => {
    const strings = createStringStore(
      fakeClient([], [null]),
      defineKeyspace("text", codecs.string())
    );

    await expect(strings.getex("missing", 60)).resolves.toBeNull();
  });

  it("throws on unexpected string command replies and invalid args", async () => {
    const strings = defineKeyspace("text", codecs.string());

    await expect(
      createStringStore(fakeClient([], ["5"]), strings).append(
        "greeting",
        "hello"
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createStringStore(fakeClient([], [1]), strings).getrange("greeting", 0, 4)
    ).rejects.toThrow(TypeError);
    await expect(
      createStringStore(fakeClient([], ["11"]), strings).setrange(
        "greeting",
        0,
        "hello"
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createStringStore(fakeClient([], [11]), strings).setrange(
        "greeting",
        -1,
        "hello"
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createStringStore(fakeClient([], ["11"]), strings).strlen("greeting")
    ).rejects.toThrow(TypeError);
    await expect(
      createStringStore(fakeClient([], [1]), strings).getex("greeting", 60)
    ).rejects.toThrow(TypeError);
    await expect(
      createStringStore(fakeClient([], ["hello"]), strings).getex("greeting", 0)
    ).rejects.toThrow(TypeError);
  });
});

describe("Pub/Sub", () => {
  it("defines typed channels and publishes encoded messages", async () => {
    const commands: RedisCommand[] = [];
    const channel = definePubSubChannel(
      "events:user",
      codecs.json<{ id: string; action: "created" }>()
    );
    const publisher = createPubSubPublisher(fakeClient(commands, [2]));

    await expect(
      publisher.publish(channel, { id: "42", action: "created" })
    ).resolves.toBe(2);
    expect(channel.decode('{"id":"42","action":"created"}')).toEqual({
      id: "42",
      action: "created"
    });
    expect(commands).toEqual([
      ["PUBLISH", "events:user", '{"id":"42","action":"created"}']
    ]);
  });

  it("throws on unexpected PUBLISH replies", async () => {
    const channel = definePubSubChannel("events:user", codecs.string());
    const publisher = createPubSubPublisher(fakeClient([], ["1"]));

    await expect(publisher.publish(channel, "created")).rejects.toThrow(
      TypeError
    );
  });

  it("defines typed patterns that decode subscribed messages", () => {
    const pattern = definePubSubPattern(
      "events:*",
      codecs.json<{ id: string }>()
    );

    expect(pattern.pattern).toBe("events:*");
    expect(pattern.decode('{"id":"42"}')).toEqual({ id: "42" });
  });
});

describe("createSetStore", () => {
  it("supports typed SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, and DEL", async () => {
    const commands: RedisCommand[] = [];
    const roles = createSetStore(
      fakeClient(commands, [2, 1, 1, [1, 0], ["admin", "user"], 2, "admin", 1]),
      defineSet("roles", codecs.string())
    );

    await expect(roles.sadd("42", ["admin", "user"])).resolves.toBe(2);
    await expect(roles.srem("42", ["guest"])).resolves.toBe(1);
    await expect(roles.sismember("42", "admin")).resolves.toBe(true);
    await expect(roles.smismember("42", ["admin", "guest"])).resolves.toEqual([
      true,
      false
    ]);
    await expect(roles.smembers("42")).resolves.toEqual(["admin", "user"]);
    await expect(roles.scard("42")).resolves.toBe(2);
    await expect(roles.spop("42")).resolves.toBe("admin");
    await expect(roles.del("42")).resolves.toBe(1);

    expect(commands).toEqual([
      ["SADD", "roles:42", "admin", "user"],
      ["SREM", "roles:42", "guest"],
      ["SISMEMBER", "roles:42", "admin"],
      ["SMISMEMBER", "roles:42", "admin", "guest"],
      ["SMEMBERS", "roles:42"],
      ["SCARD", "roles:42"],
      ["SPOP", "roles:42"],
      ["DEL", "roles:42"]
    ]);
  });

  it("skips empty set member inputs", async () => {
    const commands: RedisCommand[] = [];
    const roles = createSetStore(
      fakeClient(commands, []),
      defineSet("roles", codecs.string())
    );

    await expect(roles.sadd("42", [])).resolves.toBe(0);
    await expect(roles.srem("42", [])).resolves.toBe(0);
    await expect(roles.smismember("42", [])).resolves.toEqual([]);
    expect(commands).toEqual([]);
  });

  it("returns null when SPOP misses", async () => {
    const roles = createSetStore(
      fakeClient([], [null]),
      defineSet("roles", codecs.string())
    );

    await expect(roles.spop("42")).resolves.toBeNull();
  });

  it("supports typed set algebra and random member commands", async () => {
    const commands: RedisCommand[] = [];
    const roles = createSetStore(
      fakeClient(commands, [
        "admin",
        ["admin", "user"],
        ["admin"],
        ["user"],
        1,
        2,
        1,
        1,
        1
      ]),
      defineSet("roles", codecs.string())
    );

    await expect(roles.srandmember("a")).resolves.toBe("admin");
    await expect(roles.sunion("a", ["b"])).resolves.toEqual(["admin", "user"]);
    await expect(roles.sinter("a", ["b"])).resolves.toEqual(["admin"]);
    await expect(roles.sdiff("a", ["b"])).resolves.toEqual(["user"]);
    await expect(roles.sintercard("a", ["b"])).resolves.toBe(1);
    await expect(roles.sunionstore("out", "a", ["b"])).resolves.toBe(2);
    await expect(roles.sinterstore("out", "a", ["b"])).resolves.toBe(1);
    await expect(roles.sdiffstore("out", "a", ["b"])).resolves.toBe(1);
    await expect(roles.smove("a", "b", "admin")).resolves.toBe(true);

    expect(commands).toEqual([
      ["SRANDMEMBER", "roles:a"],
      ["SUNION", "roles:a", "roles:b"],
      ["SINTER", "roles:a", "roles:b"],
      ["SDIFF", "roles:a", "roles:b"],
      ["SINTERCARD", 2, "roles:a", "roles:b"],
      ["SUNIONSTORE", "roles:out", "roles:a", "roles:b"],
      ["SINTERSTORE", "roles:out", "roles:a", "roles:b"],
      ["SDIFFSTORE", "roles:out", "roles:a", "roles:b"],
      ["SMOVE", "roles:a", "roles:b", "admin"]
    ]);
  });

  it("throws on unexpected set replies", async () => {
    const roles = defineSet("roles", codecs.string());

    await expect(
      createSetStore(fakeClient([], ["1"]), roles).sadd("42", ["admin"])
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["1"]), roles).srem("42", ["admin"])
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["1"]), roles).sismember("42", "admin")
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["1"]), roles).smismember("42", ["admin"])
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], [["1"]]), roles).smismember("42", ["admin"])
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["admin"]), roles).smembers("42")
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], [[1]]), roles).smembers("42")
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["2"]), roles).scard("42")
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], [1]), roles).spop("42")
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], [1]), roles).srandmember("42")
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["admin"]), roles).sunion("a", ["b"])
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], [[1]]), roles).sinter("a", ["b"])
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["1"]), roles).sdiffstore("out", "a", ["b"])
    ).rejects.toThrow(TypeError);
    await expect(
      createSetStore(fakeClient([], ["1"]), roles).smove("a", "b", "admin")
    ).rejects.toThrow(TypeError);
  });
});

describe("createListStore", () => {
  it("supports typed LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LTRIM, LREM, LMOVE, and DEL", async () => {
    const commands: RedisCommand[] = [];
    const lists = createListStore(
      fakeClient(commands, [
        2,
        3,
        "left",
        "right",
        ["middle", "tail"],
        2,
        "middle",
        "OK",
        "OK",
        1,
        "moved",
        1
      ]),
      defineList("jobs", codecs.string())
    );

    await expect(lists.lpush("a", ["left", "middle"])).resolves.toBe(2);
    await expect(lists.rpush("a", ["tail"])).resolves.toBe(3);
    await expect(lists.lpop("a")).resolves.toBe("left");
    await expect(lists.rpop("a")).resolves.toBe("right");
    await expect(lists.lrange("a", 0, -1)).resolves.toEqual(["middle", "tail"]);
    await expect(lists.llen("a")).resolves.toBe(2);
    await expect(lists.lindex("a", 0)).resolves.toBe("middle");
    await expect(lists.lset("a", 0, "updated")).resolves.toBeUndefined();
    await expect(lists.ltrim("a", 0, 1)).resolves.toBeUndefined();
    await expect(lists.lrem("a", 1, "tail")).resolves.toBe(1);
    await expect(lists.lmove("a", "b", "right", "left")).resolves.toBe("moved");
    await expect(lists.del("a")).resolves.toBe(1);

    expect(commands).toEqual([
      ["LPUSH", "jobs:a", "left", "middle"],
      ["RPUSH", "jobs:a", "tail"],
      ["LPOP", "jobs:a"],
      ["RPOP", "jobs:a"],
      ["LRANGE", "jobs:a", 0, -1],
      ["LLEN", "jobs:a"],
      ["LINDEX", "jobs:a", 0],
      ["LSET", "jobs:a", 0, "updated"],
      ["LTRIM", "jobs:a", 0, 1],
      ["LREM", "jobs:a", 1, "tail"],
      ["LMOVE", "jobs:a", "jobs:b", "RIGHT", "LEFT"],
      ["DEL", "jobs:a"]
    ]);
  });

  it("skips empty list pushes and returns null for missing items", async () => {
    const commands: RedisCommand[] = [];
    const lists = createListStore(
      fakeClient(commands, [null, null, null]),
      defineList("jobs", codecs.string())
    );

    await expect(lists.lpush("a", [])).resolves.toBe(0);
    await expect(lists.rpush("a", [])).resolves.toBe(0);
    await expect(lists.lpop("a")).resolves.toBeNull();
    await expect(lists.lindex("a", 0)).resolves.toBeNull();
    await expect(lists.lmove("a", "b", "left", "right")).resolves.toBeNull();

    expect(commands).toEqual([
      ["LPOP", "jobs:a"],
      ["LINDEX", "jobs:a", 0],
      ["LMOVE", "jobs:a", "jobs:b", "LEFT", "RIGHT"]
    ]);
  });

  it("throws on unexpected list replies", async () => {
    const lists = defineList("jobs", codecs.string());

    await expect(
      createListStore(fakeClient([], ["1"]), lists).lpush("a", ["x"])
    ).rejects.toThrow(TypeError);
    await expect(
      createListStore(fakeClient([], [1]), lists).lpop("a")
    ).rejects.toThrow(TypeError);
    await expect(
      createListStore(fakeClient([], ["x"]), lists).lrange("a", 0, -1)
    ).rejects.toThrow(TypeError);
    await expect(
      createListStore(fakeClient([], [[1]]), lists).lrange("a", 0, -1)
    ).rejects.toThrow(TypeError);
    await expect(
      createListStore(fakeClient([], ["2"]), lists).llen("a")
    ).rejects.toThrow(TypeError);
    await expect(
      createListStore(fakeClient([], [1]), lists).lset("a", 0, "x")
    ).rejects.toThrow(TypeError);
    await expect(
      createListStore(fakeClient([], [1]), lists).ltrim("a", 0, 1)
    ).rejects.toThrow(TypeError);
  });
});

describe("createSortedSetStore", () => {
  it("supports typed ZADD, ZSCORE, ZRANK, ZCARD, ZCOUNT, ZRANGE, ZREM, ZINCRBY, ZPOPMIN, ZPOPMAX, and DEL", async () => {
    const commands: RedisCommand[] = [];
    const leaderboard = createSortedSetStore(
      fakeClient(commands, [
        2,
        "10",
        0,
        2,
        1,
        ["alice", "bob"],
        [
          ["alice", "10"],
          ["bob", 20]
        ],
        1,
        "12",
        ["alice", "12"],
        ["bob", "20"],
        1
      ]),
      defineSortedSet("leaderboard", codecs.string())
    );

    await expect(
      leaderboard.zadd("game", [
        { member: "alice", score: 10 },
        { member: "bob", score: 20 }
      ])
    ).resolves.toBe(2);
    await expect(leaderboard.zscore("game", "alice")).resolves.toBe(10);
    await expect(leaderboard.zrank("game", "alice")).resolves.toBe(0);
    await expect(leaderboard.zcard("game")).resolves.toBe(2);
    await expect(leaderboard.zcount("game", 0, 15)).resolves.toBe(1);
    await expect(
      leaderboard.zrange("game", { start: 0, stop: -1 })
    ).resolves.toEqual(["alice", "bob"]);
    await expect(
      leaderboard.zrange("game", { start: 0, stop: -1, withScores: true })
    ).resolves.toEqual([
      { member: "alice", score: 10 },
      { member: "bob", score: 20 }
    ]);
    await expect(leaderboard.zrem("game", ["bob"])).resolves.toBe(1);
    await expect(leaderboard.zincrby("game", 2, "alice")).resolves.toBe(12);
    await expect(leaderboard.zpopmin("game")).resolves.toEqual({
      member: "alice",
      score: 12
    });
    await expect(leaderboard.zpopmax("game")).resolves.toEqual({
      member: "bob",
      score: 20
    });
    await expect(leaderboard.del("game")).resolves.toBe(1);

    expect(commands).toEqual([
      ["ZADD", "leaderboard:game", 10, "alice", 20, "bob"],
      ["ZSCORE", "leaderboard:game", "alice"],
      ["ZRANK", "leaderboard:game", "alice"],
      ["ZCARD", "leaderboard:game"],
      ["ZCOUNT", "leaderboard:game", 0, 15],
      ["ZRANGE", "leaderboard:game", 0, -1],
      ["ZRANGE", "leaderboard:game", 0, -1, "WITHSCORES"],
      ["ZREM", "leaderboard:game", "bob"],
      ["ZINCRBY", "leaderboard:game", 2, "alice"],
      ["ZPOPMIN", "leaderboard:game"],
      ["ZPOPMAX", "leaderboard:game"],
      ["DEL", "leaderboard:game"]
    ]);
  });

  it("skips empty sorted-set member inputs and returns null for missing values", async () => {
    const commands: RedisCommand[] = [];
    const leaderboard = createSortedSetStore(
      fakeClient(commands, [null, null, []]),
      defineSortedSet("leaderboard", codecs.string())
    );

    await expect(leaderboard.zadd("game", [])).resolves.toBe(0);
    await expect(leaderboard.zrem("game", [])).resolves.toBe(0);
    await expect(leaderboard.zscore("game", "alice")).resolves.toBeNull();
    await expect(leaderboard.zrank("game", "alice")).resolves.toBeNull();
    await expect(leaderboard.zpopmin("game")).resolves.toBeNull();

    expect(commands).toEqual([
      ["ZSCORE", "leaderboard:game", "alice"],
      ["ZRANK", "leaderboard:game", "alice"],
      ["ZPOPMIN", "leaderboard:game"]
    ]);
  });

  it("throws on unexpected sorted-set replies", async () => {
    const leaderboard = defineSortedSet("leaderboard", codecs.string());

    await expect(
      createSortedSetStore(fakeClient([], ["2"]), leaderboard).zadd("game", [
        { member: "alice", score: 1 }
      ])
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], [true]), leaderboard).zscore(
        "game",
        "alice"
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], ["nope"]), leaderboard).zscore(
        "game",
        "alice"
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], ["0"]), leaderboard).zrank(
        "game",
        "alice"
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], ["2"]), leaderboard).zcard("game")
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], ["alice"]), leaderboard).zrange(
        "game",
        { start: 0, stop: -1 }
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], [["alice"]]), leaderboard).zrange(
        "game",
        { start: 0, stop: -1, withScores: true }
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], [[1, "1"]]), leaderboard).zrange(
        "game",
        { start: 0, stop: -1, withScores: true }
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(
        fakeClient([], [["alice", "nope"]]),
        leaderboard
      ).zrange("game", { start: 0, stop: -1, withScores: true })
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], [null]), leaderboard).zincrby(
        "game",
        1,
        "alice"
      )
    ).rejects.toThrow(TypeError);
    await expect(
      createSortedSetStore(fakeClient([], ["1"]), leaderboard).del("game")
    ).rejects.toThrow(TypeError);
  });
});

describe("createHashStore", () => {
  it("emits HSET, HGET, and DEL commands", async () => {
    const commands: RedisCommand[] = [];
    const replies: RedisReply[] = [2, ["beni", "42"], 1];
    const client = fakeClient(commands, replies);
    const users = defineHash("user", {
      name: codecs.string(),
      score: codecs.number()
    });
    const store = createHashStore(client, users);

    await store.hset("42", { name: "beni", score: 42 });
    await expect(store.hget("42")).resolves.toEqual({
      name: "beni",
      score: 42
    });
    await expect(store.del("42")).resolves.toBe(1);

    expect(commands).toEqual([
      ["HSET", "user:42", "name", "beni", "score", "42"],
      ["HMGET", "user:42", "name", "score"],
      ["DEL", "user:42"]
    ]);
  });

  it("supports typed field-level HSET, HGET, HDEL, HEXISTS, and HINCRBY", async () => {
    const commands: RedisCommand[] = [];
    const users = defineHash("user", {
      name: codecs.string(),
      score: codecs.number()
    });
    const store = createHashStore(
      fakeClient(commands, [1, "beni", 1, 1, 43]),
      users
    );

    await expect(store.hset("42", "name", "beni")).resolves.toBe(1);
    await expect(store.hget("42", "name")).resolves.toBe("beni");
    await expect(store.hdel("42", "name")).resolves.toBe(1);
    await expect(store.hexists("42", "score")).resolves.toBe(true);
    await expect(store.hincrby("42", "score", 1)).resolves.toBe(43);

    expect(commands).toEqual([
      ["HSET", "user:42", "name", "beni"],
      ["HGET", "user:42", "name"],
      ["HDEL", "user:42", "name"],
      ["HEXISTS", "user:42", "score"],
      ["HINCRBY", "user:42", "score", 1]
    ]);
  });

  it("returns null for missing hash fields", async () => {
    const store = createHashStore(
      fakeClient([], [null]),
      defineHash("user", {
        name: codecs.string()
      })
    );

    await expect(store.hget("42", "name")).resolves.toBeNull();
  });

  it("adds EXPIRE after HSET commands", async () => {
    const commands: RedisCommand[] = [];
    const users = defineHash("user", {
      name: codecs.string(),
      score: codecs.number()
    });
    const store = createHashStore(fakeClient(commands, [2, 1]), users);

    await store.hset("42", { name: "beni", score: 42 }, { ttlSeconds: 60 });

    expect(commands).toEqual([
      ["HSET", "user:42", "name", "beni", "score", "42"],
      ["EXPIRE", "user:42", 60]
    ]);
  });

  it("returns null for missing hashes", async () => {
    const store = createHashStore(
      fakeClient([], [[null, null]]),
      defineHash("user", {
        name: codecs.string(),
        score: codecs.number()
      })
    );

    await expect(store.hget("missing")).resolves.toBeNull();
  });

  it("throws on unexpected HSET, HGET, and DEL replies", async () => {
    const users = defineHash("user", {
      name: codecs.string()
    });

    await expect(
      createHashStore(fakeClient([], ["OK"]), users).hset("42", {
        name: "beni"
      })
    ).rejects.toThrow(TypeError);

    await expect(
      createHashStore(fakeClient([], [1]), users).hget("42")
    ).rejects.toThrow(TypeError);

    await expect(
      createHashStore(fakeClient([], ["1"]), users).del("42")
    ).rejects.toThrow(TypeError);
  });

  it("throws on unexpected field-level hash replies", async () => {
    const users = defineHash("user", {
      name: codecs.string(),
      score: codecs.number()
    });

    await expect(
      createHashStore(fakeClient([], ["1"]), users).hset("42", "name", "beni")
    ).rejects.toThrow(TypeError);
    await expect(
      createHashStore(fakeClient([], [1]), users).hget("42", "name")
    ).rejects.toThrow(TypeError);
    await expect(
      createHashStore(fakeClient([], ["1"]), users).hdel("42", "name")
    ).rejects.toThrow(TypeError);
    await expect(
      createHashStore(fakeClient([], ["1"]), users).hexists("42", "name")
    ).rejects.toThrow(TypeError);
    await expect(
      createHashStore(fakeClient([], ["43"]), users).hincrby("42", "score", 1)
    ).rejects.toThrow(TypeError);
    await expect(
      createHashStore(fakeClient([], [43]), users).hincrby("42", "score", 1.5)
    ).rejects.toThrow(TypeError);
  });

  it("throws on unknown hash fields at runtime", async () => {
    const store = createHashStore(
      fakeClient([], []),
      defineHash("user", {
        name: codecs.string()
      })
    );

    await expect(store.hget("42", "missing" as never)).rejects.toThrow(
      TypeError
    );
  });
});

describe("shared key lifecycle ops (EXISTS/TTL/EXPIRE/PERSIST)", () => {
  // Every keyed store spreads the same createKeyLifecycleOps helper; hash and
  // sorted-set stand in for the whole family.
  it("emits the kv wire shapes from a hash store", async () => {
    const commands: RedisCommand[] = [];
    const store = createHashStore(
      fakeClient(commands, [1, 60, 1, 0]),
      defineHash("user", { name: codecs.string() })
    );

    await expect(store.exists("42")).resolves.toBe(true);
    await expect(store.ttl("42")).resolves.toBe(60);
    await expect(store.expire("42", 30)).resolves.toBe(true);
    await expect(store.persist("42")).resolves.toBe(false);

    expect(commands).toEqual([
      ["EXISTS", "user:42"],
      ["TTL", "user:42"],
      ["EXPIRE", "user:42", 30],
      ["PERSIST", "user:42"]
    ]);
  });

  it("maps numeric replies to booleans on a sorted-set store", async () => {
    const commands: RedisCommand[] = [];
    const store = createSortedSetStore(
      fakeClient(commands, [0, -2, 0, 1]),
      defineSortedSet("leaderboard", codecs.string())
    );

    await expect(store.exists("game")).resolves.toBe(false);
    await expect(store.ttl("game")).resolves.toBe(-2);
    await expect(store.expire("game", 30)).resolves.toBe(false);
    await expect(store.persist("game")).resolves.toBe(true);

    expect(commands).toEqual([
      ["EXISTS", "leaderboard:game"],
      ["TTL", "leaderboard:game"],
      ["EXPIRE", "leaderboard:game", 30],
      ["PERSIST", "leaderboard:game"]
    ]);
  });

  it("rejects invalid expire ttls before sending any command", async () => {
    const commands: RedisCommand[] = [];
    const store = createHashStore(
      fakeClient(commands, []),
      defineHash("user", { name: codecs.string() })
    );

    await expect(store.expire("42", 0)).rejects.toThrow(ValidationError);
    await expect(store.expire("42", 1.5)).rejects.toThrow(
      "ttlSeconds must be a positive safe integer"
    );
    expect(commands).toEqual([]);
  });

  it("throws ReplyShapeError on non-number lifecycle replies", async () => {
    const leaderboard = defineSortedSet("leaderboard", codecs.string());

    await expect(
      createSortedSetStore(fakeClient([], ["1"]), leaderboard).exists("game")
    ).rejects.toThrow(ReplyShapeError);
    await expect(
      createSortedSetStore(fakeClient([], ["60"]), leaderboard).ttl("game")
    ).rejects.toThrow(ReplyShapeError);
    await expect(
      createSortedSetStore(fakeClient([], ["1"]), leaderboard).expire(
        "game",
        30
      )
    ).rejects.toThrow(ReplyShapeError);
    await expect(
      createSortedSetStore(fakeClient([], ["1"]), leaderboard).persist("game")
    ).rejects.toThrow(ReplyShapeError);
  });
});

function fakeClient(
  commands: RedisCommand[],
  replies: RedisReply[]
): RedisClient {
  return {
    async send(command) {
      commands.push(command);
      const reply = replies.shift();
      if (reply === undefined) throw new Error("No fake Redis reply queued");
      return reply;
    },
    async pipeline(pipelineCommands) {
      commands.push(...pipelineCommands);
      return replies.splice(0, pipelineCommands.length);
    },
    async close() {}
  };
}

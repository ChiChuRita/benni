import { describe, expect, it } from "vitest";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  RedisSession
} from "../src/core/types.js";
import { benni } from "../src/index.js";
import {
  channel,
  hash,
  kv,
  list,
  lock,
  number,
  pattern,
  ratelimit,
  script,
  string,
  zset
} from "../src/schema.js";
import { fakeClient, fakeSession } from "./fake-client.js";

const users = hash("user", { name: string() });
const views = kv("views", number());
const jobs = list("jobs", string());
const board = zset("board", string());
const apiLimit = ratelimit("api", { limit: 10, windowMs: 60_000 });
const orderLock = lock("order", { ttlMs: 10_000 });
const events = channel("events", string());
const everything = pattern("events:*", string());
const bump = script("bump", {
  keys: ["counter"],
  args: {},
  returns: number(),
  lua: 'return redis.call("INCR", KEYS[1])'
});
const schema = {
  users,
  views,
  jobs,
  board,
  apiLimit,
  orderLock,
  events,
  everything,
  bump
};

/** A client whose sessions log into `sessionCommands`, apart from the shared ones. */
function sessionClient(
  shared: RedisCommand[],
  sharedReplies: RedisReply[],
  sessionCommands: RedisCommand[],
  sessionReplies: RedisReply[]
): RedisClient & { session(): Promise<RedisSession> } {
  return {
    ...fakeClient(shared, sharedReplies),
    async session() {
      return fakeSession(sessionCommands, sessionReplies);
    }
  };
}

describe("one reach rule", () => {
  it("has no per-kind accessors on the handle", () => {
    const redis = benni({ client: fakeClient([], []), schema });
    for (const accessor of [
      "kv",
      "hash",
      "set",
      "list",
      "zset",
      "stream",
      "geo",
      "hll",
      "bitmap",
      "counter",
      "string",
      "script"
    ]) {
      expect(redis).not.toHaveProperty(accessor);
    }
    expect(Object.keys(redis.pubsub)).toEqual(["close"]);
  });

  it("returns from redis.store() the very resource redis.query holds", () => {
    const redis = benni({ client: fakeClient([], []), schema });
    expect(redis.store(users)).toBe(redis.query.users);
    expect(redis.store(apiLimit)).toBe(redis.query.apiLimit);
    expect(redis.store(events)).toBe(redis.query.events);
    expect(redis.store(bump)).toBe(redis.query.bump);
  });

  it("builds one resource per unbound schema and keeps it", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni({ client: fakeClient(commands, [1]) });
    const other = kv("other", number());

    expect(redis.store(other)).toBe(redis.store(other));
    await expect(redis.store(other).incr("a")).resolves.toBe(1);
    expect(commands).toEqual([["INCR", "other:a"]]);
  });

  it("reaches primitives, channels, and scripts through store()", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni({ client: fakeClient(commands, [1, "sha", 5]) });

    await expect(redis.store(events).publish("hi")).resolves.toBe(1);
    await expect(
      redis.store(bump).run({ keys: { counter: "c" }, args: {} })
    ).resolves.toBe(5);
    expect(typeof redis.store(orderLock).acquire).toBe("function");
    expect(redis.store(events).at("42").channelName()).toBe("events:42");
    expect(commands[0]).toEqual(["PUBLISH", "events", "hi"]);
  });

  it("refuses an object that is not a benni schema", () => {
    const redis = benni({ client: fakeClient([], []) });
    expect(() => redis.store({ kind: "kv" } as never)).toThrow(
      /redis\.store\(\) schema was not built by a benni schema builder/
    );
  });
});

describe("session.query", () => {
  it("holds the data stores of the bound module, bound to the session", async () => {
    const shared: RedisCommand[] = [];
    const leased: RedisCommand[] = [];
    const redis = benni({
      client: sessionClient(shared, [], leased, ["Ada", 3]),
      schema
    });

    await redis.session(async (s) => {
      expect(Object.keys(s.query).sort()).toEqual([
        "board",
        "jobs",
        "users",
        "views"
      ]);
      await expect(s.query.users.hget("1", "name")).resolves.toBe("Ada");
      await expect(s.query.views.incr("p")).resolves.toBe(3);
      // The blocking superset, which only a session has.
      expect(typeof s.query.jobs.blpop).toBe("function");
      expect(typeof s.query.board.bzpopmin).toBe("function");
    });

    expect(leased).toEqual([
      ["HGET", "user:1", "name"],
      ["INCR", "views:p"]
    ]);
    expect(shared).toEqual([]);
  });

  it("resolves each entry once per session", async () => {
    const redis = benni({
      client: sessionClient([], [], [], []),
      schema
    });
    await redis.session(async (s) => {
      expect(s.query.users).toBe(s.query.users);
      expect(s.store(users)).toBe(s.query.users);
    });
  });

  it("runs incr with a TTL on the session's own connection", async () => {
    const shared: RedisCommand[] = [];
    const leased: RedisCommand[] = [];
    const redis = benni({
      client: sessionClient(shared, [], leased, ["sha", 1]),
      schema
    });

    await redis.session((s) => s.query.views.incr("p", { ttlMs: 1_000 }));

    expect(leased.map((command) => command[0])).toEqual(["SCRIPT", "EVALSHA"]);
    expect(shared).toEqual([]);
  });

  it("reaches an unbound data store through session.store()", async () => {
    const leased: RedisCommand[] = [];
    const redis = benni({ client: sessionClient([], [], leased, [2]) });
    const queue = list("queue", string());

    await redis.session(async (s) => {
      expect(typeof s.store(queue).brpop).toBe("function");
      await expect(s.store(queue).llen("q")).resolves.toBe(2);
    });
    expect(leased).toEqual([["LLEN", "queue:q"]]);
  });

  it("refuses a primitive, a channel, or a script, naming the handle", async () => {
    const redis = benni({ client: sessionClient([], [], [], []) });
    await redis.session(async (s) => {
      for (const declared of [orderLock, events, bump]) {
        expect(() => s.store(declared as never)).toThrow(
          /A session reaches only the data stores .*use the handle/
        );
      }
    });
  });

  it("types session.query against the bound schema", async () => {
    const redis = benni({
      client: sessionClient([], [], [], []),
      schema
    });
    await redis.session(async (s) => {
      // @ts-expect-error primitives are reached from the handle.
      void s.query.apiLimit;
      // @ts-expect-error so are channels.
      void s.query.events;
      // @ts-expect-error session.store() takes data-store schemas only.
      void (() => s.store(orderLock));
    });
  });
});

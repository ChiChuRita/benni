import { describe, expect, it } from "vitest";
import type { BenniSession } from "../src/database.js";
import {
  benni,
  numberReply,
  okReply,
  type RedisClient,
  type RedisCommand,
  type RedisReply,
  type RedisSession,
  WatchRetriesExceededError
} from "../src/index.js";
import {
  bitmap,
  channel,
  geo,
  hash,
  hll,
  json,
  kv,
  list,
  number,
  script,
  set,
  stream,
  string,
  zset
} from "../src/schema.js";
import {
  type FakeWatchedResult,
  fakeClient,
  fakeSession
} from "./fake-client.js";

/**
 * A shared client whose session() hands out a fakeSession backed by the same
 * commands log, so session and shared-connection traffic can be asserted in
 * one ordered array. Both the shared and session reply queues are drained
 * from the same `replies` list (FIFO), matching the fakes' behavior.
 */
function fakeSessionClient(
  commands: RedisCommand[],
  replies: RedisReply[],
  watchedResults: FakeWatchedResult[] = []
): RedisClient {
  const base = fakeClient(commands, replies);
  return {
    ...base,
    async session(): Promise<RedisSession> {
      return fakeSession(commands, replies, watchedResults);
    }
  };
}

describe("benni", () => {
  it("binds key-value schemas to a client", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, ["OK", '{"name":"Ada"}']));
    const profiles = kv("profile", json<{ name: string }>());

    const store = db.kv(profiles);

    await store.set("42", { name: "Ada" }, { ttlSeconds: 60 });
    await expect(store.get("42")).resolves.toEqual({ name: "Ada" });

    expect(store.key("42")).toBe("profile:42");
    expect(commands).toEqual([
      ["SET", "profile:42", '{"name":"Ada"}', "EX", 60],
      ["GET", "profile:42"]
    ]);
  });

  it("rejects a set combining nx and xx", async () => {
    const db = benni(fakeClient([], []));
    const profiles = kv("profile", json<{ name: string }>());

    await expect(
      db
        .kv(profiles)
        // @ts-expect-error nx+xx no longer compiles; pin the runtime guard for JS callers
        .set("42", { name: "Ada" }, { nx: true, xx: true })
    ).rejects.toThrow("nx cannot be combined with xx");
  });

  it("binds hash schemas to a client", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [2, 1, ["Ada", "10"]]));
    const users = hash("user", {
      name: string(),
      score: number()
    });

    const store = db.hash(users);

    await store.hset("42", { name: "Ada", score: 10 }, { ttlSeconds: 120 });
    await expect(store.hget("42")).resolves.toEqual({ name: "Ada", score: 10 });

    expect(store.key("42")).toBe("user:42");
    expect(commands).toEqual([
      ["HSET", "user:42", "name", "Ada", "score", "10"],
      ["EXPIRE", "user:42", 120],
      ["HMGET", "user:42", "name", "score"]
    ]);
  });

  it("publishes typed channel messages through the raw client", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [1]));
    const events = channel("events:user", json<{ id: string }>());

    await expect(db.pubsub.channel(events).publish({ id: "42" })).resolves.toBe(
      1
    );

    expect(commands).toEqual([["PUBLISH", "events:user", '{"id":"42"}']]);
  });

  it("binds zset and hll schemas to a client", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [1, 1]));
    const leaderboard = zset("leaderboard", string());
    const visitors = hll("visitors", string());

    await expect(
      db.zset(leaderboard).zadd("daily", [{ member: "ada", score: 100 }])
    ).resolves.toBe(1);
    await expect(db.hll(visitors).pfadd("today", ["u1"])).resolves.toBe(true);

    expect(commands).toEqual([
      ["ZADD", "leaderboard:daily", 100, "ada"],
      ["PFADD", "visitors:today", "u1"]
    ]);
  });

  it("runs typed scripts with named keys and args", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, ["sha-1", 6]));
    const incrementBy = script("incrementBy", {
      keys: ["counter"],
      args: {
        amount: number()
      },
      returns: number(),
      lua: "return redis.call('INCRBY', KEYS[1], ARGV[1])"
    });

    await expect(
      db.script(incrementBy).run({
        keys: {
          counter: "counter:page-views"
        },
        args: {
          amount: 5
        }
      })
    ).resolves.toBe(6);

    expect(commands).toEqual([
      ["SCRIPT", "LOAD", "return redis.call('INCRBY', KEYS[1], ARGV[1])"],
      ["EVALSHA", "sha-1", 1, "counter:page-views", "5"]
    ]);
  });

  it("binds stream schemas to a client", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(
      fakeClient(commands, ["1-1", [["1-1", ["type", "click", "size", "2"]]]])
    );
    const events = stream("events", {
      type: string(),
      size: number()
    });

    const store = db.stream(events);

    await expect(store.xadd("42", { type: "click", size: 2 })).resolves.toBe(
      "1-1"
    );
    await expect(store.xrange("42")).resolves.toEqual([
      { id: "1-1", value: { type: "click", size: 2 } }
    ]);

    expect(store.key("42")).toBe("events:42");
    expect(commands).toEqual([
      ["XADD", "events:42", "*", "type", "click", "size", "2"],
      ["XRANGE", "events:42", "-", "+"]
    ]);
  });

  it("binds bitmap schemas to a client", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [0, 1]));
    const flags = bitmap("flags");

    const store = db.bitmap(flags);

    await expect(store.setbit("42", 7, true)).resolves.toBe(false);
    await expect(store.bitcount("42")).resolves.toBe(1);

    expect(store.key("42")).toBe("flags:42");
    expect(commands).toEqual([
      ["SETBIT", "flags:42", 7, 1],
      ["BITCOUNT", "flags:42"]
    ]);
  });

  it("binds geo schemas to a client", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [1, "877.4"]));
    const cities = geo("cities", string());

    const store = db.geo(cities);

    await expect(
      store.geoadd("eu", [
        { member: "berlin", longitude: 13.4, latitude: 52.5 }
      ])
    ).resolves.toBe(1);
    await expect(store.geodist("eu", "berlin", "paris", "km")).resolves.toBe(
      877.4
    );

    expect(store.key("eu")).toBe("cities:eu");
    expect(commands).toEqual([
      ["GEOADD", "cities:eu", 13.4, 52.5, "berlin"],
      ["GEODIST", "cities:eu", "berlin", "paris", "km"]
    ]);
  });

  it("binds counter keyspaces to a client and deletes with DEL", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [1, 1]));
    const hits = kv("hits", number());

    const store = db.counter(hits);

    await expect(store.incr("page")).resolves.toBe(1);
    await expect(store.del("page")).resolves.toBe(1);

    expect(store.key("page")).toBe("hits:page");
    expect(commands).toEqual([
      ["INCR", "hits:page"],
      ["DEL", "hits:page"]
    ]);
  });

  it("binds string keyspaces to a client and deletes with DEL", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [2, "hi", 1]));
    const notes = kv("note", string());

    const store = db.string(notes);

    await expect(store.append("42", "hi")).resolves.toBe(2);
    await expect(store.getex("42", 60)).resolves.toBe("hi");
    await expect(store.del("42")).resolves.toBe(1);

    expect(store.key("42")).toBe("note:42");
    expect(commands).toEqual([
      ["APPEND", "note:42", "hi"],
      ["GETEX", "note:42", "EX", 60],
      ["DEL", "note:42"]
    ]);
  });

  it("scans keys across cursor pages", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(
      fakeClient(commands, [
        ["3", ["a:1", "a:2"]],
        ["0", ["a:3"]]
      ])
    );

    const keys: string[] = [];
    for await (const key of db.scan.keys({ match: "a:*" })) {
      keys.push(key);
    }

    expect(keys).toEqual(["a:1", "a:2", "a:3"]);
    expect(commands).toEqual([
      ["SCAN", "0", "MATCH", "a:*"],
      ["SCAN", "3", "MATCH", "a:*"]
    ]);
  });

  it("scans set members across cursor pages", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(
      fakeClient(commands, [
        ["3", ["admin"]],
        ["0", ["editor"]]
      ])
    );
    const roles = set("roles", string());

    const members: string[] = [];
    for await (const member of db.scan.set(roles, "42")) {
      members.push(member);
    }

    expect(members).toEqual(["admin", "editor"]);
    expect(commands).toEqual([
      ["SSCAN", "roles:42", "0"],
      ["SSCAN", "roles:42", "3"]
    ]);
  });

  it("throws eagerly when the client does not support sessions", async () => {
    const db = benni(fakeClient([], []));

    await expect(db.session()).rejects.toThrow(
      "Redis client does not support sessions"
    );
    await expect(
      db.watch("k", async (s) => s.multi().add(["SET", "k", "1"], okReply))
    ).rejects.toThrow("Redis client does not support sessions");
  });

  it("scoped db.session(fn) closes the session on success", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeSessionClient(commands, ["v"]));
    const notes = kv("note", string());

    let leased: BenniSession | undefined;
    const result = await db.session(async (s) => {
      leased = s;
      expect(s.closed).toBe(false);
      return s.kv(notes).get("42");
    });

    expect(result).toBe("v");
    expect(leased?.closed).toBe(true);
    expect(commands).toEqual([["GET", "note:42"]]);
  });

  it("scoped db.session(fn) closes the session when the body throws", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeSessionClient(commands, []));

    let leased: BenniSession | undefined;
    await expect(
      db.session(async (s) => {
        leased = s;
        throw new Error("boom");
      })
    ).rejects.toThrow("boom");

    expect(leased?.closed).toBe(true);
  });

  it("no-arg db.session() leases a session the caller owns", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeSessionClient(commands, [["jobs:pending", "job"]]));
    const jobs = list("jobs", string());

    const session = await db.session();
    try {
      await expect(
        session.list(jobs).blpop("pending", { timeoutSeconds: 5 })
      ).resolves.toBe("job");
      expect(session.closed).toBe(false);
    } finally {
      await session.close();
    }

    expect(session.closed).toBe(true);
    // BLPOP encodes the answering key + value pair; timeout as a string.
    expect(commands).toEqual([["BLPOP", "jobs:pending", "5"]]);
  });

  it("sends the exact wire args for a session blocking pop", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(
      fakeSessionClient(commands, [["jobs:pending", "email-1"]])
    );
    const jobs = list("jobs", string());

    await db.session(async (s) => {
      await expect(
        s.list(jobs).blpop(["urgent", "pending"], { timeoutSeconds: 2.5 })
      ).resolves.toEqual({ id: "pending", value: "email-1" });
    });

    expect(commands).toEqual([["BLPOP", "jobs:urgent", "jobs:pending", "2.5"]]);
  });

  it("reads through a consumer group over the db surface", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(
      fakeClient(commands, [
        [["audit:login", [["1-1", ["type", "click", "userId", "u1"]]]]]
      ])
    );
    const auditEvents = stream("audit", {
      type: string(),
      userId: string()
    });

    const entries = await db
      .stream(auditEvents)
      .group("processors")
      .consumer("c-1")
      .xreadgroup("login");

    expect(entries).toEqual([
      { id: "1-1", value: { type: "click", userId: "u1" } }
    ]);
    expect(commands).toEqual([
      [
        "XREADGROUP",
        "GROUP",
        "processors",
        "c-1",
        "STREAMS",
        "audit:login",
        ">"
      ]
    ]);
  });

  it("reads through a session-only blocking consumer group", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(
      fakeSessionClient(commands, [
        [["audit:login", [["2-1", ["type", "view", "userId", "u2"]]]]]
      ])
    );
    const auditEvents = stream("audit", {
      type: string(),
      userId: string()
    });

    await db.session(async (s) => {
      const live = s.stream(auditEvents).group("processors").consumer("c-1");
      await expect(
        live.xreadgroup("login", { timeoutSeconds: 5, count: 20 })
      ).resolves.toEqual([
        { id: "2-1", value: { type: "view", userId: "u2" } }
      ]);
    });

    // COUNT before BLOCK, BLOCK in ms, then STREAMS key ">".
    expect(commands).toEqual([
      [
        "XREADGROUP",
        "GROUP",
        "processors",
        "c-1",
        "COUNT",
        20,
        "BLOCK",
        "5000",
        "STREAMS",
        "audit:login",
        ">"
      ]
    ]);
  });

  it("db.watch retries after a single conflict then commits", async () => {
    const commands: RedisCommand[] = [];
    // Reply queue (FIFO across shared + session sends): WATCH ok, GET, WATCH
    // ok, GET. watchedResults: attempt 1 aborts (null), attempt 2 commits.
    const db = benni(
      fakeSessionClient(commands, ["OK", null, "OK", "7"], [null, ["OK", 8]])
    );
    const views = kv("views", number());

    const attempts: number[] = [];
    const result = await db.watch(
      views.key("home"),
      async (s) => {
        const current = (await s.kv(views).get("home")) ?? 0;
        return s
          .multi()
          .add(["SET", views.key("home"), current + 1], okReply)
          .add(["INCR", `${views.key("home")}:writes`], numberReply);
      },
      { onAbort: ({ attempt }) => attempts.push(attempt) }
    );

    expect(result).toEqual([undefined, 8]);
    expect(attempts).toEqual([1]);
    expect(commands).toEqual([
      ["WATCH", "views:home"],
      ["GET", "views:home"],
      ["SET", "views:home", 1],
      ["INCR", "views:home:writes"],
      ["WATCH", "views:home"],
      ["GET", "views:home"],
      ["SET", "views:home", 8],
      ["INCR", "views:home:writes"]
    ]);
  });

  it("db.watch throws WatchRetriesExceededError once attempts run out", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeSessionClient(commands, ["OK", "OK"], [null, null]));

    const seen: number[] = [];
    await expect(
      db.watch(
        "views:home",
        async (s) => s.multi().add(["INCR", "views:home"], numberReply),
        { attempts: 2, onAbort: ({ attempt }) => seen.push(attempt) }
      )
    ).rejects.toBeInstanceOf(WatchRetriesExceededError);

    expect(seen).toEqual([1, 2]);
  });

  it("db.watch resolves null and UNWATCHes when the body opts out", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeSessionClient(commands, ["OK", "OK"]));

    const result = await db.watch("k", async () => null);

    expect(result).toBeNull();
    expect(commands).toEqual([["WATCH", "k"], ["UNWATCH"]]);
  });

  it("db.watch does not close a borrowed session", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeSessionClient(commands, ["OK"], [[1]]));

    const borrowed = await db.session();
    try {
      const result = await db.watch(
        "k",
        async (s) => s.multi().add(["INCR", "k"], numberReply),
        { session: borrowed }
      );
      expect(result).toEqual([1]);
      expect(borrowed.closed).toBe(false);
    } finally {
      await borrowed.close();
    }

    expect(borrowed.closed).toBe(true);
  });
});

describe("db.query registry", () => {
  const users = hash("user", { name: string(), score: number() });
  const profiles = kv("profile", json<{ tier: string }>());
  const board = zset("board", string());
  const visits = kv("visits", number());
  const schema = { users, profiles, board, visits };

  it("dispatches each schema to its typed store by kind", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [2, 1, "OK", '{"tier":"gold"}']), {
      schema
    });

    await db.query.users.hset("42", { name: "Ada", score: 10 });
    await db.query.board.zadd("daily", [{ member: "ada", score: 100 }]);
    await db.query.profiles.set("42", { tier: "gold" }, { ttlSeconds: 60 });
    await expect(db.query.profiles.get("42")).resolves.toEqual({
      tier: "gold"
    });

    expect(commands).toEqual([
      ["HSET", "user:42", "name", "Ada", "score", "10"],
      ["ZADD", "board:daily", 100, "ada"],
      ["SET", "profile:42", '{"tier":"gold"}', "EX", 60],
      ["GET", "profile:42"]
    ]);
  });

  it("exposes key() and delete() on registry resources", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [1]), { schema });

    expect(db.query.users.key("42")).toBe("user:42");
    await expect(db.query.users.del("42")).resolves.toBe(1);
    expect(commands).toEqual([["DEL", "user:42"]]);
  });

  it("is an empty object when no schema is bound", () => {
    const db = benni(fakeClient([], []));
    expect(db.query).toEqual({});
  });

  it("skips schema-module entries that are not schemas", async () => {
    const commands: RedisCommand[] = [];
    const db = benni(fakeClient(commands, [1, 1]), {
      schema: {
        users,
        NOT_A_SCHEMA: { hello: "world" } as unknown as typeof users
      }
    });

    await db.query.users.hset("7", { name: "Grace", score: 1 });
    expect(commands).toEqual([
      ["HSET", "user:7", "name", "Grace", "score", "1"]
    ]);
    expect("NOT_A_SCHEMA" in db.query).toBe(false);
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typeClient = null as unknown as RedisClient;
const typeDb = benni(typeClient);

const typeProfiles = kv("type-profile", json<{ name: string }>());
const typeKvStore = typeDb.kv(typeProfiles);
const typeRoles = set("type-role", json<{ role: string }>());
const typeEvents = stream("type-event", {
  type: string(),
  size: number()
});
const typeStreamStore = typeDb.stream(typeEvents);

type StreamAddValue = Parameters<typeof typeStreamStore.xadd>[1];
type _StreamAddValue = Expect<
  Equal<StreamAddValue, { type: string; size: number }>
>;

function databaseTypeAssertions() {
  const plainSet = typeKvStore.set("42", { name: "Ada" });
  const ttlSet = typeKvStore.set("42", { name: "Ada" }, { ttlSeconds: 60 });
  const nxSet = typeKvStore.set("42", { name: "Ada" }, { nx: true });
  const xxSet = typeKvStore.set(
    "42",
    { name: "Ada" },
    { ttlSeconds: 60, xx: true }
  );
  const scannedMembers = typeDb.scan.set(typeRoles, "42");
  type _PlainSet = Expect<Equal<typeof plainSet, Promise<void>>>;
  type _TtlSet = Expect<Equal<typeof ttlSet, Promise<void>>>;
  type _NxSet = Expect<Equal<typeof nxSet, Promise<boolean>>>;
  type _XxSet = Expect<Equal<typeof xxSet, Promise<boolean>>>;
  type _ScannedMembers = Expect<
    Equal<typeof scannedMembers, AsyncIterable<{ role: string }>>
  >;
  void plainSet;
  void ttlSet;
  void nxSet;
  void xxSet;
  void scannedMembers;

  // nx and xx are now mutually exclusive at runtime, not the type level:
  // ConditionalSetOptions accepts the combination, and set() throws. See the
  // "rejects a set combining nx and xx" runtime test above.

  // @ts-expect-error counters require a number-codec keyspace.
  void typeDb.counter(kv("type-note", string()));

  // @ts-expect-error stream values must match the declared field codecs.
  void typeStreamStore.xadd("42", { type: "click", size: "2" });
}

void databaseTypeAssertions;

const registryDb = benni(typeClient, {
  schema: {
    users: hash("q-user", { name: string(), score: number() }),
    board: zset("q-board", string()),
    profiles: kv("q-profile", json<{ tier: string }>()),
    increment: script("q-incr", {
      keys: ["counter"],
      args: { amount: number() },
      returns: number(),
      lua: "return 1"
    })
  }
});

function registryTypeAssertions() {
  // db.query.<name> resolves each schema to its typed store, dispatched by
  // the schema's kind — the same resource db.<kind>(schema) returns.
  // hset is overloaded (whole-record vs single-field); Parameters resolves to
  // the last overload, so probe the whole-record form via a call.
  const usersRecordSet = (value: { name: string; score: number }) =>
    registryDb.query.users.hset("42", value);
  type UsersSetValue = Parameters<typeof usersRecordSet>[0];
  type _UsersSetValue = Expect<
    Equal<UsersSetValue, { name: string; score: number }>
  >;

  type ProfileGet = Awaited<ReturnType<typeof registryDb.query.profiles.get>>;
  type _ProfileGet = Expect<Equal<ProfileGet, { tier: string } | null>>;

  // A zset schema resolves to the sorted-set resource (zrange with
  // withScores returns member/score entries, not a plain member array).
  const scores = registryDb.query.board.zrange("daily", {
    start: 0,
    stop: -1,
    withScores: true
  });
  type _Scores = Expect<
    Equal<
      Awaited<typeof scores>,
      Array<{ readonly member: string; readonly score: number }>
    >
  >;
  void scores;

  // A script schema resolves to the runnable resource.
  const scriptRun = registryDb.query.increment.run({
    keys: { counter: "page:views" },
    args: { amount: 5 }
  });
  type _ScriptRun = Expect<Equal<Awaited<typeof scriptRun>, number>>;
  void scriptRun;

  // A db without a bound schema has an empty registry.
  type EmptyQuery = typeof typeDb.query;
  type _EmptyQuery = Expect<Equal<keyof EmptyQuery, never>>;
}

void registryTypeAssertions;

const typeJobs = list("type-jobs", string());
const typeViews = kv("type-views", number());

function sessionTypeAssertions() {
  const session = null as unknown as BenniSession;

  // Blocking methods exist only on the session's list accessor — the shared
  // db.list has no blpop (ts(2339), not a runtime throw).
  const sessionList = session.list(typeJobs);
  void sessionList.blpop;

  // @ts-expect-error blocking pops are session-only, absent on the shared store.
  void typeDb.list(typeJobs).blpop;

  // { timeoutSeconds: "forever" } removes null from the return type: the call can
  // time out never — it either resolves a value or rejects on close.
  const forever = sessionList.blpop("pending", {
    timeoutSeconds: "forever"
  });
  const maybe = sessionList.blpop("pending", { timeoutSeconds: 5 });
  type _ForeverPop = Expect<Equal<Awaited<typeof forever>, string>>;
  type _MaybePop = Expect<Equal<Awaited<typeof maybe>, string | null>>;
  void forever;
  void maybe;

  // The session zset accessor is a superset too (blocking + non-blocking).
  const sessionZset = session.zset(zset("type-scores", string()));
  void sessionZset.bzpopmin;
  void sessionZset.zadd;

  // benni() rejects a session: RedisSession is not assignable to RedisClient
  // (no pipeline, no plain transaction), so there is no typed route from a
  // session to the shared surface.
  // @ts-expect-error benni(session.raw) must not compile.
  void benni(session.raw);

  // The watched builder infers a growing tuple through add() and exec()
  // returns that tuple or null (abort).
  const built = session
    .multi()
    .add(["SET", "k", "1"], okReply)
    .add(["INCR", "n"], numberReply);
  type BuiltResult = Awaited<ReturnType<typeof built.exec>>;
  type _BuiltResult = Expect<Equal<BuiltResult, [void, number] | null>>;

  const watched = typeDb.watch(typeViews.key("home"), async (s) =>
    s
      .kv(typeViews)
      .get("home")
      .then(() => s.multi().add(["INCR", typeViews.key("home")], numberReply))
  );
  type _WatchResult = Expect<Equal<Awaited<typeof watched>, [number] | null>>;
  void watched;
}

void sessionTypeAssertions;

import { expect } from "vitest";
import {
  codecs,
  createCounterStore,
  createHashStore,
  createKeyValueStore,
  createListStore,
  createSetStore,
  createSortedSetStore,
  createStringStore,
  defineHash,
  defineKeyspace,
  defineList,
  defineSet,
  defineSortedSet,
  type RedisClient,
  RedisServerError
} from "../src/core/index.js";
import {
  booleanNumberReply,
  createTransaction,
  numberReply,
  okReply,
  stringOrNullReply
} from "../src/core/transaction.js";

export type RedisClientFactory = () => Promise<RedisClient>;

/**
 * Resolve to whatever a promise rejected with. `expect(...).rejects` cannot
 * hand the error back for property assertions, and this suite runs under two
 * test runners, so it stays a plain helper.
 */
async function captureError(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("Expected the operation to reject, but it resolved");
}

/**
 * Runner-agnostic poll: this contract suite executes under both Vitest and
 * `bun test`, so it cannot use helpers exclusive to either (vi.waitUntil).
 */
async function waitUntil(
  predicate: () => boolean,
  timeoutMs = 1000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for a Pub/Sub message");
}

export type RedisClientContractOptions = {
  /**
   * Set when the transport cannot carry a Redis error reply for a *failed
   * transaction*, so `transaction()` rejects with a plain transport error rather
   * than a `RedisServerError`.
   *
   * This is a property of the endpoint, not of Benni. Over the REST protocol a
   * service sits in front of Redis and decides what a failed `MULTI`/`EXEC`
   * looks like on the wire. `hiett/serverless-redis-http`, the endpoint CI runs
   * against, answers with an empty-bodied HTTP 500:
   *
   * ```text
   * POST /pipeline   [["PING"],["ZADD","str","1","member"]]
   *   -> 200  [{"result":"PONG"},{"error":"WRONGTYPE Operation against..."}]
   * POST /multi-exec [["PING"],["ZADD","str","1","member"]]
   *   -> 500  (no body)
   * ```
   *
   * There is no reply to normalize, and inventing a `.code` from a gateway's
   * status line is exactly what `benni/upstash` refuses to do (see the 5xx
   * boundary in `src/upstash/index.ts`). The rejection itself is still
   * contractual, so the assertion below narrows rather than disappearing.
   */
  readonly transactionErrorsCarryNoReply?: boolean;
};

export async function expectRedisClientContract(
  createClient: RedisClientFactory,
  options: RedisClientContractOptions = {}
): Promise<void> {
  const client = await createClient();
  const id = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  const rawKey = `benni:test:${id}`;
  const profiles = defineKeyspace(
    "benni:profile",
    codecs.json<{ name: string; score: number }>()
  );
  const users = defineHash("benni:user", {
    name: codecs.string(),
    score: codecs.number()
  });
  const profileStore = createKeyValueStore(client, profiles);
  const texts = defineKeyspace("benni:text", codecs.string());
  const textStore = createStringStore(client, texts);
  const counters = defineKeyspace("benni:counter", codecs.number());
  const counterStore = createCounterStore(client, counters);
  const roles = defineSet("benni:roles", codecs.string());
  const roleStore = createSetStore(client, roles);
  const userStore = createHashStore(client, users);
  const jobs = defineList("benni:jobs", codecs.json<{ name: string }>());
  const jobStore = createListStore(client, jobs);
  const leaderboard = defineSortedSet("benni:leaderboard", codecs.string());
  const leaderboardStore = createSortedSetStore(client, leaderboard);

  try {
    await expect(client.send(["PING"])).resolves.toBe("PONG");
    await expect(client.send(["SET", rawKey, "benni"])).resolves.toBe("OK");
    await expect(client.send(["GET", rawKey])).resolves.toBe("benni");
    await expect(
      client.pipeline([
        ["SET", rawKey, "pipeline"],
        ["GET", rawKey]
      ])
    ).resolves.toEqual(["OK", "pipeline"]);

    if (client.transaction) {
      const transactionKey = `${rawKey}:transaction`;
      const transactionResults = await createTransaction(client)
        .add(["SET", transactionKey, "transaction"], okReply)
        .add(["GET", transactionKey], stringOrNullReply)
        .add(["EXISTS", transactionKey], booleanNumberReply)
        .add(["DEL", transactionKey], numberReply)
        .exec();
      expect(transactionResults).toEqual([undefined, "transaction", true, 1]);
    }

    // Server errors are normalized. Whatever taxonomy the underlying client
    // uses — node-redis's SimpleError, ioredis's ReplyError, Bun's RedisError,
    // an Upstash `{ error }` payload — what reaches the caller is one
    // RedisServerError carrying the parsed code, so `catch` is portable.
    const wrongTypeKey = `${rawKey}:wrongtype`;
    await client.send(["DEL", wrongTypeKey]);
    await client.send(["HSET", wrongTypeKey, "field", "1"]);
    const sendError = await captureError(
      client.send(["ZADD", wrongTypeKey, "1", "member"])
    );
    expect(sendError).toBeInstanceOf(RedisServerError);
    const normalized = sendError as RedisServerError;
    expect(normalized.code).toBe("WRONGTYPE");
    // The message stays the server's verbatim, code first, which is what the
    // NOSCRIPT and BUSYGROUP checks in core match on.
    expect(normalized.message.startsWith("WRONGTYPE ")).toBe(true);
    // Attribution is available on a single send, where the throw site knows it.
    expect(normalized.command).toBe("ZADD");
    // Nothing is discarded: the adapter-native error (or raw REST payload) is
    // still reachable.
    expect(normalized.cause).toBeDefined();

    // The batch paths normalize too. Which entry failed is not recoverable on
    // every adapter, so only the type and code are contractual here.
    const pipelineError = await captureError(
      client.pipeline([["PING"], ["ZADD", wrongTypeKey, "1", "member"]])
    );
    expect(pipelineError).toBeInstanceOf(RedisServerError);
    expect((pipelineError as RedisServerError).code).toBe("WRONGTYPE");

    if (client.transaction) {
      const transactionError = await captureError(
        client.transaction([["PING"], ["ZADD", wrongTypeKey, "1", "member"]])
      );
      // Rejecting is contractual on every transport: a transaction that failed
      // must never resolve as if it had committed.
      expect(transactionError).toBeInstanceOf(Error);
      if (options.transactionErrorsCarryNoReply !== true) {
        expect(transactionError).toBeInstanceOf(RedisServerError);
        expect((transactionError as RedisServerError).code).toBe("WRONGTYPE");
      }
    }
    await client.send(["DEL", wrongTypeKey]);

    if (client.session) {
      const sessionKey = `${rawKey}:session`;
      const session = await client.session();
      try {
        // Blocking timeout: BLPOP on an empty key resolves null after the
        // server-side timeout (range assertion, not exact).
        const timeoutStarted = Date.now();
        await expect(
          session.send(["BLPOP", `${sessionKey}:empty`, "0.2"])
        ).resolves.toBeNull();
        const timeoutElapsed = Date.now() - timeoutStarted;
        expect(timeoutElapsed).toBeGreaterThanOrEqual(150);
        expect(timeoutElapsed).toBeLessThanOrEqual(1500);

        // The shared client stays responsive while the session blocks, and
        // the session resolves the value pushed from the shared client.
        const blocked = session.send(["BLPOP", `${sessionKey}:queue`, "0.3"]);
        await expect(client.send(["PING"])).resolves.toBe("PONG");
        await client.send(["LPUSH", `${sessionKey}:queue`, "job"]);
        await expect(blocked).resolves.toEqual([`${sessionKey}:queue`, "job"]);

        // Ordered dispatch: sends fired without awaiting reply in
        // invocation order.
        await expect(
          Promise.all([
            session.send(["ECHO", "first"]),
            session.send(["ECHO", "second"]),
            session.send(["ECHO", "third"])
          ])
        ).resolves.toEqual(["first", "second", "third"]);

        // Unviolated watched transaction resolves the per-command replies.
        await client.send(["SET", `${sessionKey}:watched`, "before"]);
        await session.send(["WATCH", `${sessionKey}:watched`]);
        await expect(
          session.watchedTransaction([
            ["SET", `${sessionKey}:watched`, "after"],
            ["GET", `${sessionKey}:watched`]
          ])
        ).resolves.toEqual(["OK", "after"]);

        // Clobbering a watched key from the shared client aborts EXEC: the
        // single cross-adapter abort signal is null, and nothing committed.
        await session.send(["WATCH", `${sessionKey}:watched`]);
        await client.send(["SET", `${sessionKey}:watched`, "clobbered"]);
        await expect(
          session.watchedTransaction([["SET", `${sessionKey}:watched`, "lost"]])
        ).resolves.toBeNull();
        await expect(
          client.send(["GET", `${sessionKey}:watched`])
        ).resolves.toBe("clobbered");

        // A per-command runtime error (WRONGTYPE) inside a committed EXEC
        // rejects, the sibling command still committed (MULTI has no
        // rollback), and the session connection stays usable.
        await client.send(["LPUSH", `${sessionKey}:wrongtype`, "entry"]);
        await session.send(["WATCH", `${sessionKey}:watched`]);
        await expect(
          session.watchedTransaction([
            ["SET", `${sessionKey}:committed`, "yes"],
            ["INCR", `${sessionKey}:wrongtype`]
          ])
        ).rejects.toThrow();
        await expect(session.send(["PING"])).resolves.toBe("PONG");
        await expect(
          client.send(["GET", `${sessionKey}:committed`])
        ).resolves.toBe("yes");
        expect(session.closed).toBe(false);
      } finally {
        await session.close();
      }
      expect(session.closed).toBe(true);

      // close() during a blocked read rejects promptly (must not wait out
      // the server-side timeout), flips closed, and is idempotent.
      const closingSession = await client.session();
      const pendingBlock = closingSession.send([
        "BLPOP",
        `${sessionKey}:never`,
        "0.3"
      ]);
      await new Promise((resolve) => setTimeout(resolve, 25));
      const closeStarted = Date.now();
      await closingSession.close();
      await expect(pendingBlock).rejects.toThrow();
      expect(Date.now() - closeStarted).toBeLessThan(500);
      expect(closingSession.closed).toBe(true);
      await expect(closingSession.close()).resolves.toBeUndefined();
      expect(closingSession.closed).toBe(true);

      // Leak backstop: closing the parent client force-closes a surviving
      // session leased from it.
      const parent = await createClient();
      expect(parent.session).toBeDefined();
      const survivor = await parent.session!();
      expect(survivor.closed).toBe(false);
      await parent.close();
      expect(survivor.closed).toBe(true);
    }

    // Subscriber contract: an adapter that advertises subscriber() must
    // deliver on the leased connection, keep the shared client usable while
    // subscribed, honour unsubscribe, and be force-closed by the parent.
    if (client.subscriber) {
      const channelName = `${rawKey}:channel`;
      const subscriber = await client.subscriber();
      const seen: string[] = [];

      try {
        await subscriber.subscribe(channelName, (message) => {
          seen.push(message);
        });

        // The shared client publishes while the subscriber holds its own
        // connection; PUBLISH returns the number of receivers.
        await expect(
          client.send(["PUBLISH", channelName, "first"])
        ).resolves.toBe(1);
        await waitUntil(() => seen.length === 1);
        expect(seen).toEqual(["first"]);

        // Subscriber mode must not stall the shared connection.
        await expect(client.send(["PING"])).resolves.toBe("PONG");

        await subscriber.unsubscribe(channelName);
        await expect(
          client.send(["PUBLISH", channelName, "second"])
        ).resolves.toBe(0);
        expect(seen).toEqual(["first"]);

        // Pattern support is optional; when present it reports the channel.
        if (subscriber.psubscribe && subscriber.punsubscribe) {
          const matched: Array<[string, string]> = [];
          await subscriber.psubscribe(`${rawKey}:p:*`, (message, channel) => {
            matched.push([message, channel]);
          });
          await client.send(["PUBLISH", `${rawKey}:p:one`, "hello"]);
          await waitUntil(() => matched.length === 1);
          expect(matched).toEqual([["hello", `${rawKey}:p:one`]]);
          await subscriber.punsubscribe(`${rawKey}:p:*`);
        }
      } finally {
        await subscriber.close();
      }
      expect(subscriber.closed).toBe(true);
      await expect(subscriber.close()).resolves.toBeUndefined();

      // Leak backstop: the parent client force-closes a surviving subscriber.
      const parentWithSub = await createClient();
      const survivingSubscriber = await parentWithSub.subscriber!();
      expect(survivingSubscriber.closed).toBe(false);
      await parentWithSub.close();
      expect(survivingSubscriber.closed).toBe(true);
    }

    await profileStore.set(id, { name: "benni", score: 1 }, { ttlSeconds: 60 });
    await expect(profileStore.get(id)).resolves.toEqual({
      name: "benni",
      score: 1
    });
    await expect(
      client.send(["TTL", profiles.key(id)])
    ).resolves.toBeGreaterThan(0);
    await expect(profileStore.exists(id)).resolves.toBe(true);
    await expect(profileStore.persist(id)).resolves.toBe(true);
    await expect(profileStore.ttl(id)).resolves.toBe(-1);
    await expect(profileStore.expire(id, 60)).resolves.toBe(true);
    await expect(
      profileStore.getset(id, { name: "updated", score: 2 })
    ).resolves.toEqual({
      name: "benni",
      score: 1
    });
    await expect(
      profileStore.mset([[`${id}:a`, { name: "a", score: 1 }]])
    ).resolves.toBeUndefined();
    await expect(
      profileStore.mget([`${id}:a`, `${id}:missing`])
    ).resolves.toEqual([{ name: "a", score: 1 }, null]);
    await expect(profileStore.getdel(`${id}:a`)).resolves.toEqual({
      name: "a",
      score: 1
    });
    await expect(profileStore.del(id)).resolves.toBe(1);

    await expect(textStore.append(id, "hello")).resolves.toBe(5);
    await expect(textStore.append(id, " world")).resolves.toBe(11);
    await expect(textStore.getrange(id, 0, 4)).resolves.toBe("hello");
    await expect(textStore.setrange(id, 6, "Redis")).resolves.toBe(11);
    await expect(textStore.strlen(id)).resolves.toBe(11);
    await expect(textStore.getex(id, 60)).resolves.toBe("hello Redis");

    await expect(counterStore.incr(id)).resolves.toBe(1);
    await expect(counterStore.incrby(id, 4)).resolves.toBe(5);
    await expect(counterStore.decr(id)).resolves.toBe(4);
    await expect(counterStore.decrby(id, 2)).resolves.toBe(2);

    await expect(roleStore.sadd(id, ["admin", "user"])).resolves.toBe(2);
    await expect(
      roleStore.sadd(`${id}:other`, ["admin", "guest"])
    ).resolves.toBe(2);
    await expect(roleStore.sismember(id, "admin")).resolves.toBe(true);
    await expect(roleStore.smismember(id, ["admin", "guest"])).resolves.toEqual(
      [true, false]
    );
    await expect(roleStore.smembers(id)).resolves.toEqual(
      expect.arrayContaining(["admin", "user"])
    );
    await expect(roleStore.scard(id)).resolves.toBe(2);
    await expect(roleStore.srandmember(id)).resolves.toEqual(
      expect.stringMatching(/admin|user/)
    );
    await expect(roleStore.sunion(id, [`${id}:other`])).resolves.toEqual(
      expect.arrayContaining(["admin", "user", "guest"])
    );
    await expect(roleStore.sinter(id, [`${id}:other`])).resolves.toEqual([
      "admin"
    ]);
    await expect(roleStore.sdiff(id, [`${id}:other`])).resolves.toEqual([
      "user"
    ]);
    await expect(roleStore.sintercard(id, [`${id}:other`])).resolves.toBe(1);
    await expect(
      roleStore.sunionstore(`${id}:union`, id, [`${id}:other`])
    ).resolves.toBe(3);
    await expect(
      roleStore.sinterstore(`${id}:intersection`, id, [`${id}:other`])
    ).resolves.toBe(1);
    await expect(
      roleStore.sdiffstore(`${id}:difference`, id, [`${id}:other`])
    ).resolves.toBe(1);
    await expect(roleStore.smove(`${id}:other`, id, "guest")).resolves.toBe(
      true
    );
    await expect(roleStore.srem(id, ["user"])).resolves.toBe(1);
    await expect(roleStore.spop(id)).resolves.toEqual(
      expect.stringMatching(/admin|guest/)
    );
    await expect(roleStore.del(id)).resolves.toBeGreaterThanOrEqual(0);

    await expect(
      jobStore.rpush(id, [{ name: "one" }, { name: "two" }])
    ).resolves.toBe(2);
    await expect(jobStore.lpush(id, [{ name: "zero" }])).resolves.toBe(3);
    await expect(jobStore.lrange(id, 0, -1)).resolves.toEqual([
      { name: "zero" },
      { name: "one" },
      { name: "two" }
    ]);
    await expect(jobStore.lindex(id, 1)).resolves.toEqual({ name: "one" });
    await expect(
      jobStore.lset(id, 1, { name: "updated" })
    ).resolves.toBeUndefined();
    await expect(jobStore.ltrim(id, 0, 1)).resolves.toBeUndefined();
    await expect(jobStore.llen(id)).resolves.toBe(2);
    await expect(jobStore.lrem(id, 1, { name: "updated" })).resolves.toBe(1);
    await expect(jobStore.lpop(id)).resolves.toEqual({ name: "zero" });
    await expect(jobStore.rpop(id)).resolves.toBeNull();
    await expect(
      jobStore.rpush(`${id}:source`, [{ name: "moved" }])
    ).resolves.toBe(1);
    await expect(
      jobStore.lmove(`${id}:source`, `${id}:dest`, "right", "left")
    ).resolves.toEqual({
      name: "moved"
    });
    await expect(jobStore.del(id)).resolves.toBeGreaterThanOrEqual(0);

    await expect(
      leaderboardStore.zadd(id, [
        { member: "alice", score: 10 },
        { member: "bob", score: 20 }
      ])
    ).resolves.toBe(2);
    await expect(leaderboardStore.zscore(id, "alice")).resolves.toBe(10);
    await expect(leaderboardStore.zrank(id, "alice")).resolves.toBe(0);
    await expect(leaderboardStore.zcard(id)).resolves.toBe(2);
    await expect(leaderboardStore.zcount(id, 0, 15)).resolves.toBe(1);
    await expect(
      leaderboardStore.zrange(id, { start: 0, stop: -1 })
    ).resolves.toEqual(["alice", "bob"]);
    await expect(
      leaderboardStore.zrange(id, { start: 0, stop: -1, withScores: true })
    ).resolves.toEqual([
      { member: "alice", score: 10 },
      { member: "bob", score: 20 }
    ]);
    await expect(leaderboardStore.zincrby(id, 5, "alice")).resolves.toBe(15);
    await expect(leaderboardStore.zrem(id, ["bob"])).resolves.toBe(1);
    await expect(leaderboardStore.zpopmin(id)).resolves.toEqual({
      member: "alice",
      score: 15
    });
    await expect(
      leaderboardStore.zadd(id, [{ member: "charlie", score: 30 }])
    ).resolves.toBe(1);
    await expect(leaderboardStore.zpopmax(id)).resolves.toEqual({
      member: "charlie",
      score: 30
    });
    await expect(leaderboardStore.del(id)).resolves.toBeGreaterThanOrEqual(0);

    await userStore.hset(id, { name: "benni", score: 2 }, { ttlSeconds: 60 });
    await expect(userStore.hget(id)).resolves.toEqual({
      name: "benni",
      score: 2
    });
    await expect(userStore.hset(id, "name", "updated")).resolves.toBe(0);
    await expect(userStore.hget(id, "name")).resolves.toBe("updated");
    await expect(userStore.hexists(id, "score")).resolves.toBe(true);
    await expect(userStore.hincrby(id, "score", 3)).resolves.toBe(5);
    await expect(userStore.hdel(id, "name")).resolves.toBe(1);
    await expect(client.send(["TTL", users.key(id)])).resolves.toBeGreaterThan(
      0
    );
    await expect(userStore.del(id)).resolves.toBe(1);
  } finally {
    await client.send(["DEL", rawKey]);
    await client.send(["DEL", `${rawKey}:transaction`]);
    await client.send([
      "DEL",
      `${rawKey}:session:empty`,
      `${rawKey}:session:queue`,
      `${rawKey}:session:watched`,
      `${rawKey}:session:wrongtype`,
      `${rawKey}:session:committed`,
      `${rawKey}:session:never`
    ]);
    await client.send(["DEL", profiles.key(id)]);
    await client.send(["DEL", profiles.key(`${id}:a`)]);
    await client.send(["DEL", texts.key(id)]);
    await client.send(["DEL", counters.key(id)]);
    await client.send(["DEL", roles.key(id)]);
    await client.send(["DEL", roles.key(`${id}:other`)]);
    await client.send(["DEL", roles.key(`${id}:union`)]);
    await client.send(["DEL", roles.key(`${id}:intersection`)]);
    await client.send(["DEL", roles.key(`${id}:difference`)]);
    await client.send(["DEL", jobs.key(id)]);
    await client.send(["DEL", jobs.key(`${id}:source`)]);
    await client.send(["DEL", jobs.key(`${id}:dest`)]);
    await client.send(["DEL", leaderboard.key(id)]);
    await client.send(["DEL", users.key(id)]);
    await client.close();
  }
}

import { describe, expect, it, vi } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { createHashStore, defineHash } from "../src/core/hash.js";
import { createBenniSession, runWatch } from "../src/core/session.js";
import { numberReply, okReply } from "../src/core/transaction.js";
import type {
  RedisCommand,
  RedisReply,
  RedisSession
} from "../src/core/types.js";
import { benni } from "../src/index.js";
import { node } from "../src/node/index.js";
import { fakeClient, fakeSession } from "./fake-client.js";

const users = defineHash("hunt:user", {
  name: codecs.string(),
  score: codecs.number()
});

const flags = defineHash("hunt:flags", {
  active: codecs.boolean()
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function sessionWithSpy(commands: RedisCommand[], replies: RedisReply[]) {
  const watchedTransaction = vi.fn(async () => [1, 1] as RedisReply[]);
  const raw: RedisSession = {
    ...fakeSession(commands, replies),
    watchedTransaction
  };
  return { kernel: createBenniSession(raw), watchedTransaction };
}

describe("session-bound hset with a ttl (F1/F9)", () => {
  it("pipelines HSET+EXPIRE instead of clearing the session's WATCH", async () => {
    const commands: RedisCommand[] = [];
    const { kernel, watchedTransaction } = sessionWithSpy(commands, [
      "OK",
      1,
      1
    ]);
    const store = createHashStore(kernel.client, users);

    await kernel.watch(["hunt:ctr"]);
    await store.hset("42", { name: "benni", score: 7 }, { ttlSeconds: 60 });

    expect(watchedTransaction).not.toHaveBeenCalled();
    expect(commands).toEqual([
      ["WATCH", "hunt:ctr"],
      ["HSET", "hunt:user:42", "name", "benni", "score", "7"],
      ["EXPIRE", "hunt:user:42", 60]
    ]);
  });

  it("keeps MULTI/EXEC when no WATCH is armed on the session", async () => {
    const commands: RedisCommand[] = [];
    const { kernel, watchedTransaction } = sessionWithSpy(commands, []);
    const store = createHashStore(kernel.client, users);

    await store.hset("42", { name: "benni", score: 7 }, { ttlSeconds: 60 });

    expect(watchedTransaction).toHaveBeenCalledTimes(1);
  });

  it("goes back to MULTI/EXEC once the session UNWATCHes", async () => {
    const commands: RedisCommand[] = [];
    const { kernel, watchedTransaction } = sessionWithSpy(commands, [
      "OK",
      "OK"
    ]);
    const store = createHashStore(kernel.client, users);

    await kernel.watch(["hunt:ctr"]);
    await kernel.unwatch();
    await store.hset("42", { name: "benni", score: 7 }, { ttlSeconds: 60 });

    expect(watchedTransaction).toHaveBeenCalledTimes(1);
  });

  it("goes back to MULTI/EXEC once an EXEC has dropped the watch set", async () => {
    const commands: RedisCommand[] = [];
    const raw = fakeSession(commands, ["OK"], [[1], [1, 1]]);
    const watchedTransaction = vi.fn(raw.watchedTransaction);
    const kernel = createBenniSession({ ...raw, watchedTransaction });
    const store = createHashStore(kernel.client, users);

    await kernel.watch(["hunt:ctr"]);
    await kernel.multi().add(["INCR", "hunt:ctr"], numberReply).exec();
    await store.hset("42", { name: "benni", score: 7 }, { ttlSeconds: 60 });

    expect(watchedTransaction).toHaveBeenCalledTimes(2);
  });
});

describe("concurrent watch windows on one borrowed session (F10)", () => {
  it("queues the second window instead of interleaving the WATCH sets", async () => {
    const commands: RedisCommand[] = [];
    const kernel = createBenniSession(
      fakeSession(commands, ["OK", "OK"], [[1], [2]])
    );
    const gate = deferred<void>();
    const order: string[] = [];

    const first = runWatch(
      async () => kernel,
      "hunt:a",
      async (session) => {
        order.push("a");
        await gate.promise;
        return session.multi().add(["INCR", "hunt:a"], numberReply);
      },
      { session: kernel }
    );
    const second = runWatch(
      async () => kernel,
      "hunt:b",
      async (session) => {
        order.push("b");
        return session.multi().add(["INCR", "hunt:b"], numberReply);
      },
      { session: kernel }
    );

    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(order).toEqual(["a"]);
    expect(commands).toEqual([["WATCH", "hunt:a"]]);

    gate.resolve();
    await expect(first).resolves.toEqual([1]);
    await expect(second).resolves.toEqual([2]);
    expect(commands).toEqual([
      ["WATCH", "hunt:a"],
      ["INCR", "hunt:a"],
      ["WATCH", "hunt:b"],
      ["INCR", "hunt:b"]
    ]);
    expect(kernel.closed).toBe(false);
  });

  it("releases the window when the body throws", async () => {
    const commands: RedisCommand[] = [];
    const kernel = createBenniSession(
      fakeSession(commands, ["OK", "OK", "OK"], [[1]])
    );
    const boom = new Error("boom");

    await expect(
      runWatch(
        async () => kernel,
        "hunt:a",
        async () => {
          throw boom;
        },
        { session: kernel }
      )
    ).rejects.toBe(boom);
    await expect(
      runWatch(
        async () => kernel,
        "hunt:a",
        async (session) => session.multi().add(["INCR", "hunt:a"], numberReply),
        { session: kernel }
      )
    ).resolves.toEqual([1]);
  });
});

describe("hsetex with an undefined field value (F2)", () => {
  it('rejects the write instead of storing the string "undefined"', async () => {
    const commands: RedisCommand[] = [];
    const store = createHashStore(fakeClient(commands, []), users);

    await expect(store.hsetex("42", { name: undefined })).rejects.toThrow(
      "hsetex received undefined for field 'name'"
    );
    expect(commands).toEqual([]);
  });

  it("rejects the write instead of storing a boolean field as false", async () => {
    const commands: RedisCommand[] = [];
    const store = createHashStore(fakeClient(commands, []), flags);

    await expect(store.hsetex("42", { active: undefined })).rejects.toThrow(
      "hsetex received undefined for field 'active'"
    );
    expect(commands).toEqual([]);
  });

  it("still writes the fields that are present", async () => {
    const commands: RedisCommand[] = [];
    const store = createHashStore(fakeClient(commands, [1]), users);

    await expect(store.hsetex("42", { score: 7 })).resolves.toBe(true);
    expect(commands).toEqual([
      ["HSETEX", "hunt:user:42", "FIELDS", 1, "score", "7"]
    ]);
  });
});

describe("hgetex with an empty field list (F36)", () => {
  it("rejects an expiry that would be silently dropped", async () => {
    const commands: RedisCommand[] = [];
    const store = createHashStore(fakeClient(commands, []), users);

    await expect(store.hgetex("42", [], { persist: true })).rejects.toThrow(
      "hgetex was given an expiry but no fields"
    );
    await expect(store.hgetex("42", [], { ttlSeconds: 60 })).rejects.toThrow(
      "hgetex was given an expiry but no fields"
    );
    expect(commands).toEqual([]);
  });

  it("still short-circuits a read with no expiry", async () => {
    const commands: RedisCommand[] = [];
    const store = createHashStore(fakeClient(commands, []), users);

    await expect(store.hgetex("42", [])).resolves.toEqual({});
    expect(commands).toEqual([]);
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("session-bound hset with a ttl against a live server", () => {
  const unique = (label: string) =>
    `hunt:${label}:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  it("aborts the caller's watched transaction instead of losing the update", async () => {
    const client = await node({ url: redisUrl });
    const other = await node({ url: redisUrl });
    const redis = benni(client);
    const counter = unique("ctr");
    const profiles = defineHash(unique("profile"), {
      name: codecs.string(),
      score: codecs.number()
    });
    const aborts: number[] = [];
    let bodyRuns = 0;

    try {
      await client.send(["SET", counter, "1"]);
      const result = await redis.watch(
        counter,
        async (session) => {
          bodyRuns += 1;
          const seen = Number(await session.raw.send(["GET", counter]));
          // Only the first attempt races: a third party bumps the watched
          // key while the body holds it.
          if (bodyRuns === 1) await other.send(["INCR", counter]);
          // The write under test. Its TTL used to turn it into a MULTI/EXEC
          // that disarmed the WATCH above.
          await session
            .hash(profiles)
            .hset("42", { name: "benni", score: seen }, { ttlSeconds: 60 });
          return session
            .multi()
            .add(["SET", counter, String(seen + 10)], okReply);
        },
        { onAbort: ({ attempt }) => aborts.push(attempt) }
      );

      expect(result).toEqual([undefined]);
      expect(aborts).toEqual([1]);
      expect(bodyRuns).toBe(2);
      // The retry read 2 and committed 12; the lost-update path commits 11.
      await expect(client.send(["GET", counter])).resolves.toBe("12");
      await expect(redis.hash(profiles).hget("42")).resolves.toEqual({
        name: "benni",
        score: 2
      });
      await expect(
        client.send(["TTL", profiles.key("42")])
      ).resolves.toBeGreaterThan(0);
    } finally {
      await client.send(["DEL", counter, profiles.key("42")]);
      await client.close();
      await other.close();
    }
  });
});

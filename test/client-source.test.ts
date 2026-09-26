import { describe, expect, it } from "vitest";
import {
  resolveClient,
  TRANSACTION_UNSUPPORTED
} from "../src/core/client-source.js";
import { numberReply } from "../src/core/transaction.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { benni } from "../src/index.js";
import {
  hash,
  json,
  kv,
  lock,
  number,
  ratelimit,
  string
} from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// 0.2: a client source is an adapter's client or a benni handle, nothing
// else. Adapters return their client synchronously and connect on first use,
// so the promise and factory forms 0.1 accepted are refused with a message
// that says what to write instead.

const users = hash("user", { name: string(), score: number() });

/**
 * The whole required contract and nothing else: `send`, `pipeline`, `close`.
 * What a hand-written client over some in-house transport looks like.
 */
function minimalClient(commands: RedisCommand[]): RedisClient {
  return {
    async send(command) {
      commands.push(command);
      return 1;
    },
    async pipeline(batch) {
      commands.push(...batch);
      return batch.map(() => 1);
    },
    async close() {}
  };
}

describe("resolveClient", () => {
  it("returns a client as-is, so nothing wraps the hot path", () => {
    const client = fakeClient([], []);
    expect(resolveClient(client)).toBe(client);
  });

  it("unwraps the client a benni handle carries", () => {
    const redis = benni({ client: fakeClient([], []) });
    expect(resolveClient(redis)).toBe(redis.raw);
  });

  it("refuses a promise, pointing at the synchronous adapters", () => {
    const pending = Promise.resolve(fakeClient([], []));
    expect(() => resolveClient(pending as never)).toThrow(
      /no longer takes a promise of a client/
    );
  });

  it("refuses a factory the same way", () => {
    expect(() => resolveClient((() => fakeClient([], [])) as never)).toThrow(
      /no longer takes a client factory/
    );
  });

  it("tells an ioredis user to wrap the instance", () => {
    // What an ioredis Redis or Cluster looks like from here: call() and
    // duplicate(), and no send().
    const instance = {
      call() {},
      duplicate() {},
      sendCommand() {},
      pipeline() {}
    };
    expect(() => resolveClient(instance as never)).toThrow(
      /ioredis\(instance\)/
    );
  });

  it("tells a node-redis user that adoption does not exist", () => {
    const instance = { sendCommand() {}, duplicate() {}, multi() {} };
    expect(() => resolveClient(instance as never)).toThrow(
      /cannot adopt an existing node-redis client/
    );
  });

  it("refuses a source that is not a client", () => {
    expect(() => resolveClient(null as never)).toThrow(/benni adapter/);
    expect(() => resolveClient({} as never)).toThrow(/benni adapter/);
  });

  it("refuses a session, which has send() but cannot pipeline", () => {
    // A session (and a handle's `session.raw`) has send(), so a send-only
    // check took it for a client; the failure then surfaced commands later as
    // "pipeline is not a function".
    const session = {
      async send() {
        return null;
      },
      async watchedTransaction() {
        return null;
      },
      closed: false,
      async close() {}
    };
    expect(() => resolveClient(session as never)).toThrow(/benni adapter/);
    expect(() => resolveClient({ raw: session } as never)).toThrow(
      /benni adapter/
    );
  });
});

describe("benni() takes one config object", () => {
  it("binds the client and the schema", async () => {
    const commands: RedisCommand[] = [];
    const profiles = kv("profile", json<{ name: string }>());
    const redis = benni({
      client: fakeClient(commands, ['{"name":"Ada"}']),
      schema: { profiles }
    });

    await expect(redis.query.profiles.get("42")).resolves.toEqual({
      name: "Ada"
    });
    expect(commands).toEqual([["GET", "profile:42"]]);
  });

  it("refuses the removed positional form at runtime too", () => {
    const client = fakeClient([], []);
    expect(() => (benni as (...args: unknown[]) => unknown)(client)).toThrow(
      /one config object/
    );
    expect(() =>
      (benni as (...args: unknown[]) => unknown)(client, { schema: { users } })
    ).toThrow(/one config object/);
  });

  it("passes a wrong client through resolveClient's message", () => {
    const ioredisInstance = { call() {}, duplicate() {} };
    expect(() => benni({ client: ioredisInstance as never })).toThrow(
      /ioredis\(instance\)/
    );
  });
});

describe("capabilities are checked where a call needs them", () => {
  it("falls back to a pipeline for hset with ttlSeconds without MULTI", async () => {
    // hset(id, value, { ttlSeconds }) wants HSET+EXPIRE atomic and asks for a
    // transaction, but is correct (just weaker) over a pipeline.
    const commands: RedisCommand[] = [];
    const redis = benni({ client: minimalClient(commands), schema: { users } });
    await redis.query.users.hset(
      "42",
      { name: "Ada", score: 10 },
      { ttlSeconds: 60 }
    );
    expect(commands.map((command) => command[0])).toEqual(["HSET", "EXPIRE"]);
  });

  it("does not silently turn redis.multi() into a pipeline", async () => {
    // multi() exists for MULTI/EXEC atomicity; degrading it to a pipeline
    // would drop that without telling anyone, which is strictly worse than
    // refusing. It must throw, and must send nothing.
    const commands: RedisCommand[] = [];
    await expect(
      benni({ client: minimalClient(commands) })
        .multi()
        .add(["INCR", "visits"], numberReply)
        .exec()
    ).rejects.toThrow(TRANSACTION_UNSUPPORTED);
    expect(commands).toEqual([]);
  });
});

describe("a primitive over a client with no schema module", () => {
  it("is reached with benni({ client }).store(schema)", async () => {
    const commands: RedisCommand[] = [];
    // The acquire script's SCRIPT LOAD, then its EVALSHA returning the fence.
    const locks = benni({ client: fakeClient(commands, ["sha", 1]) }).store(
      lock("lock", { ttlMs: 10_000 })
    );
    const handle = await locks.acquire("order:42");

    expect(handle?.key).toBe("lock:order:42");
    expect(commands[1]?.slice(0, 4)).toEqual([
      "EVALSHA",
      "sha",
      2,
      "lock:order:42"
    ]);
  });

  it("ignores a prefix smuggled into the options: the first argument names it", () => {
    const limiter = ratelimit("api", {
      limit: 10,
      windowMs: 60_000,
      // @ts-expect-error the key prefix is the builder's first argument.
      prefix: "other"
    });
    expect(limiter.prefix).toBe("api");
  });
});

describe("the registry still refuses what it always refused", () => {
  it("rejects a copied schema at bind time, naming the export", () => {
    const profiles = kv("profile", json<{ name: string }>());
    expect(() =>
      benni({
        client: fakeClient([], []),
        schema: { profiles: { ...profiles } }
      })
    ).toThrow(/schema\.profiles/);
  });
});

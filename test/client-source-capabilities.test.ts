import { describe, expect, it } from "vitest";
import {
  CLIENT_CLOSED,
  resolveClient,
  SESSION_UNSUPPORTED,
  SUBSCRIBER_UNSUPPORTED,
  TRANSACTION_UNSUPPORTED
} from "../src/core/client-source.js";
import {
  RedisServerError,
  UnsupportedCapabilityError
} from "../src/core/errors.js";
import { numberReply } from "../src/core/transaction.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { benni } from "../src/index.js";
import { hash, number, string } from "../src/schema.js";

// Two defects in the lazy client facade, both of which reduce to "the facade
// must be indistinguishable from the client it will resolve to".
//
// 1. The facade defines every optional method, because at bind time there is
//    nothing to interrogate. Callers feature-detect the optional half of the
//    contract by presence, so a presence check passed for a client that cannot
//    actually do the thing, and a caller with a legitimate fallback took the
//    wrong branch. The fix is a distinguishable error, not a silent downgrade.
// 2. close() peeks rather than resolves, so closing an unused factory opens
//    nothing (correct, and load-bearing). But it recorded nothing, so a command
//    landing after shutdown still called the factory and opened a socket past
//    the point anything would close it.

const users = hash("user", { name: string(), score: number() });

/**
 * The whole required contract and nothing else: `send`, `pipeline`, `close`.
 * What a hand-written client over some in-house transport looks like, and what
 * a stateless HTTP adapter looks like — no MULTI, no borrowed connection, no
 * Pub/Sub.
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

describe("a lazily resolved client reports the same capabilities as the client itself", () => {
  // The concrete divergence: hset(id, value, { ttlSeconds }) wants HSET+EXPIRE
  // atomic and asks for a transaction, but is correct (just weaker) over a
  // pipeline, so it falls back. Passed connected, the fallback ran. Passed as a
  // promise, `client.transaction?.(…)` found the facade's method and the call
  // threw "Redis client does not support transactions" instead.
  it("falls back to a pipeline for hset with ttlSeconds, however the client was passed", async () => {
    const direct: RedisCommand[] = [];
    const viaPromise: RedisCommand[] = [];
    const viaFactory: RedisCommand[] = [];

    const run = async (
      client: RedisClient | Promise<RedisClient> | (() => RedisClient)
    ) => {
      const redis = benni({ client, schema: { users } });
      await redis.query.users.hset(
        "42",
        { name: "Ada", score: 10 },
        { ttlSeconds: 60 }
      );
    };

    await run(minimalClient(direct));
    await run(Promise.resolve(minimalClient(viaPromise)));
    await run(() => minimalClient(viaFactory));

    // Not just "all three succeeded": all three sent the identical batch.
    expect(direct.map((command) => command[0])).toEqual(["HSET", "EXPIRE"]);
    expect(viaPromise).toEqual(direct);
    expect(viaFactory).toEqual(direct);
  });

  it("still uses a real transaction when the client has one", async () => {
    // The fallback must not become the default: a client that can do MULTI
    // must still get MULTI, or hset's TTL pair loses its atomicity for
    // everyone.
    const batches: RedisCommand[][] = [];
    const redis = benni({
      client: () => ({
        ...minimalClient([]),
        async transaction(commands) {
          batches.push([...commands]);
          return commands.map(() => 1);
        }
      }),
      schema: { users }
    });

    await redis.query.users.hset(
      "42",
      { name: "Ada", score: 10 },
      { ttlSeconds: 60 }
    );

    expect(batches).toHaveLength(1);
    expect(batches[0]?.map((command) => command[0])).toEqual([
      "HSET",
      "EXPIRE"
    ]);
  });

  it("does not swallow a transaction that reached Redis and failed", async () => {
    // The fallback keys off the capability error and nothing else. A MULTI that
    // was attempted and rejected is a real failure: retrying it as a pipeline
    // would run the batch a second time, non-atomically, under an error the
    // caller never saw.
    let pipelines = 0;
    const redis = benni({
      client: () => ({
        async send() {
          return 1;
        },
        async pipeline(batch) {
          pipelines += 1;
          return batch.map(() => 1);
        },
        async transaction() {
          throw new RedisServerError("OOM command not allowed");
        },
        async close() {}
      }),
      schema: { users }
    });

    await expect(
      redis.query.users.hset(
        "42",
        { name: "Ada", score: 10 },
        { ttlSeconds: 60 }
      )
    ).rejects.toThrow(RedisServerError);
    expect(pipelines).toBe(0);
  });

  it("rejects a missing capability with a distinguishable error, keeping the guard's message", async () => {
    const client = resolveClient(() => minimalClient([]));

    // instanceof TypeError still holds, so handling that predates the class
    // keeps working, and the messages are the connected-client guards' own.
    await expect(client.transaction?.([["PING"]])).rejects.toThrow(
      UnsupportedCapabilityError
    );
    await expect(client.transaction?.([["PING"]])).rejects.toThrow(TypeError);
    await expect(client.transaction?.([["PING"]])).rejects.toThrow(
      TRANSACTION_UNSUPPORTED
    );
    await expect(client.session?.()).rejects.toThrow(SESSION_UNSUPPORTED);
    await expect(client.subscriber?.()).rejects.toThrow(SUBSCRIBER_UNSUPPORTED);

    // Which capability, without parsing prose.
    await expect(client.session?.()).rejects.toMatchObject({
      name: "UnsupportedCapabilityError",
      capability: "session"
    });
    await expect(client.subscriber?.()).rejects.toMatchObject({
      capability: "subscriber"
    });
  });

  it("keeps session and subscriber presence semantics identical to the connected client", async () => {
    // Presence cannot be honest on the facade (nothing is resolved yet), so
    // parity is measured where callers actually feel it: what happens when the
    // capability is used. `redis.session()` refuses either way.
    const direct = benni(minimalClient([]));
    const lazy = benni({ client: () => minimalClient([]) });

    await expect(direct.session()).rejects.toThrow(SESSION_UNSUPPORTED);
    await expect(lazy.session()).rejects.toThrow(SESSION_UNSUPPORTED);

    // And a client that does provide them is not held back by the facade: the
    // guard is a report of what resolved, not a ceiling on it.
    const leased: RedisCommand[] = [];
    const subscribed: string[] = [];
    const capable = resolveClient(() => ({
      ...minimalClient([]),
      async session() {
        return {
          async send(command: RedisCommand) {
            leased.push(command);
            return 1;
          },
          async watchedTransaction() {
            return null;
          },
          closed: false,
          async close() {}
        };
      },
      async subscriber() {
        return {
          async subscribe(channel: string) {
            subscribed.push(channel);
          },
          async unsubscribe() {},
          async psubscribe() {},
          async punsubscribe() {},
          closed: false,
          async close() {}
        };
      }
    }));

    const session = await capable.session?.();
    await session?.send(["PING"]);
    expect(leased).toEqual([["PING"]]);

    const subscriber = await capable.subscriber?.();
    await subscriber?.subscribe("news", () => {});
    expect(subscribed).toEqual(["news"]);
  });

  it("does not silently turn redis.multi() into a pipeline", async () => {
    // The guard on the whole approach. multi() exists for MULTI/EXEC
    // atomicity; degrading it to a pipeline would drop that without telling
    // anyone, which is strictly worse than refusing. So the fallback stays
    // opt-in per call site: this one must throw, and must send nothing.
    const commands: RedisCommand[] = [];
    const lazy = benni({ client: () => minimalClient(commands) });

    await expect(
      lazy
        .multi()
        .add(["INCR", "visits"], numberReply)
        .add(["INCR", "hits"], numberReply)
        .exec()
    ).rejects.toThrow(TRANSACTION_UNSUPPORTED);
    expect(commands).toEqual([]);

    // Same refusal connected, which is the parity that matters here.
    const connected: RedisCommand[] = [];
    await expect(
      benni(minimalClient(connected))
        .multi()
        .add(["INCR", "visits"], numberReply)
        .exec()
    ).rejects.toThrow(TRANSACTION_UNSUPPORTED);
    expect(connected).toEqual([]);
  });
});

describe("close() on a lazily resolved client is terminal", () => {
  it("opens nothing for an unused factory and bars every later command", async () => {
    let calls = 0;
    const client = resolveClient(() => {
      calls += 1;
      return minimalClient([]);
    });

    await client.close();
    expect(calls).toBe(0);

    // The defect: without a record of the close, this call invoked the factory
    // and opened a connection after shutdown — in Node a live socket pins the
    // event loop, so a request racing shutdown turned a graceful exit into a
    // hang.
    await expect(client.send(["PING"])).rejects.toThrow(CLIENT_CLOSED);
    await expect(client.pipeline([["PING"]])).rejects.toThrow(CLIENT_CLOSED);
    await expect(client.transaction?.([["PING"]])).rejects.toThrow(
      CLIENT_CLOSED
    );
    await expect(client.session?.()).rejects.toThrow(CLIENT_CLOSED);
    await expect(client.subscriber?.()).rejects.toThrow(CLIENT_CLOSED);
    expect(calls).toBe(0);
  });

  it("is idempotent, and closes the underlying client exactly once", async () => {
    let closes = 0;
    const client = resolveClient(() => ({
      ...minimalClient([]),
      async close() {
        closes += 1;
      }
    }));

    await client.send(["PING"]);
    await client.close();
    await expect(client.close()).resolves.toBeUndefined();
    await expect(client.close()).resolves.toBeUndefined();

    expect(closes).toBe(1);
    await expect(client.send(["PING"])).rejects.toThrow(CLIENT_CLOSED);
  });

  it("stays a no-op when the source never resolved", async () => {
    // A connect that already failed left nothing to close, and reporting its
    // rejection from close() would break every shutdown path that closes in a
    // finally block.
    const client = resolveClient(Promise.reject(new Error("connect refused")));
    await new Promise((resolve) => setTimeout(resolve, 0));

    await expect(client.close()).resolves.toBeUndefined();
    await expect(client.close()).resolves.toBeUndefined();
    // Still terminal: a closed client does not retry the connect.
    await expect(client.send(["PING"])).rejects.toThrow(CLIENT_CLOSED);
  });
});

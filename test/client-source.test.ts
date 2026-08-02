import { describe, expect, it } from "vitest";
import {
  resolveClient,
  SESSION_UNSUPPORTED,
  SUBSCRIBER_UNSUPPORTED,
  TRANSACTION_UNSUPPORTED
} from "../src/core/client-source.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { benni } from "../src/index.js";
import { cache, lock, ratelimit } from "../src/primitives/index.js";
import { hash, json, kv, number, string } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// The 2026-08-02 DX pass: one client source everywhere (connected, connecting,
// or not created yet), one options-object call shape, and a benni handle
// accepted wherever a client is.

const users = hash("user", { name: string(), score: number() });

describe("resolveClient", () => {
  it("returns a connected client as-is, so nothing wraps the hot path", () => {
    const client = fakeClient([], []);
    expect(resolveClient(client)).toBe(client);
  });

  it("unwraps the client a benni handle carries", () => {
    const client = fakeClient([], []);
    expect(resolveClient(benni(client))).toBe(client);
  });

  it("sends nothing until a command, over a promise source", async () => {
    const commands: RedisCommand[] = [];
    const pending = Promise.resolve(fakeClient(commands, ["PONG"]));

    const client = resolveClient(pending);
    await Promise.resolve();
    expect(commands).toEqual([]);

    await expect(client.send(["PING"])).resolves.toBe("PONG");
    expect(commands).toEqual([["PING"]]);
  });

  it("observes a rejecting promise source rather than letting it go unhandled", async () => {
    // A promise handed here is already in flight, so nothing is waiting to
    // attach a rejection handler. Left unobserved until the first command,
    // `benni({ client: node({ url: bad }) })` killed the process with an
    // unhandledRejection before any command could report the failure.
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      const client = resolveClient(Promise.reject(new Error("ECONNREFUSED")));
      // Long enough for the rejection to be reported if nothing observed it.
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(unhandled).toEqual([]);

      // The failure is still delivered, through the command, every time.
      await expect(client.send(["PING"])).rejects.toThrow("ECONNREFUSED");
      await expect(client.send(["PING"])).rejects.toThrow("ECONNREFUSED");
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  it("calls a factory once and reuses the client", async () => {
    const commands: RedisCommand[] = [];
    let calls = 0;
    const client = resolveClient(() => {
      calls += 1;
      return fakeClient(commands, ["a", "b"]);
    });

    await client.send(["GET", "one"]);
    await client.send(["GET", "two"]);
    expect(calls).toBe(1);
    expect(commands).toHaveLength(2);
  });

  it("retries the factory after a failed connect", async () => {
    let calls = 0;
    const client = resolveClient(() => {
      calls += 1;
      if (calls === 1) return Promise.reject(new Error("ECONNREFUSED"));
      return fakeClient([], ["PONG"]);
    });

    await expect(client.send(["PING"])).rejects.toThrow("ECONNREFUSED");
    await expect(client.send(["PING"])).resolves.toBe("PONG");
    expect(calls).toBe(2);
  });

  it("closing a factory client that was never used opens no connection", async () => {
    let calls = 0;
    const client = resolveClient(() => {
      calls += 1;
      return fakeClient([], []);
    });

    await client.close();
    expect(calls).toBe(0);
  });

  it("closes a promise-backed client that was never used", async () => {
    // The factory rule does not transfer: this connection is already open, so
    // skipping the close leaks it and the process never exits.
    let closed = false;
    const client = resolveClient(
      Promise.resolve({
        async send() {
          return null;
        },
        async pipeline() {
          return [];
        },
        async close() {
          closed = true;
        }
      })
    );

    await client.close();
    expect(closed).toBe(true);
  });

  it("raises the capability guard's own message once resolved", async () => {
    // A stateless adapter (HTTP): no transaction, session, or subscriber.
    const client = resolveClient(() => fakeClientWithout());

    await expect(client.transaction?.([["PING"]])).rejects.toThrow(
      TRANSACTION_UNSUPPORTED
    );
    await expect(client.session?.()).rejects.toThrow(SESSION_UNSUPPORTED);
    await expect(client.subscriber?.()).rejects.toThrow(SUBSCRIBER_UNSUPPORTED);
  });

  it("refuses a source that is not a client", () => {
    expect(() => resolveClient(null as unknown as RedisClient)).toThrow(
      TypeError
    );
    expect(() => resolveClient({} as unknown as RedisClient)).toThrow(
      /neither a client/
    );
  });
});

describe("benni() call shapes", () => {
  it("takes the config object and the positional client alike", async () => {
    const positional: RedisCommand[] = [];
    const config: RedisCommand[] = [];
    const schema = { users };

    const a = benni(fakeClient(positional, [["Ada", "10"]]), { schema });
    const b = benni({ client: fakeClient(config, [["Ada", "10"]]), schema });

    await a.query.users.hget("42");
    await b.query.users.hget("42");
    expect(config).toEqual(positional);
  });

  it("binds a promise without a top-level await", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni({
      client: Promise.resolve(fakeClient(commands, [1])),
      schema: { users }
    });

    await redis.query.users.hset("42", { name: "Ada", score: 10 });
    expect(commands[0]?.[0]).toBe("HSET");
  });

  it("keeps redis.raw identical to a client passed connected", () => {
    const client = fakeClient([], []);
    expect(benni(client).raw).toBe(client);
  });
});

describe("primitives take a handle, a client, or a config object", () => {
  it("accepts the benni handle in the config form", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, ["OK"]));

    const locks = lock({ client: redis, ttlMs: 10_000 });
    const handle = await locks.acquire("order:42");

    expect(handle?.key).toBe("lock:order:42");
    expect(commands[0]?.slice(0, 2)).toEqual(["SET", "lock:order:42"]);
  });

  it("still accepts the positional client", async () => {
    const commands: RedisCommand[] = [];
    const limiter = ratelimit(fakeClient(commands, ["sha", [1, 9, 1000, 0]]), {
      limit: 10,
      windowMs: 60_000
    });

    const result = await limiter.check("user:1");
    expect(result.success).toBe(true);
    expect(result.remaining).toBe(9);
  });

  it("reads through a lazily resolved client", async () => {
    const commands: RedisCommand[] = [];
    const profiles = cache<{ name: string }>({
      client: () => fakeClient(commands, [JSON.stringify({ name: "Ada" })]),
      ttlMs: 60_000
    });

    await expect(profiles.peek("42")).resolves.toEqual({ name: "Ada" });
    expect(commands).toEqual([["GET", "cache:{42}"]]);
  });
});

describe("the registry still refuses what it always refused", () => {
  it("rejects a copied schema at bind time, naming the export", () => {
    const profiles = kv("profile", json<{ name: string }>());
    expect(() =>
      benni(fakeClient([], []), { schema: { profiles: { ...profiles } } })
    ).toThrow(/schema\.profiles/);
  });
});

/** A client with none of the optional capabilities, like the HTTP adapter. */
function fakeClientWithout(): RedisClient {
  return {
    async send() {
      return null;
    },
    async pipeline() {
      return [];
    },
    async close() {}
  };
}

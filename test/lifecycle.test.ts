import { describe, expect, it } from "vitest";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  RedisSession,
  RedisSubscriber
} from "../src/core/types.js";
import { benni } from "../src/index.js";
import { channel, json, kv, queue, string } from "../src/schema.js";
import { fakeSession } from "./fake-client.js";

// redis.close() is the one shutdown call: Pub/Sub, then queue workers, then
// sessions, then the client, and only what this handle opened.

const notes = kv("note", string());
const news = channel("news", json<{ text: string }>());
const jobs = queue<{ n: number }, number>("jobs");

/**
 * A client with every capability that logs lifecycle events in order. Plain
 * commands answer `replies` in order; the queue worker's scripts fail, which
 * keeps its loop polling until it is stopped.
 */
function recordingClient(events: string[], replies: RedisReply[] = []) {
  const commands: RedisCommand[] = [];
  const client = {
    commands,
    async send(command: RedisCommand) {
      commands.push(command);
      const name = String(command[0]);
      if (name === "SCRIPT" || name.startsWith("EVAL")) {
        throw new Error("no scripts in this fake");
      }
      return replies.shift() ?? "OK";
    },
    async pipeline(batch: readonly RedisCommand[]) {
      commands.push(...batch);
      return batch.map(() => "OK" as RedisReply);
    },
    async transaction(batch: readonly RedisCommand[]) {
      commands.push(...batch);
      return batch.map(() => "OK" as RedisReply);
    },
    async session(): Promise<RedisSession> {
      events.push("session:open");
      const session = fakeSession(commands, []);
      const close = session.close.bind(session);
      return Object.assign(session, {
        async close() {
          events.push("session:close");
          await close();
        }
      });
    },
    async subscriber(): Promise<RedisSubscriber> {
      events.push("subscriber:open");
      let closed = false;
      return {
        async subscribe() {},
        async unsubscribe() {},
        get closed() {
          return closed;
        },
        async close() {
          closed = true;
          events.push("subscriber:close");
        }
      };
    },
    async close() {
      events.push("client:close");
    }
  };
  return client;
}

describe("redis.close()", () => {
  it("closes Pub/Sub, then workers, then sessions, then the client", async () => {
    const events: string[] = [];
    const client = recordingClient(events);
    const redis = benni({ client, schema: { news, jobs } });

    await redis.query.news.subscribe(() => {});
    const worker = redis.query.jobs.worker(async (job) => job.payload.n, {
      pollMs: 5,
      onError: () => {}
    });
    const stop = worker.stop;
    worker.stop = async (options) => {
      events.push("worker:stop");
      await stop(options);
      events.push("worker:stopped");
    };
    await redis.session();

    await redis.close();

    expect(events).toEqual([
      "subscriber:open",
      "session:open",
      "subscriber:close",
      "worker:stop",
      "worker:stopped",
      "session:close",
      "client:close"
    ]);
  });

  it("does not stop a worker twice once its own stop() completed", async () => {
    const events: string[] = [];
    const redis = benni({ client: recordingClient(events), schema: { jobs } });
    const worker = redis.query.jobs.worker(async () => 1, {
      pollMs: 5,
      onError: () => {}
    });
    await worker.stop();
    const stop = worker.stop;
    worker.stop = async () => {
      events.push("worker:stop again");
      await stop();
    };

    await redis.close();
    expect(events).toEqual(["client:close"]);
  });

  it("forgets a session the caller already closed", async () => {
    const events: string[] = [];
    const redis = benni({ client: recordingClient(events) });
    await redis.session(async (s) =>
      s
        .kv(notes)
        .get("a")
        .catch(() => null)
    );

    await redis.close();
    expect(events).toEqual(["session:open", "session:close", "client:close"]);
  });

  it("is idempotent, and a second call waits for the first", async () => {
    const events: string[] = [];
    let release: () => void = () => {};
    const client = recordingClient(events);
    client.close = () =>
      new Promise<void>((resolve) => {
        events.push("client:close");
        release = resolve;
      });
    const redis = benni({ client });

    const first = redis.close();
    const second = redis.close();
    let secondDone = false;
    void second.then(() => {
      secondDone = true;
    });
    while (!events.includes("client:close")) await Promise.resolve();
    await Promise.resolve();
    expect(secondDone).toBe(false);
    release();
    await Promise.all([first, second]);
    expect(events).toEqual(["client:close"]);
    await redis[Symbol.asyncDispose]();
    expect(events).toEqual(["client:close"]);
  });

  it("refuses commands and leases once it has run", async () => {
    const events: string[] = [];
    const redis = benni({ client: recordingClient(events), schema: { notes } });
    await redis.close();

    await expect(redis.query.notes.get("a")).rejects.toThrow(
      /benni handle is closed/
    );
    await expect(redis.raw.send(["PING"])).rejects.toThrow(
      /benni handle is closed/
    );
    await expect(redis.session()).rejects.toThrow(/benni handle is closed/);
    await expect(redis.query.notes.set("a", "b")).rejects.toThrow(
      /benni handle is closed/
    );
  });

  it("is what redis.raw.close() and await using do", async () => {
    const events: string[] = [];
    const viaRaw = benni({ client: recordingClient(events) });
    await viaRaw.raw.close();
    expect(events).toEqual(["client:close"]);

    const disposed: string[] = [];
    {
      await using redis = benni({ client: recordingClient(disposed) });
      void redis;
    }
    expect(disposed).toEqual(["client:close"]);
  });

  it("leaves the client of a handle it was built over open", async () => {
    const events: string[] = [];
    const parent = benni({
      client: recordingClient(events),
      schema: { notes }
    });
    const child = benni({ client: parent, schema: { notes } });

    await child.session();
    await child.close();
    // The child's own session is closed; the shared client is not.
    expect(events).toEqual(["session:open", "session:close"]);
    await expect(child.query.notes.get("a")).rejects.toThrow(
      /benni handle is closed/
    );
    await expect(parent.query.notes.get("a")).resolves.toBe("OK");

    await parent.close();
    expect(events.at(-1)).toBe("client:close");
  });

  it("closes a session whose lease lands after close() began", async () => {
    const events: string[] = [];
    const client = recordingClient(events);
    let grant: () => void = () => {};
    const lease = client.session;
    client.session = async () => {
      await new Promise<void>((resolve) => {
        grant = resolve;
      });
      return lease();
    };
    const redis = benni({ client });

    const pending = redis.session();
    await Promise.resolve();
    const closing = redis.close();
    grant();
    await expect(pending).rejects.toThrow(/benni handle is closed/);
    await closing;
    expect(events).toEqual(
      expect.arrayContaining(["session:open", "session:close", "client:close"])
    );
  });

  it("runs every stage even if one fails, then reports the first failure", async () => {
    const events: string[] = [];
    const client = recordingClient(events);
    const lease = client.subscriber;
    client.subscriber = async () => {
      const subscriber = await lease();
      return Object.assign(subscriber, {
        async close() {
          throw new Error("subscriber would not close");
        }
      });
    };
    const redis = benni({ client, schema: { news } });
    await redis.query.news.subscribe(() => {});

    await expect(redis.close()).rejects.toThrow("subscriber would not close");
    expect(events.at(-1)).toBe("client:close");
  });

  it("keeps the client's optional members exactly as the client has them", () => {
    const minimal: RedisClient = {
      async send() {
        return null;
      },
      async pipeline() {
        return [];
      },
      async close() {}
    };
    const redis = benni({ client: minimal });
    expect(redis.raw.transaction).toBeUndefined();
    expect(redis.raw.session).toBeUndefined();
    expect(redis.raw.subscriber).toBeUndefined();
  });
});

import IORedis from "ioredis";
import { createClient } from "redis";
import { describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import { ioredis } from "../src/ioredis/index.js";
import { node } from "../src/node/index.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

const url = () => redisUrl as string;

/**
 * The adapters under test, so the leak-backstop assertions run against every
 * TCP adapter rather than only the one that happened to be reproduced first.
 * The Bun adapter has the same guards; it is covered by
 * test/bun.integration.bun.ts, since it cannot run under Vitest.
 */
const adapters: ReadonlyArray<[string, () => Promise<RedisClient>]> = [
  ["node", () => node({ url: url() })],
  ["ioredis", () => ioredis(url())]
];

/**
 * The `id` of the pubsub connection carrying `name`. CLIENT KILL has no name
 * filter, so find the id through CLIENT LIST and kill exactly ours instead of
 * every pubsub connection on a shared server.
 */
async function pubsubClientId(
  admin: { sendCommand(args: string[]): Promise<unknown> },
  name: string
): Promise<string> {
  const list = String(
    await admin.sendCommand(["CLIENT", "LIST", "TYPE", "pubsub"])
  );
  const line = list
    .split("\n")
    .find((entry) => entry.includes(` name=${name} `));
  if (line === undefined) throw new Error(`no pubsub connection named ${name}`);
  return line.slice(3, line.indexOf(" "));
}

describeRedis("adapter leak backstop", () => {
  for (const [label, open] of adapters) {
    it(`${label}: refuses a lease whose connect is in flight when close() runs`, async () => {
      const client = await open();
      // The Set is populated only after connect() resolves, so this lease used
      // to land behind the drain loop and stay open forever.
      const pending = client.session?.();
      if (!pending) throw new Error("session() is required on this adapter");
      await client.close();
      await expect(pending).rejects.toThrow(/client is closed/);
    });

    it(`${label}: refuses session() and subscriber() after close()`, async () => {
      const client = await open();
      await client.close();
      await expect(client.session?.()).rejects.toThrow(/client is closed/);
      await expect(client.subscriber?.()).rejects.toThrow(/client is closed/);
    });
  }
});

describeRedis("beni/node close()", () => {
  it("is idempotent, like every other adapter", async () => {
    const client = await node({ url: url() });
    await client.close();
    // node-redis's own close() throws ClientClosedError on a second call, so a
    // SIGTERM and a SIGINT handler both calling close() used to produce an
    // unhandled rejection mid-shutdown.
    await expect(client.close()).resolves.toBeUndefined();
  });
});

describeRedis("beni/node subscriber", () => {
  it("reports closed once its socket is terminally gone", async () => {
    const name = `beni-hunt-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    // reconnectStrategy: false makes a dropped socket terminal rather than a
    // reconnect window, which is the state the getter has to surface.
    const client = await node({
      url: url(),
      name,
      socket: { reconnectStrategy: false }
    });
    const subscriber = await client.subscriber?.();
    if (!subscriber) throw new Error("subscriber() is required");
    const admin = await createClient({ url: url() }).connect();
    try {
      await subscriber.subscribe(`beni:hunt:f58:${name}`, () => {});
      const id = await pubsubClientId(admin, name);
      await admin.sendCommand(["CLIENT", "KILL", "ID", id]);
      await expect.poll(() => subscriber.closed, { timeout: 2000 }).toBe(true);
    } finally {
      await admin.close();
      await client.close();
    }
  });
});

describeRedis("beni/ioredis send()", () => {
  it("returns the same reply shape whatever case the command is written in", async () => {
    const client = await ioredis(url());
    const key = `beni:hunt:f79:${Date.now()}`;
    try {
      await client.send(["HSET", key, "a", "1", "b", "2"]);
      const upper = await client.send(["HGETALL", key]);
      // ioredis keys its reply transformers by lowercase name, so this used to
      // come back as a plain object while every other adapter gave the RESP2
      // flat array.
      const lower = await client.send(["hgetall", key]);
      expect(upper).toEqual(["a", "1", "b", "2"]);
      expect(lower).toEqual(upper);
    } finally {
      await client.send(["DEL", key]);
      await client.close();
    }
  });
});

const clusterUrl = process.env.BENI_REDIS_CLUSTER_URL;
const describeCluster = clusterUrl ? describe : describe.skip;

describeCluster("beni/ioredis session on an adopted Cluster", () => {
  it("stays closed after its connection drops instead of reconnecting", async () => {
    const parsed = new URL(clusterUrl as string);
    const host = parsed.hostname;
    const port = Number(parsed.port || 6379);
    const cluster = new IORedis.Cluster([{ host, port }]);
    cluster.on("error", () => {});
    await new Promise((resolve) => cluster.once("ready", resolve));
    const client = await ioredis(cluster);
    const admin = new IORedis({ host, port });
    admin.on("error", () => {});
    try {
      const session = await client.session?.();
      if (!session) throw new Error("session() is required on this adapter");
      const id = await session.send(["CLIENT", "ID"]);
      // Standalone retry options do not reach a Cluster duplicate, so the
      // default clusterRetryStrategy used to bring this session back to life
      // with its WATCH state silently gone.
      await admin.call("CLIENT", "KILL", "ID", String(id));
      await expect.poll(() => session.closed, { timeout: 3000 }).toBe(true);
      await expect(session.send(["PING"])).rejects.toThrow();
      await session.close();
    } finally {
      admin.disconnect();
      await client.close();
      cluster.disconnect();
    }
  });
});

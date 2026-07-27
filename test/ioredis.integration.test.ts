import IORedis from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import {
  codecs,
  definePubSubChannel,
  type RedisClient
} from "../src/core/index.js";
import { beni } from "../src/index.js";
import { ioredis } from "../src/ioredis/index.js";
import { queue } from "../src/primitives/index.js";
import { json, kv } from "../src/schema.js";
import { expectRedisClientContract } from "./redis-contract.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("ioredis", () => {
  it("passes the shared Redis client contract", async () => {
    expect(redisUrl).toBeDefined();
    await expectRedisClientContract(() => ioredis({ url: redisUrl }));
  });

  it("accepts a bare URL string", async () => {
    const client = await ioredis(redisUrl as string);
    try {
      await expect(client.send(["PING"])).resolves.toBe("PONG");
    } finally {
      await client.close();
    }
  });

  it("accepts host/port options without a url", async () => {
    const { hostname, port } = new URL(redisUrl as string);
    const client = await ioredis({
      host: hostname,
      port: Number(port || 6379)
    });
    try {
      await expect(client.send(["PING"])).resolves.toBe("PONG");
    } finally {
      await client.close();
    }
  });
});

describeRedis("ioredis (adopted client)", () => {
  const owned = new IORedis(redisUrl as string, { lazyConnect: true });
  owned.on("error", () => {});

  afterAll(() => {
    owned.disconnect();
  });

  it("adopts an existing instance instead of dialing its own", async () => {
    await owned.connect();
    const client = await ioredis(owned);
    const key = `beni:test:adopt:${Date.now()}`;

    await expect(client.send(["PING"])).resolves.toBe("PONG");
    await client.send(["SET", key, "adopted"]);
    // The value is visible through the caller's own handle: same connection.
    await expect(owned.get(key)).resolves.toBe("adopted");
    await owned.del(key);
  });

  it("leaves an adopted client open on close, but reaps what it leased", async () => {
    const client = await ioredis(owned);
    const session = await client.session?.();
    expect(session?.closed).toBe(false);

    await client.close();

    // Beni's leased session is gone...
    expect(session?.closed).toBe(true);
    // ...but the caller's client is untouched, because they still own it.
    expect(owned.status).toBe("ready");
    await expect(owned.ping()).resolves.toBe("PONG");
  });
});

describeRedis("ioredis: typed client and primitives", () => {
  const unique = (label: string) =>
    `beni:test:${label}:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  it("runs the typed store API end to end", async () => {
    const client = await ioredis({ url: redisUrl });
    const redis = beni(client);
    const id = unique("kv");
    try {
      const profiles = redis.kv(
        kv("beni:test:profile", json<{ name: string; n: number }>())
      );
      await profiles.set(id, { name: "Ada", n: 1 });
      await expect(profiles.get(id)).resolves.toEqual({ name: "Ada", n: 1 });
      await profiles.del(id);
    } finally {
      await client.close();
    }
  });

  it("delivers typed Pub/Sub over a leased subscriber", async () => {
    const client = await ioredis({ url: redisUrl });
    const redis = beni(client);
    const channel = definePubSubChannel(
      unique("channel"),
      codecs.json<{ id: string; action: string }>()
    );
    const seen: Array<{ id: string; action: string }> = [];
    const first = new Promise<void>((resolve) => {
      void redis.pubsub.channel(channel).subscribe((message) => {
        seen.push(message);
        resolve();
      });
    });

    try {
      // Give the subscribe round trip time to land before publishing.
      await new Promise((resolve) => setTimeout(resolve, 100));
      await redis.pubsub.channel(channel).publish({ id: "1", action: "made" });
      await first;
      expect(seen).toEqual([{ id: "1", action: "made" }]);
    } finally {
      await client.close();
    }
  });

  it("runs the AI job queue, Lua and all", async () => {
    const client: RedisClient = await ioredis({ url: redisUrl });
    const jobs = queue<{ prompt: string }, string>(client, {
      prefix: unique("queue")
    });
    const worker = jobs.worker(async (job) => {
      for (const token of ["Hel", "lo"]) await job.emit(token);
      return `done:${job.payload.prompt}`;
    });

    try {
      const { id } = await jobs.enqueue({ prompt: "hi" });
      const chunks: string[] = [];
      for await (const event of jobs.watch(id)) {
        if (event.type === "chunk") chunks.push(event.data);
      }
      expect(chunks.join("")).toBe("Hello");
      await expect(jobs.wait(id)).resolves.toBe("done:hi");
    } finally {
      await worker.stop();
      await client.close();
    }
  });
});

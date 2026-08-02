import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { bun } from "../src/bun/index.js";
import {
  codecs,
  definePubSubChannel,
  definePubSubPattern
} from "../src/core/index.js";
import { benni } from "../src/index.js";
import { expectRedisClientContract } from "./redis-contract.js";

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("bun", () => {
  it("passes the shared Redis client contract", async () => {
    expect(redisUrl).toBeDefined();
    await expectRedisClientContract(() => bun({ url: redisUrl }));
  });

  it("refuses to lease past close() instead of leaking the connection", async () => {
    expect(redisUrl).toBeDefined();
    const client = await bun({ url: redisUrl });
    // A lease still connecting when close() drains the backstop used to land
    // in a Set nobody iterates again, leaving a live socket behind.
    const pending = client.session?.();
    await client.close();
    await expect(pending).rejects.toThrow(/client is closed/);
    await expect(client.session?.()).rejects.toThrow(/client is closed/);
    await expect(client.subscriber?.()).rejects.toThrow(/client is closed/);
  });
});

describe("bun connect failure", () => {
  it("leaves no orphan reconnect loop behind", async () => {
    // The leak is a process that never exits, so prove it in a subprocess. A
    // Bun client with autoReconnect on cannot be cancelled: when its first
    // connect() rejects, the reconnect timer keeps running, close() does not
    // stop it, and the orphan pins the process forever.
    const adapter = new URL("../src/bun/index.ts", import.meta.url).pathname;
    const child = spawn(
      "bun",
      [
        "-e",
        `import { bun } from "${adapter}";
           await bun({ url: "redis://127.0.0.1:6399" }).catch(() => {});`
      ],
      { stdio: "ignore" }
    );
    const outcome = await Promise.race([
      new Promise<number | null>((resolve) =>
        child.once("exit", (code) => resolve(code))
      ),
      new Promise<"never exited">((resolve) =>
        setTimeout(() => resolve("never exited"), 8000)
      )
    ]);
    child.kill();
    expect(outcome).toBe(0);
  }, 20000);
});

describeRedis("bun pubsub", () => {
  it("publishes and subscribes typed messages over a leased subscriber", async () => {
    expect(redisUrl).toBeDefined();
    const client = await bun({ url: redisUrl });
    const redis = benni(client);
    const channel = definePubSubChannel(
      `benni:test:events:${Date.now()}:${Math.random().toString(36).slice(2)}`,
      codecs.json<{ id: string; action: "created" }>()
    );
    const seen: Array<{ id: string; action: "created" }> = [];
    const first = new Promise<void>((resolve) => {
      void redis.pubsub.channel(channel).subscribe((message) => {
        seen.push(message);
        resolve();
      });
    });

    try {
      await new Promise((resolve) => setTimeout(resolve, 50));
      await expect(
        redis.pubsub.channel(channel).publish({ id: "42", action: "created" })
      ).resolves.toBe(1);
      await first;
      expect(seen).toEqual([{ id: "42", action: "created" }]);
    } finally {
      await redis.pubsub.close();
      await client.close();
    }
  });

  it("reports pattern subscribe as unsupported instead of hanging", async () => {
    expect(redisUrl).toBeDefined();
    const client = await bun({ url: redisUrl });
    const redis = benni(client);
    // Must be a real builder-made schema: a bare object literal carries no
    // store binding, so pattern() would throw synchronously before the
    // adapter's missing psubscribe is ever reached.
    const pattern = definePubSubPattern("benni:test:none:*", codecs.string());

    try {
      await expect(
        redis.pubsub.pattern(pattern).subscribe(() => {})
      ).rejects.toThrow(TypeError);
    } finally {
      await client.close();
    }
  });
});

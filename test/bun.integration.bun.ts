import { describe, expect, it } from "vitest";
import { bun } from "../src/bun/index.js";
import { codecs, definePubSubChannel } from "../src/core/index.js";
import { beni } from "../src/index.js";
import { expectRedisClientContract } from "./redis-contract.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("bun", () => {
  it("passes the shared Redis client contract", async () => {
    expect(redisUrl).toBeDefined();
    await expectRedisClientContract(() => bun({ url: redisUrl }));
  });
});

describeRedis("bun pubsub", () => {
  it("publishes and subscribes typed messages over a leased subscriber", async () => {
    expect(redisUrl).toBeDefined();
    const client = await bun({ url: redisUrl });
    const redis = beni(client);
    const channel = definePubSubChannel(
      `beni:test:events:${Date.now()}:${Math.random().toString(36).slice(2)}`,
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
    const redis = beni(client);
    const pattern = {
      kind: "pattern" as const,
      pattern: "beni:test:none:*",
      decode: (message: string) => message
    };

    try {
      await expect(
        redis.pubsub.pattern(pattern).subscribe(() => {})
      ).rejects.toThrow(TypeError);
    } finally {
      await client.close();
    }
  });
});

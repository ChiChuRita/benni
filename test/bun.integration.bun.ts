import { describe, expect, it } from "vitest";
import { bun } from "../src/bun/index.js";
import { codecs, definePubSubChannel } from "../src/core/index.js";
import { expectRedisClientContract } from "./redis-contract.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("bun", () => {
  it("passes the shared Redis client contract", async () => {
    expect(redisUrl).toBeDefined();
    await expectRedisClientContract(() => bun({ url: redisUrl }));
  });
});

describeRedis("bun.pubsub", () => {
  it("publishes and subscribes typed messages", async () => {
    expect(redisUrl).toBeDefined();
    const pubsub = await bun.pubsub({ url: redisUrl });
    const channel = definePubSubChannel(
      `beni:test:events:${Date.now()}:${Math.random().toString(36).slice(2)}`,
      codecs.json<{ id: string; action: "created" }>()
    );
    let subscription: { unsubscribe(): Promise<void> } | undefined;
    let resolveReceived!: (message: { id: string; action: "created" }) => void;
    let rejectReceived!: (error: Error) => void;
    const received = new Promise<{ id: string; action: "created" }>(
      (resolve, reject) => {
        resolveReceived = resolve;
        rejectReceived = reject;
      }
    );
    const timeout = setTimeout(
      () => rejectReceived(new Error("Timed out waiting for pubsub message")),
      1000
    );

    try {
      subscription = await pubsub.subscribe(channel, (message) => {
        clearTimeout(timeout);
        resolveReceived(message);
      });
      await expect(
        pubsub.publish(channel, { id: "42", action: "created" })
      ).resolves.toBe(1);
      await expect(received).resolves.toEqual({ id: "42", action: "created" });
    } finally {
      clearTimeout(timeout);
      await subscription?.unsubscribe();
      await pubsub.close();
    }
  });
});

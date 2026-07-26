import { describe, expect, it } from "vitest";
import {
  codecs,
  definePubSubChannel,
  definePubSubPattern
} from "../src/core/index.js";
import { node, pubsub } from "../src/node/index.js";
import { expectRedisClientContract } from "./redis-contract.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("node", () => {
  it("passes the shared Redis client contract", async () => {
    expect(redisUrl).toBeDefined();
    await expectRedisClientContract(() => node({ url: redisUrl }));
  });
});

describeRedis("node pubsub", () => {
  it("publishes and subscribes typed messages", async () => {
    expect(redisUrl).toBeDefined();
    const pubsubAdapter = await pubsub({ url: redisUrl });
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
      subscription = await pubsubAdapter.subscribe(channel, (message) => {
        clearTimeout(timeout);
        resolveReceived(message);
      });
      await expect(
        pubsubAdapter.publish(channel, { id: "42", action: "created" })
      ).resolves.toBe(1);
      await expect(received).resolves.toEqual({ id: "42", action: "created" });
    } finally {
      clearTimeout(timeout);
      await subscription?.unsubscribe();
      await pubsubAdapter.close();
    }
  });

  it("subscribes typed patterns and reports the matched channel", async () => {
    expect(redisUrl).toBeDefined();
    const pubsubAdapter = await pubsub({ url: redisUrl });
    const prefix = `beni:test:pattern:${Date.now()}:${Math.random()
      .toString(36)
      .slice(2)}`;
    const pattern = definePubSubPattern(
      `${prefix}:*`,
      codecs.json<{ id: string }>()
    );
    const channel = definePubSubChannel(
      `${prefix}:created`,
      codecs.json<{ id: string }>()
    );
    let subscription: { unsubscribe(): Promise<void> } | undefined;
    let resolveReceived!: (received: {
      message: { id: string };
      channel: string;
    }) => void;
    let rejectReceived!: (error: Error) => void;
    const received = new Promise<{ message: { id: string }; channel: string }>(
      (resolve, reject) => {
        resolveReceived = resolve;
        rejectReceived = reject;
      }
    );
    const timeout = setTimeout(
      () => rejectReceived(new Error("Timed out waiting for pattern message")),
      1000
    );

    try {
      subscription = await pubsubAdapter.subscribePattern(
        pattern,
        (message, name) => {
          clearTimeout(timeout);
          resolveReceived({ message, channel: name });
        }
      );
      await expect(pubsubAdapter.publish(channel, { id: "42" })).resolves.toBe(
        1
      );
      await expect(received).resolves.toEqual({
        message: { id: "42" },
        channel: channel.name
      });
    } finally {
      clearTimeout(timeout);
      await subscription?.unsubscribe();
      await pubsubAdapter.close();
    }
  });
});

import { describe, expect, it } from "vitest";
import {
  codecs,
  definePubSubChannel,
  definePubSubPattern
} from "../src/core/index.js";
import { beni } from "../src/index.js";
import { node } from "../src/node/index.js";
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
  const unique = (label: string) =>
    `beni:test:${label}:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  it("publishes and subscribes typed messages over a leased subscriber", async () => {
    const client = await node({ url: redisUrl });
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

  it("multiplexes many subscriptions onto one connection and releases it", async () => {
    const client = await node({ url: redisUrl });
    const redis = beni(client);
    const channel = definePubSubChannel(
      unique("multiplex"),
      codecs.json<{ n: number }>()
    );
    const a: number[] = [];
    const b: number[] = [];
    const subA = await redis.pubsub.channel(channel).subscribe((m) => {
      a.push(m.n);
    });
    const subB = await redis.pubsub.channel(channel).subscribe((m) => {
      b.push(m.n);
    });

    try {
      await redis.pubsub.channel(channel).publish({ n: 1 });
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(a).toEqual([1]);
      expect(b).toEqual([1]);

      // Dropping one handler must not tear down the other's delivery.
      await subA.unsubscribe();
      await redis.pubsub.channel(channel).publish({ n: 2 });
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(a).toEqual([1]);
      expect(b).toEqual([1, 2]);
    } finally {
      await subB.unsubscribe();
      await client.close();
    }
  });

  it("streams messages as an async iterable and stops on abort", async () => {
    const client = await node({ url: redisUrl });
    const redis = beni(client);
    const channel = definePubSubChannel(
      unique("stream"),
      codecs.json<{ n: number }>()
    );
    const controller = new AbortController();
    const received: number[] = [];

    const consume = (async () => {
      for await (const message of redis.pubsub
        .channel(channel)
        .stream({ signal: controller.signal })) {
        received.push(message.n);
        if (received.length === 2) controller.abort();
      }
    })();

    try {
      await new Promise((resolve) => setTimeout(resolve, 100));
      await redis.pubsub.channel(channel).publish({ n: 1 });
      await redis.pubsub.channel(channel).publish({ n: 2 });
      await consume;
      expect(received).toEqual([1, 2]);
    } finally {
      await client.close();
    }
  });

  it("subscribes typed patterns and reports the matched channel", async () => {
    const client = await node({ url: redisUrl });
    const redis = beni(client);
    const prefix = unique("pattern");
    const pattern = definePubSubPattern(
      `${prefix}:*`,
      codecs.json<{ id: string }>()
    );
    const channel = definePubSubChannel(
      `${prefix}:created`,
      codecs.json<{ id: string }>()
    );
    const seen: Array<{ id: string; channel: string }> = [];
    const first = new Promise<void>((resolve) => {
      void redis.pubsub.pattern(pattern).subscribe((message, name) => {
        seen.push({ id: message.id, channel: name });
        resolve();
      });
    });

    try {
      await new Promise((resolve) => setTimeout(resolve, 50));
      await expect(
        redis.pubsub.channel(channel).publish({ id: "42" })
      ).resolves.toBe(1);
      await first;
      expect(seen).toEqual([{ id: "42", channel: `${prefix}:created` }]);
    } finally {
      await redis.pubsub.close();
      await client.close();
    }
  });
});

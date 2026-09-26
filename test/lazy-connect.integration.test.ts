import { describe, expect, it } from "vitest";
import type { ConnectionEvents } from "../src/core/connection.js";
import type { FullRedisClient } from "../src/core/types.js";
import { benni } from "../src/index.js";
import { ioredis } from "../src/ioredis/index.js";
import { node } from "../src/node/index.js";
import { channel, string } from "../src/schema.js";
import { tcpProxy } from "./tcp-proxy.js";

// Adapters return their client synchronously and connect on first use. The
// review finding behind it: `benni({ client: node({ url }) })` adopted the
// connect promise, and after one failed connect every command re-reported it
// forever, so a pod that booted during a Redis restart stayed dead.

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

function activeSockets(): number {
  const handles = (
    process as unknown as {
      _getActiveHandles(): Array<{ constructor?: { name?: string } }>;
    }
  )._getActiveHandles();
  return handles.filter((handle) => handle.constructor?.name === "Socket")
    .length;
}

async function waitUntil(condition: () => boolean, ms = 5000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("condition never held");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const adapters: ReadonlyArray<
  [string, (port: number, events?: ConnectionEvents) => FullRedisClient]
> = [
  [
    "node",
    (port, events) => node({ url: `redis://127.0.0.1:${port}`, ...events })
  ],
  ["ioredis", (port, events) => ioredis({ host: "127.0.0.1", port, ...events })]
];

describeRedis("adapters connect on first use", () => {
  for (const [label, make] of adapters) {
    it(`${label}: opens no socket until a command needs one`, async () => {
      const proxy = await tcpProxy(redisUrl as string);
      await proxy.start();
      const before = activeSockets();
      const redis = benni({ client: make(proxy.port) });
      await sleep(50);
      expect(activeSockets()).toBe(before);

      await expect(redis.raw.send(["PING"])).resolves.toBe("PONG");
      await redis.close();
      await proxy.stop();
    });

    it(`${label}: a failed first connect fails its commands, and a later command connects`, async () => {
      const proxy = await tcpProxy(redisUrl as string);
      const errors: unknown[] = [];
      const redis = benni({
        client: make(proxy.port, { onError: (error) => errors.push(error) })
      });
      const before = activeSockets();

      // Both commands wait on the one connect, and both get its failure.
      const results = await Promise.allSettled([
        redis.raw.send(["PING"]),
        redis.raw.send(["PING"])
      ]);
      for (const result of results) {
        expect(result.status).toBe("rejected");
        expect(String((result as PromiseRejectedResult).reason)).toMatch(
          new RegExp(`benni/${label} could not connect to Redis`)
        );
      }
      expect(errors.length).toBeGreaterThan(0);
      // The failed attempt left nothing reconnecting in the background.
      await sleep(300);
      expect(activeSockets()).toBeLessThanOrEqual(before);

      // Redis comes up; once the backoff has passed, the next command dials.
      await proxy.start();
      await sleep(150);
      await expect(redis.raw.send(["PING"])).resolves.toBe("PONG");

      await redis.close();
      await proxy.stop();
    });

    it(`${label}: reports a reconnect of the client and of the subscriber`, async () => {
      const proxy = await tcpProxy(redisUrl as string);
      await proxy.start();
      const reconnects: string[] = [];
      const news = channel("benni:test:lazy:news", string());
      const redis = benni({
        client: make(proxy.port, {
          onReconnect: (connection) => reconnects.push(connection),
          onError: () => {}
        }),
        schema: { news }
      });
      const received: string[] = [];
      await redis.query.news.subscribe((message) => {
        received.push(message);
      });
      await redis.raw.send(["PING"]);
      expect(reconnects).toEqual([]);

      proxy.dropConnections();
      await waitUntil(
        () => reconnects.includes("client") && reconnects.includes("subscriber")
      );
      // Resubscribed natively, so delivery resumes after the gap.
      await waitUntil(() => {
        void redis.query.news.publish("after");
        return received.includes("after");
      });

      await redis.close();
      await proxy.stop();
    });

    it(`${label}: close() on a client that never connected opens nothing`, async () => {
      const proxy = await tcpProxy(redisUrl as string);
      await proxy.start();
      const before = activeSockets();
      const redis = benni({ client: make(proxy.port) });
      await redis.close();
      await expect(redis.raw.send(["PING"])).rejects.toThrow(/closed/);
      await sleep(50);
      expect(activeSockets()).toBeLessThanOrEqual(before);
      await proxy.stop();
    });
  }
});

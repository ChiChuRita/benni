import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { bun } from "../src/bun/index.js";
import { codecs, type FullRedisClient } from "../src/core/index.js";
import {
  definePubSubChannel,
  definePubSubPattern
} from "../src/core/pubsub.js";
import { benni } from "../src/index.js";
import { freePort } from "./free-port.js";
import {
  expectPubSubSurvivesReconnect,
  expectRedisClientContract
} from "./redis-contract.js";
import { tcpProxy } from "./tcp-proxy.js";

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("bun", () => {
  it("passes the shared Redis client contract", async () => {
    expect(redisUrl).toBeDefined();
    await expectRedisClientContract(() => bun({ url: redisUrl }));
  });

  it("refuses to lease past close() instead of leaking the connection", async () => {
    expect(redisUrl).toBeDefined();
    const client = bun({ url: redisUrl });
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
    const port = await freePort();
    const child = spawn(
      "bun",
      [
        "-e",
        `import { bun } from "${adapter}";
           bun({ url: "redis://127.0.0.1:${port}" }).send(["PING"]).catch(() => {});`
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

describeRedis("bun connects on first use", () => {
  it("fails the commands of a failed first connect, and a later command connects", async () => {
    // A pod booting while Redis restarts: the first commands fail, and the
    // client is not poisoned by it.
    const proxy = await tcpProxy(redisUrl as string);
    const errors: unknown[] = [];
    const client = bun({
      url: `redis://127.0.0.1:${proxy.port}`,
      onError: (error) => errors.push(error)
    });
    try {
      const results = await Promise.allSettled([
        client.send(["PING"]),
        client.send(["PING"])
      ]);
      for (const result of results) {
        expect(result.status).toBe("rejected");
        expect(String((result as PromiseRejectedResult).reason)).toMatch(
          /benni\/bun could not connect to Redis/
        );
      }
      expect(errors.length).toBeGreaterThan(0);

      await proxy.start();
      await new Promise((resolve) => setTimeout(resolve, 150));
      await expect(client.send(["PING"])).resolves.toBe("PONG");
    } finally {
      await client.close();
      await proxy.stop();
    }
  });

  it("reports a reconnect after a drop", async () => {
    const proxy = await tcpProxy(redisUrl as string);
    await proxy.start();
    const reconnects: string[] = [];
    const client = bun({
      url: `redis://127.0.0.1:${proxy.port}`,
      onReconnect: (connection) => reconnects.push(connection)
    });
    try {
      await client.send(["PING"]);
      proxy.dropConnections();
      const deadline = Date.now() + 5000;
      while (!reconnects.includes("client") && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      expect(reconnects).toContain("client");
      await expect(client.send(["PING"])).resolves.toBe("PONG");
    } finally {
      await client.close();
      await proxy.stop();
    }
  });
});

describeRedis("bun subscriber close", () => {
  it("lets the process exit when close() runs with subscriptions live", async () => {
    // Closing a Bun client that still holds subscriptions pins the process
    // forever (verified on 1.4.2), and the parent close() force-closes a
    // subscriber with its subscriptions intact. Prove it exits in a subprocess.
    const adapter = new URL("../src/bun/index.ts", import.meta.url).pathname;
    const child = spawn(
      "bun",
      [
        "-e",
        `import { bun } from "${adapter}";
           const client = bun({ url: "${redisUrl}" });
           const subscriber = await client.subscriber();
           await subscriber.subscribe("benni:test:exit:${Date.now()}", () => {});
           await client.close();`
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
    const client = bun({ url: redisUrl });
    const redis = benni({ client: client });
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

  it("keeps delivering after the subscriber connection is killed", async () => {
    // Bun reconnects the subscriber on its own but used to come back with no
    // subscriptions: PUBLISH reported 0 receivers and every handler went
    // silent while the lease still claimed to be open.
    await expectPubSubSurvivesReconnect(() => bun({ url: redisUrl }));
  }, 20000);

  it("reports pattern subscribe as unsupported instead of hanging", async () => {
    expect(redisUrl).toBeDefined();
    const client = bun({ url: redisUrl });
    // The handle's type has no pattern() over a Bun client; the cast forces
    // the call through to pin the runtime backstop behind that.
    const redis = benni({ client: client as unknown as FullRedisClient });
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

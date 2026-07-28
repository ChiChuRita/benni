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

/** Open TCP handles, so a client left reconnecting in the background shows up. */
function activeSockets(): number {
  const handles = (
    process as unknown as {
      _getActiveHandles(): Array<{ constructor?: { name?: string } }>;
    }
  )._getActiveHandles();
  return handles.filter((handle) => handle.constructor?.name === "Socket")
    .length;
}

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

  it("refuses ioredis keyPrefix rather than silently breaking scans", async () => {
    // ioredis prefixes key arguments but not SCAN patterns, so a prefixed
    // client stored at `app:user:1` while every MATCH pattern and
    // schema.key() still said `user:1`. Scans returned nothing, silently.
    await expect(ioredis({ url: redisUrl, keyPrefix: "app:" })).rejects.toThrow(
      /keyPrefix/
    );

    const raw = new IORedis(redisUrl as string, { keyPrefix: "app:" });
    try {
      await expect(ioredis(raw)).rejects.toThrow(/keyPrefix/);
    } finally {
      raw.disconnect();
    }
  });

  it("does not leave a retrying client behind when connect fails", async () => {
    // The absorbing 'error' listener went on after await connect(), so a
    // failed connect left an orphan reconnecting forever: "Unhandled error
    // event" on repeat, and a process that never exits.
    // ioredis reports it through console.error rather than throwing, and the
    // retries are what keep the socket alive, so watch for both: the log line
    // and the leftover handle.
    const logged: string[] = [];
    const original = console.error;
    console.error = (...parts: unknown[]) => {
      logged.push(parts.map(String).join(" "));
    };
    const socketsBefore = activeSockets();
    try {
      await expect(
        ioredis({ host: "127.0.0.1", port: 6399, connectTimeout: 300 })
      ).rejects.toThrow();
      // Long enough for at least one reconnect attempt to fire.
      await new Promise((resolve) => setTimeout(resolve, 900));
    } finally {
      console.error = original;
    }

    expect(logged.filter((line) => line.includes("Unhandled error"))).toEqual(
      []
    );
    expect(activeSockets()).toBeLessThanOrEqual(socketsBefore);
  });
});

const clusterUrl = process.env.BENI_REDIS_CLUSTER_URL;
const describeCluster = clusterUrl ? describe : describe.skip;

describeCluster("ioredis (adopted Cluster)", () => {
  // Cluster.duplicate takes options as its *second* argument, so the adapter's
  // single-object call landed them in overrideStartupNodes and dropped every
  // one, lazyConnect included. The duplicate dialed on its own and connect()
  // then failed with "Redis is already connecting/connected", which took out
  // session() and subscriber() on every adopted Cluster.
  const nodeOf = (url: string) => {
    const parsed = new URL(url);
    return {
      host: parsed.hostname,
      port: Number(parsed.port || 6379)
    };
  };

  async function adopt() {
    const cluster = new IORedis.Cluster([nodeOf(clusterUrl as string)]);
    cluster.on("error", () => {});
    await new Promise((resolve) => cluster.once("ready", resolve));
    return { cluster, client: await ioredis(cluster) };
  }

  it("supports session() on an adopted Cluster", async () => {
    const { cluster, client } = await adopt();
    try {
      const session = await client.session?.();
      if (!session) throw new Error("session() is required on this adapter");
      // Hash-tagged so the watched key and the transaction share a slot.
      await session.send(["SET", "{beni-t}:k", "v1"]);
      await session.send(["WATCH", "{beni-t}:k"]);
      await expect(
        session.watchedTransaction([["GET", "{beni-t}:k"]])
      ).resolves.toEqual(["v1"]);
      expect(session.closed).toBe(false);
      await session.close();
      expect(session.closed).toBe(true);
      await client.send(["DEL", "{beni-t}:k"]);
    } finally {
      await client.close();
      cluster.disconnect();
    }
  });

  it("supports subscriber() on an adopted Cluster", async () => {
    const { cluster, client } = await adopt();
    try {
      const subscriber = await client.subscriber?.();
      if (!subscriber)
        throw new Error("subscriber() is required on this adapter");
      const received = new Promise<string>((resolve) => {
        void subscriber.subscribe("beni-t-chan", resolve);
      });
      await new Promise((resolve) => setTimeout(resolve, 200));
      await client.send(["PUBLISH", "beni-t-chan", "hello"]);
      await expect(received).resolves.toBe("hello");
      await subscriber.close();
    } finally {
      await client.close();
      cluster.disconnect();
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

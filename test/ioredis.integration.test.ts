import IORedis from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import { codecs, type RedisClient } from "../src/core/index.js";
import { definePubSubChannel } from "../src/core/pubsub.js";
import { benni } from "../src/index.js";
import { ioredis } from "../src/ioredis/index.js";
import { queue } from "../src/primitives/index.js";
import { json, kv, number, script } from "../src/schema.js";
import { freePort } from "./free-port.js";
import {
  expectPubSubSurvivesReconnect,
  expectRedisClientContract
} from "./redis-contract.js";

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
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

  it("keeps Pub/Sub delivering after the subscriber connection is killed", async () => {
    await expectPubSubSurvivesReconnect(() => ioredis({ url: redisUrl }), {
      patterns: true
    });
  }, 30_000);

  it("reports the subscriber closed once ioredis gives up reconnecting", async () => {
    const connectionName = `benni-ioredis-sub-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    // retryStrategy returning null makes a drop terminal: ioredis goes to
    // "end" instead of "reconnecting", the state the getter has to surface.
    // The subscriber duplicate inherits both options from the parent.
    const client = ioredis({
      url: redisUrl,
      connectionName,
      retryStrategy: () => null
    });
    const admin = new IORedis(redisUrl as string);
    admin.on("error", () => {});
    try {
      const subscriber = await client.subscriber?.();
      if (!subscriber) throw new Error("subscriber() is required");
      await subscriber.subscribe(`benni:test:end:${connectionName}`, () => {});
      expect(subscriber.closed).toBe(false);
      const list = String(await admin.call("CLIENT", "LIST", "TYPE", "pubsub"));
      const line = list
        .split("\n")
        .find((entry) => entry.includes(` name=${connectionName} `));
      if (line === undefined)
        throw new Error("subscriber connection not found");
      await admin.call(
        "CLIENT",
        "KILL",
        "ID",
        line.slice(3, line.indexOf(" "))
      );
      // The flag used to be local-only, so core kept handing the next
      // subscribe this dead lease.
      await expect.poll(() => subscriber.closed, { timeout: 2000 }).toBe(true);
    } finally {
      admin.disconnect();
      await client.close();
    }
  });

  it("accepts a bare URL string", async () => {
    const client = ioredis(redisUrl as string);
    try {
      await expect(client.send(["PING"])).resolves.toBe("PONG");
    } finally {
      await client.close();
    }
  });

  it("accepts host/port options without a url", async () => {
    const { hostname, port } = new URL(redisUrl as string);
    const client = ioredis({
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
    expect(() => ioredis({ url: redisUrl, keyPrefix: "app:" })).toThrow(
      /keyPrefix/
    );

    const raw = new IORedis(redisUrl as string, {
      keyPrefix: "app:",
      protocol: 2
    });
    try {
      expect(() => ioredis(raw)).toThrow(/keyPrefix/);
    } finally {
      raw.disconnect();
    }
  });

  it("pins the clients it creates to RESP2", async () => {
    // ioredis 6 defaults to RESP3, whose XREAD reply is a map: every stream
    // read then failed with ReplyShapeError. Ask the server what it speaks.
    for (const client of [
      ioredis(redisUrl as string),
      ioredis({ url: redisUrl, protocol: undefined })
    ]) {
      try {
        await expect(client.send(["CLIENT", "INFO"])).resolves.toMatch(
          / resp=2 /
        );
      } finally {
        await client.close();
      }
    }
  });

  it("refuses RESP3 instead of failing on the first stream read", async () => {
    expect(() => ioredis({ url: redisUrl, protocol: 3 })).toThrow(
      /protocol: 2/
    );
    expect(() => ioredis(`${redisUrl}?protocol=3`)).toThrow(/protocol: 2/);

    const raw = new IORedis(redisUrl as string, {
      lazyConnect: true,
      protocol: 3
    });
    const cluster = new IORedis.Cluster([], {
      lazyConnect: true,
      redisOptions: { protocol: 3 }
    });
    try {
      expect(() => ioredis(raw)).toThrow(/protocol: 2/);
      expect(() => ioredis(cluster)).toThrow(/protocol: 2/);
    } finally {
      raw.disconnect();
      cluster.disconnect();
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
    const port = await freePort();
    const socketsBefore = activeSockets();
    try {
      const client = ioredis({ host: "127.0.0.1", port, connectTimeout: 300 });
      await expect(client.send(["PING"])).rejects.toThrow(
        /could not connect to Redis/
      );
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

const clusterUrl = process.env.BENNI_REDIS_CLUSTER_URL;
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
    const cluster = new IORedis.Cluster([nodeOf(clusterUrl as string)], {
      redisOptions: { protocol: 2 }
    });
    cluster.on("error", () => {});
    await new Promise((resolve) => cluster.once("ready", resolve));
    return { cluster, client: ioredis(cluster) };
  }

  it("supports session() on an adopted Cluster", async () => {
    const { cluster, client } = await adopt();
    try {
      const session = await client.session?.();
      if (!session) throw new Error("session() is required on this adapter");
      // Hash-tagged so the watched key and the transaction share a slot.
      await session.send(["SET", "{benni-t}:k", "v1"]);
      await session.send(["WATCH", "{benni-t}:k"]);
      await expect(
        session.watchedTransaction([["GET", "{benni-t}:k"]])
      ).resolves.toEqual(["v1"]);
      expect(session.closed).toBe(false);
      await session.close();
      expect(session.closed).toBe(true);
      await client.send(["DEL", "{benni-t}:k"]);
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
        void subscriber.subscribe("benni-t-chan", resolve);
      });
      await new Promise((resolve) => setTimeout(resolve, 200));
      await client.send(["PUBLISH", "benni-t-chan", "hello"]);
      await expect(received).resolves.toBe("hello");
      await subscriber.close();
    } finally {
      await client.close();
      cluster.disconnect();
    }
  });

  it("runs scripts on whichever node owns the keys", async () => {
    // SCRIPT LOAD carries no key, so a Cluster sends it to a random node,
    // and EVALSHA then went to the node owning the key and drew NOSCRIPT. The
    // retry reloaded through the same keyless route, so with several nodes
    // most scripts failed. The runner now falls back to EVAL, which carries
    // the keys. A single-node cluster (CI) cannot show the routing, but still
    // runs the fallback against a real Cluster client after the flush below;
    // point BENNI_REDIS_CLUSTER_URL at a multi-node cluster to see the rest.
    const { cluster, client } = await adopt();
    const counter = script("benni-cluster-counter", {
      keys: ["counter"],
      args: { by: number() },
      returns: number(),
      lua: 'return redis.call("INCRBY", KEYS[1], ARGV[1])'
    });
    const redis = benni({ client, schema: { counter } });
    const keys = Array.from(
      { length: 24 },
      (_, index) => `benni-t-script:${index}`
    );
    const flushAll = () =>
      Promise.all(
        cluster.nodes("master").map((master) => master.script("FLUSH"))
      );
    try {
      await Promise.all(keys.map((key) => cluster.del(key)));
      // No node has the script: the one load lands on a single node.
      await flushAll();
      for (const key of keys) {
        await expect(
          redis.query.counter.run({ keys: { counter: key }, args: { by: 2 } })
        ).resolves.toBe(2);
      }
      // Every node forgets it again, under concurrent callers this time.
      await flushAll();
      const again = await Promise.all(
        keys.map((key) =>
          redis.query.counter.run({ keys: { counter: key }, args: { by: 3 } })
        )
      );
      expect(again).toEqual(keys.map(() => 5));
    } finally {
      await Promise.all(keys.map((key) => cluster.del(key)));
      await client.close();
      cluster.disconnect();
    }
  });
});

describeRedis("ioredis (adopted client)", () => {
  const owned = new IORedis(redisUrl as string, {
    lazyConnect: true,
    protocol: 2
  });
  owned.on("error", () => {});

  afterAll(() => {
    owned.disconnect();
  });

  it("adopts an existing instance instead of dialing its own", async () => {
    await owned.connect();
    const client = ioredis(owned);
    const key = `benni:test:adopt:${Date.now()}`;

    await expect(client.send(["PING"])).resolves.toBe("PONG");
    await client.send(["SET", key, "adopted"]);
    // The value is visible through the caller's own handle: same connection.
    await expect(owned.get(key)).resolves.toBe("adopted");
    await owned.del(key);
  });

  it("leaves an adopted client open on close, but reaps what it leased", async () => {
    const client = ioredis(owned);
    const session = await client.session?.();
    expect(session?.closed).toBe(false);

    await client.close();

    // Benni's leased session is gone...
    expect(session?.closed).toBe(true);
    // ...but the caller's client is untouched, because they still own it.
    expect(owned.status).toBe("ready");
    await expect(owned.ping()).resolves.toBe("PONG");
  });
});

describeRedis("ioredis: typed client and primitives", () => {
  const unique = (label: string) =>
    `benni:test:${label}:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  it("runs the typed store API end to end", async () => {
    const client = ioredis({ url: redisUrl });
    const redis = benni({ client: client });
    const id = unique("kv");
    try {
      const profiles = redis.kv(
        kv("benni:test:profile", json<{ name: string; n: number }>())
      );
      await profiles.set(id, { name: "Ada", n: 1 });
      await expect(profiles.get(id)).resolves.toEqual({ name: "Ada", n: 1 });
      await profiles.del(id);
    } finally {
      await client.close();
    }
  });

  it("delivers typed Pub/Sub over a leased subscriber", async () => {
    const client = ioredis({ url: redisUrl });
    const redis = benni({ client: client });
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
    const client: RedisClient = ioredis({ url: redisUrl });
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

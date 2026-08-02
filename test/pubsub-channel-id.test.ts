import { describe, expect, it, vi } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { defineKeyspace } from "../src/core/key-value.js";
import {
  createPubSubPublisher,
  definePubSubChannel,
  definePubSubPattern
} from "../src/core/pubsub.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  RedisSubscriber
} from "../src/core/types.js";
import { benni } from "../src/database.js";
import { fakeClient } from "./fake-client.js";

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

type RoomMessage = { text: string };

const roomEvents = definePubSubChannel("chat:room", codecs.json<RoomMessage>());

const roomPattern = definePubSubPattern(
  "chat:room:*",
  codecs.json<RoomMessage>()
);

/**
 * A client that plays the server for Pub/Sub: PUBLISH is routed to the listener
 * registered for that exact channel and to every pattern listener whose glob
 * matches it, so a publish and a subscription can be checked against each other
 * in one process. That is what makes "an id-scoped publish reaches a pattern
 * subscriber" a real assertion here rather than a string comparison; the live
 * server confirms the same thing in node.integration.test.ts.
 */
function pubsubServer() {
  const log: string[] = [];
  const channelListeners = new Map<string, (message: string) => void>();
  const patternListeners = new Map<
    string,
    (message: string, channel: string) => void
  >();

  // Redis's own matcher knows `*`, `?`, `[...]` and `\`; these tests only use
  // the first two, so escape every regex metacharacter and translate those.
  function matches(pattern: string, channel: string): boolean {
    const source = pattern
      .replace(/[.+^${}()|[\]\\]/g, "\\$&")
      .replace(/\*/g, ".*")
      .replace(/\?/g, ".");
    return new RegExp(`^${source}$`).test(channel);
  }

  const client: RedisClient = {
    async send(command: RedisCommand) {
      const [name, channel, message] = command as [string, string, string];
      if (name !== "PUBLISH") throw new Error(`unexpected command: ${name}`);
      log.push(`publish:${channel}`);
      let delivered = 0;
      const direct = channelListeners.get(channel);
      if (direct) {
        delivered += 1;
        direct(message);
      }
      for (const [pattern, listener] of patternListeners) {
        if (!matches(pattern, channel)) continue;
        delivered += 1;
        listener(message, channel);
      }
      return delivered as RedisReply;
    },
    async pipeline() {
      return [];
    },
    async subscriber(): Promise<RedisSubscriber> {
      let closed = false;
      log.push("lease");
      return {
        async subscribe(channel, listener) {
          log.push(`subscribe:${channel}`);
          channelListeners.set(channel, listener);
        },
        async unsubscribe(channel) {
          log.push(`unsubscribe:${channel}`);
          channelListeners.delete(channel);
        },
        async psubscribe(pattern, listener) {
          log.push(`psubscribe:${pattern}`);
          patternListeners.set(pattern, listener);
        },
        async punsubscribe(pattern) {
          log.push(`punsubscribe:${pattern}`);
          patternListeners.delete(pattern);
        },
        get closed() {
          return closed;
        },
        async close() {
          log.push("close");
          closed = true;
        }
      };
    },
    async close() {}
  };

  return { client, log };
}

describe("channel name derivation", () => {
  it("resolves to the bare name with no id", () => {
    expect(roomEvents.name).toBe("chat:room");
    expect(roomEvents.channelName()).toBe("chat:room");
  });

  it("derives an id-scoped channel exactly as a keyspace derives its key", () => {
    // The point of routing through the shared key builder: a channel and a
    // keyspace with the same prefix must produce the same string for the same
    // id, or a pattern written against one would stop matching the other.
    const rooms = defineKeyspace("chat:room", codecs.string());
    expect(roomEvents.channelName("42")).toBe(rooms.key("42"));
    expect(roomEvents.channelName("42")).toBe("chat:room:42");
  });

  it("accepts number and bigint ids the way keyspace ids do", () => {
    expect(roomEvents.channelName(42)).toBe("chat:room:42");
    expect(roomEvents.channelName(42n)).toBe("chat:room:42");
  });

  it("keeps the schema enumerable as the plain data it looks like", () => {
    expect(Object.keys(roomEvents)).toEqual([
      "kind",
      "name",
      "channelName",
      "encode",
      "decode"
    ]);
    expect(roomEvents.kind).toBe("channel");
  });
});

describe("publishing to a per-entity channel", () => {
  it("publishes to the bare channel when no id is given", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, [1]));

    await expect(
      redis.pubsub.channel(roomEvents).publish({ text: "hi" })
    ).resolves.toBe(1);
    expect(commands).toEqual([["PUBLISH", "chat:room", '{"text":"hi"}']]);
  });

  it("publishes to prefix:id when an id is given", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, [2]));

    await expect(
      redis.pubsub.channel(roomEvents, 42).publish({ text: "hi" })
    ).resolves.toBe(2);
    expect(commands).toEqual([["PUBLISH", "chat:room:42", '{"text":"hi"}']]);
  });

  it("exposes the resolved channel on the resource, without the schema", () => {
    const redis = benni(fakeClient([], []));
    expect(redis.pubsub.channel(roomEvents).channelName("42")).toBe(
      "chat:room:42"
    );
    expect(redis.pubsub.channel(roomEvents, "42").channelName()).toBe(
      "chat:room:42"
    );
  });

  it("reaches per-entity channels through the schema registry", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, [1]), {
      schema: { roomEvents }
    });

    await redis.query.roomEvents.at("42").publish({ text: "hi" });
    expect(commands).toEqual([["PUBLISH", "chat:room:42", '{"text":"hi"}']]);
  });

  it("publishes to prefix:id from the low-level publisher too", async () => {
    const commands: RedisCommand[] = [];
    const publisher = createPubSubPublisher(fakeClient(commands, [3]));

    await expect(
      publisher.publish(roomEvents, { text: "hi" }, "42")
    ).resolves.toBe(3);
    expect(commands).toEqual([["PUBLISH", "chat:room:42", '{"text":"hi"}']]);
  });
});

describe("subscribing to a per-entity channel", () => {
  it("subscribes to prefix:id and decodes with the schema codec", async () => {
    const server = pubsubServer();
    const redis = benni(server.client);
    const seen: RoomMessage[] = [];

    const subscription = await redis.pubsub
      .channel(roomEvents, 42)
      .subscribe((message) => {
        seen.push(message);
      });

    expect(server.log).toEqual(["lease", "subscribe:chat:room:42"]);
    await redis.pubsub.channel(roomEvents, 42).publish({ text: "hi" });
    expect(seen).toEqual([{ text: "hi" }]);

    await subscription.unsubscribe();
    expect(server.log.at(-2)).toBe("unsubscribe:chat:room:42");
    expect(server.log.at(-1)).toBe("close");
  });

  it("keeps the bare channel and an id-scoped one apart", async () => {
    const server = pubsubServer();
    const redis = benni(server.client);
    const bare: RoomMessage[] = [];
    const scoped: RoomMessage[] = [];

    await redis.pubsub.channel(roomEvents).subscribe((message) => {
      bare.push(message);
    });
    await redis.pubsub.channel(roomEvents, 42).subscribe((message) => {
      scoped.push(message);
    });

    try {
      await redis.pubsub.channel(roomEvents).publish({ text: "all" });
      await redis.pubsub.channel(roomEvents, 42).publish({ text: "room" });

      expect(bare).toEqual([{ text: "all" }]);
      expect(scoped).toEqual([{ text: "room" }]);
    } finally {
      await redis.pubsub.close();
    }
  });

  it("multiplexes two resources for the same id onto one subscription", async () => {
    const server = pubsubServer();
    const redis = benni(server.client);
    const first: RoomMessage[] = [];
    const second: RoomMessage[] = [];

    // Two independently built resources, so the hub can only share the
    // subscription if the id-scoped channels agree on their name.
    const a = await redis.pubsub.channel(roomEvents, 42).subscribe((m) => {
      first.push(m);
    });
    const b = await redis.pubsub.channel(roomEvents, 42).subscribe((m) => {
      second.push(m);
    });

    expect(
      server.log.filter((entry) => entry === "subscribe:chat:room:42")
    ).toHaveLength(1);

    await redis.pubsub.channel(roomEvents, 42).publish({ text: "hi" });
    expect(first).toEqual([{ text: "hi" }]);
    expect(second).toEqual([{ text: "hi" }]);

    // The channel is only dropped when the last handler for it leaves.
    await a.unsubscribe();
    expect(server.log).not.toContain("unsubscribe:chat:room:42");
    await b.unsubscribe();
    expect(server.log).toContain("unsubscribe:chat:room:42");
  });

  it("delivers an id-scoped publish to a pattern subscriber", async () => {
    const server = pubsubServer();
    const redis = benni(server.client);
    const seen: Array<[RoomMessage, string]> = [];

    const subscription = await redis.pubsub
      .pattern(roomPattern)
      .subscribe((message, channel) => {
        seen.push([message, channel]);
      });

    try {
      await expect(
        redis.pubsub.channel(roomEvents, 42).publish({ text: "hi" })
      ).resolves.toBe(1);
      expect(seen).toEqual([[{ text: "hi" }, "chat:room:42"]]);

      // A second room lands on the same pattern subscription.
      await redis.pubsub.channel(roomEvents, 7).publish({ text: "yo" });
      expect(seen.at(-1)).toEqual([{ text: "yo" }, "chat:room:7"]);
    } finally {
      await subscription.unsubscribe();
    }
  });

  it("streams an id-scoped channel and releases it on abort", async () => {
    const server = pubsubServer();
    const redis = benni(server.client);
    const controller = new AbortController();
    const received: string[] = [];

    const consume = (async () => {
      for await (const message of redis.pubsub
        .channel(roomEvents, 42)
        .stream({ signal: controller.signal })) {
        received.push(message.text);
        controller.abort();
      }
    })();

    await vi.waitUntil(() => server.log.includes("subscribe:chat:room:42"));
    await redis.pubsub.channel(roomEvents, 42).publish({ text: "hi" });
    await consume;

    expect(received).toEqual(["hi"]);
    expect(server.log.at(-1)).toBe("close");
  });
});

describe("per-entity channel types", () => {
  it("narrows the id and the resolved channel name", () => {
    const known = definePubSubChannel("chat:room", codecs.string(), {
      ids: ["lobby", "support"]
    });

    const resolved = roomEvents.channelName("42");
    const fromResource = benni(fakeClient([], []))
      .pubsub.channel(roomEvents, 42)
      .channelName();
    const knownResolved = known.channelName("lobby");
    type _Resolved = Expect<Equal<typeof resolved, "chat:room:42">>;
    type _FromResource = Expect<Equal<typeof fromResource, "chat:room:42">>;
    type _KnownResolved = Expect<
      Equal<typeof knownResolved, "chat:room:lobby">
    >;
    type _KnownId = Expect<
      Equal<Parameters<typeof known.channelName<"lobby">>[0], "lobby">
    >;

    // @ts-expect-error — "nope" is not one of the declared ids.
    known.channelName("nope");
    // @ts-expect-error — an object is not a RedisKeyPart.
    roomEvents.channelName({ id: 1 });

    expect(resolved).toBe("chat:room:42");
    expect(knownResolved).toBe("chat:room:lobby");
  });

  it("keeps the message type on the id-scoped resource", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, [1]));
    const scoped = redis.pubsub.channel(roomEvents, 42);
    type Published = Parameters<typeof scoped.publish>[0];
    type _Published = Expect<Equal<Published, RoomMessage>>;

    // @ts-expect-error — the codec's message shape still applies.
    await scoped.publish({ nope: true });
    expect(commands).toHaveLength(1);
  });
});

import { describe, expect, it, vi } from "vitest";
import { codecs } from "../src/core/codecs.js";
import {
  createPubSubHub,
  definePubSubChannel,
  definePubSubPattern
} from "../src/core/pubsub.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  RedisSubscriber
} from "../src/core/types.js";

type Log = string[];

/**
 * A client whose subscriber() is scripted so the lease lifecycle is
 * observable: every adapter-level call appends to `log`, and `emit` pushes a
 * raw message the way a real server would.
 */
function subscriberClient(options: { patterns?: boolean } = {}) {
  const log: Log = [];
  const commands: RedisCommand[] = [];
  const channelListeners = new Map<string, (message: string) => void>();
  const patternListeners = new Map<
    string,
    (message: string, channel: string) => void
  >();
  let leases = 0;
  let live: RedisSubscriber | null = null;

  const client: RedisClient = {
    async send(command) {
      commands.push(command);
      return 1 as RedisReply;
    },
    async pipeline() {
      return [];
    },
    async subscriber() {
      leases += 1;
      log.push("lease");
      let closed = false;
      const subscriber: RedisSubscriber = {
        async subscribe(channel, listener) {
          log.push(`subscribe:${channel}`);
          channelListeners.set(channel, listener);
        },
        async unsubscribe(channel) {
          log.push(`unsubscribe:${channel}`);
          channelListeners.delete(channel);
        },
        get closed() {
          return closed;
        },
        async close() {
          log.push("close");
          closed = true;
          live = null;
        }
      };
      if (options.patterns !== false) {
        subscriber.psubscribe = async (pattern, listener) => {
          log.push(`psubscribe:${pattern}`);
          patternListeners.set(pattern, listener);
        };
        subscriber.punsubscribe = async (pattern) => {
          log.push(`punsubscribe:${pattern}`);
          patternListeners.delete(pattern);
        };
      }
      live = subscriber;
      return subscriber;
    },
    async close() {}
  };

  return {
    client,
    log,
    commands,
    get leases() {
      return leases;
    },
    get live() {
      return live;
    },
    emit(channel: string, message: string) {
      channelListeners.get(channel)?.(message);
    },
    emitPattern(pattern: string, message: string, channel: string) {
      patternListeners.get(pattern)?.(message, channel);
    }
  };
}

const events = definePubSubChannel("events", codecs.json<{ n: number }>());

describe("pub/sub hub lease lifecycle", () => {
  it("does not lease a connection until the first subscribe", async () => {
    const fake = subscriberClient();
    createPubSubHub(fake.client);
    expect(fake.leases).toBe(0);
    expect(fake.log).toEqual([]);
  });

  it("multiplexes one adapter subscription per channel across handlers", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);
    const a: number[] = [];
    const b: number[] = [];

    const subA = await hub.subscribeChannel(events, (m) => {
      a.push(m.n);
    });
    const subB = await hub.subscribeChannel(events, (m) => {
      b.push(m.n);
    });

    expect(fake.leases).toBe(1);
    expect(fake.log).toEqual(["lease", "subscribe:events"]);

    fake.emit("events", JSON.stringify({ n: 1 }));
    expect(a).toEqual([1]);
    expect(b).toEqual([1]);

    await subA.unsubscribe();
    expect(fake.log).toEqual(["lease", "subscribe:events"]);
    fake.emit("events", JSON.stringify({ n: 2 }));
    expect(a).toEqual([1]);
    expect(b).toEqual([1, 2]);

    await subB.unsubscribe();
    expect(fake.log).toEqual([
      "lease",
      "subscribe:events",
      "unsubscribe:events",
      "close"
    ]);
  });

  it("releases the connection when the last subscription goes and re-leases later", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);

    const first = await hub.subscribeChannel(events, () => {});
    await first.unsubscribe();
    expect(fake.live).toBeNull();

    const second = await hub.subscribeChannel(events, () => {});
    expect(fake.leases).toBe(2);
    await second.unsubscribe();
  });

  it("is idempotent when the same subscription unsubscribes twice", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);
    const sub = await hub.subscribeChannel(events, () => {});

    await sub.unsubscribe();
    await sub.unsubscribe();

    expect(fake.log.filter((entry) => entry === "close")).toHaveLength(1);
  });

  it("decodes per subscription so two schemas can share a channel name", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);
    const raw = definePubSubChannel("events", codecs.string());
    const typed: number[] = [];
    const text: string[] = [];

    await hub.subscribeChannel(events, (m) => {
      typed.push(m.n);
    });
    await hub.subscribeChannel(raw, (m) => {
      text.push(m);
    });

    fake.emit("events", JSON.stringify({ n: 7 }));
    expect(typed).toEqual([7]);
    expect(text).toEqual(['{"n":7}']);
  });

  it("keeps fanning out when one handler throws, reporting via onError", async () => {
    const fake = subscriberClient();
    const errors: unknown[] = [];
    const hub = createPubSubHub(fake.client, (error) => errors.push(error));
    const delivered: number[] = [];
    await hub.subscribeChannel(events, () => {
      throw new Error("handler blew up");
    });
    await hub.subscribeChannel(events, (m) => {
      delivered.push(m.n);
    });

    fake.emit("events", JSON.stringify({ n: 5 }));
    expect(delivered).toEqual([5]);
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("handler blew up");
  });

  it("delivers to a handler that returns a truthy non-promise", async () => {
    // `(m) => arr.push(m)` returns the new array length. Treating every truthy
    // return as a promise threw `result.catch is not a function`, which the
    // default onError rethrows asynchronously — an uncaught exception that
    // takes the process down. The declared handler type is
    // `void | Promise<void>`, which TypeScript will not let a number satisfy,
    // so this reaches us from JavaScript callers and through `any`; the cast
    // stands in for both. Every existing test used a block body.
    const fake = subscriberClient();
    const errors: unknown[] = [];
    const hub = createPubSubHub(fake.client, (error) => errors.push(error));
    const delivered: number[] = [];
    const pushing = ((m: { n: number }) =>
      delivered.push(m.n)) as unknown as (message: { n: number }) => void;
    await hub.subscribeChannel(events, pushing);

    fake.emit("events", JSON.stringify({ n: 3 }));
    expect(delivered).toEqual([3]);
    expect(errors).toEqual([]);
  });

  it("still routes a rejected async handler to onError", async () => {
    const fake = subscriberClient();
    const errors: unknown[] = [];
    const hub = createPubSubHub(fake.client, (error) => errors.push(error));
    await hub.subscribeChannel(events, async () => {
      throw new Error("async blew up");
    });

    fake.emit("events", JSON.stringify({ n: 1 }));
    await Promise.resolve();
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("async blew up");
  });

  it("makes a concurrent joiner wait for the first SUBSCRIBE to be acked", async () => {
    // The map entry was published before attach was awaited, so a second
    // concurrent subscriber to the same channel skipped attach and resolved
    // immediately. When the first caller's SUBSCRIBE then failed, it deleted
    // the entry and closed the leased connection — and the joiner was left
    // holding a subscription that looked live, received nothing forever, and
    // whose unsubscribe() silently no-oped.
    const fake = subscriberClient();
    let failSubscribe = true;
    const live = fake.client;
    const client: RedisClient = {
      ...live,
      async subscriber() {
        const subscriber = await live.subscriber?.();
        if (!subscriber) throw new Error("no subscriber");
        const original = subscriber.subscribe.bind(subscriber);
        subscriber.subscribe = async (channel, listener) => {
          if (failSubscribe) {
            failSubscribe = false;
            throw new Error("connection dropped mid-SUBSCRIBE");
          }
          return original(channel, listener);
        };
        return subscriber;
      }
    };
    const hub = createPubSubHub(client, () => {});

    const [first, second] = await Promise.allSettled([
      hub.subscribeChannel(events, () => {}),
      hub.subscribeChannel(events, () => {})
    ]);

    // Both learn about the failure; neither ends up with a dead handle.
    expect(first.status).toBe("rejected");
    expect(second.status).toBe("rejected");
  });

  it("closes the leased connection and drops every subscription on close()", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);
    await hub.subscribeChannel(events, () => {});
    await hub.close();

    expect(fake.log.at(-1)).toBe("close");
    expect(fake.live).toBeNull();
  });
});

describe("pub/sub hub capability guards", () => {
  it("throws TypeError when the client cannot lease a subscriber", async () => {
    const client: RedisClient = {
      async send() {
        return 1 as RedisReply;
      },
      async pipeline() {
        return [];
      },
      async close() {}
    };
    const hub = createPubSubHub(client);

    await expect(hub.subscribeChannel(events, () => {})).rejects.toThrow(
      TypeError
    );
    await expect(hub.subscribeChannel(events, () => {})).rejects.toThrow(
      /HTTP is stateless/
    );
  });

  it("throws TypeError for patterns when the subscriber omits psubscribe", async () => {
    const fake = subscriberClient({ patterns: false });
    const hub = createPubSubHub(fake.client);
    const pattern = definePubSubPattern("events:*", codecs.string());

    await expect(hub.subscribePattern(pattern, () => {})).rejects.toThrow(
      TypeError
    );
    // A guard rejection must not leave an unusable connection leased.
    expect(fake.live).toBeNull();
  });

  it("delivers pattern messages with the matched channel", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);
    const pattern = definePubSubPattern("events:*", codecs.string());
    const seen: Array<[string, string]> = [];

    await hub.subscribePattern(pattern, (message, channel) => {
      seen.push([message, channel]);
    });
    fake.emitPattern("events:*", "hello", "events:created");

    expect(seen).toEqual([["hello", "events:created"]]);
    expect(fake.log).toEqual(["lease", "psubscribe:events:*"]);
  });
});

describe("pub/sub hub streaming", () => {
  it("yields messages and releases the subscription when aborted", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);
    const controller = new AbortController();
    const received: number[] = [];

    const consume = (async () => {
      for await (const message of hub.streamChannel(events, {
        signal: controller.signal
      })) {
        received.push(message.n);
        if (received.length === 2) controller.abort();
      }
    })();

    await vi.waitUntil(() => fake.log.includes("subscribe:events"));
    fake.emit("events", JSON.stringify({ n: 1 }));
    fake.emit("events", JSON.stringify({ n: 2 }));
    await consume;

    expect(received).toEqual([1, 2]);
    expect(fake.log).toEqual([
      "lease",
      "subscribe:events",
      "unsubscribe:events",
      "close"
    ]);
  });

  it("releases the subscription when the consumer breaks early", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);

    const consume = (async () => {
      for await (const message of hub.streamChannel(events)) {
        expect(message.n).toBe(1);
        break;
      }
    })();

    await vi.waitUntil(() => fake.log.includes("subscribe:events"));
    fake.emit("events", JSON.stringify({ n: 1 }));
    await consume;

    expect(fake.log.at(-1)).toBe("close");
  });

  it("yields nothing when the signal is already aborted", async () => {
    const fake = subscriberClient();
    const hub = createPubSubHub(fake.client);
    const received: number[] = [];

    for await (const message of hub.streamChannel(events, {
      signal: AbortSignal.abort()
    })) {
      received.push(message.n);
    }

    expect(received).toEqual([]);
    expect(fake.leases).toBe(0);
  });
});

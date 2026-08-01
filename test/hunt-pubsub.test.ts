import { getEventListeners } from "node:events";
import { describe, expect, it, vi } from "vitest";
import { codecs } from "../src/core/codecs.js";
import {
  createPubSubHub,
  definePubSubChannel,
  definePubSubPattern
} from "../src/core/pubsub.js";
import type {
  RedisClient,
  RedisReply,
  RedisSubscriber
} from "../src/core/types.js";
import { beni } from "../src/index.js";
import { node } from "../src/node/index.js";

/**
 * A subscriber client whose every command can be held open, so a subscribe and
 * an unsubscribe can be forced to overlap deterministically. Each command logs
 * when it is issued and again when the server applies it, which is what makes
 * ordering on the wire observable.
 */
function gatedClient(options: { hangAfterClose?: boolean } = {}) {
  const log: string[] = [];
  const gates = new Map<string, () => void>();
  const waits = new Map<string, Promise<void>>();
  const channelListeners = new Map<string, (message: string) => void>();
  const patternListeners = new Map<
    string,
    (message: string, channel: string) => void
  >();
  let leases = 0;
  let live: RedisSubscriber | null = null;

  const client: RedisClient = {
    async send() {
      return 1 as RedisReply;
    },
    async pipeline() {
      return [];
    },
    async subscriber() {
      leases += 1;
      log.push("lease");
      let closed = false;
      const settle = async (step: string) => {
        log.push(step);
        await waits.get(step);
        // A command still in flight when the connection dies never settles on
        // node-redis, which is what turns a stale in-flight entry into a
        // permanent wedge rather than an error.
        if (closed && options.hangAfterClose) await new Promise(() => {});
        log.push(`${step} ok`);
      };
      const subscriber: RedisSubscriber = {
        async subscribe(channel, listener) {
          await settle(`subscribe:${channel}`);
          channelListeners.set(channel, listener);
        },
        async unsubscribe(channel) {
          await settle(`unsubscribe:${channel}`);
          channelListeners.delete(channel);
        },
        async psubscribe(pattern, listener) {
          await settle(`psubscribe:${pattern}`);
          patternListeners.set(pattern, listener);
        },
        async punsubscribe(pattern) {
          await settle(`punsubscribe:${pattern}`);
          patternListeners.delete(pattern);
        },
        get closed() {
          return closed;
        },
        async close() {
          log.push("close");
          closed = true;
          live = null;
          channelListeners.clear();
          patternListeners.clear();
        }
      };
      live = subscriber;
      return subscriber;
    },
    async close() {}
  };

  return {
    client,
    log,
    get leases() {
      return leases;
    },
    get live() {
      return live;
    },
    hold(step: string) {
      waits.set(
        step,
        new Promise<void>((resolve) => {
          gates.set(step, resolve);
        })
      );
    },
    release(step: string) {
      gates.get(step)?.();
      gates.delete(step);
      waits.delete(step);
    },
    issued(step: string) {
      return log.includes(step);
    },
    emit(channel: string, message: string) {
      channelListeners.get(channel)?.(message);
    },
    emitPattern(pattern: string, message: string, channel: string) {
      patternListeners.get(pattern)?.(message, channel);
    }
  };
}

/** Reports whether a promise settled at all, so a wedge fails as an assertion. */
async function outcome(promise: Promise<unknown>, ms = 100) {
  return Promise.race([
    promise.then(
      () => "resolved" as const,
      () => "rejected" as const
    ),
    new Promise<"hung">((resolve) => {
      setTimeout(() => resolve("hung"), ms);
    })
  ]);
}

const events = definePubSubChannel("events", codecs.json<{ n: number }>());
const other = definePubSubChannel("other", codecs.json<{ n: number }>());

describe("pub/sub hub subscribe/unsubscribe overlap", () => {
  it("keeps the lease while another channel's SUBSCRIBE is still on the wire", async () => {
    // Idleness was measured from the two name maps alone, and a subscribe
    // publishes its entry only once SUBSCRIBE is acked. An unsubscribe that
    // landed in that window saw an empty hub, closed the leased connection,
    // and the subscribe still resolved with a handle that received nothing.
    const fake = gatedClient();
    const hub = createPubSubHub(fake.client, () => {});
    const first = await hub.subscribeChannel(other, () => {});
    const seen: number[] = [];

    fake.hold("subscribe:events");
    const subscribing = hub.subscribeChannel(events, (m) => {
      seen.push(m.n);
    });
    await vi.waitUntil(() => fake.issued("subscribe:events"));
    await first.unsubscribe();

    expect(fake.log).not.toContain("close");
    fake.release("subscribe:events");
    await subscribing;
    fake.emit("events", JSON.stringify({ n: 1 }));
    expect(seen).toEqual([1]);
  });

  it("queues a re-subscribe behind the UNSUBSCRIBE for the same name", async () => {
    // Both commands used to be on the wire at once for the same channel. The
    // server applies the stale UNSUBSCRIBE last (node-redis coalesces the pair
    // away outright), so the new subscription was dead on arrival and every
    // later subscribe joined its poisoned map entry.
    const fake = gatedClient();
    const hub = createPubSubHub(fake.client, () => {});
    const first = await hub.subscribeChannel(events, () => {});
    const seen: number[] = [];

    fake.hold("unsubscribe:events");
    const unsubscribing = first.unsubscribe();
    await vi.waitUntil(() => fake.issued("unsubscribe:events"));
    const resubscribing = hub.subscribeChannel(events, (m) => {
      seen.push(m.n);
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    fake.release("unsubscribe:events");
    await Promise.all([unsubscribing, resubscribing]);

    expect(fake.log).toEqual([
      "lease",
      "subscribe:events",
      "subscribe:events ok",
      "unsubscribe:events",
      "unsubscribe:events ok",
      "subscribe:events",
      "subscribe:events ok"
    ]);
    fake.emit("events", JSON.stringify({ n: 2 }));
    expect(seen).toEqual([2]);
  });

  it("rejects a subscribe whose connection died before the ack", async () => {
    const fake = gatedClient();
    const hub = createPubSubHub(fake.client, () => {});
    fake.hold("subscribe:events");
    const subscribing = hub.subscribeChannel(events, () => {});

    await vi.waitUntil(() => fake.issued("subscribe:events"));
    await fake.live?.close();
    fake.release("subscribe:events");

    await expect(subscribing).rejects.toThrow(/lost before SUBSCRIBE/);
  });
});

describe("pub/sub hub close() during an in-flight subscribe", () => {
  it("settles the subscribe and leaves the channel name usable", async () => {
    // close() tore the connection down under a SUBSCRIBE that had not been
    // acked. The command never settled, so its entry in the in-flight map
    // never cleared, and every later subscribe to that name awaited the same
    // dead promise: a silent hang for the life of the process.
    const fake = gatedClient({ hangAfterClose: true });
    const hub = createPubSubHub(fake.client, () => {});
    // Pre-warm the lease so the race is against the attach, not the lease.
    await hub.subscribeChannel(other, () => {});

    fake.hold("subscribe:events");
    const subscribing = hub.subscribeChannel(events, () => {});
    await vi.waitUntil(() => fake.issued("subscribe:events"));
    await hub.close();
    fake.release("subscribe:events");

    expect(await outcome(subscribing)).toBe("rejected");

    const seen: number[] = [];
    const again = hub.subscribeChannel(events, (m) => {
      seen.push(m.n);
    });
    expect(await outcome(again)).toBe("resolved");
    fake.emit("events", JSON.stringify({ n: 3 }));
    expect(seen).toEqual([3]);
  });

  it("wakes a stream() consumer that has no abort signal", async () => {
    // Only the push callback and the abort listener ever woke a parked
    // iterator, so a signal-less `for await` survived close() forever and its
    // finally, the only thing that releases the subscription, never ran.
    const fake = gatedClient();
    const hub = createPubSubHub(fake.client, () => {});
    const seen: number[] = [];

    const consume = (async () => {
      for await (const message of hub.streamChannel(events)) {
        seen.push(message.n);
      }
    })();
    await vi.waitUntil(() => fake.issued("subscribe:events"));
    fake.emit("events", JSON.stringify({ n: 1 }));
    await vi.waitUntil(() => seen.length === 1);

    await hub.close();

    expect(await outcome(consume)).toBe("resolved");
    expect(seen).toEqual([1]);
  });
});

describe("pub/sub hub channel and pattern sharing one name", () => {
  it("does not let a pattern subscribe join a channel's attach", async () => {
    // The in-flight map was keyed by the bare name, so a pattern whose string
    // matched a channel joined that channel's SUBSCRIBE: PSUBSCRIBE was never
    // issued, the pattern handler saw literal traffic, and its unsubscribe()
    // silently no-oped because `patterns` never got an entry.
    const fake = gatedClient();
    const hub = createPubSubHub(fake.client, () => {});
    const channel = definePubSubChannel("orders:*", codecs.string());
    const pattern = definePubSubPattern("orders:*", codecs.string());
    const literal: string[] = [];
    const matched: Array<[string, string]> = [];

    fake.hold("subscribe:orders:*");
    const subscribingChannel = hub.subscribeChannel(channel, (m) => {
      literal.push(m);
    });
    await vi.waitUntil(() => fake.issued("subscribe:orders:*"));
    const subscribingPattern = hub.subscribePattern(pattern, (m, c) => {
      matched.push([m, c]);
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    fake.release("subscribe:orders:*");
    const [, patterned] = await Promise.all([
      subscribingChannel,
      subscribingPattern
    ]);

    expect(fake.log).toContain("psubscribe:orders:*");
    fake.emit("orders:*", "literal");
    expect(literal).toEqual(["literal"]);
    expect(matched).toEqual([]);
    fake.emitPattern("orders:*", "glob", "orders:new");
    expect(matched).toEqual([["glob", "orders:new"]]);

    await patterned.unsubscribe();
    expect(fake.log).toContain("punsubscribe:orders:*");
  });
});

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("pub/sub overlap against a live server", () => {
  it("survives an unsubscribe overlapping a subscribe to another channel", async () => {
    const client = await node({ url: redisUrl });
    const redis = beni(client);
    const suffix = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
    const first = definePubSubChannel(
      `beni:test:overlap:a:${suffix}`,
      codecs.json<{ n: number }>()
    );
    const second = definePubSubChannel(
      `beni:test:overlap:b:${suffix}`,
      codecs.json<{ n: number }>()
    );
    const seen: number[] = [];

    try {
      const subscription = await redis.pubsub
        .channel(first)
        .subscribe(() => {});
      await Promise.all([
        subscription.unsubscribe(),
        redis.pubsub.channel(second).subscribe((message) => {
          seen.push(message.n);
        })
      ]);

      await expect(
        redis.pubsub.channel(second).publish({ n: 1 })
      ).resolves.toBe(1);
      await vi.waitUntil(() => seen.length === 1);
      expect(seen).toEqual([1]);
    } finally {
      await redis.pubsub.close();
      await client.close();
    }
  });
});

describe("pub/sub hub stream() cleanup", () => {
  it("removes the abort listener when opening the subscription fails", async () => {
    // The listener was registered before the open and removed only by the
    // generator's finally, which a rejecting open never reaches. One
    // long-lived signal plus a retry loop then leaked a listener, and the dead
    // generator's closure with it, on every attempt.
    const client: RedisClient = {
      async send() {
        return 1 as RedisReply;
      },
      async pipeline() {
        return [];
      },
      async close() {}
    };
    const hub = createPubSubHub(client, () => {});
    const controller = new AbortController();

    for (let attempt = 0; attempt < 5; attempt += 1) {
      const consume = (async () => {
        for await (const _ of hub.streamChannel(events, {
          signal: controller.signal
        })) {
          // Unreachable: the adapter cannot hold a subscriber connection.
        }
      })();
      await expect(consume).rejects.toThrow(TypeError);
    }

    expect(getEventListeners(controller.signal, "abort")).toEqual([]);
  });
});

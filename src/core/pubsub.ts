import { replyShapeError } from "./errors.js";
import { expectNumber } from "./helpers.js";
import {
  PUBSUB_HUB_KEY,
  type StoreBinding,
  type StoreContext,
  withStore
} from "./store.js";
import type { Codec, RedisClient, RedisSubscriber } from "./types.js";

export type PubSubChannel<
  TInput,
  TOutput = TInput,
  TName extends string = string
> = {
  readonly kind: "channel";
  readonly name: TName;
  encode(message: TInput): string;
  decode(message: string): TOutput;
};

export type PubSubHandler<TMessage> = (
  message: TMessage
) => void | Promise<void>;

export type PubSubPattern<TOutput, TPattern extends string = string> = {
  readonly kind: "pattern";
  readonly pattern: TPattern;
  decode(message: string): TOutput;
};

export type PubSubPatternHandler<TMessage> = (
  message: TMessage,
  channel: string
) => void | Promise<void>;

export type PubSubSubscription = {
  unsubscribe(): Promise<void>;
};

export type InferPubSubInput<TChannel> =
  TChannel extends PubSubChannel<infer TInput> ? TInput : never;

export function definePubSubChannel<
  TName extends string,
  TInput,
  TOutput = TInput
>(
  name: TName,
  codec: Codec<TInput, TOutput>
): PubSubChannel<TInput, TOutput, TName> {
  const schema: PubSubChannel<TInput, TOutput, TName> = {
    kind: "channel",
    name,
    encode(message) {
      return codec.encode(message);
    },
    decode(message) {
      return codec.decode(message);
    }
  };
  return withStore(schema, channelBinding);
}

export function definePubSubPattern<
  TPattern extends string,
  TInput,
  TOutput = TInput
>(
  pattern: TPattern,
  codec: Codec<TInput, TOutput>
): PubSubPattern<TOutput, TPattern> {
  const schema: PubSubPattern<TOutput, TPattern> = {
    kind: "pattern",
    pattern,
    decode(message) {
      return codec.decode(message);
    }
  };
  return withStore(schema, patternBinding);
}

export type PubSubStreamOptions = {
  /** Abort to end iteration; the subscription is released on the way out. */
  readonly signal?: AbortSignal;
};

export type PubSubPatternMessage<TOutput> = {
  readonly message: TOutput;
  readonly channel: string;
};

type Fan = (message: string, channel: string) => void;

function rethrowAsync(error: unknown): void {
  queueMicrotask(() => {
    throw error;
  });
}

/**
 * Run a subscriber handler so one bad handler cannot stop the fan-out to the
 * others. Failures go to onError; the default rethrows asynchronously so a
 * thrown handler is never silently swallowed.
 */
function runHandler(
  fn: () => void | Promise<void>,
  onError: (error: unknown) => void
): void {
  try {
    // Duck-typed rather than `if (result)`: TypeScript's void-return rule lets
    // a value-returning function satisfy `() => void`, so the common
    // `(m) => arr.push(m)` handler arrives here as a truthy number.
    const result = fn() as unknown;
    if (typeof (result as Promise<void>)?.catch === "function") {
      (result as Promise<void>).catch(onError);
    }
  } catch (error) {
    onError(error);
  }
}

/**
 * Pattern support is optional on the subscriber contract, so resolve it
 * against the connection the attach actually runs on rather than against a
 * subscriber captured earlier, which a teardown may since have replaced.
 */
function patternOps(subscriber: RedisSubscriber) {
  if (!subscriber.psubscribe || !subscriber.punsubscribe) {
    throw new TypeError(
      "Pub/Sub pattern subscribe is not supported by this adapter; subscribe to individual channels instead."
    );
  }
  return {
    psubscribe: subscriber.psubscribe.bind(subscriber),
    punsubscribe: subscriber.punsubscribe.bind(subscriber)
  };
}

async function* iterate<T>(
  open: (push: (value: T) => void) => Promise<PubSubSubscription>,
  live: Set<() => void>,
  signal?: AbortSignal
): AsyncGenerator<T, void, undefined> {
  if (signal?.aborted) return;
  const queue: T[] = [];
  let wake: (() => void) | null = null;
  let done = false;
  let subscription: PubSubSubscription | null = null;
  const stop = () => {
    done = true;
    wake?.();
  };
  // Both registrations happen before the open and are undone by the finally
  // below: an open that rejects used to escape before the try was entered,
  // leaving the abort listener — and the whole dead generator closure with
  // it — attached to a long-lived signal for good.
  signal?.addEventListener("abort", stop, { once: true });
  live.add(stop);
  try {
    subscription = await open((value) => {
      queue.push(value);
      wake?.();
    });
    while (true) {
      if (queue.length > 0) {
        yield queue.shift() as T;
        continue;
      }
      if (done) return;
      await new Promise<void>((resolve) => {
        wake = () => {
          wake = null;
          resolve();
        };
      });
    }
  } finally {
    signal?.removeEventListener("abort", stop);
    live.delete(stop);
    await subscription?.unsubscribe();
  }
}

/**
 * Owns the one subscriber connection a client is allowed to lease. Channels and
 * patterns are multiplexed onto it: core registers a single adapter listener
 * per name and fans out to its own handlers, ref-counted, so N subscriptions
 * cost one connection. The lease is taken on first subscribe and closed when
 * the last subscription goes away, which keeps "no idle connections" true
 * without asking callers to manage a second object.
 */
export function createPubSubHub(
  client: RedisClient,
  onError: (error: unknown) => void = rethrowAsync
) {
  const channels = new Map<string, Set<Fan>>();
  const patterns = new Map<string, Set<Fan>>();
  /** In-flight attaches, keyed by kind and name; see `add`. */
  const attaching = new Map<string, Promise<Set<Fan>>>();
  /** In-flight teardowns, same keys: an attach queues behind one. */
  const detaching = new Map<string, Promise<void>>();
  /** Live stream() consumers, so close() can wake them; see `iterate`. */
  const streaming = new Set<() => void>();
  let leased: RedisSubscriber | null = null;
  let leasing: Promise<RedisSubscriber> | null = null;
  /** Subscribes that have not published their map entry yet. */
  let subscribing = 0;
  /** Bumped by close(); a subscribe from an older epoch has lost its hub. */
  let epoch = 0;

  /** Throws when close() ran since `era` was captured. */
  function assertLive(era: number): void {
    if (era !== epoch) {
      throw new Error(
        "The pub/sub hub was closed while this subscription was still being established."
      );
    }
  }

  async function acquire(): Promise<RedisSubscriber> {
    if (leased && !leased.closed) return leased;
    if (leasing) return leasing;
    if (!client.subscriber) {
      throw new TypeError(
        "Pub/Sub subscribe requires a client that can hold a connection; this adapter provides none (HTTP is stateless). Publishing still works — subscribe through beni/node or beni/bun."
      );
    }
    leasing = client
      .subscriber()
      .then((subscriber) => {
        leased = subscriber;
        leasing = null;
        return subscriber;
      })
      .catch((error) => {
        leasing = null;
        throw error;
      });
    return leasing;
  }

  async function releaseIfIdle(): Promise<void> {
    // A subscribe that has not published its map entry yet still owns the
    // lease. Measuring idleness from the maps alone let any overlapping
    // unsubscribe close the connection under an in-flight SUBSCRIBE, and the
    // caller was handed a subscription that received nothing forever.
    if (subscribing > 0 || channels.size > 0 || patterns.size > 0) return;
    const subscriber = leased;
    leased = null;
    if (subscriber && !subscriber.closed) await subscriber.close();
  }

  function deliver(set: Set<Fan>, message: string, channel: string): void {
    for (const fan of [...set]) fan(message, channel);
  }

  async function add(
    kind: "channel" | "pattern",
    name: string,
    fan: Fan,
    attach: (subscriber: RedisSubscriber, set: Set<Fan>) => Promise<void>,
    detach: (subscriber: RedisSubscriber) => Promise<void>
  ): Promise<PubSubSubscription> {
    const map = kind === "channel" ? channels : patterns;
    // The kind is part of the key: a channel and a pattern that spell the
    // same string are different subscriptions, and sharing one in-flight
    // attach between them wired the pattern handler to literal traffic with
    // no PSUBSCRIBE ever issued and no way to unsubscribe it again.
    const key = `${kind}:${name}`;
    const era = epoch;
    subscribing += 1;
    try {
      const subscriber = await acquire();
      assertLive(era);
      let set = map.get(name);
      if (!set) {
        // Join an attach already in flight instead of treating a published map
        // entry as proof that SUBSCRIBE was acknowledged. Publishing the set
        // before awaiting attach let a second concurrent caller resolve against
        // an unacked subscription: if the first caller's SUBSCRIBE then failed,
        // it tore the entry and the leased connection down, and the second held
        // a subscription that looked live, received nothing forever, and whose
        // unsubscribe() silently no-oped. Mirrors how `leasing` guards acquire().
        const pending = attaching.get(key);
        if (pending) {
          set = await pending;
        } else {
          const fresh = new Set<Fan>();
          let inFlight: Promise<Set<Fan>> | null = null;
          inFlight = (async () => {
            try {
              // A teardown for this same name may still be on the wire.
              // Issuing SUBSCRIBE over it lets the server apply the stale
              // UNSUBSCRIBE last, and some clients coalesce the pair away
              // entirely: either way the caller ends up attached to nothing.
              await detaching.get(key)?.catch(() => undefined);
              await attach(subscriber, fresh);
              assertLive(era);
              if (subscriber.closed) {
                throw new Error(
                  "The pub/sub connection was lost before SUBSCRIBE was acknowledged."
                );
              }
              map.set(name, fresh);
              return fresh;
            } finally {
              // close() evicts in-flight attaches, so only ever delete the
              // entry this attach still owns.
              if (attaching.get(key) === inFlight) attaching.delete(key);
            }
          })();
          attaching.set(key, inFlight);
          set = await inFlight;
        }
      }
      set.add(fan);
      let active = true;
      return {
        async unsubscribe() {
          if (!active) return;
          active = false;
          const current = map.get(name);
          if (!current) return;
          current.delete(fan);
          if (current.size > 0) return;
          map.delete(name);
          // Publish the teardown before awaiting it so a subscribe for the
          // same name queues behind it instead of racing it on the wire.
          const teardown = (async () => {
            try {
              if (leased && !leased.closed) await detach(leased);
            } finally {
              // The lease has to be released even when UNSUBSCRIBE rejects.
              // On a connection that has already died the detach always
              // rejects, and letting that skip the release left the dead
              // subscriber cached as the hub's lease.
              await releaseIfIdle();
            }
          })();
          detaching.set(key, teardown);
          try {
            await teardown;
          } finally {
            if (detaching.get(key) === teardown) detaching.delete(key);
          }
        }
      };
    } finally {
      subscribing -= 1;
      // Whether we attached or threw: an unsubscribe that overlapped us left
      // the lease alone because we held it, and a failed attach may have been
      // the only thing keeping it open.
      await releaseIfIdle();
    }
  }

  function subscribeChannel<TOutput>(
    channel: PubSubChannel<any, TOutput>,
    handler: PubSubHandler<TOutput>
  ): Promise<PubSubSubscription> {
    const fan: Fan = (message) =>
      runHandler(() => handler(channel.decode(message)), onError);
    return add(
      "channel",
      channel.name,
      fan,
      (subscriber, set) =>
        subscriber.subscribe(channel.name, (message) =>
          deliver(set, message, channel.name)
        ),
      (subscriber) => subscriber.unsubscribe(channel.name)
    );
  }

  function subscribePattern<TOutput>(
    pattern: PubSubPattern<TOutput>,
    handler: PubSubPatternHandler<TOutput>
  ): Promise<PubSubSubscription> {
    const fan: Fan = (message, channel) =>
      runHandler(() => handler(pattern.decode(message), channel), onError);
    return add(
      "pattern",
      pattern.pattern,
      fan,
      (subscriber, set) =>
        patternOps(subscriber).psubscribe(pattern.pattern, (message, channel) =>
          deliver(set, message, channel)
        ),
      (subscriber) => patternOps(subscriber).punsubscribe(pattern.pattern)
    );
  }

  return {
    subscribeChannel,
    subscribePattern,
    streamChannel<TOutput>(
      channel: PubSubChannel<any, TOutput>,
      options: PubSubStreamOptions = {}
    ): AsyncGenerator<TOutput, void, undefined> {
      return iterate<TOutput>(
        (push) => subscribeChannel(channel, (message) => push(message)),
        streaming,
        options.signal
      );
    },
    streamPattern<TOutput>(
      pattern: PubSubPattern<TOutput>,
      options: PubSubStreamOptions = {}
    ): AsyncGenerator<PubSubPatternMessage<TOutput>, void, undefined> {
      return iterate<PubSubPatternMessage<TOutput>>(
        (push) =>
          subscribePattern(pattern, (message, channel) =>
            push({ message, channel })
          ),
        streaming,
        options.signal
      );
    },
    /** Drop every subscription and close the leased connection. */
    async close(): Promise<void> {
      epoch += 1;
      // A parked stream() consumer is woken by its own signal or by us and by
      // nothing else, so without this its `for await` never returns and the
      // generator's finally, the only thing that releases its subscription,
      // never runs.
      for (const stop of [...streaming]) stop();
      channels.clear();
      patterns.clear();
      // In-flight attaches are evicted rather than awaited: the epoch check
      // stops them publishing, and an entry left behind here would wedge
      // every later subscribe to that name on a promise this hub no longer
      // owns and may never see settle.
      attaching.clear();
      // A lease still being established is not in `leased` yet, so
      // releaseIfIdle would find nothing to close and the connection would
      // outlive close() — a leak for anyone who closes while the very first
      // subscribe is still in flight.
      if (leasing) await leasing.catch(() => undefined);
      await releaseIfIdle();
    }
  };
}

export type PubSubHub = ReturnType<typeof createPubSubHub>;

export function createPubSubPublisher(client: RedisClient) {
  return {
    async publish<TChannel extends PubSubChannel<any>>(
      channel: TChannel,
      message: InferPubSubInput<TChannel>
    ): Promise<number> {
      return expectNumber(
        await client.send(["PUBLISH", channel.name, channel.encode(message)]),
        "PUBLISH"
      );
    }
  };
}

/**
 * The per-handle pub/sub hub, created on first use — so that
 * `createPubSubHub` is named only by this module, and an app that declares no
 * channel or pattern never pulls the hub in.
 */
export function hubFor(ctx: StoreContext): PubSubHub {
  return ctx.shared(PUBSUB_HUB_KEY, () =>
    createPubSubHub(ctx.client, ctx.onPubSubError)
  );
}

/**
 * A pub/sub channel resource: publish through the adapter (or the raw client
 * when no adapter is configured) and subscribe through the adapter's dedicated
 * subscriber connection.
 */
export function createChannelResource<TInput, TOutput>(
  ctx: StoreContext,
  channel: PubSubChannel<TInput, TOutput>
) {
  const { client } = ctx;
  return {
    /** PUBLISH — a stateless command, so it rides the bound client. */
    publish(message: TInput): Promise<number> {
      return client
        .send(["PUBLISH", channel.name, channel.encode(message)])
        .then((reply) => {
          if (typeof reply !== "number") {
            throw replyShapeError("PUBLISH", "number", reply);
          }
          return reply;
        });
    },
    subscribe(handler: (message: TOutput) => void | Promise<void>) {
      return hubFor(ctx).subscribeChannel(channel, handler);
    },
    /** Consume as an async iterable; abort the signal to stop and release. */
    stream(options?: PubSubStreamOptions) {
      return hubFor(ctx).streamChannel(channel, options);
    }
  };
}

/** A pub/sub pattern resource: pattern-subscribe over the leased connection. */
export function createPatternResource<TOutput>(
  ctx: StoreContext,
  pattern: PubSubPattern<TOutput>
) {
  return {
    subscribe(
      handler: (message: TOutput, channel: string) => void | Promise<void>
    ) {
      return hubFor(ctx).subscribePattern(pattern, handler);
    },
    stream(options?: PubSubStreamOptions) {
      return hubFor(ctx).streamPattern(pattern, options);
    }
  };
}

const channelBinding: StoreBinding = { resource: createChannelResource };
const patternBinding: StoreBinding = { resource: createPatternResource };

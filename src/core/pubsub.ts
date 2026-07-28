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

async function* iterate<T>(
  open: (push: (value: T) => void) => Promise<PubSubSubscription>,
  signal?: AbortSignal
): AsyncGenerator<T, void, undefined> {
  if (signal?.aborted) return;
  const queue: T[] = [];
  let wake: (() => void) | null = null;
  let done = false;
  const stop = () => {
    done = true;
    wake?.();
  };
  signal?.addEventListener("abort", stop, { once: true });
  const subscription = await open((value) => {
    queue.push(value);
    wake?.();
  });
  try {
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
    await subscription.unsubscribe();
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
  /** In-flight attaches, keyed by channel/pattern name; see `add`. */
  const attaching = new Map<string, Promise<Set<Fan>>>();
  let leased: RedisSubscriber | null = null;
  let leasing: Promise<RedisSubscriber> | null = null;

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
    if (channels.size > 0 || patterns.size > 0) return;
    const subscriber = leased;
    leased = null;
    if (subscriber && !subscriber.closed) await subscriber.close();
  }

  function deliver(set: Set<Fan>, message: string, channel: string): void {
    for (const fan of [...set]) fan(message, channel);
  }

  async function add(
    map: Map<string, Set<Fan>>,
    name: string,
    fan: Fan,
    attach: (subscriber: RedisSubscriber, set: Set<Fan>) => Promise<void>,
    detach: (subscriber: RedisSubscriber) => Promise<void>
  ): Promise<PubSubSubscription> {
    const subscriber = await acquire();
    let set = map.get(name);
    if (!set) {
      // Join an attach already in flight instead of treating a published map
      // entry as proof that SUBSCRIBE was acknowledged. Publishing the set
      // before awaiting attach let a second concurrent caller resolve against
      // an unacked subscription: if the first caller's SUBSCRIBE then failed,
      // it tore the entry and the leased connection down, and the second held
      // a subscription that looked live, received nothing forever, and whose
      // unsubscribe() silently no-oped. Mirrors how `leasing` guards acquire().
      const pending = attaching.get(name);
      if (pending) {
        set = await pending;
      } else {
        const fresh = new Set<Fan>();
        const inFlight = (async () => {
          try {
            await attach(subscriber, fresh);
            map.set(name, fresh);
            return fresh;
          } catch (error) {
            await releaseIfIdle();
            throw error;
          } finally {
            attaching.delete(name);
          }
        })();
        attaching.set(name, inFlight);
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
        if (leased && !leased.closed) await detach(leased);
        await releaseIfIdle();
      }
    };
  }

  function subscribeChannel<TOutput>(
    channel: PubSubChannel<any, TOutput>,
    handler: PubSubHandler<TOutput>
  ): Promise<PubSubSubscription> {
    const fan: Fan = (message) =>
      runHandler(() => handler(channel.decode(message)), onError);
    return add(
      channels,
      channel.name,
      fan,
      (subscriber, set) =>
        subscriber.subscribe(channel.name, (message) =>
          deliver(set, message, channel.name)
        ),
      (subscriber) => subscriber.unsubscribe(channel.name)
    );
  }

  async function subscribePattern<TOutput>(
    pattern: PubSubPattern<TOutput>,
    handler: PubSubPatternHandler<TOutput>
  ): Promise<PubSubSubscription> {
    const subscriber = await acquire();
    if (!subscriber.psubscribe || !subscriber.punsubscribe) {
      await releaseIfIdle();
      throw new TypeError(
        "Pub/Sub pattern subscribe is not supported by this adapter; subscribe to individual channels instead."
      );
    }
    const psubscribe = subscriber.psubscribe.bind(subscriber);
    const punsubscribe = subscriber.punsubscribe.bind(subscriber);
    const fan: Fan = (message, channel) =>
      runHandler(() => handler(pattern.decode(message), channel), onError);
    return add(
      patterns,
      pattern.pattern,
      fan,
      (_, set) =>
        psubscribe(pattern.pattern, (message, channel) =>
          deliver(set, message, channel)
        ),
      () => punsubscribe(pattern.pattern)
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
        options.signal
      );
    },
    /** Drop every subscription and close the leased connection. */
    async close(): Promise<void> {
      channels.clear();
      patterns.clear();
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

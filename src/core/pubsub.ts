import { SUBSCRIBER_UNSUPPORTED } from "./client-source.js";
import { replyShapeError, UnsupportedCapabilityError } from "./errors.js";
import { expectNumber } from "./helpers.js";
import { type KeyOptions, keyBuilder } from "./keys.js";
import {
  PUBSUB_HUB_KEY,
  type StoreBinding,
  type StoreContext,
  withStore
} from "./store.js";
import type {
  Codec,
  RedisClient,
  RedisKeyPart,
  RedisSubscriber
} from "./types.js";

/**
 * The concrete channel a schema builds for an id: the schema's name, then the
 * id, joined the way every keyspace joins a prefix and an id. Spelled as its
 * own alias so `events:room:42` shows up in hovers rather than `string`.
 */
export type ChannelName<
  TName extends string,
  TId extends RedisKeyPart
> = `${TName}:${TId}`;

/**
 * A pub/sub channel schema.
 *
 * `name` is the channel on its own — `channel("events:user", …)` publishes to
 * `events:user` and nothing else. Pass an id to get the per-entity channel
 * `name:id` (`events:room:42`), which is the same derivation, and therefore the
 * same string, a keyspace with that prefix would build for that id.
 */
export type PubSubChannel<
  TInput,
  TOutput = TInput,
  TName extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
> = {
  readonly kind: "channel";
  readonly name: TName;
  /** The channel itself, with no id: exactly `name`. */
  channelName(): TName;
  /** The per-entity channel for `id`: `name:id`. */
  channelName<TActualId extends TId>(
    id: TActualId
  ): ChannelName<TName, TActualId>;
  encode(message: TInput): string;
  decode(message: string): TOutput;
};

/**
 * The options bag `channel()` accepts: known ids, for autocomplete and a
 * narrowed id parameter, exactly as a keyspace takes them.
 *
 * There is deliberately no `hashTag`. A channel is not a key: plain Pub/Sub is
 * broadcast across a cluster rather than routed by slot, so a hash tag would
 * only add braces to the channel name and buy nothing.
 */
export type PubSubChannelOptions<
  TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
> = Pick<KeyOptions<TIds>, "ids">;

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
  TChannel extends PubSubChannel<infer TInput, any, string, any>
    ? TInput
    : never;

/** The id type a channel schema accepts, narrowed by its `ids` option. */
export type InferPubSubId<TChannel> =
  TChannel extends PubSubChannel<any, any, string, infer TId> ? TId : never;

/**
 * The one channel-schema constructor, shared by `channel()` and by the
 * id-scoped channels the resource derives. The name is fixed here and the id
 * derivation is baked into the closure, the way `keyBuilder` bakes a keyspace's
 * layout in at definition time.
 */
function buildChannel<
  TInput,
  TOutput,
  TName extends string,
  TId extends RedisKeyPart
>(
  name: TName,
  codec: Codec<TInput, TOutput>
): PubSubChannel<TInput, TOutput, TName, TId> {
  // The keyspace builder in its default (untagged) layout, rather than a local
  // template string: `events:room` plus 42 has to become `events:room:42` by
  // exactly the rule `kv("events:room").key(42)` follows, or a
  // `pattern("events:room:*")` subscriber would stop matching what this
  // publishes the moment the two derivations drifted.
  const build = keyBuilder(name, undefined);
  // channelName is one implementation behind two overloads, so cast the
  // literal — the same shape every keyed schema factory uses.
  const schema = {
    kind: "channel",
    name,
    channelName(id?: RedisKeyPart) {
      return id === undefined ? name : build(id);
    },
    encode(message: TInput) {
      return codec.encode(message);
    },
    decode(message: string) {
      return codec.decode(message);
    }
  } as PubSubChannel<TInput, TOutput, TName, TId>;
  return schema;
}

export function definePubSubChannel<
  TName extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  name: TName,
  codec: Codec<TInput, TOutput>,
  // Read for its type and never for its value: `ids` narrows the id parameter
  // and nothing about it is needed at runtime, exactly as with a keyspace,
  // whose builder also only ever looks at `hashTag`.
  options?: PubSubChannelOptions<TIds>
): PubSubChannel<TInput, TOutput, TName, TIds[number]> {
  void options;
  return withStore(
    buildChannel<TInput, TOutput, TName, TIds[number]>(name, codec),
    channelBinding
  );
}

/**
 * The channel `name:id`, as a schema in its own right: same codec, same id
 * type, so `.at()` can nest and the hub can treat it like any other channel.
 *
 * The parent doubles as the codec, so a scoped channel cannot drift from the
 * schema it came from.
 */
function scopedChannel<
  TInput,
  TOutput,
  TName extends string,
  TId extends RedisKeyPart,
  TActualId extends TId
>(
  channel: PubSubChannel<TInput, TOutput, TName, TId>,
  id: TActualId
): PubSubChannel<TInput, TOutput, ChannelName<TName, TActualId>, TId> {
  return buildChannel<TInput, TOutput, ChannelName<TName, TActualId>, TId>(
    channel.channelName(id),
    channel
  );
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
      // Same class the lazy facade throws, so the capability error is one type
      // whether the client arrived connected or behind a promise or factory.
      throw new UnsupportedCapabilityError(
        SUBSCRIBER_UNSUPPORTED,
        "subscriber"
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

/**
 * The channel a call actually addresses: the schema's own name when no id is
 * given, the per-entity channel when one is. One helper, so publish and
 * subscribe can never disagree about where an id lands.
 */
function resolveChannelName(
  channel: PubSubChannel<any, any, string, any>,
  id: RedisKeyPart | undefined
): string {
  return id === undefined ? channel.name : channel.channelName(id);
}

export function createPubSubPublisher(client: RedisClient) {
  return {
    async publish<TChannel extends PubSubChannel<any, any, string, any>>(
      channel: TChannel,
      message: InferPubSubInput<TChannel>,
      /** Publish to the per-entity channel `name:id` instead of `name`. */
      id?: InferPubSubId<TChannel>
    ): Promise<number> {
      return expectNumber(
        await client.send([
          "PUBLISH",
          resolveChannelName(channel, id),
          channel.encode(message)
        ]),
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
 * A pub/sub channel resource, bound to one concrete channel: publish through
 * the bound client and subscribe through the leased subscriber connection.
 *
 * Spelled out as a named type rather than inferred, because `at()` returns one
 * of these and TypeScript cannot infer a type that refers to itself.
 */
export type PubSubChannelResource<
  TInput,
  TOutput,
  TName extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
> = {
  /** PUBLISH — a stateless command, so it rides the bound client. */
  publish(message: TInput): Promise<number>;
  subscribe(
    handler: (message: TOutput) => void | Promise<void>
  ): Promise<PubSubSubscription>;
  /** Consume as an async iterable; abort the signal to stop and release. */
  stream(
    options?: PubSubStreamOptions
  ): AsyncGenerator<TOutput, void, undefined>;
  /** This resource's own channel, with no id: `name`. */
  channelName(): TName;
  /** The per-entity channel for `id`: `name:id`. */
  channelName<TActualId extends TId>(
    id: TActualId
  ): ChannelName<TName, TActualId>;
  /**
   * The same resource for the per-entity channel `name:id` — one channel per
   * room, per user, per job.
   * @example redis.query.roomEvents.at(42).publish({ text: "hi" })
   */
  at<TActualId extends TId>(
    id: TActualId
  ): PubSubChannelResource<TInput, TOutput, ChannelName<TName, TActualId>, TId>;
};

/**
 * A pub/sub channel resource: publish through the adapter (or the raw client
 * when no adapter is configured) and subscribe through the adapter's dedicated
 * subscriber connection.
 */
export function createChannelResource<
  TInput,
  TOutput,
  TName extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
>(
  ctx: StoreContext,
  channel: PubSubChannel<TInput, TOutput, TName, TId>
): PubSubChannelResource<TInput, TOutput, TName, TId> {
  const { client } = ctx;
  return {
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
    stream(options?: PubSubStreamOptions) {
      return hubFor(ctx).streamChannel(channel, options);
    },
    // Carried the way every keyspace resource carries `key()`, so a caller who
    // reached this through `redis.query.<name>` can resolve a concrete channel
    // without holding the schema.
    channelName: channel.channelName.bind(channel) as PubSubChannel<
      TInput,
      TOutput,
      TName,
      TId
    >["channelName"],
    at<TActualId extends TId>(id: TActualId) {
      // A derived schema rather than an id threaded through publish/subscribe:
      // the hub keys its multiplexing on `channel.name`, so handing it the
      // resolved channel is what makes two `.at(42)` resources share one
      // SUBSCRIBE and makes an id-scoped unsubscribe find its own entry.
      return createChannelResource<
        TInput,
        TOutput,
        ChannelName<TName, TActualId>,
        TId
      >(ctx, scopedChannel(channel, id));
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

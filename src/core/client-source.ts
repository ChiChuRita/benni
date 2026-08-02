import type { RedisClient } from "./types.js";

/**
 * Anything that carries a bound client on `raw` — in practice the handle
 * `benni()` returns. Accepting it is what lets `cache({ client: redis })` work
 * instead of forcing `cache(redis.raw, …)` on callers who already hold a handle
 * and would otherwise have to thread two objects through their app.
 */
export type ClientProvider = { readonly raw: RedisClient };

/**
 * A client, however you have it: connected, still connecting, or not yet
 * created. Every entry point that needs a client takes this, so an adapter's
 * promise never has to be awaited at module scope:
 *
 * ```ts
 * export const redis = benni({ client: node({ url }), schema });
 * ```
 *
 * The two lazy forms are not the same kind of lazy, and the difference is what
 * `close()` and error reporting hang on:
 *
 * - A **promise** is already in flight by the time it is passed here, so it is
 *   adopted at bind time. Its rejection is observed immediately (an unobserved
 *   one is a process-killing `unhandledRejection`, not an error the first
 *   command could report), and a client it opens is tracked so `close()` can
 *   close it even if no command was ever sent. A settled rejection cannot be
 *   retried, so every command reports that same connect failure.
 * - A **factory** is not called until the first command. Nothing is opened, so
 *   `close()` on an unused client opens nothing, and a failed connect is
 *   genuinely retried on the next command.
 *
 * Either way a connection failure surfaces at the first command rather than at
 * construction, the same trade `benni/hono` and `benni/next` already make.
 */
export type ClientSource =
  | RedisClient
  | ClientProvider
  | Promise<RedisClient | ClientProvider>
  | (() =>
      | RedisClient
      | ClientProvider
      | Promise<RedisClient | ClientProvider>);

/**
 * The capability messages a lazily resolved client has to raise itself.
 *
 * A resolved client advertises the optional parts of the contract by having
 * `transaction`/`session`/`subscriber` defined, and every caller guards on that
 * before calling. A facade over an unresolved source cannot know yet, so it
 * defines all three and raises the guard's own message from inside the call
 * instead. The strings therefore have to be the ones the guards use, which is
 * why they live here — the one module both sides can import without pulling
 * anything else in (`core/pubsub.ts` imports the subscriber message from here
 * rather than the reverse, which would pin the whole Pub/Sub hub into every
 * bundle that binds a client).
 */
export const SESSION_UNSUPPORTED = "Redis client does not support sessions";
export const TRANSACTION_UNSUPPORTED =
  "Redis client does not support transactions";
export const SUBSCRIBER_UNSUPPORTED =
  "Pub/Sub subscribe requires a client that can hold a connection; this adapter provides none (HTTP is stateless). Publishing still works — subscribe through benni/node or benni/bun.";

function isClient(value: object): value is RedisClient {
  return typeof (value as RedisClient).send === "function";
}

function isProvider(value: object): value is ClientProvider {
  const raw = (value as Partial<ClientProvider>).raw;
  return typeof raw === "object" && raw !== null && isClient(raw);
}

function unwrap(resolved: RedisClient | ClientProvider): RedisClient {
  if (isClient(resolved)) return resolved;
  if (isProvider(resolved)) return resolved.raw;
  throw new TypeError(
    "Redis client source resolved to something that is neither a client (no send()) nor a benni handle (no raw client)."
  );
}

type Resolver = {
  /** Resolve on first call, then hand back the same client. */
  get(): Promise<RedisClient>;
  /** The in-flight or settled resolution, or undefined if none was started. */
  peek(): Promise<RedisClient> | undefined;
};

function resolveOnce(
  source: Exclude<ClientSource, RedisClient | ClientProvider>
): Resolver {
  let cached: Promise<RedisClient> | undefined;
  return {
    get() {
      if (!cached) {
        cached = Promise.resolve(
          typeof source === "function" ? source() : source
        ).then(unwrap);
        // Two jobs, and the catch is load-bearing for both. It keeps the
        // rejection observed, so a connect that fails before any command is
        // reported through a command rather than crashing the process with an
        // unhandledRejection. And it drops the failed resolution, so a factory
        // is called again on the next command instead of being poisoned by one
        // bad connect. (A promise re-derives from the same settled rejection
        // and so reports that same failure every time, which is the most a
        // caller who handed over an already-failed promise can be given.)
        cached.catch(() => {
          cached = undefined;
        });
      }
      return cached;
    },
    peek() {
      return cached;
    }
  };
}

/**
 * A `RedisClient` over a source that is not a client yet. Every method resolves
 * first, so the handle can be built (and its options validated) synchronously
 * while the connection opens on first use.
 */
function lazyClient(resolver: Resolver): RedisClient {
  return {
    async send(command) {
      return (await resolver.get()).send(command);
    },
    async pipeline(commands) {
      return (await resolver.get()).pipeline(commands);
    },
    async transaction(commands) {
      const client = await resolver.get();
      if (client.transaction === undefined) {
        throw new TypeError(TRANSACTION_UNSUPPORTED);
      }
      return client.transaction(commands);
    },
    async session() {
      const client = await resolver.get();
      if (client.session === undefined) {
        throw new TypeError(SESSION_UNSUPPORTED);
      }
      return client.session();
    },
    async subscriber() {
      const client = await resolver.get();
      if (client.subscriber === undefined) {
        throw new TypeError(SUBSCRIBER_UNSUPPORTED);
      }
      return client.subscriber();
    },
    async close() {
      // Closing a client that was never used must not open one, which is why
      // this peeks rather than resolves: an unused factory stays uncalled. A
      // promise was adopted at bind time, so it is peekable here and the
      // client it opened gets closed even though no command was ever sent.
      // A resolution that failed left nothing to close, so its rejection is
      // not an error here either.
      const pending = resolver.peek();
      if (pending === undefined) return;
      const client = await pending.catch(() => undefined);
      await client?.close();
    }
  };
}

/**
 * Narrow a {@link ClientSource} to the `RedisClient` the internals speak.
 *
 * A client (or a handle carrying one) is returned as-is, so the common path
 * adds no wrapper and no indirection, and `redis.raw === client` still holds.
 * Only a promise or factory gets the lazy facade.
 */
export function resolveClient(source: ClientSource): RedisClient {
  if (typeof source === "function") return lazyClient(resolveOnce(source));
  if (typeof source !== "object" || source === null) {
    throw new TypeError(
      "Expected a Redis client, a promise of one, a factory, or a benni handle."
    );
  }
  if (typeof (source as PromiseLike<unknown>).then === "function") {
    const resolver = resolveOnce(source as Promise<RedisClient>);
    // Adopt it now. The caller's `node({ url })` is already connecting, so
    // there is nothing lazy left to preserve, and waiting for the first
    // command to look at it costs two things: an `unhandledRejection` kills
    // the process before any command can report the failure, and a client
    // that connected fine is invisible to `close()` until someone sends. A
    // factory, which has started nothing, is deliberately not touched here.
    void resolver.get();
    return lazyClient(resolver);
  }
  return unwrap(source as RedisClient | ClientProvider);
}

/**
 * Accept either call shape — `f({ client, …options })` or the older
 * `f(client, options)` — and hand back what the implementation needs.
 *
 * The config form is recognized by having a `client` property and no `send`,
 * which no client, promise, factory, or benni handle has. The config object is
 * passed straight through as the options bag: every option is read by name, so
 * the extra `client` key is inert.
 */
export function clientArgs<TOptions extends object>(
  source: ClientSource | (TOptions & { readonly client: ClientSource }),
  options?: TOptions
): { client: RedisClient; options: TOptions } {
  if (
    typeof source === "object" &&
    source !== null &&
    !isClient(source) &&
    "client" in source
  ) {
    const config = source as TOptions & { readonly client: ClientSource };
    return { client: resolveClient(config.client), options: config };
  }
  return {
    client: resolveClient(source as ClientSource),
    options: (options ?? ({} as TOptions)) as TOptions
  };
}

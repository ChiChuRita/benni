import { Buffer } from "node:buffer";
import type { RedisArgument } from "redis";
import { createClient, ErrorReply, MultiErrorReply, WatchError } from "redis";
import {
  type ConnectionEvents,
  lazyConnection,
  reporter
} from "../core/connection.js";
import { redisServerError } from "../core/errors.js";
import type {
  FullRedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisPatternSubscriber,
  RedisReply,
  RedisSession
} from "../core/index.js";

/** node-redis client options, plus the {@link ConnectionEvents} hooks. */
export type NodeOptions = NonNullable<Parameters<typeof createClient>[0]> &
  ConnectionEvents;

type SocketOptions = NonNullable<NonNullable<NodeOptions>["socket"]>;
type ReconnectStrategy = NonNullable<
  { reconnectStrategy?: unknown } & SocketOptions
>["reconnectStrategy"];

/**
 * The connection's own reconnect strategy while it is up, and none while it is
 * being dialed.
 *
 * node-redis uses one strategy for both, and by default it retries a first
 * connect forever while commands wait in the offline queue, so a process that
 * started while Redis was down hung every request instead of failing it. While
 * `dialing()` holds, this refuses, the connect rejects, and {@link
 * lazyConnection} decides when to try again. A drop after the connection was
 * ready still reconnects in the background the way node-redis always has.
 */
function unlessDialing(
  strategy: ReconnectStrategy,
  dialing: () => boolean
): (retries: number, cause: Error) => false | Error | number {
  return (retries, cause) => {
    if (dialing()) return false;
    if (strategy === undefined) return defaultReconnectDelay(retries, cause);
    if (strategy === false || typeof strategy === "number") return strategy;
    return (
      strategy as (retries: number, cause: Error) => false | Error | number
    )(retries, cause);
  };
}

/**
 * node-redis 6's own default, restated because a custom strategy replaces it
 * and the library does not export it: no reconnect after a socket timeout,
 * otherwise exponential backoff from 50 ms capped at 2 s, plus up to 200 ms of
 * jitter.
 */
function defaultReconnectDelay(retries: number, cause: Error): false | number {
  if (cause.name === "SocketTimeoutError") return false;
  return Math.min(2 ** retries * 50, 2000) + Math.floor(Math.random() * 200);
}

// node-redis decodes RESP3 map replies (HGETALL, CONFIG GET, ...) as plain
// objects, which fall outside the RedisReply union the typed stores validate
// against, and returns doubles as numbers where RESP2 gives strings. So the
// typed API needs RESP2, and this pins it.
//
// Passing `RESP: 3` yourself still reaches node-redis, but it is not a
// supported configuration: hash reads throw ReplyShapeError and ZSCORE
// changes type under you. Use it only with `client.send()` directly.
function withReplyDefaults(options?: NodeOptions): NodeOptions {
  // Resolved, not spread over. `{ RESP: 2, ...options }` looks equivalent but
  // an options object carrying an explicit `RESP: undefined` — the ordinary
  // result of forwarding an optional config field — overwrites the default,
  // and node-redis resolves it with `?? DEFAULT_RESP`, which is 3. HGETALL
  // then arrives as a plain object and every hash read throws.
  const merged = { ...options } as NodeOptions & { RESP?: 2 | 3 };
  if (merged.RESP === undefined) merged.RESP = 2;
  return merged;
}

/**
 * node-redis rejects a committed `MULTI` with a `MultiErrorReply`, an
 * aggregate whose message is only "N commands failed, see .replies and
 * .errorIndexes". Surface the first real error instead, so a per-command
 * failure reads the same here as it does through `benni/ioredis`.
 */
function unwrapMultiError(error: unknown): unknown {
  if (!(error instanceof MultiErrorReply)) return error;
  for (const inner of error.errors()) {
    if (inner instanceof Error) return inner;
  }
  return error;
}

/**
 * The single normalization point for anything node-redis rejects with. A server
 * error reply arrives as an `ErrorReply` (`SimpleError`, `BlobError`) whose
 * `name` is only "Error" — see {@link RedisServerError} for why every adapter
 * converts that to one shared type.
 *
 * Order matters. `MultiErrorReply` also extends `ErrorReply`, so the aggregate
 * is unwrapped to the failing command's own error *first*; wrapping it as-is
 * would normalize "N commands failed, see .replies and .errorIndexes" and lose
 * the WRONGTYPE underneath. An aggregate that carried no inner `Error` is left
 * alone rather than flattened, so `.replies` / `.errorIndexes` stay reachable.
 *
 * Everything else — `WatchError`, `ClientClosedError`, socket errors — passes
 * through untouched: those are client-side, not the server's answer.
 */
function normalizeError(error: unknown, command?: string): unknown {
  const unwrapped = unwrapMultiError(error);
  if (unwrapped instanceof MultiErrorReply) return unwrapped;
  if (unwrapped instanceof ErrorReply) {
    return redisServerError(unwrapped, command);
  }
  return unwrapped;
}

/** The command name for error attribution, matching Redis's own casing. */
function commandName(command: RedisCommand): string {
  return String(command[0]).toUpperCase();
}

/**
 * The Node.js adapter: returns the `RedisClient` `benni()` binds to, backed by
 * a [node-redis](https://www.npmjs.com/package/redis) client. Accepts every
 * node-redis option (`url`, `socket`, `username`/`password`, ...) plus
 * `onError` and `onReconnect`. Replies default to RESP2 for stable wire
 * shapes. Deno uses this same adapter via `npm:` specifiers.
 *
 * Returns synchronously and connects on the first command, so nothing touches
 * the network at import time. A connect that fails rejects the commands
 * waiting on it and the next command tries again, with backoff. Once
 * connected, a dropped connection reconnects in the background under
 * node-redis's own `reconnectStrategy`.
 *
 * Supports sessions and Pub/Sub, pattern subscriptions included: core leases
 * duplicate connections through `session()` and `subscriber()` as needed.
 *
 * @example
 * ```ts
 * import { node } from "benni/node";
 * const redis = benni({ client: node({ url: process.env.REDIS_URL }), schema });
 * ```
 */
export function node(options?: NodeOptions): FullRedisClient {
  const { onError, onReconnect, ...clientOptions } = options ?? {};
  const report = reporter(onError);
  const userStrategy = (clientOptions.socket as { reconnectStrategy?: unknown })
    ?.reconnectStrategy as ReconnectStrategy;
  let dialing = false;
  const client = createClient(
    withReplyDefaults({
      ...clientOptions,
      socket: {
        ...clientOptions.socket,
        reconnectStrategy: unlessDialing(userStrategy, () => dialing)
      } as SocketOptions
    })
  );
  // node-redis re-emits socket errors as client 'error' events; with no
  // listener, a network blip while idle crashes the process (unhandled
  // 'error'). The client reconnects on its own; the listener only reports.
  client.on("error", report);
  let wasReady = false;
  client.on("ready", () => {
    if (wasReady) onReconnect?.("client");
    wasReady = true;
  });
  const connection = lazyConnection("benni/node", async () => {
    dialing = true;
    try {
      await client.connect();
    } finally {
      dialing = false;
    }
  });
  // Leak backstop: live sessions leased from this client. The parent close()
  // force-closes survivors so a leaked session cannot pin a connection past
  // the client's lifetime.
  const sessions = new Set<RedisSession>();
  // Same backstop for the subscriber connection core may lease.
  const subscribers = new Set<RedisPatternSubscriber>();
  // The backstop only drains what it can see. A lease requested after close(),
  // or one whose connect() is still in flight when close() drains the Sets,
  // would open a live socket nobody will ever iterate again — and in Node a
  // live socket pins the event loop, so a "graceful" shutdown never exits.
  let clientClosed = false;

  /**
   * Connect if the client is not open: never used, or node-redis gave up
   * reconnecting. Commands issued while that connect is in flight wait for it
   * too (node-redis reports the client open as soon as it starts dialing), so
   * a failed connect rejects them all with the same error. While node-redis
   * is reconnecting after a drop, commands wait in its offline queue, as they
   * always have.
   */
  async function connected(): Promise<void> {
    if (clientClosed) throw closedError();
    if (dialing || !client.isOpen) await connection.ready();
    if (clientClosed) throw closedError();
  }

  return {
    async send(command: RedisCommand) {
      await connected();
      try {
        return await client.sendCommand<RedisReply>(toRedisArguments(command));
      } catch (error) {
        throw normalizeError(error, commandName(command));
      }
    },
    async pipeline(commands: readonly RedisCommand[]) {
      await connected();
      const pipeline = client.multi();
      for (const command of commands) {
        pipeline.sendCommand(toRedisArguments(command));
      }
      try {
        return (await pipeline.execAsPipeline()) as unknown as RedisReply[];
      } catch (error) {
        // node-redis rejects the whole pipeline with the first failing
        // command's own error, so there is nothing to attribute it to.
        throw normalizeError(error);
      }
    },
    async transaction(commands: readonly RedisCommand[]) {
      await connected();
      const transaction = client.multi();
      for (const command of commands) {
        transaction.sendCommand(toRedisArguments(command));
      }
      try {
        return (await transaction.exec()) as unknown as RedisReply[];
      } catch (error) {
        throw normalizeError(error);
      }
    },
    async session(): Promise<RedisSession> {
      if (clientClosed) throw closedError();
      // reconnectStrategy: false makes the leased connection fail-fast: a
      // drop rejects in-flight and subsequent commands instead of silently
      // reconnecting (which would lose WATCH state and blocked reads).
      // duplicate() shallow-merges overrides, so spread the caller's socket
      // options — replacing the whole object would drop host/port/tls and
      // dial the default localhost instead of the configured server.
      const duplicate = client.duplicate({
        socket: { ...clientOptions.socket, reconnectStrategy: false }
      });
      await duplicate.connect();
      if (clientClosed) throw discardOnClose(duplicate);
      let closed = false;
      duplicate.on("error", () => {
        closed = true;
      });
      const session: RedisSession = {
        async send(command: RedisCommand) {
          try {
            return await duplicate.sendCommand<RedisReply>(
              toRedisArguments(command)
            );
          } catch (error) {
            throw normalizeError(error, commandName(command));
          }
        },
        async watchedTransaction(commands: readonly RedisCommand[]) {
          const transaction = duplicate.multi();
          for (const command of commands) {
            transaction.sendCommand(toRedisArguments(command));
          }
          try {
            return (await transaction.exec()) as unknown as RedisReply[];
          } catch (error) {
            // WATCH violation -> the one cross-adapter abort signal. A
            // per-command runtime error inside a committed EXEC surfaces as
            // the failing command's own error, not node-redis's aggregate.
            if (error instanceof WatchError) return null;
            throw normalizeError(error);
          }
        },
        get closed() {
          return closed || !duplicate.isReady;
        },
        async close() {
          closed = true;
          sessions.delete(session);
          try {
            // destroy(), not graceful close(): graceful close waits out an
            // in-flight server-side blocking timeout; destroy rejects the
            // in-flight command immediately.
            duplicate.destroy();
          } catch {
            // Already destroyed or the connection already dropped — close()
            // is idempotent by contract.
          }
        }
      };
      sessions.add(session);
      return session;
    },
    async subscriber(): Promise<RedisPatternSubscriber> {
      if (clientClosed) throw closedError();
      // Subscriber mode monopolizes a connection, so duplicate rather than
      // borrow the shared one. It gets its own strategy rather than the
      // parent's: the same fail-fast first connect, so a subscribe while Redis
      // is down rejects instead of hanging, then node-redis's reconnect (and
      // native resubscribe) once it has been up.
      let subscriberDialing = true;
      const duplicate = client.duplicate({
        socket: {
          ...clientOptions.socket,
          reconnectStrategy: unlessDialing(
            userStrategy,
            () => subscriberDialing
          )
        } as SocketOptions
      });
      duplicate.on("error", report);
      try {
        await duplicate.connect();
      } finally {
        subscriberDialing = false;
      }
      duplicate.on("ready", () => onReconnect?.("subscriber"));
      if (clientClosed) throw discardOnClose(duplicate);
      let closed = false;
      const subscriber: RedisPatternSubscriber = {
        async subscribe(channel, listener) {
          await duplicate.subscribe(channel, (message: string) =>
            listener(message)
          );
        },
        async unsubscribe(channel) {
          await duplicate.unsubscribe(channel);
        },
        async psubscribe(pattern, listener) {
          await duplicate.pSubscribe(pattern, (message: string, ch: string) =>
            listener(message, ch)
          );
        },
        async punsubscribe(pattern) {
          await duplicate.pUnsubscribe(pattern);
        },
        // isOpen, not isReady: a subscriber connection is allowed to
        // reconnect, and isOpen stays true across that window while isReady
        // dips. It goes false only once node-redis has given up, which is the
        // terminal state core must see so it drops the dead lease instead of
        // handing the next subscribe a socket that will never come back.
        get closed() {
          return closed || !duplicate.isOpen;
        },
        async close() {
          closed = true;
          subscribers.delete(subscriber);
          try {
            await duplicate.close();
          } catch {
            // Already closed or dropped — close() is idempotent by contract.
          }
        }
      };
      subscribers.add(subscriber);
      return subscriber;
    },
    async close() {
      clientClosed = true;
      for (const session of [...sessions]) {
        await session.close();
      }
      for (const subscriber of [...subscribers]) {
        await subscriber.close();
      }
      try {
        await client.close();
      } catch {
        // node-redis throws ClientClosedError on a second close(); every other
        // adapter treats a repeat close as a no-op, so swallow it.
      }
      try {
        // Backstop, and the reason the catch above is not enough: a graceful
        // close() issued while node-redis is mid-reconnect resolves but can
        // leave the socket alive, pinning the Node event loop. destroy()
        // releases it.
        client.destroy();
      } catch {
        // Already destroyed — nothing left to release.
      }
    }
  };
}

/**
 * Refused because the parent client is closed. Leasing past close() would open
 * a connection the leak backstop has already stopped watching.
 */
function closedError(): Error {
  return new Error("benni/node client is closed");
}

/**
 * A lease whose connect() landed after close() drained the backstop: tear the
 * fresh connection down rather than hand back a socket nothing will close.
 */
function discardOnClose(duplicate: { destroy(): void }): Error {
  try {
    duplicate.destroy();
  } catch {
    // Already gone; the refusal is what matters.
  }
  return closedError();
}

/**
 * The Node adapter, backed by node-redis. `node(options)` returns a
 * `RedisClient` that can lease both a session and a subscriber connection, so
 * Pub/Sub needs no second object. Options are node-redis client options;
 * replies default to RESP2.
 */

function toRedisArguments(command: RedisCommand): RedisArgument[] {
  return command.map(toRedisArgument);
}

function toRedisArgument(
  argument: string | RedisCommandArgument
): RedisArgument {
  if (argument instanceof Uint8Array) return Buffer.from(argument);
  return String(argument);
}

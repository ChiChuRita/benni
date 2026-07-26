import { Buffer } from "node:buffer";
import type { RedisArgument } from "redis";
import { createClient, WatchError } from "redis";
import type {
  PubSubChannel,
  PubSubHandler,
  PubSubPattern,
  PubSubPatternHandler,
  PubSubSubscription,
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisReply,
  RedisSession
} from "../core/index.js";
import { createPubSubPublisher } from "../core/index.js";

export type NodeOptions = Parameters<typeof createClient>[0];

// node-redis decodes RESP3 map replies (HGETALL, XREAD, CONFIG GET, ...) as
// plain objects, which fall outside the RedisReply union the typed stores
// validate against. Default to RESP2 flat-array replies; callers can still
// opt into RESP3 explicitly.
function withReplyDefaults(options?: NodeOptions): NodeOptions {
  return { RESP: 2, ...options };
}

/**
 * The Node.js adapter: connects a [node-redis](https://www.npmjs.com/package/redis)
 * client and returns the `RedisClient` handle `beni()` binds to. Accepts
 * every node-redis option (`url`, `socket`, `username`/`password`, ...).
 * Replies default to RESP2 for stable wire shapes. Deno uses this same
 * adapter via `npm:` specifiers. For Pub/Sub, see `node.pubsub()`.
 *
 * @example
 * ```ts
 * import { node } from "beni/node";
 * const client = await node({ url: process.env.REDIS_URL });
 * const redis = beni(client, { schema });
 * ```
 */
export async function node(options?: NodeOptions): Promise<RedisClient> {
  const client = await createClient(withReplyDefaults(options)).connect();
  // node-redis re-emits socket errors as client 'error' events; with no
  // listener, a network blip while idle crashes the process (unhandled
  // 'error'). The client reconnects on its own — the listener just absorbs.
  client.on("error", () => {});
  // Leak backstop: live sessions leased from this client. The parent close()
  // force-closes survivors so a leaked session cannot pin a connection past
  // the client's lifetime.
  const sessions = new Set<RedisSession>();

  return {
    async send(command: RedisCommand) {
      return client.sendCommand<RedisReply>(toRedisArguments(command));
    },
    async pipeline(commands: readonly RedisCommand[]) {
      const pipeline = client.multi();
      for (const command of commands) {
        pipeline.sendCommand(toRedisArguments(command));
      }
      return pipeline.execAsPipeline() as unknown as Promise<RedisReply[]>;
    },
    async transaction(commands: readonly RedisCommand[]) {
      const transaction = client.multi();
      for (const command of commands) {
        transaction.sendCommand(toRedisArguments(command));
      }
      return transaction.exec() as unknown as Promise<RedisReply[]>;
    },
    async session(): Promise<RedisSession> {
      // reconnectStrategy: false makes the leased connection fail-fast: a
      // drop rejects in-flight and subsequent commands instead of silently
      // reconnecting (which would lose WATCH state and blocked reads).
      // duplicate() shallow-merges overrides, so spread the caller's socket
      // options — replacing the whole object would drop host/port/tls and
      // dial the default localhost instead of the configured server.
      const duplicate = client.duplicate({
        socket: { ...options?.socket, reconnectStrategy: false }
      });
      await duplicate.connect();
      let closed = false;
      duplicate.on("error", () => {
        closed = true;
      });
      const session: RedisSession = {
        async send(command: RedisCommand) {
          return duplicate.sendCommand<RedisReply>(toRedisArguments(command));
        },
        async watchedTransaction(commands: readonly RedisCommand[]) {
          const transaction = duplicate.multi();
          for (const command of commands) {
            transaction.sendCommand(toRedisArguments(command));
          }
          try {
            return (await transaction.exec()) as unknown as RedisReply[];
          } catch (error) {
            // WATCH violation -> the one cross-adapter abort signal. Other
            // failures (e.g. MultiErrorReply for a per-command runtime
            // error inside a committed EXEC) rethrow unchanged.
            if (error instanceof WatchError) return null;
            throw error;
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
    async close() {
      for (const session of [...sessions]) {
        await session.close();
      }
      await client.close();
    }
  };
}

export async function pubsub(options?: NodeOptions) {
  const publisherClient = await node(options);
  let subscriberClient: ReturnType<typeof createClient>;
  try {
    subscriberClient = await createClient(withReplyDefaults(options)).connect();
  } catch (error) {
    // Don't leak the already-connected publisher when the subscriber fails.
    await publisherClient.close();
    throw error;
  }
  // Absorb socket errors (see node()); the subscriber reconnects on its own.
  subscriberClient.on("error", () => {});
  const publisher = createPubSubPublisher(publisherClient);

  return {
    publish: publisher.publish,
    async subscribe<TInput, TOutput>(
      channel: PubSubChannel<TInput, TOutput>,
      handler: PubSubHandler<TOutput>
    ): Promise<PubSubSubscription> {
      const listener = (message: string) => {
        void handler(channel.decode(message));
      };
      await subscriberClient.subscribe(channel.name, listener);
      return {
        async unsubscribe() {
          await subscriberClient.unsubscribe(channel.name, listener);
        }
      };
    },
    async subscribePattern<TOutput>(
      pattern: PubSubPattern<TOutput>,
      handler: PubSubPatternHandler<TOutput>
    ): Promise<PubSubSubscription> {
      const listener = (message: string, channel: string) => {
        void handler(pattern.decode(message), channel);
      };
      await subscriberClient.pSubscribe(pattern.pattern, listener);
      return {
        async unsubscribe() {
          await subscriberClient.pUnsubscribe(pattern.pattern, listener);
        }
      };
    },
    async close() {
      await subscriberClient.close();
      await publisherClient.close();
    }
  };
}

/**
 * The Node adapter, backed by node-redis. Call `node(options)` for a
 * `RedisClient`, or `pubsub(options)` for the Pub/Sub adapter (channel and
 * pattern subscriptions). Options are node-redis client options; replies
 * default to RESP2.
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

import type {
  PubSubChannel,
  PubSubHandler,
  PubSubSubscription,
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisReply,
  RedisSession
} from "../core/index.js";
import { createPubSubPublisher } from "../core/index.js";

export type BunOptions = {
  readonly url?: string;
} & Bun.RedisOptions;

async function connectBunClient(
  options?: BunOptions
): Promise<Bun.RedisClient> {
  if (typeof Bun === "undefined") {
    throw new TypeError("bun requires the Bun runtime");
  }
  const { url, ...clientOptions } = options ?? {};
  const client = new Bun.RedisClient(url, clientOptions);
  await client.connect();
  return client;
}

async function bunClient(options?: BunOptions): Promise<RedisClient> {
  const client = await connectBunClient(options);
  // Bun's duplicate() takes no option overrides (verified on 1.3.14), so
  // sessions are constructed fresh from the closed-over url/options instead.
  const { url, ...clientOptions } = options ?? {};
  // Leak backstop: live sessions leased from this client. The parent close()
  // force-closes survivors so a leaked session cannot pin a connection past
  // the client's lifetime.
  const sessions = new Set<RedisSession>();

  return {
    async send(command: RedisCommand) {
      return sendCommand(client, command);
    },
    async pipeline(commands: readonly RedisCommand[]) {
      // Bun auto-pipelines: enqueueing every send synchronously batches the
      // commands into one write, preserving enqueue order on the wire.
      const settled = await Promise.allSettled(
        commands.map((command) => sendCommand(client, command))
      );
      const replies: RedisReply[] = [];
      for (const result of settled) {
        if (result.status === "rejected") throw result.reason;
        replies.push(result.value);
      }
      return replies;
    },
    async transaction(commands: readonly RedisCommand[]) {
      // MULTI, the queued commands, and EXEC must all be enqueued
      // synchronously: Bun's auto-pipelining writes them contiguously in
      // enqueue order, so no other command on this connection can interleave
      // into the transaction. Awaiting between sends would break that.
      const pending = [
        sendCommand(client, ["MULTI"]),
        ...commands.map((command) => sendCommand(client, command)),
        sendCommand(client, ["EXEC"])
      ];
      const settled = await Promise.allSettled(pending);
      const execResult = settled[settled.length - 1]!;
      if (execResult.status === "rejected") throw execResult.reason;
      const reply = execResult.value;
      if (Array.isArray(reply)) {
        // A per-command runtime error inside a committed EXEC (e.g.
        // WRONGTYPE) decodes as an Error element; reject with it instead of
        // handing the caller an Error where a reply belongs — matching the
        // Node adapter's MultiErrorReply rejection.
        for (const element of reply) {
          if (element instanceof Error) throw element;
        }
      }
      return reply as RedisReply[];
    },
    async session(): Promise<RedisSession> {
      // autoReconnect: false makes the leased connection fail-fast: a drop
      // rejects in-flight and subsequent commands instead of silently
      // reconnecting (which would lose WATCH state and blocked reads).
      // enableOfflineQueue: false keeps post-drop sends from being buffered
      // instead of rejected.
      const duplicate = new Bun.RedisClient(url, {
        ...clientOptions,
        autoReconnect: false,
        enableOfflineQueue: false
      });
      await duplicate.connect();
      let closed = false;
      const session: RedisSession = {
        async send(command: RedisCommand) {
          return sendCommand(duplicate, command);
        },
        async watchedTransaction(commands: readonly RedisCommand[]) {
          // MULTI, the queued commands, and EXEC enqueued synchronously so
          // Bun auto-pipelines them contiguously (same trick as the shared
          // client's transaction()).
          const pending = [
            rawSend(duplicate, ["MULTI"]),
            ...commands.map((command) => rawSend(duplicate, command)),
            rawSend(duplicate, ["EXEC"])
          ];
          const settled = await Promise.allSettled(pending);
          const execResult = settled[settled.length - 1]!;
          if (execResult.status === "rejected") throw execResult.reason;
          const reply = execResult.value;
          // RESP3 abort signal (verified on 1.3.14).
          if (reply === null || reply === undefined) return null;
          if (!Array.isArray(reply)) {
            throw new TypeError("Expected Redis EXEC to return array or null");
          }
          // Defends against the RESP2 *-1 -> [] decode on other Bun
          // versions; unambiguous because core never sends a zero-command
          // watched EXEC.
          if (reply.length === 0 && commands.length > 0) return null;
          // A per-command runtime error inside a committed EXEC (e.g.
          // WRONGTYPE) arrives as a plain Error element; reject with the
          // first one before normalization can touch it.
          for (const element of reply) {
            if (element instanceof Error) throw element;
          }
          return reply.map(normalizeReply);
        },
        get closed() {
          return closed || !duplicate.connected;
        },
        async close() {
          closed = true;
          sessions.delete(session);
          duplicate.close();
        }
      };
      sessions.add(session);
      return session;
    },
    async close() {
      for (const session of [...sessions]) {
        await session.close();
      }
      client.close();
    }
  };
}

async function bunPubSub(options?: BunOptions) {
  const publisherClient = await bunClient(options);
  // Subscribing takes over a connection, so use a dedicated subscriber client.
  let subscriberClient: Bun.RedisClient;
  try {
    subscriberClient = await connectBunClient(options);
  } catch (error) {
    // Don't leak the already-connected publisher when the subscriber fails.
    await publisherClient.close();
    throw error;
  }
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
    // subscribePattern is intentionally not implemented: Bun 1.3.14's
    // psubscribe hangs, so the method is omitted entirely to make its
    // absence a compile-time fact instead of a runtime failure.
    async close() {
      subscriberClient.close();
      await publisherClient.close();
    }
  };
}

/**
 * The Bun adapter, backed by Bun's built-in Redis client. Call `bun(options)`
 * for a `RedisClient`, or `bun.pubsub(options)` for the Pub/Sub adapter
 * (channel subscriptions only — Bun 1.3.14's `psubscribe` is broken upstream).
 */
export const bun = Object.assign(bunClient, { pubsub: bunPubSub });

async function sendCommand(
  client: Bun.RedisClient,
  command: RedisCommand
): Promise<RedisReply> {
  return normalizeReply(await rawSend(client, command));
}

function rawSend(
  client: Bun.RedisClient,
  command: RedisCommand
): Promise<unknown> {
  const [name, ...args] = command;
  return client.send(name, args.map(toBunArgument));
}

function toBunArgument(argument: RedisCommandArgument): string | Uint8Array {
  if (argument instanceof Uint8Array) return argument;
  return String(argument);
}

// Bun's RESP3 client decodes map replies (HGETALL, XREAD, CONFIG GET, ...)
// as null-prototype plain objects, which fall outside the RedisReply union.
// Convert them to Maps, which the typed stores already accept.
function normalizeReply(reply: unknown): RedisReply {
  if (reply === null || reply === undefined) return null;
  // A per-command runtime error inside a committed EXEC decodes as a plain
  // Error element in the reply array. Pass it through unchanged — the
  // Object.entries branch below would silently mangle it into an empty Map.
  if (reply instanceof Error) return reply as unknown as RedisReply;
  if (Array.isArray(reply)) return reply.map(normalizeReply);
  // Insurance against Bun version drift: a real Map/Set reply must not fall
  // into the Object.entries branch, which would mangle it into an empty Map.
  if (reply instanceof Map) {
    return new Map(
      [...reply.entries()].map(
        ([field, value]) =>
          [normalizeReply(field), normalizeReply(value)] as const
      )
    );
  }
  if (reply instanceof Set) {
    return [...reply].map(normalizeReply);
  }
  if (typeof reply === "object" && !(reply instanceof Uint8Array)) {
    return new Map(
      Object.entries(reply).map(
        ([field, value]) => [field, normalizeReply(value)] as const
      )
    );
  }
  return reply as RedisReply;
}

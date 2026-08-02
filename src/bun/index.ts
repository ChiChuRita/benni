import type {
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisReply,
  RedisSession,
  RedisSubscriber
} from "../core/index.js";

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
  // Dial once fail-fast before building the real client. A Bun client with
  // autoReconnect on cannot be cancelled: if its first connect() rejects, the
  // background reconnect timer keeps running, close() does not stop it, and
  // the orphan pins the process forever (verified on 1.3.14). An
  // autoReconnect: false client rejects immediately and releases everything,
  // so an unreachable server is reported by the probe and the reconnecting
  // client is only ever constructed against a server we just reached.
  const probe = new Bun.RedisClient(url, {
    ...clientOptions,
    autoReconnect: false
  });
  await probe.connect();
  probe.close();
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
  const subscribers = new Set<RedisSubscriber>();
  // The backstops only drain what they can see. A lease requested after
  // close(), or one whose connect() is still in flight when close() drains the
  // Sets, would open a live socket nobody will ever iterate again.
  let clientClosed = false;

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
      if (clientClosed) throw closedError();
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
      if (clientClosed) throw discardOnClose(duplicate);
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
    async subscriber(): Promise<RedisSubscriber> {
      if (clientClosed) throw closedError();
      // Subscribing takes over a connection, so open a dedicated one.
      const subscriberClient = await connectBunClient(options);
      if (clientClosed) throw discardOnClose(subscriberClient);
      const listeners = new Map<string, (message: string) => void>();
      let closed = false;
      const subscriber: RedisSubscriber = {
        async subscribe(channel, listener) {
          const wrapped = (message: string) => listener(message);
          listeners.set(channel, wrapped);
          await subscriberClient.subscribe(channel, wrapped);
        },
        async unsubscribe(channel) {
          const wrapped = listeners.get(channel);
          listeners.delete(channel);
          if (wrapped) await subscriberClient.unsubscribe(channel, wrapped);
        },
        // psubscribe/punsubscribe are intentionally absent: Bun 1.3.14's
        // psubscribe hangs, so core reports pattern subscribe as unsupported
        // rather than deadlocking on it.
        get closed() {
          return closed;
        },
        async close() {
          closed = true;
          subscribers.delete(subscriber);
          listeners.clear();
          subscriberClient.close();
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
      client.close();
    }
  };
}

/**
 * Refused because the parent client is closed. Leasing past close() would open
 * a connection the leak backstop has already stopped watching.
 */
function closedError(): Error {
  return new Error("benni/bun client is closed");
}

/**
 * A lease whose connect() landed after close() drained the backstop: tear the
 * fresh connection down rather than hand back a socket nothing will close.
 */
function discardOnClose(duplicate: Bun.RedisClient): Error {
  try {
    duplicate.close();
  } catch {
    // Already gone; the refusal is what matters.
  }
  return closedError();
}

/**
 * The Bun adapter, backed by Bun's built-in Redis client. `bun(options)`
 * returns a `RedisClient` that leases sessions and a subscriber connection.
 * Channel subscriptions only — Bun 1.3.14's `psubscribe` is broken upstream, so
 * the subscriber omits pattern support and core surfaces a clear error.
 */
export const bun = bunClient;

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

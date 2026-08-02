import { Buffer } from "node:buffer";
import IORedis, {
  type ClusterOptions,
  type Redis as IORedisClient,
  type RedisOptions
} from "ioredis";
import type {
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisReply,
  RedisSession,
  RedisSubscriber
} from "../core/index.js";

/**
 * The subset of an ioredis client this adapter drives. Structural rather than
 * `instanceof`, so a client from a different copy of ioredis in the tree — or a
 * `Cluster` — is still adopted instead of being mistaken for an options object.
 */
type AdoptableClient = Pick<
  IORedisClient,
  "call" | "pipeline" | "multi" | "quit" | "disconnect"
> & {
  /**
   * Widened from `RedisStatus`. `Cluster` reports its own union (it adds
   * `"disconnecting"`), so picking `Redis`'s status made a Cluster fail to
   * satisfy the very type this comment says it should. Only ever compared
   * against terminal names, so the narrow union bought nothing.
   */
  readonly status: string;
  /**
   * Widened for the same reason: the two shapes disagree on both parameters
   * and return type (`Redis.duplicate(override): Redis` against
   * `Cluster.duplicate(nodes, options): Cluster`). `duplicateOf` dispatches on
   * the actual shape and is the only caller.
   */
  duplicate(...args: never[]): unknown;
};

export type IoredisOptions = RedisOptions & {
  /** Connection URL, e.g. `redis://localhost:6379`. */
  readonly url?: string;
};

/** What `ioredis()` accepts: a URL, options, or an existing client to adopt. */
export type IoredisSource = string | IoredisOptions | AdoptableClient;

function isAdoptable(source: IoredisSource): source is AdoptableClient {
  return (
    typeof source === "object" &&
    source !== null &&
    typeof (source as AdoptableClient).call === "function" &&
    typeof (source as AdoptableClient).duplicate === "function"
  );
}

/** `Cluster` exposes `nodes()`; a plain `Redis` does not. */
function isCluster(client: AdoptableClient): boolean {
  return typeof (client as { nodes?: unknown }).nodes === "function";
}

/**
 * Duplicate a client, applying `overrides` whichever shape it is.
 *
 * `Redis.duplicate(override)` takes one options object, but
 * `Cluster.duplicate(overrideStartupNodes, overrideOptions)` takes options
 * *second*. Handing a Cluster a single object lands it in
 * `overrideStartupNodes`, which is then dropped for having no `length` — so
 * every override is silently lost, `lazyConnect` included. The duplicate
 * starts dialing on its own and the connect below fails with "Redis is
 * already connecting/connected", which took out `session()` and
 * `subscriber()` on every adopted Cluster.
 */
function duplicateOf(
  client: AdoptableClient,
  overrides: RedisOptions | ClusterOptions
): IORedisClient {
  const duplicate = isCluster(client)
    ? (
        client.duplicate as unknown as (
          nodes: unknown[],
          options: RedisOptions
        ) => IORedisClient
      )([], overrides)
    : (client.duplicate as (options: RedisOptions) => IORedisClient)(
        overrides as RedisOptions
      );
  return duplicate;
}

/**
 * Fail-fast overrides for a session duplicate, per the session contract: a
 * drop must reject in-flight and subsequent commands rather than silently
 * reconnecting, which would lose WATCH state and blocked reads.
 *
 * The two client shapes take different knobs, and mixing them up fails
 * silently. `retryStrategy` and `maxRetriesPerRequest` are plain-Redis
 * options; a Cluster ignores them at the top level and consults its own
 * `clusterRetryStrategy` when the last connection closes, which by default
 * reconnects the whole duplicate. So an adopted Cluster's session came back to
 * life after a drop, still reporting `closed === false`, with its WATCH state
 * silently gone. Leaving `clusterRetryStrategy` null makes that close terminal
 * (ioredis then sets status "end" and flushes the queue).
 *
 * `lazyConnect` in both shapes keeps the duplicate from dialing before we own
 * it, so open() is the single place the connection is established.
 */
function sessionOverrides(
  client: AdoptableClient
): RedisOptions | ClusterOptions {
  if (isCluster(client)) {
    return {
      clusterRetryStrategy: null,
      enableOfflineQueue: false,
      lazyConnect: true
    };
  }
  return {
    retryStrategy: () => null,
    maxRetriesPerRequest: null,
    enableOfflineQueue: false,
    lazyConnect: true
  };
}

/**
 * ioredis rewrites key *arguments* with `keyPrefix` but leaves SCAN patterns
 * alone, so a prefixed client stores at `<prefix><key>` while `schema.key()`,
 * every `MATCH` pattern, and any key a Lua script builds from a prefix argument
 * still say `<key>`. Scans then return nothing, silently. Benni's schemas own
 * key naming, so refuse the option rather than half-honour it.
 */
function assertNoKeyPrefix(keyPrefix: unknown, where: string): void {
  if (typeof keyPrefix !== "string" || keyPrefix.length === 0) return;
  throw new TypeError(
    `benni/ioredis cannot be used with ioredis's keyPrefix (${where} sets "${keyPrefix}"). ` +
      "ioredis prefixes key arguments but not SCAN patterns, so every scan would " +
      "silently return nothing and schema.key() would not match what is stored. " +
      'Drop keyPrefix and build the prefix into the schema name instead, e.g. hash(prefix + "user", …).'
  );
}

/**
 * The ioredis adapter: returns the `RedisClient` handle `benni()` binds to,
 * backed by [ioredis](https://www.npmjs.com/package/ioredis).
 *
 * Three ways in — the third is the point, because it means adopting Benni does
 * not mean swapping out the Redis client you already run in production:
 *
 * ```ts
 * import { ioredis } from "benni/ioredis";
 *
 * const client = await ioredis("redis://localhost:6379");   // URL
 * const client = await ioredis({ host, port, password });   // options
 * const client = await ioredis(existingIoredisInstance);    // adopt
 * ```
 *
 * An adopted client is *borrowed*: `close()` shuts down the sessions and
 * subscriber connections Benni leased from it, but leaves the client itself
 * open, because whoever created it still owns its lifetime. An adopted client
 * should also carry its own `"error"` listener — this adapter does not attach
 * one, since silently swallowing errors on a client it does not own would hide
 * failures from the code that does.
 *
 * ioredis speaks RESP2, whose flat reply shapes are exactly what the typed
 * stores decode, so replies pass through without normalization. Sessions,
 * `WATCH` transactions, and Pub/Sub — including pattern subscriptions — are all
 * supported.
 */
export async function ioredis(source?: IoredisSource): Promise<RedisClient> {
  const adopted = source !== undefined && isAdoptable(source);
  if (adopted) {
    assertNoKeyPrefix(
      (source as { options?: RedisOptions }).options?.keyPrefix,
      "the adopted client"
    );
  }
  const client: AdoptableClient = adopted
    ? source
    : await open(createFrom(source as string | IoredisOptions | undefined));

  // Leak backstops: the parent close() force-closes any survivors, so a leaked
  // session or subscriber cannot pin a connection past the client's lifetime.
  const sessions = new Set<RedisSession>();
  const subscribers = new Set<RedisSubscriber>();
  // The backstops only drain what they can see. A lease requested after
  // close(), or one whose connect() is still in flight when close() drains the
  // Sets, would open a live socket nobody will ever iterate again.
  let clientClosed = false;

  return {
    async send(command: RedisCommand) {
      return normalize(await client.call(name(command), args(command)));
    },
    async pipeline(commands: readonly RedisCommand[]) {
      if (commands.length === 0) return [];
      const pipeline = client.pipeline();
      for (const command of commands) {
        pipeline.call(name(command), args(command));
      }
      return unwrap(await pipeline.exec(), "pipeline");
    },
    async transaction(commands: readonly RedisCommand[]) {
      if (commands.length === 0) return [];
      const transaction = client.multi();
      for (const command of commands) {
        transaction.call(name(command), args(command));
      }
      return unwrap(await transaction.exec(), "transaction");
    },
    async session(): Promise<RedisSession> {
      if (clientClosed) throw closedError();
      const duplicate = await open(
        duplicateOf(client, sessionOverrides(client))
      );
      if (clientClosed) throw discardOnClose(duplicate);
      let closed = false;
      duplicate.on("error", () => {
        closed = true;
      });
      duplicate.on("end", () => {
        closed = true;
      });

      const session: RedisSession = {
        async send(command: RedisCommand) {
          return normalize(await duplicate.call(name(command), args(command)));
        },
        async watchedTransaction(commands: readonly RedisCommand[]) {
          const transaction = duplicate.multi();
          for (const command of commands) {
            transaction.call(name(command), args(command));
          }
          const replies = await transaction.exec();
          // ioredis resolves EXEC as null when a key watched on this
          // connection changed — the one cross-adapter abort signal.
          if (replies === null) return null;
          return unwrap(replies, "watched transaction");
        },
        get closed() {
          return closed || isFinished(duplicate.status);
        },
        async close() {
          closed = true;
          sessions.delete(session);
          try {
            // disconnect(), not quit(): quit waits for the server to answer,
            // which means waiting out an in-flight blocking timeout. disconnect
            // tears the socket down and rejects the in-flight command at once.
            duplicate.disconnect();
          } catch {
            // Already gone — close() is idempotent by contract.
          }
        }
      };
      sessions.add(session);
      return session;
    },
    async subscriber(): Promise<RedisSubscriber> {
      if (clientClosed) throw closedError();
      // Subscriber mode monopolizes a connection, so duplicate rather than
      // borrow the shared one.
      const duplicate = await open(duplicateOf(client, { lazyConnect: true }));
      if (clientClosed) throw discardOnClose(duplicate);
      let closed = false;

      // ioredis delivers every subscription through one 'message'/'pmessage'
      // event, so route by name here. Core registers exactly one listener per
      // channel and pattern, so a plain Map is the whole bookkeeping.
      const channels = new Map<string, (message: string) => void>();
      const patterns = new Map<
        string,
        (message: string, channel: string) => void
      >();
      duplicate.on("message", (channel: string, message: string) => {
        channels.get(channel)?.(message);
      });
      duplicate.on(
        "pmessage",
        (pattern: string, channel: string, message: string) => {
          patterns.get(pattern)?.(message, channel);
        }
      );

      const subscriber: RedisSubscriber = {
        async subscribe(channel, listener) {
          channels.set(channel, listener);
          await duplicate.subscribe(channel);
        },
        async unsubscribe(channel) {
          channels.delete(channel);
          await duplicate.unsubscribe(channel);
        },
        async psubscribe(pattern, listener) {
          patterns.set(pattern, listener);
          await duplicate.psubscribe(pattern);
        },
        async punsubscribe(pattern) {
          patterns.delete(pattern);
          await duplicate.punsubscribe(pattern);
        },
        get closed() {
          return closed;
        },
        async close() {
          closed = true;
          channels.clear();
          patterns.clear();
          subscribers.delete(subscriber);
          try {
            duplicate.disconnect();
          } catch {
            // Already gone — close() is idempotent by contract.
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
      // An adopted client belongs to the caller; only close what we opened.
      if (adopted) return;
      try {
        await client.quit();
      } catch {
        // Already closing or dropped; make sure the socket is really gone.
        try {
          client.disconnect();
        } catch {
          // Nothing left to close.
        }
      }
    }
  };
}

function createFrom(
  source: string | IoredisOptions | undefined
): IORedisClient {
  if (typeof source === "string") {
    return new IORedis(source, { lazyConnect: true });
  }
  const { url, ...options } = source ?? {};
  assertNoKeyPrefix(options.keyPrefix, "the options passed to ioredis()");
  return url === undefined
    ? new IORedis({ ...options, lazyConnect: true })
    : new IORedis(url, { ...options, lazyConnect: true });
}

/**
 * Every client this adapter opens is created lazily and connected here, so a
 * command is never issued against a socket that is not ready — which matters
 * because sessions disable the offline queue and would reject instead of
 * waiting.
 *
 * The 'error' listener goes on *before* connecting, for two reasons. ioredis
 * re-emits socket errors as client 'error' events and a client with no
 * listener crashes the process, so attaching afterwards leaves the whole
 * connect window uncovered. And a rejected connect does not stop the client:
 * it keeps retrying on its own schedule, so an unguarded failure left an
 * orphan reconnecting forever, logging "Unhandled error event" and holding the
 * process open. Tear it down instead.
 *
 * Only ever called for clients this adapter created; an adopted client carries
 * its own listener, per the note on `ioredis()`.
 */
async function open(client: IORedisClient): Promise<IORedisClient> {
  client.on("error", () => {});
  if (client.status === "ready") return client;
  try {
    await client.connect();
  } catch (error) {
    try {
      client.disconnect();
    } catch {
      // Nothing left to close; the connect failure is what matters.
    }
    throw error;
  }
  return client;
}

function isFinished(status: string): boolean {
  return status === "end" || status === "close";
}

/**
 * Uppercased on purpose. ioredis keys its reply transformers by *lowercase*
 * command name, so `send(["hgetall", key])` came back as a plain JS object
 * while every other adapter returned the RESP2 flat array, and the typed shape
 * helpers then rejected it on ioredis alone. Command names are case-insensitive
 * on the wire, and ioredis's own flag and key-index lookups already lowercase
 * what they are given, so uppercasing only sidesteps the transformer table.
 */
function name(command: RedisCommand): string {
  return String(command[0]).toUpperCase();
}

/**
 * Refused because the parent client is closed. Leasing past close() would open
 * a connection the leak backstop has already stopped watching.
 */
function closedError(): Error {
  return new Error("benni/ioredis client is closed");
}

/**
 * A lease whose connect() landed after close() drained the backstop: tear the
 * fresh connection down rather than hand back a socket nothing will close.
 */
function discardOnClose(duplicate: IORedisClient): Error {
  try {
    duplicate.disconnect();
  } catch {
    // Already gone; the refusal is what matters.
  }
  return closedError();
}

function args(command: RedisCommand): Array<string | Buffer> {
  const rest: Array<string | Buffer> = [];
  for (let index = 1; index < command.length; index += 1) {
    rest.push(toArgument(command[index] as RedisCommandArgument));
  }
  return rest;
}

function toArgument(argument: RedisCommandArgument): string | Buffer {
  if (argument instanceof Uint8Array) return Buffer.from(argument);
  return String(argument);
}

/**
 * ioredis reports pipeline and transaction results as `[error, reply]` tuples.
 * Surface a per-command failure by throwing it, matching the other adapters:
 * handing back an `Error` where a reply belongs would push the failure into the
 * typed decoders, which report it as a reply-shape problem instead.
 */
function unwrap(
  replies: Array<[Error | null, unknown]> | null,
  what: string
): RedisReply[] {
  if (replies === null) {
    throw new Error(
      `Redis aborted the ${what} (EXEC returned nil); a watched key changed.`
    );
  }
  return replies.map(([error, reply]) => {
    if (error) throw error;
    return normalize(reply);
  });
}

/**
 * ioredis speaks RESP2, so replies already match the `RedisReply` union —
 * integers as numbers, nil as null, maps as flat arrays. The only fix-up is
 * `undefined`, which a skipped pipeline entry can produce and which the typed
 * decoders do not model.
 */
function normalize(reply: unknown): RedisReply {
  return (reply ?? null) as RedisReply;
}

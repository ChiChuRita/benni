import { redisServerError } from "../core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisReply,
  RedisSession,
  RedisSubscriber
} from "../core/index.js";

/**
 * Bun rejects with a `RedisError` for everything and tells the cases apart by
 * `code`: a server error reply is `ERR_REDIS_INVALID_RESPONSE`, while its own
 * client-side failures use codes of their own
 * (`ERR_REDIS_CONNECTION_CLOSED`, ...) — verified on 1.3.14. So the code, not
 * the class, is the signal for "the server answered with an error".
 */
const SERVER_REPLY_CODE = "ERR_REDIS_INVALID_RESPONSE";

/**
 * The single normalization point for anything Bun's client rejects with: a
 * server error reply becomes a `RedisServerError`, everything else (connection
 * closed, offline queue disabled, timeouts) passes through untouched.
 */
function normalizeError(error: unknown, command?: string): unknown {
  if (!(error instanceof Error)) return error;
  if ((error as { code?: unknown }).code !== SERVER_REPLY_CODE) return error;
  return redisServerError(error, command);
}

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
        if (result.status === "rejected") throw normalizeError(result.reason);
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
        rawSend(client, ["MULTI"]),
        ...commands.map((command) => rawSend(client, command)),
        rawSend(client, ["EXEC"])
      ];
      const settled = await Promise.allSettled(pending);
      const execResult = settled[settled.length - 1]!;
      if (execResult.status === "rejected") {
        throw normalizeError(execResult.reason);
      }
      const reply = execResult.value;
      if (Array.isArray(reply)) {
        // A per-command runtime error inside a committed EXEC (e.g.
        // WRONGTYPE) decodes as an Error element; reject with it instead of
        // handing the caller an Error where a reply belongs — matching the
        // Node adapter's MultiErrorReply rejection. An Error where EXEC puts a
        // reply can only be the server's own error, so it is normalized
        // unconditionally rather than through the `code` gate.
        for (const element of reply) {
          if (element instanceof Error) throw redisServerError(element);
        }
        // Each element is reshaped as the command that produced it.
        return reply.map((element, index) =>
          normalizeReply(commands[index]!, element)
        );
      }
      return toResp2Structure(reply) as RedisReply[];
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
          if (execResult.status === "rejected") {
            throw normalizeError(execResult.reason);
          }
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
          // first one, normalized, before reply normalization can touch it.
          for (const element of reply) {
            if (element instanceof Error) throw redisServerError(element);
          }
          return reply.map((element, index) =>
            normalizeReply(commands[index]!, element)
          );
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
      // Bun fires onclose only once the connection is gone for good: close(),
      // or autoReconnect giving up. A drop it recovers from does not fire it
      // (verified on 1.4.2), so this is the terminal state core must see to
      // drop the dead lease, the equivalent of node-redis's `isOpen` going
      // false. `connected` is not: it dips during every reconnect.
      let gone = false;
      subscriberClient.onclose = () => {
        gone = true;
      };
      // Bun reconnects a dropped subscriber connection on its own but does not
      // resubscribe (verified on 1.4.2): the socket comes back with no
      // subscriptions, PUBLISH reports 0 receivers, and every handler goes
      // silent while `closed` still says false. node-redis and ioredis both
      // resubscribe natively, so this restores the same behaviour here, on the
      // reconnect Bun already made, rather than asking core to re-lease.
      //
      // One raw SUBSCRIBE for every channel still wanted, not the public
      // subscribe(): Bun still holds our listeners across the reconnect, and
      // subscribe() would register each a second time (double delivery,
      // verified). The channel list is read and the command written in one
      // synchronous turn, so an unsubscribe() from core either ran first (its
      // channel is not in the list) or is written after (and wins on the
      // wire). SUBSCRIBE is idempotent server-side, so overlapping a
      // subscribe() Bun queued during the outage is harmless.
      subscriberClient.onconnect = () => {
        if (closed || listeners.size === 0) return;
        subscriberClient.send("SUBSCRIBE", [...listeners.keys()]).catch(() => {
          // The connection dropped again mid-resubscribe; Bun's next
          // reconnect fires onconnect and this runs again.
        });
      };
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
          return closed || gone;
        },
        async close() {
          closed = true;
          subscribers.delete(subscriber);
          listeners.clear();
          // Drop Bun's own listeners before closing: a client closed while it
          // still holds subscriptions pins the process forever, while one
          // unsubscribed first exits (verified on 1.4.2). The parent close()
          // reaches this with subscriptions live. Not awaited: on a dead or
          // reconnecting socket the UNSUBSCRIBE would wait for a reconnect,
          // and close() below rejects it anyway.
          try {
            subscriberClient.unsubscribe().catch(() => {});
          } catch {
            // Already closed; nothing left to unsubscribe.
          }
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
  return normalizeReply(command, await rawSend(client, command));
}

async function rawSend(
  client: Bun.RedisClient,
  command: RedisCommand
): Promise<unknown> {
  const [name, ...args] = command;
  try {
    return await client.send(name, args.map(toBunArgument));
  } catch (error) {
    throw normalizeError(error, String(name).toUpperCase());
  }
}

function toBunArgument(argument: RedisCommandArgument): string | Uint8Array {
  if (argument instanceof Uint8Array) return argument;
  return String(argument);
}

/**
 * Reshape a reply from Bun's RESP3 decoding into the RESP2 shape the adapter
 * contract promises (see "Reply shapes" on `RedisClient` in core/types.ts).
 *
 * Bun always speaks RESP3 and has no option to speak RESP2, so without this
 * `redis.raw.send()` and user decoders saw different shapes on Bun than on
 * every other adapter: HGETALL as a map, ZSCORE as a number, WITHSCORES as
 * nested pairs. Two passes:
 *
 * 1. Structural, for every command: a map reply (Bun decodes RESP3 maps as
 *    null-prototype plain objects) becomes the flat `[field, value, ...]`
 *    array RESP2 sends, except XREAD/XREADGROUP, whose RESP2 reply is an
 *    array of `[stream, entries]` pairs. A set becomes an array.
 * 2. Per command, because a RESP3 double and an integer both decode to a JS
 *    number and only the command says which one it was: doubles become
 *    strings, and the replies RESP3 nests as `[member, score]` pairs are
 *    flattened. See {@link reshapeDoubles}.
 */
function normalizeReply(command: RedisCommand, reply: unknown): RedisReply {
  const name = String(command[0]).toUpperCase();
  const structural = toResp2Structure(
    reply,
    name === "XREAD" || name === "XREADGROUP"
  );
  return reshapeDoubles(name, command, structural);
}

function toResp2Structure(reply: unknown, mapAsPairs = false): RedisReply {
  if (reply === null || reply === undefined) return null;
  // A per-command runtime error inside a committed EXEC decodes as a plain
  // Error element in the reply array. Pass it through unchanged — the
  // Object.entries branch below would silently mangle it into an empty array.
  if (reply instanceof Error) return reply as unknown as RedisReply;
  if (Array.isArray(reply)) {
    return reply.map((element) => toResp2Structure(element));
  }
  if (reply instanceof Set) {
    return [...reply].map((element) => toResp2Structure(element));
  }
  // A real Map is insurance against Bun version drift: today maps arrive as
  // null-prototype objects, handled below.
  const entries =
    reply instanceof Map
      ? [...reply.entries()]
      : typeof reply === "object" && !(reply instanceof Uint8Array)
        ? Object.entries(reply)
        : undefined;
  if (entries === undefined) return reply as RedisReply;
  if (mapAsPairs) {
    return entries.map(([field, value]) => [
      toResp2Structure(field),
      toResp2Structure(value)
    ]);
  }
  return entries.flatMap(([field, value]) => [
    toResp2Structure(field),
    toResp2Structure(value)
  ]);
}

/** True when `flag` appears among the command's arguments, any case. */
function hasFlag(command: RedisCommand, flag: string): boolean {
  for (let index = 1; index < command.length; index += 1) {
    const argument = command[index];
    if (typeof argument === "string" && argument.toUpperCase() === flag) {
      return true;
    }
  }
  return false;
}

/**
 * A RESP3 double as RESP2 would have sent it: a bulk string. The digits may
 * differ from the server's own formatting, but they parse back to the same
 * number, and the infinities use Redis's spelling so the score decoders read
 * them.
 */
function doubleToString(value: number): string {
  if (value === Number.POSITIVE_INFINITY) return "inf";
  if (value === Number.NEGATIVE_INFINITY) return "-inf";
  return String(value);
}

/** Every number in the reply, at any depth, as a RESP2 double string. */
function doublesToStrings(reply: RedisReply): RedisReply {
  if (typeof reply === "number") return doubleToString(reply);
  if (Array.isArray(reply)) return reply.map(doublesToStrings);
  return reply;
}

/** `[[a, 1], [b, 2]]` -> `[a, 1, b, 2]`; already-flat elements pass through. */
function flattenPairs(reply: RedisReply): RedisReply {
  if (!Array.isArray(reply)) return reply;
  return reply.flatMap((element) =>
    Array.isArray(element) ? element : [element]
  );
}

/** Sorted-set range-style commands that nest `[member, score]` under WITHSCORES. */
const WITHSCORES_COMMANDS = new Set([
  "ZRANGE",
  "ZRANGEBYSCORE",
  "ZREVRANGE",
  "ZREVRANGEBYSCORE",
  "ZUNION",
  "ZINTER",
  "ZDIFF",
  "ZRANDMEMBER"
]);

/** Geo commands whose WITHCOORD coordinates are RESP3 doubles. */
const GEO_SEARCH_COMMANDS = new Set([
  "GEOSEARCH",
  "GEORADIUS",
  "GEORADIUS_RO",
  "GEORADIUSBYMEMBER",
  "GEORADIUSBYMEMBER_RO"
]);

/**
 * The per-command half of {@link normalizeReply}. Every entry was checked
 * against what Bun 1.4.2 decodes from redis 8 (RESP3) and what node-redis
 * receives for the same command over RESP2. Replies that are strings in both
 * protocols (GEODIST, GEOSEARCH WITHDIST, INCRBYFLOAT, HINCRBYFLOAT, ZSCAN)
 * need nothing.
 *
 * A command missing here keeps its RESP3 doubles as numbers. The contract
 * test pins the commonly used ones on every adapter, so a gap shows up there
 * rather than in a user's decoder.
 */
function reshapeDoubles(
  name: string,
  command: RedisCommand,
  reply: RedisReply
): RedisReply {
  switch (name) {
    // Every number in these replies is a double (a score or a coordinate).
    case "ZSCORE":
    case "ZMSCORE":
    case "ZINCRBY":
    case "GEOPOS":
    case "BZPOPMIN":
    case "BZPOPMAX":
    case "ZMPOP":
    case "BZMPOP":
      return doublesToStrings(reply);
    // ZADD answers with a count, except under INCR, where it is the new score.
    case "ZADD":
      return hasFlag(command, "INCR") ? doublesToStrings(reply) : reply;
    // RESP2 is flat `[member, score, ...]`; RESP3 nests a pair per member
    // when a count is given and sends one flat pair when it is not.
    case "ZPOPMIN":
    case "ZPOPMAX":
      return doublesToStrings(flattenPairs(reply));
    // `[rank, score]`: the rank is an integer, only the score is a double.
    case "ZRANK":
    case "ZREVRANK":
      if (hasFlag(command, "WITHSCORE") && Array.isArray(reply)) {
        return reply.map((element, index) =>
          index === 1 && typeof element === "number"
            ? doubleToString(element)
            : element
        );
      }
      return reply;
    case "HRANDFIELD":
      return hasFlag(command, "WITHVALUES") ? flattenPairs(reply) : reply;
  }
  if (WITHSCORES_COMMANDS.has(name) && hasFlag(command, "WITHSCORES")) {
    return doublesToStrings(flattenPairs(reply));
  }
  // `[member, dist?, hash?, [lon, lat]?]` per match: only the coordinate
  // pair holds doubles; WITHHASH is an integer and must stay one.
  if (
    GEO_SEARCH_COMMANDS.has(name) &&
    hasFlag(command, "WITHCOORD") &&
    Array.isArray(reply)
  ) {
    return reply.map((match) =>
      Array.isArray(match)
        ? match.map((part) =>
            Array.isArray(part) ? doublesToStrings(part) : part
          )
        : match
    );
  }
  return reply;
}

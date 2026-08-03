import { TRANSACTION_UNSUPPORTED } from "./client-source.js";
import {
  ReplyShapeError,
  replyShapeError,
  UnsupportedCapabilityError,
  ValidationError
} from "./errors.js";
import { expectNumber } from "./helpers.js";
import type { SameSlotList, SlotAnchor } from "./keys.js";
import type { SlotGuard } from "./slot.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  RedisSession
} from "./types.js";

/**
 * Turns one raw Redis reply into a typed value. Used to tell
 * `RedisTransaction.add` what the queued command returns; the built-in
 * decoders are {@link okReply}, {@link numberReply}, {@link stringReply},
 * {@link stringOrNullReply}, and {@link booleanNumberReply}.
 */
export type RedisReplyDecoder<T> = (reply: RedisReply) => T;

/**
 * A MULTI/EXEC transaction that accumulates its result types as you queue
 * commands, so `exec()` resolves to a tuple typed in queue order. Get one from
 * `redis.multi()`.
 *
 * @example
 * ```ts
 * const [visits, name] = await redis
 *   .multi()
 *   .add(["INCR", "visits"], numberReply)
 *   .add(["GET", "user:42:name"], stringOrNullReply)
 *   .exec();
 * //     ^? [number, string | null]
 * ```
 */
export type RedisTransaction<
  TResults extends readonly unknown[],
  TAnchor extends string = never
> = {
  /** Queue a command with the decoder for its reply, widening the result tuple. */
  add<T>(
    command: RedisCommand,
    decode: RedisReplyDecoder<T>
  ): RedisTransaction<[...TResults, T], TAnchor>;
  /**
   * Declare the keys this transaction touches. **This is a declaration, not a
   * derivation:** Benni does not parse key positions out of queued commands, so
   * a key you queue but do not declare here is not checked.
   *
   * Under `benni(client, { cluster: assertSameSlot })`, `exec()` verifies the declared
   * keys share one Cluster slot before sending MULTI. Without that option it
   * queues nothing, sends nothing, and costs nothing.
   *
   * Keys accumulate, so `.keys(a).keys(b)` declares both, and each call is
   * checked at compile time against the first provable hash tag seen so far.
   */
  keys<const TKeys extends readonly string[]>(
    declared: TKeys & SameSlotList<TKeys, SlotAnchor<TAnchor, TKeys>>
  ): RedisTransaction<TResults, SlotAnchor<TAnchor, TKeys>>;
  /** Send MULTI/EXEC and decode each reply. Resolves to `[]` if nothing was queued. */
  exec(): Promise<TResults>;
};

type QueuedCommand = {
  readonly command: RedisCommand;
  readonly decode: RedisReplyDecoder<unknown>;
};

export function createTransaction(
  client: RedisClient,
  assertSameSlot?: SlotGuard
): RedisTransaction<[]> {
  return buildTransaction(client, [], [], assertSameSlot);
}

function buildTransaction<TResults extends readonly unknown[]>(
  client: RedisClient,
  queued: readonly QueuedCommand[],
  declared: readonly string[],
  assertSameSlot?: SlotGuard
): RedisTransaction<TResults> {
  return {
    add<T>(command: RedisCommand, decode: RedisReplyDecoder<T>) {
      return buildTransaction<[...TResults, T]>(
        client,
        [...queued, { command, decode }],
        declared,
        assertSameSlot
      );
    },
    keys(next: readonly string[]) {
      return buildTransaction<TResults>(
        client,
        queued,
        [...declared, ...next],
        assertSameSlot
      );
    },
    async exec(): Promise<TResults> {
      if (queued.length === 0) {
        return [] as unknown as TResults;
      }
      assertSameSlot?.("EXEC", declared);
      if (client.transaction === undefined) {
        // The same class the lazy facade throws, so `catch
        // (UnsupportedCapabilityError)` works whether the client was handed
        // over connected or behind a promise or factory.
        throw new UnsupportedCapabilityError(
          TRANSACTION_UNSUPPORTED,
          "transaction"
        );
      }
      const replies = await client.transaction(
        queued.map((entry) => entry.command)
      );
      if (!Array.isArray(replies)) {
        throw replyShapeError("EXEC", "array", replies);
      }
      if (replies.length !== queued.length) {
        throw new ReplyShapeError(
          `Expected Redis EXEC to return ${queued.length} replies`,
          replies
        );
      }
      return queued.map((entry, index) =>
        entry.decode(replies[index])
      ) as unknown as TResults;
    }
  };
}

export type WatchedRedisTransaction<
  TResults extends readonly unknown[],
  TAnchor extends string = never
> = {
  add<T>(
    command: RedisCommand,
    decode: RedisReplyDecoder<T>
  ): WatchedRedisTransaction<[...TResults, T], TAnchor>;
  /**
   * Declare the keys this transaction touches; see
   * {@link RedisTransaction.keys}. Note the WATCHed keys must share the slot
   * too, and `session.watch()` checks those separately.
   */
  keys<const TKeys extends readonly string[]>(
    declared: TKeys & SameSlotList<TKeys, SlotAnchor<TAnchor, TKeys>>
  ): WatchedRedisTransaction<TResults, SlotAnchor<TAnchor, TKeys>>;
  /** null = EXEC aborted because a WATCHed key changed. */
  exec(): Promise<TResults | null>;
};

export function createWatchedTransaction(
  session: Pick<RedisSession, "watchedTransaction">,
  assertSameSlot?: SlotGuard
): WatchedRedisTransaction<[]> {
  return buildWatchedTransaction(session, [], [], assertSameSlot);
}

function buildWatchedTransaction<TResults extends readonly unknown[]>(
  session: Pick<RedisSession, "watchedTransaction">,
  queued: readonly QueuedCommand[],
  declared: readonly string[],
  assertSameSlot?: SlotGuard
): WatchedRedisTransaction<TResults> {
  return {
    add<T>(command: RedisCommand, decode: RedisReplyDecoder<T>) {
      return buildWatchedTransaction<[...TResults, T]>(
        session,
        [...queued, { command, decode }],
        declared,
        assertSameSlot
      );
    },
    keys(next: readonly string[]) {
      return buildWatchedTransaction<TResults>(
        session,
        queued,
        [...declared, ...next],
        assertSameSlot
      );
    },
    async exec(): Promise<TResults | null> {
      if (queued.length === 0) {
        // Unlike createTransaction's client-side short-circuit, an empty
        // watched exec is banned: short-circuiting would silently leave
        // WATCH armed on the connection, and never sending a zero-command
        // watched EXEC is what keeps the Bun [] abort decode unambiguous.
        throw new ValidationError(
          "Cannot exec an empty watched transaction; queue at least one command"
        );
      }
      assertSameSlot?.("EXEC", declared);
      const replies = await session.watchedTransaction(
        queued.map((entry) => entry.command)
      );
      if (replies === null) return null;
      if (!Array.isArray(replies)) {
        throw replyShapeError("EXEC", "array or null", replies);
      }
      if (replies.length !== queued.length) {
        throw new ReplyShapeError(
          `Expected Redis EXEC to return ${queued.length} replies`,
          replies
        );
      }
      return queued.map((entry, index) =>
        entry.decode(replies[index])
      ) as unknown as TResults;
    }
  };
}

/**
 * Transaction decoder for commands that reply with a simple `OK` (`SET`,
 * `MSET`, `RENAME`, …). Yields `void`; throws if the reply is anything else.
 */
export function okReply(reply: RedisReply): void {
  if (reply !== "OK") {
    throw new ReplyShapeError(
      "Expected Redis transaction reply to return OK",
      reply
    );
  }
}

/**
 * Transaction decoder for integer replies (`INCR`, `DEL`, `LLEN`, `ZADD`, …).
 */
export function numberReply(reply: RedisReply): number {
  return expectNumber(reply, "transaction reply");
}

/**
 * Transaction decoder for commands that always reply with a bulk string. Use
 * {@link stringOrNullReply} for reads that can miss.
 */
export function stringReply(reply: RedisReply): string {
  if (typeof reply !== "string") {
    throw new ReplyShapeError(
      "Expected Redis transaction reply to return string",
      reply
    );
  }
  return reply;
}

/**
 * Transaction decoder for reads that may miss (`GET`, `HGET`, `LPOP`, …), where
 * a missing key comes back as `null`.
 */
export function stringOrNullReply(reply: RedisReply): string | null {
  if (reply === null) return null;
  if (typeof reply !== "string") {
    throw new ReplyShapeError(
      "Expected Redis transaction reply to return string or null",
      reply
    );
  }
  return reply;
}

/**
 * Transaction decoder for the `0`/`1` integer replies that mean "did it happen"
 * (`EXPIRE`, `SETNX`, `SISMEMBER`, …), narrowed to a boolean.
 */
export function booleanNumberReply(reply: RedisReply): boolean {
  return expectNumber(reply, "transaction reply") === 1;
}

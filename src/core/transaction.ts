import { ReplyShapeError, replyShapeError, ValidationError } from "./errors.js";
import { expectNumber } from "./helpers.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply,
  RedisSession
} from "./types.js";

export type RedisReplyDecoder<T> = (reply: RedisReply) => T;

export type RedisTransaction<TResults extends readonly unknown[]> = {
  add<T>(
    command: RedisCommand,
    decode: RedisReplyDecoder<T>
  ): RedisTransaction<[...TResults, T]>;
  exec(): Promise<TResults>;
};

type QueuedCommand = {
  readonly command: RedisCommand;
  readonly decode: RedisReplyDecoder<unknown>;
};

export function createTransaction(client: RedisClient): RedisTransaction<[]> {
  return buildTransaction(client, []);
}

function buildTransaction<TResults extends readonly unknown[]>(
  client: RedisClient,
  queued: readonly QueuedCommand[]
): RedisTransaction<TResults> {
  return {
    add<T>(command: RedisCommand, decode: RedisReplyDecoder<T>) {
      return buildTransaction<[...TResults, T]>(client, [
        ...queued,
        { command, decode }
      ]);
    },
    async exec(): Promise<TResults> {
      if (queued.length === 0) {
        return [] as unknown as TResults;
      }
      if (client.transaction === undefined) {
        throw new TypeError("Redis client does not support transactions");
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

export type WatchedRedisTransaction<TResults extends readonly unknown[]> = {
  add<T>(
    command: RedisCommand,
    decode: RedisReplyDecoder<T>
  ): WatchedRedisTransaction<[...TResults, T]>;
  /** null = EXEC aborted because a WATCHed key changed. */
  exec(): Promise<TResults | null>;
};

export function createWatchedTransaction(
  session: Pick<RedisSession, "watchedTransaction">
): WatchedRedisTransaction<[]> {
  return buildWatchedTransaction(session, []);
}

function buildWatchedTransaction<TResults extends readonly unknown[]>(
  session: Pick<RedisSession, "watchedTransaction">,
  queued: readonly QueuedCommand[]
): WatchedRedisTransaction<TResults> {
  return {
    add<T>(command: RedisCommand, decode: RedisReplyDecoder<T>) {
      return buildWatchedTransaction<[...TResults, T]>(session, [
        ...queued,
        { command, decode }
      ]);
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

export function okReply(reply: RedisReply): void {
  if (reply !== "OK") {
    throw new ReplyShapeError(
      "Expected Redis transaction reply to return OK",
      reply
    );
  }
}

export function numberReply(reply: RedisReply): number {
  return expectNumber(reply, "transaction reply");
}

export function stringReply(reply: RedisReply): string {
  if (typeof reply !== "string") {
    throw new ReplyShapeError(
      "Expected Redis transaction reply to return string",
      reply
    );
  }
  return reply;
}

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

export function booleanNumberReply(reply: RedisReply): boolean {
  return expectNumber(reply, "transaction reply") === 1;
}

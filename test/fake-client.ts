import { createKvResource } from "../src/core/key-value.js";
import type { HashTagLayout } from "../src/core/keys.js";
import type { SlotGuard } from "../src/core/slot.js";
import { createStoreContext } from "../src/core/store.js";
import type {
  Keyspace,
  RedisClient,
  RedisCommand,
  RedisKeyPart,
  RedisReply,
  RedisSession
} from "../src/core/types.js";

export function fakeClient(
  commands: RedisCommand[],
  replies: RedisReply[]
): RedisClient {
  return {
    async send(command) {
      commands.push(command);
      const reply = replies.shift();
      if (reply === undefined) throw new Error("No fake Redis reply queued");
      return reply;
    },
    async pipeline(pipelineCommands) {
      commands.push(...pipelineCommands);
      return replies.splice(0, pipelineCommands.length);
    },
    async transaction(transactionCommands) {
      commands.push(...transactionCommands);
      return replies.splice(0, transactionCommands.length);
    },
    async close() {}
  };
}

/**
 * Scripted watchedTransaction outcomes, consumed in order per call:
 * an array resolves the per-command replies, `null` resolves null (WATCH
 * abort), and an Error rejects (per-command runtime error inside a
 * committed EXEC).
 */
export type FakeWatchedResult = RedisReply[] | null | Error;

export function fakeSession(
  commands: RedisCommand[],
  replies: RedisReply[],
  watchedResults: FakeWatchedResult[] = []
): RedisSession {
  let closed = false;
  return {
    async send(command) {
      if (closed) throw new Error("Fake session connection closed");
      commands.push(command);
      const reply = replies.shift();
      if (reply === undefined) throw new Error("No fake Redis reply queued");
      return reply;
    },
    async watchedTransaction(transactionCommands) {
      if (closed) throw new Error("Fake session connection closed");
      commands.push(...transactionCommands);
      if (watchedResults.length === 0) {
        throw new Error("No fake watchedTransaction result queued");
      }
      const result = watchedResults.shift();
      if (result instanceof Error) throw result;
      return result ?? null;
    },
    get closed() {
      return closed;
    },
    async close() {
      closed = true;
    }
  };
}

/**
 * A kv store over `client` the way `redis.query` builds one, with the counter
 * or string commands its codec's `format` brings.
 */
export function kvResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined,
  TFormat
>(
  client: RedisClient,
  schema: Keyspace<TInput, TOutput, TPrefix, TId, THashTag, TFormat>,
  assertSameSlot?: SlotGuard
) {
  return createKvResource(
    createStoreContext(client, undefined, assertSameSlot),
    schema
  );
}

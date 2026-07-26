// Public API of the root entrypoint. The full low-level surface (store
// builders, keyspace helpers, reply utilities) lives under `beni/core`.

// Codecs.
export { codecs } from "./core/codecs.js";
// Errors.
export { ReplyShapeError, ValidationError } from "./core/errors.js";
// Typed Lua scripts.
export {
  createScriptRunner,
  type DefineScriptOptions,
  defineScript,
  type RedisScript,
  type ScriptRunner
} from "./core/script.js";
export {
  type BlockingTimeout,
  type BlockingWait,
  SessionClosedError,
  WatchRetriesExceededError
} from "./core/session.js";
// Typed transactions — reply decoders for `multi().add(command, decoder)`.
export {
  booleanNumberReply,
  numberReply,
  okReply,
  type RedisReplyDecoder,
  type RedisTransaction,
  stringOrNullReply,
  stringReply
} from "./core/transaction.js";
// Client contract — what an adapter provides and `redis.raw` speaks.
export type {
  Codec,
  InferInput,
  InferOutput,
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisKey,
  RedisKeyPart,
  RedisReply,
  RedisSession
} from "./core/types.js";
export {
  type Beni,
  type BeniOptions,
  type BeniSchema,
  type BeniSession,
  type BeniWatchOptions,
  beni,
  type QueryRegistry,
  type QueryResource,
  type SchemaKind
} from "./database.js";

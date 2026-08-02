// Public API of the root entrypoint. The full low-level surface (store
// builders, keyspace helpers, reply utilities) lives under `benni/core`.

// Codecs.
export { codecs } from "./core/codecs.js";
// Errors.
export {
  PartialRecordError,
  RedisServerError,
  type RedisServerErrorOptions,
  ReplyShapeError,
  redisErrorCode,
  // Named for custom adapters: the normalizer every built-in adapter runs a
  // server error reply through.
  redisServerError,
  ValidationError
} from "./core/errors.js";
export type { HashTagLayout, KeyOptions } from "./core/keys.js";
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
// `CrossSlotError`, `slotOf`, `hashTagOf`, and the guard itself live in
// `benni/cluster`, NOT here: naming them from the root entry would put the
// CRC16 table and the error's fix-hint prose in every bundle, including the
// ones that never enable the check. Only the erased types stay.
export type { SlotGuard, SlotHint } from "./core/slot.js";
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
  RedisSession,
  RedisSubscriber
} from "./core/types.js";
export {
  type Benni,
  type BenniOptions,
  type BenniSchema,
  type BenniSession,
  type BenniWatchOptions,
  benni,
  type QueryRegistry,
  type QueryResource,
  type SchemaKind
} from "./database.js";

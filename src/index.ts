// Public API of the root entrypoint: `benni()`, its errors, and the types an
// app names. Schema declarations live under `benni/schema`; the adapter-author
// surface (the client contract, `resolveClient`, the server-error normalizer,
// the script runner, the store builders) lives under `benni/core`.

// Codecs.
export { codecs } from "./core/codecs.js";
// Errors.
export {
  PartialRecordError,
  type RedisClientCapability,
  RedisServerError,
  type RedisServerErrorOptions,
  ReplyShapeError,
  redisErrorCode,
  UnsupportedCapabilityError,
  ValidationError
} from "./core/errors.js";
export type { HashTagLayout, KeyOptions } from "./core/keys.js";
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
export type {
  Codec,
  InferInput,
  InferOutput,
  RedisKey,
  RedisKeyPart
} from "./core/types.js";
export {
  type AnyBenni,
  type Benni,
  type BenniBase,
  type BenniCloseOptions,
  type BenniConfig,
  type BenniNoSessions,
  type BenniOptions,
  type BenniPubSub,
  type BenniScan,
  type BenniSchema,
  type BenniSession,
  type BenniSessions,
  type BenniWatchOptions,
  benni,
  type ChannelResourceFor,
  type PrimitiveResource,
  type PubSubPublisher,
  type QueryRegistry,
  type QueryResource,
  // The module-augmentation target: declare `schema` on it once and every
  // `Benni` in the app is typed without being handed `typeof schema` again.
  // Apps only, once per program; libraries take `AnyBenni`.
  type Register,
  type RegisteredSchema,
  type SchemaKind,
  type SessionQueryRegistry,
  type SessionQueryResource,
  type SessionSchemaKind,
  type StorableSchema
} from "./database.js";

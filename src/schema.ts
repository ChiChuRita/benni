import { decodeBase64, encodeBase64 } from "./core/base64.js";
import { defineBitmap } from "./core/bitmap.js";
import { codecs } from "./core/codecs.js";
import { ReplyShapeError } from "./core/errors.js";
import { defineGeoSet } from "./core/geo.js";
import { defineHash } from "./core/hash.js";
import { defineHyperLogLog } from "./core/hyperloglog.js";
import { defineKeyspace } from "./core/key-value.js";
import { defineList } from "./core/list.js";
import { definePubSubChannel, definePubSubPattern } from "./core/pubsub.js";
import { defineSet } from "./core/set.js";
import { defineSortedSet } from "./core/sorted-set.js";
import { defineStream } from "./core/stream-resource.js";
import type { Codec, RedisKeyPart } from "./core/types.js";

export type {
  InferStandardInput,
  InferStandardOutput,
  StandardSchemaV1
} from "./core/standard-schema.js";
export type {
  Codec,
  InferHashInput,
  InferHashOutput,
  InferInput,
  InferOutput,
  NumberCodec,
  OptionalCodec,
  StringCodec
} from "./core/types.js";

/**
 * Codec: store and read a value as a UTF-8 string. A kv over it carries the
 * string commands (`append`, `getrange`, `setrange`, `strlen`, `lcs`).
 */
export const string = codecs.string;
/**
 * Codec: store a JS number as its decimal string (rejects NaN/Infinity on
 * write). A kv over it carries the counter commands (`incr`, `incrby`,
 * `decr`, `decrby`, `incrbyfloat`).
 */
export const number = codecs.number;
/** Codec: store a boolean as `"1"` / `"0"` (also decodes `"true"` / `"false"`). */
export const boolean = codecs.boolean;
/**
 * Codec: store a value as JSON. Two forms:
 * - `json<Profile>()` — the type is trusted, not validated at runtime.
 * - `json(validator)` — pass any Standard Schema validator (Zod, Valibot,
 *   ArkType, …); reads are validated and the value type is inferred from it.
 */
export const json = codecs.json;
/**
 * Codec: a string field constrained to a fixed set of literals, stored as the
 * plain string and validated on decode.
 * @example
 * ```ts
 * const status = enumOf(["pending", "active", "done"]);
 * //    ^? Codec<"pending" | "active" | "done">
 * ```
 */
export const enumOf = codecs.enumOf;

/**
 * Marks a hash field that a stored record may lack: whole-record reads leave
 * it off (typed `?:`) instead of throwing `PartialRecordError`, and
 * whole-record writes may omit it. Only hash schemas read the marker.
 * @example
 * ```ts
 * const users = hash("user", { name: string(), bio: optional(string()) });
 * ```
 */
export const optional = codecs.optional;

/**
 * A ReplyShapeError, not a bare TypeError: every other decoder attaches the
 * offending value, and the documented recovery is
 * `catch (e) { if (e instanceof ReplyShapeError) quarantine(e.reply) }`.
 */
function bytesShapeError(stored: string): ReplyShapeError {
  return new ReplyShapeError("Expected Redis value to decode to bytes", stored);
}

/** Codec: store a `Uint8Array` as a base64 string. */
export function bytes(): Codec<Uint8Array, Uint8Array> {
  return {
    encode(input) {
      return encodeBase64(input);
    },
    decode(stored) {
      const decoded = decodeBase64(stored);
      if (decoded === undefined) throw bytesShapeError(stored);
      return decoded;
    }
  };
}

/**
 * A key-value schema: one Redis string per id, keyed `prefix:<id>`.
 * @example
 * ```ts
 * const profiles = kv("profile", json<Profile>());
 * await redis.query.profiles.set("42", profile);
 *
 * // The codec decides the extra commands: number() brings the counters.
 * const views = kv("views", number());
 * await redis.query.views.incr("post-1");
 * ```
 */
export const kv = defineKeyspace;
/**
 * A hash schema: object-like data with a per-field codec, keyed `prefix:<id>`.
 * @example
 * ```ts
 * const users = hash("user", { name: string(), score: number() });
 * ```
 */
export const hash = defineHash;
/** A set schema: an unordered collection of unique members with a member codec. */
export const set = defineSet;
/** A list schema: an ordered sequence with an item codec. */
export const list = defineList;
/** A sorted-set schema: members ranked by numeric score, with a member codec. */
export const zset = defineSortedSet;
/** A HyperLogLog schema: probabilistic unique-count over added members. */
export const hll = defineHyperLogLog;
/** A stream schema: an append-only log of entries with per-field codecs. */
export const stream = defineStream;
/** A bitmap schema: bit-addressable flags under one key (takes no codec). */
export const bitmap = defineBitmap;
/** A geo schema: members with longitude/latitude, queryable by radius or box. */
export const geo = defineGeoSet;
/**
 * A pub/sub channel schema: publish/subscribe with a message codec.
 *
 * Reach the channel itself with `redis.query.<name>`, or the per-entity
 * channel `prefix:<id>` with `redis.query.<name>.at(id)` — derived exactly
 * the way a keyspace derives a key, so it pairs with a
 * `pattern("chat:room:*")` subscriber.
 * @example
 * ```ts
 * export const roomEvents = channel("chat:room", json<{ text: string }>());
 * await redis.query.roomEvents.at("42").publish({ text: "hi" });
 * ```
 */
export const channel = definePubSubChannel;
/** A pub/sub pattern schema: subscribe to channels matching a glob pattern. */
export const pattern = definePubSubPattern;

export type { ScriptOptions, ScriptSchema } from "./core/script.js";
/**
 * A Lua script schema with named keys, typed args, and a scalar return codec.
 * Run it with `redis.query.<name>.run({ keys, args })` — the runner loads the
 * script once and executes cached `EVALSHA`.
 */
export { script } from "./core/script.js";
/**
 * A spend budget schema: units per sliding window, with reservations.
 * @example
 * ```ts
 * const tokens = budget("tokens", { limit: 1_000_000, windowMs: 86_400_000 });
 * ```
 */
export { defineBudget as budget } from "./primitives/budget.js";
/**
 * A read-through cache schema with stampede protection.
 * @example
 * ```ts
 * const profiles = cache("profile", { ttlMs: 60_000, codec: json(Profile) });
 * ```
 */
export { defineCache as cache } from "./primitives/cache.js";
/** An idempotency schema: run an effect once per key, replay its result. */
export { defineIdempotency as idempotency } from "./primitives/idempotency.js";
// The primitives declare themselves the same way the data structures do, so a
// cache or a queue is reachable by name through `redis.query` and needs no
// client of its own. `benni/primitives` keeps the client-taking form
// (`cache(client, options)`) for code that holds no handle.
export type {
  BudgetSchema,
  CacheSchema,
  IdempotencySchema,
  LockSchema,
  QueueSchema,
  RatelimitSchema,
  SemaphoreSchema
} from "./primitives/index.js";
/** A distributed lock schema: one holder per id, with lease renewal. */
export { defineLock as lock } from "./primitives/lock.js";
/** A job queue schema: typed payloads, leases, and a resumable output stream. */
export { defineQueue as queue } from "./primitives/queue.js";
/**
 * A sliding-window rate-limit schema.
 * @example
 * ```ts
 * const apiLimit = ratelimit("api", { limit: 10, windowMs: 60_000 });
 * ```
 */
export { defineRatelimit as ratelimit } from "./primitives/ratelimit.js";
/** A semaphore schema: `lock` with a number, for N concurrent holders. */
export { defineSemaphore as semaphore } from "./primitives/semaphore.js";

export type Ids<TIds extends readonly RedisKeyPart[]> = {
  readonly ids: TIds;
};

export function ids<const TIds extends readonly RedisKeyPart[]>(
  values: TIds
): Ids<TIds> {
  return { ids: values };
}

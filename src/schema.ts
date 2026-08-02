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
  InferOutput
} from "./core/types.js";

/** Codec: store and read a value as a UTF-8 string. */
export const string = codecs.string;
/** Codec: store a JS number as its decimal string (rejects NaN/Infinity on write). */
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

// Hand-rolled base64, NOT Uint8Array.toBase64/fromBase64. Those are
// runtime-missing on the Node 24 baseline (present in the type lib but throw at
// runtime — CI caught it); keep this until the minimum Node has them unflagged.
const base64Alphabet =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const base64Values = new Map<string, number>(
  [...base64Alphabet].map((char, index) => [char, index])
);

function encodeBase64(input: Uint8Array): string {
  let encoded = "";
  for (let index = 0; index < input.length; index += 3) {
    const first = input[index];
    const second = index + 1 < input.length ? input[index + 1] : undefined;
    const third = index + 2 < input.length ? input[index + 2] : undefined;
    encoded += base64Alphabet[first >> 2];
    encoded += base64Alphabet[((first & 0b11) << 4) | ((second ?? 0) >> 4)];
    encoded +=
      second === undefined
        ? "="
        : base64Alphabet[((second & 0b1111) << 2) | ((third ?? 0) >> 6)];
    encoded += third === undefined ? "=" : base64Alphabet[third & 0b111111];
  }
  return encoded;
}

/**
 * A ReplyShapeError, not a bare TypeError: every other decoder attaches the
 * offending value, and the documented recovery is
 * `catch (e) { if (e instanceof ReplyShapeError) quarantine(e.reply) }`.
 */
function bytesShapeError(stored: string): ReplyShapeError {
  return new ReplyShapeError("Expected Redis value to decode to bytes", stored);
}

function decodeBase64(stored: string): Uint8Array {
  if (stored === "") return new Uint8Array();
  if (stored.length % 4 !== 0) {
    throw bytesShapeError(stored);
  }
  const padding = stored.endsWith("==") ? 2 : stored.endsWith("=") ? 1 : 0;
  const body = stored.slice(0, stored.length - padding);
  if (body.includes("=")) {
    throw bytesShapeError(stored);
  }
  const decoded = new Uint8Array((stored.length / 4) * 3 - padding);
  let decodedIndex = 0;
  let buffer = 0;
  let bufferedBits = 0;
  for (const char of body) {
    const value = base64Values.get(char);
    if (value === undefined) {
      throw bytesShapeError(stored);
    }
    buffer = (buffer << 6) | value;
    bufferedBits += 6;
    if (bufferedBits >= 8) {
      bufferedBits -= 8;
      decoded[decodedIndex] = (buffer >> bufferedBits) & 0xff;
      decodedIndex += 1;
    }
  }
  return decoded;
}

/** Codec: store a `Uint8Array` as a base64 string. */
export function bytes(): Codec<Uint8Array, Uint8Array> {
  return {
    encode(input) {
      return encodeBase64(input);
    },
    decode(stored) {
      return decodeBase64(stored);
    }
  };
}

/**
 * A key-value schema: one Redis string per id, keyed `prefix:<id>`.
 * @example
 * ```ts
 * const profiles = kv("profile", json<Profile>());
 * await redis.kv(profiles).set("42", profile);
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
 * Reach the channel itself with `redis.pubsub.channel(schema)`, or the
 * per-entity channel `prefix:<id>` with `redis.pubsub.channel(schema, id)` —
 * derived exactly the way a keyspace derives a key, so it pairs with a
 * `pattern("chat:room:*")` subscriber.
 * @example
 * ```ts
 * const roomEvents = channel("chat:room", json<{ text: string }>());
 * await redis.pubsub.channel(roomEvents, "42").publish({ text: "hi" });
 * ```
 */
export const channel = definePubSubChannel;
/** A pub/sub pattern schema: subscribe to channels matching a glob pattern. */
export const pattern = definePubSubPattern;

export type { ScriptOptions, ScriptSchema } from "./core/script.js";
/**
 * A Lua script schema with named keys, typed args, and a scalar return codec.
 * Run it with `redis.script(schema).run({ keys, args })` — the runner loads the
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

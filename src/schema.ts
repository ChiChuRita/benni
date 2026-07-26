import { defineBitmap } from "./core/bitmap.js";
import { codecs } from "./core/codecs.js";
import { defineGeoSet } from "./core/geo.js";
import { defineHyperLogLog } from "./core/hyperloglog.js";
import { definePubSubChannel, definePubSubPattern } from "./core/pubsub.js";
import {
  defineHash,
  defineKeyspace,
  defineList,
  defineSet,
  defineSortedSet
} from "./core/schemas.js";
import { defineScript } from "./core/script.js";
import { defineStream } from "./core/stream.js";
import type {
  Codec,
  FieldCodecs,
  InferHashInput,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply
} from "./core/types.js";

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

function decodeBase64(stored: string): Uint8Array {
  if (stored === "") return new Uint8Array();
  if (stored.length % 4 !== 0) {
    throw new TypeError("Expected Redis value to decode to bytes");
  }
  const padding = stored.endsWith("==") ? 2 : stored.endsWith("=") ? 1 : 0;
  const body = stored.slice(0, stored.length - padding);
  if (body.includes("=")) {
    throw new TypeError("Expected Redis value to decode to bytes");
  }
  const decoded = new Uint8Array((stored.length / 4) * 3 - padding);
  let decodedIndex = 0;
  let buffer = 0;
  let bufferedBits = 0;
  for (const char of body) {
    const value = base64Values.get(char);
    if (value === undefined) {
      throw new TypeError("Expected Redis value to decode to bytes");
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
/** A pub/sub channel schema: publish/subscribe with a message codec. */
export const channel = definePubSubChannel;
/** A pub/sub pattern schema: subscribe to channels matching a glob pattern. */
export const pattern = definePubSubPattern;

export type ScriptSchema<
  TName extends string,
  TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
> = ReturnType<
  typeof defineScript<readonly RedisCommandArgument[], TResult>
> & {
  readonly kind: "script";
  readonly name: TName;
  readonly keys: TKeys;
  readonly args: TArgs;
  encodeArgs(args: InferHashInput<TArgs>): RedisCommandArgument[];
};

export type ScriptOptions<
  TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
> = {
  readonly keys: TKeys;
  readonly args: TArgs;
  readonly returns: Codec<TResult, TResult>;
  readonly lua: string;
};

/**
 * A Lua script schema with named keys, typed args, and a scalar return codec.
 * Run it with `redis.script(schema).run({ keys, args })` — the runner loads the
 * script once and executes cached `EVALSHA`.
 * @example
 * ```ts
 * const rateLimit = script("rate-limit", {
 *   keys: ["counter"],
 *   args: { windowSeconds: number() },
 *   returns: number(),
 *   lua: `local n = redis.call("INCR", KEYS[1])
 *         if n == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
 *         return n`
 * });
 * ```
 */
export function script<
  TName extends string,
  const TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
>(
  name: TName,
  options: ScriptOptions<TKeys, TArgs, TResult>
): ScriptSchema<TName, TKeys, TArgs, TResult> {
  const argNames = Object.keys(options.args) as Array<keyof TArgs & string>;
  const redisScript = defineScript<readonly RedisCommandArgument[], TResult>({
    lua: options.lua,
    keyCount: options.keys.length,
    decode(reply: RedisReply) {
      if (typeof reply !== "string" && typeof reply !== "number") {
        throw new TypeError(
          "Expected Redis script reply to decode from scalar"
        );
      }
      return options.returns.decode(String(reply));
    }
  });
  return {
    ...redisScript,
    kind: "script",
    name,
    keys: options.keys,
    args: options.args,
    encodeArgs(args: InferHashInput<TArgs>) {
      return argNames.map((argName) =>
        options.args[argName].encode(args[argName])
      );
    }
  };
}

export type Ids<TIds extends readonly RedisKeyPart[]> = {
  readonly ids: TIds;
};

export function ids<const TIds extends readonly RedisKeyPart[]>(
  values: TIds
): Ids<TIds> {
  return { ids: values };
}

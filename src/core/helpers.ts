import {
  describeReply,
  ReplyShapeError,
  replyShapeError,
  ValidationError
} from "./errors.js";
import type {
  ListSchema,
  RedisClient,
  RedisCommandArgument,
  RedisReply,
  SetSchema,
  SortedSetEntry,
  SortedSetSchema
} from "./types.js";

export function ttlSeconds(ttl: number): number {
  if (!Number.isSafeInteger(ttl) || ttl < 1) {
    throw new ValidationError("ttlSeconds must be a positive safe integer");
  }
  return ttl;
}

export function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new ValidationError(`${name} must be a positive safe integer`);
  }
  return value;
}

/**
 * The relative/absolute expiry vocabulary shared by GETEX and HGETEX: set a
 * TTL in seconds/milliseconds, an absolute Unix expiry in seconds/ms, or drop
 * the TTL. Exactly one mode must be set — see `expiryArgs`.
 */
export type ExpiryOptions =
  | { readonly ttlSeconds: number }
  | { readonly ttlMilliseconds: number }
  | { readonly expireAtSeconds: number }
  | { readonly expireAtMilliseconds: number }
  | { readonly persist: true };

/**
 * Maps ExpiryOptions to the `EX`/`PX`/`EXAT`/`PXAT`/`PERSIST` argument tokens
 * common to GETEX and HGETEX. Throws unless exactly one mode is provided.
 */
export function expiryArgs(options: ExpiryOptions): RedisCommandArgument[] {
  const modes = options as {
    readonly ttlSeconds?: number;
    readonly ttlMilliseconds?: number;
    readonly expireAtSeconds?: number;
    readonly expireAtMilliseconds?: number;
    readonly persist?: true;
  };
  const provided = [
    modes.ttlSeconds,
    modes.ttlMilliseconds,
    modes.expireAtSeconds,
    modes.expireAtMilliseconds,
    modes.persist
  ].filter((mode) => mode !== undefined);
  if (provided.length !== 1) {
    throw new ValidationError(
      "expiry options must set exactly one of ttlSeconds, ttlMilliseconds, expireAtSeconds, expireAtMilliseconds, or persist"
    );
  }
  if (modes.ttlSeconds !== undefined) {
    return ["EX", ttlSeconds(modes.ttlSeconds)];
  }
  if (modes.ttlMilliseconds !== undefined) {
    return [
      "PX",
      positiveSafeInteger(modes.ttlMilliseconds, "ttlMilliseconds")
    ];
  }
  if (modes.expireAtSeconds !== undefined) {
    return [
      "EXAT",
      positiveSafeInteger(modes.expireAtSeconds, "expireAtSeconds")
    ];
  }
  if (modes.expireAtMilliseconds !== undefined) {
    return [
      "PXAT",
      positiveSafeInteger(modes.expireAtMilliseconds, "expireAtMilliseconds")
    ];
  }
  if (modes.persist !== true) {
    throw new ValidationError("persist must be true");
  }
  return ["PERSIST"];
}

/**
 * Key-level lifecycle ops (EXISTS/TTL/EXPIRE/PERSIST) shared by every keyed
 * store. `key` maps the store's typed id to its concrete Redis key; the ops
 * are spread into each store's return object so a hash, set, or stream key
 * can be probed and expired exactly like a kv key.
 */
export function createKeyLifecycleOps<TId>(
  client: RedisClient,
  key: (id: TId) => string
) {
  return {
    /** EXISTS — whether the key is present. */
    async exists(id: TId): Promise<boolean> {
      const reply = await client.send(["EXISTS", key(id)]);
      if (typeof reply !== "number") {
        throw replyShapeError("EXISTS", "number", reply);
      }
      return reply === 1;
    },
    /** TTL — remaining TTL in seconds (`-1` no expiry, `-2` missing key). */
    async ttl(id: TId): Promise<number> {
      const reply = await client.send(["TTL", key(id)]);
      if (typeof reply !== "number") {
        throw replyShapeError("TTL", "number", reply);
      }
      return reply;
    },
    /** EXPIRE — set a TTL in seconds; `true` if the timeout was set. */
    async expire(id: TId, ttl: number): Promise<boolean> {
      const reply = await client.send(["EXPIRE", key(id), ttlSeconds(ttl)]);
      if (typeof reply !== "number") {
        throw replyShapeError("EXPIRE", "number", reply);
      }
      return reply === 1;
    },
    /** PERSIST — drop the TTL; `true` if an expiry was removed. */
    async persist(id: TId): Promise<boolean> {
      const reply = await client.send(["PERSIST", key(id)]);
      if (typeof reply !== "number") {
        throw replyShapeError("PERSIST", "number", reply);
      }
      return reply === 1;
    }
  };
}

export function expectNumber(reply: RedisReply, command: string): number {
  if (typeof reply !== "number") {
    throw replyShapeError(command, "number", reply);
  }
  return reply;
}

/**
 * {@link expectNumber} for the 64-bit integer replies: refuses a value past
 * `Number.MAX_SAFE_INTEGER` instead of handing back a silently rounded one.
 *
 * Redis counters and bitfields are 64-bit, so their replies can exceed what a
 * JS number holds exactly. The write paths already reject an unsafe integer;
 * a read that rounds is worse, because nothing downstream can tell that the
 * number it got is not the number Redis stores.
 */
export function expectSafeNumber(reply: RedisReply, command: string): number {
  const value = expectNumber(reply, command);
  if (!Number.isSafeInteger(value)) {
    throw new ReplyShapeError(
      `Redis ${command} returned ${String(value)}, which is past ` +
        "Number.MAX_SAFE_INTEGER and cannot be represented exactly as a " +
        "JavaScript number. Read the value with a raw command and handle it " +
        "as a string or bigint.",
      reply
    );
  }
  return value;
}

export function expectNumberLike(reply: RedisReply, command: string): number {
  if (typeof reply !== "string" && typeof reply !== "number") {
    throw replyShapeError(command, "string or number", reply);
  }
  // Redis allows +inf/-inf sorted-set scores and returns them as the bulk
  // strings "inf"/"-inf" on RESP2 (or non-finite doubles on RESP3).
  if (reply === "inf" || reply === "+inf") return Number.POSITIVE_INFINITY;
  if (reply === "-inf") return Number.NEGATIVE_INFINITY;
  if (typeof reply === "number") {
    if (Number.isNaN(reply)) {
      throw replyShapeError(command, "a number", reply);
    }
    return reply;
  }
  const value = Number(reply);
  if (!Number.isFinite(value) || reply.trim() === "") {
    throw replyShapeError(command, "a number", reply);
  }
  return value;
}

export function decodeStringArrayReply<TInput, TOutput>(
  reply: RedisReply,
  command: string,
  schema:
    | SetSchema<TInput, TOutput>
    | ListSchema<TInput, TOutput>
    | SortedSetSchema<TInput, TOutput>
): TOutput[] {
  if (!Array.isArray(reply)) {
    throw replyShapeError(command, "array", reply);
  }
  return reply.map((value) => {
    if (typeof value !== "string") {
      throw new ReplyShapeError(
        `Expected Redis ${command} item to return string, got ${describeReply(value)}`,
        value
      );
    }
    return schema.decode(value);
  });
}

export function decodeStringOrNull<TInput, TOutput>(
  reply: RedisReply,
  command: string,
  schema: ListSchema<TInput, TOutput>
): TOutput | null {
  if (reply === null) return null;
  if (typeof reply !== "string") {
    throw replyShapeError(command, "string or null", reply);
  }
  return schema.decode(reply);
}

export function decodeSortedSetEntries<TInput, TOutput>(
  reply: RedisReply,
  command: string,
  schema: SortedSetSchema<TInput, TOutput>
): Array<SortedSetEntry<TOutput>> {
  if (!Array.isArray(reply)) {
    throw replyShapeError(command, "array", reply);
  }
  if (reply.every((entry) => Array.isArray(entry))) {
    return reply.map((entry) => {
      if (!Array.isArray(entry) || entry.length !== 2) {
        throw replyShapeError(command, "member/score pairs", entry);
      }
      return decodeSortedSetEntry(entry[0], entry[1], command, schema);
    });
  }
  if (reply.length % 2 !== 0) {
    throw replyShapeError(command, "member/score pairs", reply);
  }
  const entries: Array<SortedSetEntry<TOutput>> = [];
  for (let index = 0; index < reply.length; index += 2) {
    entries.push(
      decodeSortedSetEntry(reply[index], reply[index + 1], command, schema)
    );
  }
  return entries;
}

export function decodeOneSortedSetEntry<TInput, TOutput>(
  reply: RedisReply,
  command: string,
  schema: SortedSetSchema<TInput, TOutput>
): SortedSetEntry<TOutput> | null {
  const entries = decodeSortedSetEntries(reply, command, schema);
  return entries[0] ?? null;
}

function decodeSortedSetEntry<TInput, TOutput>(
  member: RedisReply,
  score: RedisReply,
  command: string,
  schema: SortedSetSchema<TInput, TOutput>
): SortedSetEntry<TOutput> {
  if (typeof member !== "string") {
    throw replyShapeError(command, "member/score pairs", member);
  }
  return {
    member: schema.decode(member),
    score: expectNumberLike(score, command)
  };
}

import { replyShapeError, ValidationError } from "./errors.js";
import {
  createKeyLifecycleOps,
  type ExpiryOptions,
  expectNumber,
  expiryArgs,
  ttlSeconds
} from "./helpers.js";
import type { SlotGuard } from "./slot.js";
import type {
  Keyspace,
  RedisClient,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply
} from "./types.js";

/** GETEX expiry modes; shared with HGETEX (see `ExpiryOptions`). */
export type StringGetExOptions = ExpiryOptions;

/** Options for the `IDX` form of `lcs` (match positions instead of the string). */
export type LcsIdxOptions = {
  readonly idx: true;
  /** Drop matches shorter than this (`MINMATCHLEN`). */
  readonly minMatchLen?: number;
  /** Include each match's length in the result (`WITHMATCHLEN`). */
  readonly withMatchLen?: boolean;
};

export type LcsOptions = { readonly len: true } | LcsIdxOptions;

/** A single LCS match: an inclusive `[start, end]` range in each string. */
export type LcsMatch = {
  readonly a: readonly [number, number];
  readonly b: readonly [number, number];
  /** Only present when `withMatchLen` is set. */
  readonly length?: number;
};

/** The `LCS ... IDX` result: the match ranges and the total LCS length. */
export type LcsIdxResult = {
  readonly matches: LcsMatch[];
  readonly length: number;
};

function lcsRange(range: RedisReply): readonly [number, number] {
  if (!Array.isArray(range) || range.length !== 2) {
    throw replyShapeError("LCS IDX range", "a start/end pair", range);
  }
  return [expectNumber(range[0], "LCS"), expectNumber(range[1], "LCS")];
}

function lcsMatch(match: RedisReply): LcsMatch {
  if (!Array.isArray(match) || match.length < 2) {
    throw replyShapeError("LCS IDX match", "range pairs", match);
  }
  const a = lcsRange(match[0]);
  const b = lcsRange(match[1]);
  return match.length >= 3
    ? { a, b, length: expectNumber(match[2], "LCS") }
    : { a, b };
}

function decodeLcsIdx(reply: RedisReply): LcsIdxResult {
  let matches: RedisReply = null;
  let len: RedisReply = null;
  if (reply instanceof Map) {
    matches = reply.get("matches") ?? null;
    len = reply.get("len") ?? null;
  } else if (Array.isArray(reply) && reply.length % 2 === 0) {
    for (let index = 0; index < reply.length; index += 2) {
      if (reply[index] === "matches") matches = reply[index + 1];
      else if (reply[index] === "len") len = reply[index + 1];
    }
  } else {
    throw replyShapeError("LCS IDX", "an array or map", reply);
  }
  if (!Array.isArray(matches)) {
    throw replyShapeError("LCS IDX matches", "an array", matches);
  }
  return {
    matches: matches.map((match) => lcsMatch(match)),
    length: expectNumber(len, "LCS")
  };
}

export function createStringStore<TId extends RedisKeyPart = RedisKeyPart>(
  client: RedisClient,
  keyspace: Keyspace<string, string, string, TId>,
  assertSameSlot?: SlotGuard
) {
  // LCS compares two keys of this same keyspace. The reply shape depends on the
  // option: the subsequence string, its length (LEN), or the match ranges (IDX).
  function lcs(a: TId, b: TId): Promise<string>;
  function lcs(a: TId, b: TId, options: { len: true }): Promise<number>;
  function lcs(a: TId, b: TId, options: LcsIdxOptions): Promise<LcsIdxResult>;
  async function lcs(
    a: TId,
    b: TId,
    options?: LcsOptions
  ): Promise<string | number | LcsIdxResult> {
    const keyA = keyspace.key(a);
    const keyB = keyspace.key(b);
    // LCS is a two-key command, so it needs the same guard every other
    // multi-key command gets: on a cluster the server would otherwise reject
    // it with a raw CROSSSLOT, and on a single node it would pass and only
    // break in production.
    assertSameSlot?.("LCS", [keyA, keyB], keyspace);
    const command: [string, ...RedisCommandArgument[]] = ["LCS", keyA, keyB];
    if (options !== undefined && "len" in options) {
      command.push("LEN");
      return expectNumber(await client.send(command), "LCS");
    }
    if (options?.idx) {
      command.push("IDX");
      if (options.minMatchLen !== undefined) {
        if (
          !Number.isSafeInteger(options.minMatchLen) ||
          options.minMatchLen < 0
        ) {
          throw new ValidationError(
            "minMatchLen must be a non-negative safe integer"
          );
        }
        command.push("MINMATCHLEN", options.minMatchLen);
      }
      if (options.withMatchLen) command.push("WITHMATCHLEN");
      return decodeLcsIdx(await client.send(command));
    }
    const reply = await client.send(command);
    if (typeof reply !== "string") {
      throw replyShapeError("LCS", "string", reply);
    }
    return reply;
  }

  return {
    ...createKeyLifecycleOps(client, (id: TId) => keyspace.key(id)),
    /**
     * APPEND — append `value` to the string (creating it if missing);
     * returns the new length.
     * @example await redis.string(logs).append("today", "line\n");
     */
    async append(id: TId, value: string): Promise<number> {
      return expectNumber(
        await client.send(["APPEND", keyspace.key(id), value]),
        "APPEND"
      );
    },
    /** GETRANGE — substring in `[start, end]` (inclusive; negatives count from the end). */
    async getrange(id: TId, start: number, end: number): Promise<string> {
      // Negative indexes are valid (count from the end), but the values must
      // be integers — catch NaN/floats here instead of as a server error.
      if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end)) {
        throw new ValidationError(
          "getrange start and end must be safe integers"
        );
      }
      const reply = await client.send([
        "GETRANGE",
        keyspace.key(id),
        start,
        end
      ]);
      if (typeof reply !== "string") {
        throw replyShapeError("GETRANGE", "string", reply);
      }
      return reply;
    },
    /** SETRANGE — overwrite the string from `offset`; returns the new length. */
    async setrange(id: TId, offset: number, value: string): Promise<number> {
      if (!Number.isSafeInteger(offset) || offset < 0) {
        throw new ValidationError("offset must be a non-negative safe integer");
      }
      return expectNumber(
        await client.send(["SETRANGE", keyspace.key(id), offset, value]),
        "SETRANGE"
      );
    },
    /** STRLEN — length of the string (0 if the key is missing). */
    async strlen(id: TId): Promise<number> {
      return expectNumber(
        await client.send(["STRLEN", keyspace.key(id)]),
        "STRLEN"
      );
    },
    /**
     * GETEX — read the value while (re)setting its expiry: a bare number of
     * seconds, or any `ExpiryOptions` mode. `null` if the key is missing.
     */
    async getex(
      id: TId,
      ttlOrOptions: number | StringGetExOptions
    ): Promise<string | null> {
      const args =
        typeof ttlOrOptions === "number"
          ? ["EX", ttlSeconds(ttlOrOptions)]
          : expiryArgs(ttlOrOptions);
      const reply = await client.send(["GETEX", keyspace.key(id), ...args]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("GETEX", "string or null", reply);
      }
      return reply;
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", keyspace.key(id)]), "DEL");
    },
    lcs
  };
}

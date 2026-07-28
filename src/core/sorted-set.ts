import { replyShapeError, ValidationError } from "./errors.js";
import {
  createKeyLifecycleOps,
  decodeOneSortedSetEntry,
  decodeSortedSetEntries,
  decodeStringArrayReply,
  expectNumber,
  expectNumberLike
} from "./helpers.js";
import { type HashTagLayout, type KeyOptions, keyBuilder } from "./keys.js";
import { type BlockingWait, blockingTimeoutSeconds } from "./session.js";
import type { SlotGuard, SlotHint } from "./slot.js";
import {
  type StoreBinding,
  type StoreContext,
  withKey,
  withStore
} from "./store.js";
import type {
  Codec,
  RedisClient,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply,
  SortedSetEntry,
  SortedSetSchema
} from "./types.js";

export type SortedSetScoreBound = number | "-inf" | "+inf" | `(${number}`;

export type SortedSetLimit =
  | { readonly offset: number; readonly count: number }
  | { readonly offset?: undefined; readonly count?: undefined };

export type SortedSetRangeByScoreOptions = {
  readonly min: SortedSetScoreBound;
  readonly max: SortedSetScoreBound;
  readonly rev?: boolean;
} & SortedSetLimit;

/**
 * Discriminated union of range selectors for {@link createSortedSetStore}'s
 * `zrange`: by index `{ start, stop }`, by score `{ byScore: true, min, max }`,
 * or by lex `{ byLex: true, min, max }`. The index and score variants may set
 * `withScores` to switch the return type from members to `SortedSetEntry`s;
 * the lex variant may not, because Redis rejects `BYLEX WITHSCORES`. By-index
 * also accepts `rev`. Score/lex variants carry `rev` and `LIMIT offset/count` via
 * their respective option types.
 */
export type SortedSetRangeOptions<TInput> =
  | {
      readonly start: number;
      readonly stop: number;
      readonly rev?: boolean;
      readonly withScores?: boolean;
      readonly byScore?: undefined;
      readonly byLex?: undefined;
    }
  | ({
      readonly byScore: true;
      readonly byLex?: undefined;
      readonly withScores?: boolean;
    } & SortedSetRangeByScoreOptions)
  | ({
      readonly byLex: true;
      readonly byScore?: undefined;
      /** Not available with BYLEX: Redis rejects the combination outright. */
      readonly withScores?: undefined;
    } & SortedSetRangeByLexOptions<TInput>);

/**
 * A lexicographic range bound over the member type. `"-"` and `"+"` are the
 * min and max sentinels; `{ value }` is an inclusive bound (`[`) and
 * `{ value, inclusive: false }` an exclusive one (`(`). Lex ranges assume the
 * sorted set's scores are all equal, matching Redis's `BYLEX` semantics.
 */
export type SortedSetLexBound<TInput> =
  | "-"
  | "+"
  | { readonly value: TInput; readonly inclusive?: boolean };

export type SortedSetRangeByLexOptions<TInput> = {
  readonly min: SortedSetLexBound<TInput>;
  readonly max: SortedSetLexBound<TInput>;
  readonly rev?: boolean;
} & SortedSetLimit;

export type SortedSetRangeStoreOptions<TInput> =
  | {
      readonly start: number;
      readonly stop: number;
      readonly byScore?: undefined;
      readonly byLex?: undefined;
    }
  | ({
      readonly byScore: true;
      readonly byLex?: undefined;
    } & SortedSetRangeByScoreOptions)
  | ({
      readonly byLex: true;
      readonly byScore?: undefined;
    } & SortedSetRangeByLexOptions<TInput>);

export type SortedSetAggregate = "sum" | "min" | "max";

export type SortedSetCombineOptions = {
  readonly weights?: readonly number[];
  readonly aggregate?: SortedSetAggregate;
};

export type SortedSetIntersectionSizeOptions = {
  readonly limit?: number;
};

export type SortedSetMultiPopOptions = {
  readonly count?: number;
};

export type SortedSetPopOptions = {
  readonly count?: number;
};

export type SortedSetRandomMemberOptions = {
  readonly count?: number;
  readonly withScores?: boolean;
};

export type SortedSetPopEnd = { readonly min: true } | { readonly max: true };

/**
 * `ZADD` conditions, mirroring the Redis tokens. `nx` excludes `xx`, `gt`,
 * and `lt`; `gt` excludes `lt` — invalid combinations don't compile.
 */
export type SortedSetAddOptions = { readonly ch?: boolean } & (
  | {
      readonly nx?: boolean;
      readonly xx?: never;
      readonly gt?: never;
      readonly lt?: never;
    }
  | {
      readonly xx?: boolean;
      readonly gt?: boolean;
      readonly lt?: never;
      readonly nx?: never;
    }
  | {
      readonly xx?: boolean;
      readonly lt?: boolean;
      readonly gt?: never;
      readonly nx?: never;
    }
);

type SortedSetCommandArg = string | number;

function rankIndex(value: number, label: string): number {
  if (!Number.isSafeInteger(value)) {
    throw new ValidationError(`${label} must be a safe integer`);
  }
  return value;
}

function scoreBound(
  value: SortedSetScoreBound,
  label: string
): SortedSetScoreBound {
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new ValidationError(`${label} must be a finite number`);
  }
  return value;
}

function randomMemberCount(count: number): number {
  if (!Number.isSafeInteger(count) || count === 0) {
    throw new ValidationError("count must be a nonzero safe integer");
  }
  return count;
}

// Redis accepts +inf/-inf scores (its canonical wire spelling), but NaN is
// never a valid float — reject it before it becomes a server error.
function scoreArgument(score: number): number | string {
  if (Number.isNaN(score)) {
    throw new ValidationError("score must not be NaN");
  }
  if (score === Number.POSITIVE_INFINITY) return "+inf";
  if (score === Number.NEGATIVE_INFINITY) return "-inf";
  return score;
}

function popManyCount(count: number): number {
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new ValidationError("count must be a nonnegative safe integer");
  }
  return count;
}

function multiPopCount(count: number): number {
  if (!Number.isSafeInteger(count) || count < 1) {
    throw new ValidationError("count must be a positive safe integer");
  }
  return count;
}

/**
 * Per-call map of `schema.key(id)` -> typed id, used to attribute the
 * answering key of a multi-key reply back to the caller's id via exact
 * string equality (no prefix parsing). Throws on an empty ids list before
 * any command is sent.
 */
function requestedKeyIds<TId>(
  ids: readonly TId[],
  key: (id: TId) => string,
  command: string,
  assertSameSlot?: SlotGuard,
  hint?: SlotHint
): Map<string, TId> {
  if (ids.length === 0) {
    throw new ValidationError("ids must contain at least one id");
  }
  const idsByKey = new Map<string, TId>();
  for (const id of ids) idsByKey.set(key(id), id);
  // Guarding the map keys rather than `ids` also de-duplicates, which is
  // right: blpop(["a", "a", "b"]) only has two distinct slots to check.
  assertSameSlot?.(command, [...idsByKey.keys()], hint);
  return idsByKey;
}

function attributeReplyKey<TId>(
  key: RedisReply,
  command: string,
  idsByKey: ReadonlyMap<string, TId>
): TId {
  if (typeof key !== "string") {
    throw replyShapeError(command, "string key", key);
  }
  const id = idsByKey.get(key);
  if (id === undefined) {
    throw replyShapeError(command, "one of the requested keys", key);
  }
  return id;
}

function limitArgs(options: SortedSetLimit): SortedSetCommandArg[] {
  if ((options.offset === undefined) !== (options.count === undefined)) {
    throw new ValidationError("offset and count must be provided together");
  }
  if (options.offset === undefined || options.count === undefined) {
    return [];
  }
  if (!Number.isSafeInteger(options.offset) || options.offset < 0) {
    throw new ValidationError("offset must be a nonnegative safe integer");
  }
  if (!Number.isSafeInteger(options.count)) {
    throw new ValidationError("count must be a safe integer");
  }
  return ["LIMIT", options.offset, options.count];
}

function byScoreArgs(
  options: SortedSetRangeByScoreOptions
): SortedSetCommandArg[] {
  const min = scoreBound(options.min, "min");
  const max = scoreBound(options.max, "max");
  const args: SortedSetCommandArg[] =
    options.rev === true ? [max, min, "BYSCORE", "REV"] : [min, max, "BYSCORE"];
  args.push(...limitArgs(options));
  return args;
}

function lexBoundArg<TInput>(
  bound: SortedSetLexBound<TInput>,
  encode: (value: TInput) => string,
  label: string
): string {
  if (bound === "-" || bound === "+") return bound;
  if (typeof bound !== "object" || bound === null || !("value" in bound)) {
    throw new ValidationError(`${label} must be "-", "+", or { value }`);
  }
  return `${bound.inclusive === false ? "(" : "["}${encode(bound.value)}`;
}

function byLexArgs<TInput>(
  options: SortedSetRangeByLexOptions<TInput>,
  encode: (value: TInput) => string
): SortedSetCommandArg[] {
  const min = lexBoundArg(options.min, encode, "min");
  const max = lexBoundArg(options.max, encode, "max");
  const args: SortedSetCommandArg[] =
    options.rev === true ? [max, min, "BYLEX", "REV"] : [min, max, "BYLEX"];
  args.push(...limitArgs(options));
  return args;
}

function rangeSelectorArgs<TInput>(
  options: SortedSetRangeOptions<TInput>,
  encode: (value: TInput) => string
): SortedSetCommandArg[] {
  if (options.byScore === true) return byScoreArgs(options);
  if (options.byLex === true) {
    // The type forbids it, but a JavaScript caller can still get here, and
    // Redis answers a bare "ERR syntax error" that says nothing about which
    // pair is illegal.
    if ((options as { withScores?: unknown }).withScores === true) {
      throw new ValidationError(
        "withScores is not available with byLex: Redis rejects ZRANGE ... BYLEX WITHSCORES. Lex ranges order by member, so the scores are all equal anyway."
      );
    }
    return byLexArgs(options, encode);
  }
  const args: SortedSetCommandArg[] = [
    rankIndex(options.start, "start"),
    rankIndex(options.stop, "stop")
  ];
  if (options.rev === true) args.push("REV");
  return args;
}

function combineArgs(
  keys: readonly string[],
  options: SortedSetCombineOptions | undefined
): SortedSetCommandArg[] {
  const args: SortedSetCommandArg[] = [keys.length, ...keys];
  if (options?.weights !== undefined) {
    if (options.weights.length !== keys.length) {
      throw new ValidationError("weights length must match the number of keys");
    }
    for (const weight of options.weights) {
      if (!Number.isFinite(weight)) {
        throw new ValidationError("weights must be finite numbers");
      }
    }
    args.push("WEIGHTS", ...options.weights);
  }
  if (options?.aggregate !== undefined) {
    args.push("AGGREGATE", options.aggregate.toUpperCase());
  }
  return args;
}

export function createSortedSetStore<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: SortedSetSchema<TInput, TOutput, string, TId>,
  assertSameSlot?: SlotGuard
) {
  /**
   * The key list every multi-key zset command sends, and the shared point
   * where the cluster guard sees it. `command` names the caller for the error.
   */
  const combinedKeys = (
    command: string,
    id: TId,
    others: readonly TId[]
  ): string[] => {
    const keys = [schema.key(id), ...others.map((other) => schema.key(other))];
    assertSameSlot?.(command, keys, schema);
    return keys;
  };

  /**
   * A `*STORE` destination is a key too. Comparing it against the first source
   * is enough: combinedKeys has already proven the sources mutually same-slot,
   * so transitivity closes the set.
   */
  const storeTarget = (command: string, destination: TId, source: string) => {
    const target = schema.key(destination);
    assertSameSlot?.(command, [target, source], schema);
    return target;
  };
  async function popFrom<TPick extends TId>(
    end: "MIN" | "MAX",
    ids: readonly TPick[],
    options: SortedSetMultiPopOptions
  ): Promise<{ id: TPick; entries: Array<SortedSetEntry<TOutput>> } | null> {
    const idsByKey = requestedKeyIds(
      ids,
      (id) => schema.key(id),
      "ZMPOP",
      assertSameSlot,
      schema
    );
    const command: [string, ...RedisCommandArgument[]] = [
      "ZMPOP",
      ids.length,
      ...ids.map((id) => schema.key(id)),
      end
    ];
    if (options.count !== undefined) {
      command.push("COUNT", multiPopCount(options.count));
    }
    const reply = await client.send(command);
    if (reply === null) return null;
    if (!Array.isArray(reply) || reply.length !== 2) {
      throw replyShapeError("ZMPOP", "key/entries pair or null", reply);
    }
    return {
      id: attributeReplyKey(reply[0], "ZMPOP", idsByKey),
      entries: decodeSortedSetEntries(reply[1], "ZMPOP", schema)
    };
  }

  async function zrange(
    id: TId,
    options: SortedSetRangeOptions<TInput> & { withScores: true }
  ): Promise<Array<SortedSetEntry<TOutput>>>;
  async function zrange(
    id: TId,
    options: SortedSetRangeOptions<TInput>
  ): Promise<TOutput[]>;
  async function zrange(
    id: TId,
    options: SortedSetRangeOptions<TInput>
  ): Promise<TOutput[] | Array<SortedSetEntry<TOutput>>> {
    const args = rangeSelectorArgs(options, (value) => schema.encode(value));
    if (options.withScores === true) {
      const reply = await client.send([
        "ZRANGE",
        schema.key(id),
        ...args,
        "WITHSCORES"
      ]);
      return decodeSortedSetEntries(reply, "ZRANGE", schema);
    }
    return decodeStringArrayReply(
      await client.send(["ZRANGE", schema.key(id), ...args]),
      "ZRANGE",
      schema
    );
  }

  async function popEnd(
    command: "ZPOPMIN" | "ZPOPMAX",
    id: TId,
    options: SortedSetPopOptions
  ): Promise<SortedSetEntry<TOutput> | null | Array<SortedSetEntry<TOutput>>> {
    if (options.count === undefined) {
      const reply = await client.send([command, schema.key(id)]);
      return decodeOneSortedSetEntry(reply, command, schema);
    }
    if (popManyCount(options.count) === 0) return [];
    const reply = await client.send([command, schema.key(id), options.count]);
    return decodeSortedSetEntries(reply, command, schema);
  }

  function zpopmin(
    id: TId,
    options: { count: number }
  ): Promise<Array<SortedSetEntry<TOutput>>>;
  function zpopmin(
    id: TId,
    // Excludes count: the reply shape depends on whether it is present, so a
    // value typed SortedSetPopOptions (count optional) cannot be typed here.
    // It used to land on this overload and be typed as a single entry while
    // the server returned an array.
    options?: SortedSetPopOptions & { readonly count?: undefined }
  ): Promise<SortedSetEntry<TOutput> | null>;
  function zpopmin(
    id: TId,
    options: SortedSetPopOptions = {}
  ): Promise<SortedSetEntry<TOutput> | null | Array<SortedSetEntry<TOutput>>> {
    return popEnd("ZPOPMIN", id, options);
  }

  function zpopmax(
    id: TId,
    options: { count: number }
  ): Promise<Array<SortedSetEntry<TOutput>>>;
  function zpopmax(
    id: TId,
    // Excludes count: the reply shape depends on whether it is present, so a
    // value typed SortedSetPopOptions (count optional) cannot be typed here.
    // It used to land on this overload and be typed as a single entry while
    // the server returned an array.
    options?: SortedSetPopOptions & { readonly count?: undefined }
  ): Promise<SortedSetEntry<TOutput> | null>;
  function zpopmax(
    id: TId,
    options: SortedSetPopOptions = {}
  ): Promise<SortedSetEntry<TOutput> | null | Array<SortedSetEntry<TOutput>>> {
    return popEnd("ZPOPMAX", id, options);
  }

  async function combine(
    command: "ZUNION" | "ZINTER" | "ZDIFF",
    id: TId,
    others: readonly TId[],
    options: SortedSetCombineOptions & { withScores?: boolean }
  ): Promise<TOutput[] | Array<SortedSetEntry<TOutput>>> {
    // ZDIFF takes no WEIGHTS/AGGREGATE; combineArgs ignores them when the
    // caller passes none, which is the only shape zdiff forwards.
    const args = combineArgs(
      combinedKeys(command, id, others),
      command === "ZDIFF" ? undefined : options
    );
    if (options.withScores === true) {
      const reply = await client.send([command, ...args, "WITHSCORES"]);
      return decodeSortedSetEntries(reply, command, schema);
    }
    return decodeStringArrayReply(
      await client.send([command, ...args]),
      command,
      schema
    );
  }

  function zunion(
    id: TId,
    others: readonly TId[],
    options: SortedSetCombineOptions & { withScores: true }
  ): Promise<Array<SortedSetEntry<TOutput>>>;
  function zunion(
    id: TId,
    others: readonly TId[],
    options?: SortedSetCombineOptions
  ): Promise<TOutput[]>;
  function zunion(
    id: TId,
    others: readonly TId[],
    options: SortedSetCombineOptions & { withScores?: boolean } = {}
  ): Promise<TOutput[] | Array<SortedSetEntry<TOutput>>> {
    return combine("ZUNION", id, others, options);
  }

  function zinter(
    id: TId,
    others: readonly TId[],
    options: SortedSetCombineOptions & { withScores: true }
  ): Promise<Array<SortedSetEntry<TOutput>>>;
  function zinter(
    id: TId,
    others: readonly TId[],
    options?: SortedSetCombineOptions
  ): Promise<TOutput[]>;
  function zinter(
    id: TId,
    others: readonly TId[],
    options: SortedSetCombineOptions & { withScores?: boolean } = {}
  ): Promise<TOutput[] | Array<SortedSetEntry<TOutput>>> {
    return combine("ZINTER", id, others, options);
  }

  function zdiff(
    id: TId,
    others: readonly TId[],
    options: { withScores: true }
  ): Promise<Array<SortedSetEntry<TOutput>>>;
  function zdiff(
    id: TId,
    others: readonly TId[],
    options?: { withScores?: boolean }
  ): Promise<TOutput[]>;
  function zdiff(
    id: TId,
    others: readonly TId[],
    options: { withScores?: boolean } = {}
  ): Promise<TOutput[] | Array<SortedSetEntry<TOutput>>> {
    return combine("ZDIFF", id, others, options);
  }

  async function zrandmember(
    id: TId,
    options: SortedSetRandomMemberOptions & { count: number; withScores: true }
  ): Promise<Array<SortedSetEntry<TOutput>>>;
  async function zrandmember(
    id: TId,
    options: SortedSetRandomMemberOptions & { count: number }
  ): Promise<TOutput[]>;
  async function zrandmember(
    id: TId,
    options?: SortedSetRandomMemberOptions
  ): Promise<TOutput | null>;
  async function zrandmember(
    id: TId,
    options: SortedSetRandomMemberOptions = {}
  ): Promise<TOutput | null | TOutput[] | Array<SortedSetEntry<TOutput>>> {
    if (options.count === undefined) {
      if (options.withScores === true) {
        // Silently dropping the flag would hand back a member when the
        // caller asked for scores; WITHSCORES requires a count.
        throw new ValidationError("zrandmember withScores requires count");
      }
      const reply = await client.send(["ZRANDMEMBER", schema.key(id)]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("ZRANDMEMBER", "string or null", reply);
      }
      return schema.decode(reply);
    }
    const count = randomMemberCount(options.count);
    if (options.withScores === true) {
      const reply = await client.send([
        "ZRANDMEMBER",
        schema.key(id),
        count,
        "WITHSCORES"
      ]);
      return decodeSortedSetEntries(reply, "ZRANDMEMBER", schema);
    }
    return decodeStringArrayReply(
      await client.send(["ZRANDMEMBER", schema.key(id), count]),
      "ZRANDMEMBER",
      schema
    );
  }

  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    /**
     * `ZADD`. Adds one entry or several; returns the number of newly added
     * members (of *changed* members with `{ ch: true }`). Conditions mirror
     * the Redis tokens: `nx`/`xx` (add-only/update-only) and `gt`/`lt`
     * (update only when the new score is greater/less). No-op returning `0`
     * for an empty list.
     *
     * @example await redis.zset(board).zadd("global", { score: 10, member: "ada" });
     * @example await redis.zset(board).zadd("global", entries, { gt: true, ch: true });
     */
    async zadd(
      id: TId,
      entries: SortedSetEntry<TInput> | readonly SortedSetEntry<TInput>[],
      options?: SortedSetAddOptions
    ): Promise<number> {
      const list = Array.isArray(entries)
        ? (entries as readonly SortedSetEntry<TInput>[])
        : [entries as SortedSetEntry<TInput>];
      if (list.length === 0) return 0;
      const args: SortedSetCommandArg[] = [];
      // The types forbid the invalid pairs; guard for untyped JS callers.
      if (options?.nx && options?.xx) {
        throw new ValidationError("zadd cannot set both nx and xx");
      }
      if (options?.gt && options?.lt) {
        throw new ValidationError("zadd cannot set both gt and lt");
      }
      if (options?.nx && (options?.gt || options?.lt)) {
        throw new ValidationError("zadd cannot combine nx with gt/lt");
      }
      if (options?.nx) args.push("NX");
      if (options?.xx) args.push("XX");
      if (options?.gt) args.push("GT");
      if (options?.lt) args.push("LT");
      if (options?.ch) args.push("CH");
      return expectNumber(
        await client.send([
          "ZADD",
          schema.key(id),
          ...args,
          ...list.flatMap((entry) => [
            scoreArgument(entry.score),
            schema.encode(entry.member)
          ])
        ]),
        "ZADD"
      );
    },
    /** `ZSCORE`. The score of `member`, or `null` if absent. */
    async zscore(id: TId, member: TInput): Promise<number | null> {
      const reply = await client.send([
        "ZSCORE",
        schema.key(id),
        schema.encode(member)
      ]);
      if (reply === null) return null;
      return expectNumberLike(reply, "ZSCORE");
    },
    /** `ZRANK`. Zero-based rank of `member` (low→high), or `null` if absent. */
    async zrank(id: TId, member: TInput): Promise<number | null> {
      const reply = await client.send([
        "ZRANK",
        schema.key(id),
        schema.encode(member)
      ]);
      if (reply === null) return null;
      return expectNumber(reply, "ZRANK");
    },
    /** `ZCARD`. Number of members in the sorted set. */
    async zcard(id: TId): Promise<number> {
      return expectNumber(
        await client.send(["ZCARD", schema.key(id)]),
        "ZCARD"
      );
    },
    /** `ZCOUNT`. Number of members with score between `min` and `max`. */
    async zcount(
      id: TId,
      min: number | string,
      max: number | string
    ): Promise<number> {
      return expectNumber(
        await client.send(["ZCOUNT", schema.key(id), min, max]),
        "ZCOUNT"
      );
    },
    /**
     * `ZRANGE`. Reads a range by index (`{ start, stop, rev? }`), by score
     * (`{ byScore: true, min, max, ... }`), or by lex
     * (`{ byLex: true, min, max, ... }`). With `withScores: true` returns
     * `SortedSetEntry`s; otherwise members only.
     *
     * @example
     * await set.zrange(id, { start: 0, stop: -1 });
     * await set.zrange(id, { start: 0, stop: -1, rev: true });
     * await set.zrange(id, { byScore: true, min: "-inf", max: "+inf", withScores: true });
     * await set.zrange(id, { byLex: true, min: "-", max: "+" });
     */
    zrange,
    /** `ZREM`. Removes the given members; returns how many were removed. */
    async zrem(id: TId, members: readonly TInput[]): Promise<number> {
      if (members.length === 0) return 0;
      return expectNumber(
        await client.send([
          "ZREM",
          schema.key(id),
          ...members.map((member) => schema.encode(member))
        ]),
        "ZREM"
      );
    },
    /** `ZINCRBY`. Adds `amount` to `member`'s score; returns the new score. */
    async zincrby(id: TId, amount: number, member: TInput): Promise<number> {
      const reply = await client.send([
        "ZINCRBY",
        schema.key(id),
        scoreArgument(amount),
        schema.encode(member)
      ]);
      return expectNumberLike(reply, "ZINCRBY");
    },
    /**
     * `ZPOPMIN`. Without `count`, pops and returns the single lowest-scored
     * entry (or `null` if empty). With `count`, pops up to that many and
     * returns them as an array (empty when `count` is `0`).
     */
    zpopmin,
    /**
     * `ZPOPMAX`. Without `count`, pops and returns the single highest-scored
     * entry (or `null` if empty). With `count`, pops up to that many and
     * returns them as an array (empty when `count` is `0`).
     */
    zpopmax,
    /** `ZREVRANK`. Zero-based rank of `member` (high→low), or `null`. */
    async zrevrank(id: TId, member: TInput): Promise<number | null> {
      const reply = await client.send([
        "ZREVRANK",
        schema.key(id),
        schema.encode(member)
      ]);
      if (reply === null) return null;
      return expectNumber(reply, "ZREVRANK");
    },
    /**
     * `ZMSCORE`. Scores of the given members in order; `null` per absent
     * member. Empty input returns `[]` without a round trip.
     */
    async zmscore(
      id: TId,
      members: readonly TInput[]
    ): Promise<Array<number | null>> {
      if (members.length === 0) return [];
      const reply = await client.send([
        "ZMSCORE",
        schema.key(id),
        ...members.map((member) => schema.encode(member))
      ]);
      if (!Array.isArray(reply)) {
        throw replyShapeError("ZMSCORE", "array", reply);
      }
      return reply.map((value) =>
        value === null ? null : expectNumberLike(value, "ZMSCORE")
      );
    },
    /**
     * `ZRANDMEMBER`. Without `count`, returns one random member (or `null`).
     * With `count`, returns an array of members; add `withScores: true` to get
     * `SortedSetEntry`s instead.
     *
     * @example
     * await set.zrandmember(id);
     * await set.zrandmember(id, { count: 3 });
     * await set.zrandmember(id, { count: 3, withScores: true });
     */
    zrandmember,
    /** `ZLEXCOUNT`. Number of members in the lexicographic range. */
    async zlexcount(
      id: TId,
      min: SortedSetLexBound<TInput>,
      max: SortedSetLexBound<TInput>
    ): Promise<number> {
      const encode = (value: TInput) => schema.encode(value);
      return expectNumber(
        await client.send([
          "ZLEXCOUNT",
          schema.key(id),
          lexBoundArg(min, encode, "min"),
          lexBoundArg(max, encode, "max")
        ]),
        "ZLEXCOUNT"
      );
    },
    /**
     * `ZRANGESTORE`. Stores a range (by index/score/lex, per the options
     * union) from `source` into `destination`; returns the resulting
     * cardinality.
     */
    async zrangestore(
      destination: TId,
      source: TId,
      options: SortedSetRangeStoreOptions<TInput>
    ): Promise<number> {
      let args: SortedSetCommandArg[];
      if (options.byScore === true) {
        args = byScoreArgs(options);
      } else if (options.byLex === true) {
        args = byLexArgs(options, (value) => schema.encode(value));
      } else {
        args = [
          rankIndex(options.start, "start"),
          rankIndex(options.stop, "stop")
        ];
      }
      const from = schema.key(source);
      return expectNumber(
        await client.send([
          "ZRANGESTORE",
          storeTarget("ZRANGESTORE", destination, from),
          from,
          ...args
        ]),
        "ZRANGESTORE"
      );
    },
    /** `ZREMRANGEBYRANK`. Removes members in the index range; returns count. */
    async zremrangebyrank(
      id: TId,
      start: number,
      stop: number
    ): Promise<number> {
      return expectNumber(
        await client.send([
          "ZREMRANGEBYRANK",
          schema.key(id),
          rankIndex(start, "start"),
          rankIndex(stop, "stop")
        ]),
        "ZREMRANGEBYRANK"
      );
    },
    /** `ZREMRANGEBYSCORE`. Removes members in the score range; returns count. */
    async zremrangebyscore(
      id: TId,
      min: SortedSetScoreBound,
      max: SortedSetScoreBound
    ): Promise<number> {
      return expectNumber(
        await client.send([
          "ZREMRANGEBYSCORE",
          schema.key(id),
          scoreBound(min, "min"),
          scoreBound(max, "max")
        ]),
        "ZREMRANGEBYSCORE"
      );
    },
    /** `ZREMRANGEBYLEX`. Removes members in the lex range; returns count. */
    async zremrangebylex(
      id: TId,
      min: SortedSetLexBound<TInput>,
      max: SortedSetLexBound<TInput>
    ): Promise<number> {
      const encode = (value: TInput) => schema.encode(value);
      return expectNumber(
        await client.send([
          "ZREMRANGEBYLEX",
          schema.key(id),
          lexBoundArg(min, encode, "min"),
          lexBoundArg(max, encode, "max")
        ]),
        "ZREMRANGEBYLEX"
      );
    },
    /**
     * `ZUNION`. Union of `id` with `others`. Without `withScores`, returns
     * members; with `withScores: true`, returns `SortedSetEntry`s. Honors
     * `weights`/`aggregate`.
     */
    zunion,
    /**
     * `ZINTER`. Intersection of `id` with `others`. Without `withScores`,
     * returns members; with `withScores: true`, returns `SortedSetEntry`s.
     * Honors `weights`/`aggregate`.
     */
    zinter,
    /**
     * `ZDIFF`. Difference of `id` minus `others`. Without `withScores`,
     * returns members; with `withScores: true`, returns `SortedSetEntry`s.
     */
    zdiff,
    /** `ZUNIONSTORE`. Stores the union into `destination`; returns count. */
    async zunionstore(
      destination: TId,
      id: TId,
      others: readonly TId[],
      options?: SortedSetCombineOptions
    ): Promise<number> {
      const keys = combinedKeys("ZUNIONSTORE", id, others);
      return expectNumber(
        await client.send([
          "ZUNIONSTORE",
          storeTarget("ZUNIONSTORE", destination, keys[0]),
          ...combineArgs(keys, options)
        ]),
        "ZUNIONSTORE"
      );
    },
    /** `ZINTERSTORE`. Stores the intersection into `destination`; returns count. */
    async zinterstore(
      destination: TId,
      id: TId,
      others: readonly TId[],
      options?: SortedSetCombineOptions
    ): Promise<number> {
      const keys = combinedKeys("ZINTERSTORE", id, others);
      return expectNumber(
        await client.send([
          "ZINTERSTORE",
          storeTarget("ZINTERSTORE", destination, keys[0]),
          ...combineArgs(keys, options)
        ]),
        "ZINTERSTORE"
      );
    },
    /** `ZDIFFSTORE`. Stores the difference into `destination`; returns count. */
    async zdiffstore(
      destination: TId,
      id: TId,
      others: readonly TId[]
    ): Promise<number> {
      const keys = combinedKeys("ZDIFFSTORE", id, others);
      return expectNumber(
        await client.send([
          "ZDIFFSTORE",
          storeTarget("ZDIFFSTORE", destination, keys[0]),
          ...combineArgs(keys, undefined)
        ]),
        "ZDIFFSTORE"
      );
    },
    /** `ZINTERCARD`. Cardinality of the intersection, optionally capped by `limit`. */
    async zintercard(
      id: TId,
      others: readonly TId[],
      options?: SortedSetIntersectionSizeOptions
    ): Promise<number> {
      const keys = combinedKeys("ZINTERCARD", id, others);
      const args: SortedSetCommandArg[] = [keys.length, ...keys];
      if (options?.limit !== undefined) {
        if (!Number.isSafeInteger(options.limit) || options.limit < 0) {
          throw new ValidationError("limit must be a nonnegative safe integer");
        }
        args.push("LIMIT", options.limit);
      }
      return expectNumber(
        await client.send(["ZINTERCARD", ...args]),
        "ZINTERCARD"
      );
    },
    /**
     * `ZMPOP`. Pops from the first non-empty of `ids`, from the min end
     * (`{ min: true }`) or max end (`{ max: true }`), up to `count` entries
     * (passed alongside the end selector).
     * Returns the answering `id` and its popped `entries`, or `null` if all
     * were empty.
     *
     * @example
     * await set.zmpop([a, b], { min: true });
     * await set.zmpop([a, b], { max: true, count: 5 });
     */
    zmpop<const TPick extends TId>(
      ids: readonly TPick[],
      end: SortedSetPopEnd & SortedSetMultiPopOptions
    ): Promise<{ id: TPick; entries: Array<SortedSetEntry<TOutput>> } | null> {
      return popFrom(
        "min" in end ? "MIN" : "MAX",
        ids,
        end.count === undefined ? {} : { count: end.count }
      );
    },
    /** `DEL`. Deletes the sorted set; returns `1` if it existed, else `0`. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", schema.key(id)]), "DEL");
    }
  };
}

/**
 * Blocking sorted-set operations (BZPOPMIN/BZPOPMAX). Session-only: the
 * factory takes the session's gated RedisClient facade and is spread over
 * the base sorted-set store by the session accessors, so these methods are
 * structurally absent from shared-client store types.
 */
export function createBlockingSortedSetOps<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: SortedSetSchema<TInput, TOutput, string, TId>,
  assertSameSlot?: SlotGuard
) {
  function decodeBlockingPopReply(
    reply: RedisReply,
    command: string
  ): readonly [RedisReply, SortedSetEntry<TOutput>] {
    if (
      !Array.isArray(reply) ||
      reply.length !== 3 ||
      typeof reply[1] !== "string"
    ) {
      throw replyShapeError(command, "key/member/score triple or null", reply);
    }
    return [
      reply[0],
      {
        member: schema.decode(reply[1]),
        score: expectNumberLike(reply[2], command)
      }
    ];
  }

  async function popBlocking(
    command: "BZPOPMIN" | "BZPOPMAX",
    id: TId,
    options: BlockingWait
  ): Promise<SortedSetEntry<TOutput> | null> {
    const timeout = blockingTimeoutSeconds(options.timeoutSeconds);
    const reply = await client.send([command, schema.key(id), timeout]);
    if (reply === null) return null;
    return decodeBlockingPopReply(reply, command)[1];
  }

  async function popBlockingFrom<TPick extends TId>(
    command: "BZPOPMIN" | "BZPOPMAX",
    ids: readonly TPick[],
    options: BlockingWait
  ): Promise<{ id: TPick; entry: SortedSetEntry<TOutput> } | null> {
    const idsByKey = requestedKeyIds(
      ids,
      (id) => schema.key(id),
      command,
      assertSameSlot,
      schema
    );
    const timeout = blockingTimeoutSeconds(options.timeoutSeconds);
    const reply = await client.send([
      command,
      ...ids.map((id) => schema.key(id)),
      timeout
    ]);
    if (reply === null) return null;
    const [key, entry] = decodeBlockingPopReply(reply, command);
    return { id: attributeReplyKey(key, command, idsByKey), entry };
  }

  // bzpopmin/bzpopmax dispatch on whether the first arg is a single id or an
  // array of ids: a single id resolves to BZPOPMIN <key> <timeout> and yields
  // the entry (or null); an array uses the multi-key form and attributes the
  // answering key back to a TPick, yielding { id, entry } (or null). `forever`
  // timeouts get the non-null overload.
  function bzpopmin(
    id: TId,
    options: { timeoutSeconds: "forever" }
  ): Promise<SortedSetEntry<TOutput>>;
  function bzpopmin(
    id: TId,
    options: BlockingWait
  ): Promise<SortedSetEntry<TOutput> | null>;
  function bzpopmin<const TPick extends TId>(
    ids: readonly TPick[],
    options: { timeoutSeconds: "forever" }
  ): Promise<{ id: TPick; entry: SortedSetEntry<TOutput> }>;
  function bzpopmin<const TPick extends TId>(
    ids: readonly TPick[],
    options: BlockingWait
  ): Promise<{ id: TPick; entry: SortedSetEntry<TOutput> } | null>;
  function bzpopmin(
    idOrIds: TId | readonly TId[],
    options: BlockingWait
  ): Promise<
    SortedSetEntry<TOutput> | null | { id: TId; entry: SortedSetEntry<TOutput> }
  > {
    return Array.isArray(idOrIds)
      ? popBlockingFrom("BZPOPMIN", idOrIds, options)
      : popBlocking("BZPOPMIN", idOrIds as TId, options);
  }

  function bzpopmax(
    id: TId,
    options: { timeoutSeconds: "forever" }
  ): Promise<SortedSetEntry<TOutput>>;
  function bzpopmax(
    id: TId,
    options: BlockingWait
  ): Promise<SortedSetEntry<TOutput> | null>;
  function bzpopmax<const TPick extends TId>(
    ids: readonly TPick[],
    options: { timeoutSeconds: "forever" }
  ): Promise<{ id: TPick; entry: SortedSetEntry<TOutput> }>;
  function bzpopmax<const TPick extends TId>(
    ids: readonly TPick[],
    options: BlockingWait
  ): Promise<{ id: TPick; entry: SortedSetEntry<TOutput> } | null>;
  function bzpopmax(
    idOrIds: TId | readonly TId[],
    options: BlockingWait
  ): Promise<
    SortedSetEntry<TOutput> | null | { id: TId; entry: SortedSetEntry<TOutput> }
  > {
    return Array.isArray(idOrIds)
      ? popBlockingFrom("BZPOPMAX", idOrIds, options)
      : popBlocking("BZPOPMAX", idOrIds as TId, options);
  }

  async function popManyBlockingFrom<TPick extends TId>(
    end: "MIN" | "MAX",
    ids: readonly TPick[],
    options: BlockingWait & SortedSetMultiPopOptions
  ): Promise<{ id: TPick; entries: Array<SortedSetEntry<TOutput>> } | null> {
    const idsByKey = requestedKeyIds(
      ids,
      (id) => schema.key(id),
      "BZMPOP",
      assertSameSlot,
      schema
    );
    const timeout = blockingTimeoutSeconds(options.timeoutSeconds);
    const command: [string, ...RedisCommandArgument[]] = [
      "BZMPOP",
      timeout,
      ids.length,
      ...ids.map((id) => schema.key(id)),
      end
    ];
    if (options.count !== undefined) {
      command.push("COUNT", multiPopCount(options.count));
    }
    const reply = await client.send(command);
    if (reply === null) return null;
    if (!Array.isArray(reply) || reply.length !== 2) {
      throw replyShapeError("BZMPOP", "key/entries pair or null", reply);
    }
    return {
      id: attributeReplyKey(reply[0], "BZMPOP", idsByKey),
      entries: decodeSortedSetEntries(reply[1], "BZMPOP", schema)
    };
  }

  // BZMPOP — the blocking form of ZMPOP: pops up to COUNT entries from the
  // first non-empty of several keys, from the min or max end, blocking until
  // one has data. Distinct from bzpopmin/bzpopmax over an array, which pop one.
  function bzmpop<const TPick extends TId>(
    ids: readonly TPick[],
    end: SortedSetPopEnd & SortedSetMultiPopOptions,
    options: { timeoutSeconds: "forever" }
  ): Promise<{ id: TPick; entries: Array<SortedSetEntry<TOutput>> }>;
  function bzmpop<const TPick extends TId>(
    ids: readonly TPick[],
    end: SortedSetPopEnd & SortedSetMultiPopOptions,
    options: BlockingWait
  ): Promise<{ id: TPick; entries: Array<SortedSetEntry<TOutput>> } | null>;
  function bzmpop<const TPick extends TId>(
    ids: readonly TPick[],
    end: SortedSetPopEnd & SortedSetMultiPopOptions,
    options: BlockingWait
  ): Promise<{ id: TPick; entries: Array<SortedSetEntry<TOutput>> } | null> {
    return popManyBlockingFrom("min" in end ? "MIN" : "MAX", ids, {
      ...options,
      ...(end.count === undefined ? {} : { count: end.count })
    });
  }

  return {
    bzpopmin,
    bzpopmax,
    bzmpop
  };
}

/** The zset resource: the base (non-blocking) store plus the typed `key()`. */
export function createZsetResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(
  ctx: StoreContext,
  schema: SortedSetSchema<TInput, TOutput, TPrefix, TId, THashTag>
) {
  return withKey(
    schema,
    createSortedSetStore(ctx.client, schema, ctx.assertSameSlot)
  );
}

/** Session zset accessor: base store spread with the blocking pops. */
export function createZsetSessionAccessor<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(
  ctx: StoreContext,
  schema: SortedSetSchema<TInput, TOutput, TPrefix, TId, THashTag>
) {
  const store = createSortedSetStore(ctx.client, schema, ctx.assertSameSlot);
  return {
    ...withKey(schema, store),
    ...createBlockingSortedSetOps(ctx.client, schema, ctx.assertSameSlot)
  };
}

const zsetBinding: StoreBinding = {
  resource: createZsetResource,
  session: createZsetSessionAccessor
};

export function defineSortedSet<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  const THashTag extends HashTagLayout | undefined = undefined
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  options?: KeyOptions<TIds, THashTag>
): SortedSetSchema<TInput, TOutput, TPrefix, TIds[number], THashTag> {
  const hashTag = options?.hashTag as THashTag;
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    kind: "zset",
    prefix,
    // Spread so the property is absent, not `undefined`, on the default
    // layout: a schema still enumerates as the plain data it looks like.
    ...(hashTag === undefined ? {} : { hashTag }),
    key: keyBuilder(prefix, hashTag),
    encode(member) {
      return codec.encode(member);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as SortedSetSchema<TInput, TOutput, TPrefix, TIds[number], THashTag>;
  return withStore(schema, zsetBinding);
}

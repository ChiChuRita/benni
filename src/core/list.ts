import { replyShapeError, ValidationError } from "./errors.js";
import {
  createKeyLifecycleOps,
  decodeStringArrayReply,
  decodeStringOrNull,
  expectNumber
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
  ListSchema,
  RedisClient,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply
} from "./types.js";

/** Which end of a list an LMOVE/BLMOVE acts on (lowercase, like `direction`). */
export type ListEnd = "left" | "right";

/**
 * The union of both `lpos` option forms, for code that inspects them. It is
 * not the argument type: `lpos` takes either `{ rank }` (one position) or
 * `{ count, rank }` (an array), and the two return different things, so a bag
 * whose `count` is `number | undefined` matches neither overload.
 */
export type ListPosOptions = {
  readonly rank?: number;
  readonly count?: number;
};

export type ListPopOptions = {
  readonly count?: number;
};

export type ListInsertOptions = {
  readonly position: "before" | "after";
};

export type ListMultiPopOptions = {
  readonly direction: "left" | "right";
  readonly count?: number;
};

export type ListBlockingMultiPopOptions = BlockingWait & {
  readonly direction: "left" | "right";
  readonly count?: number;
};

function listRank(rank: number): number {
  if (!Number.isSafeInteger(rank) || rank === 0) {
    throw new ValidationError("rank must be a nonzero safe integer");
  }
  return rank;
}

function listMatchCount(count: number): number {
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new ValidationError("count must be a non-negative safe integer");
  }
  return count;
}

function listPopCount(count: number): number {
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

function decodePositionsReply(reply: RedisReply): number[] {
  if (!Array.isArray(reply)) {
    throw replyShapeError("LPOS", "array", reply);
  }
  return reply.map((value) => {
    if (typeof value !== "number") {
      throw replyShapeError("LPOS item", "number", value);
    }
    return value;
  });
}

export function createListStore<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: ListSchema<TInput, TOutput, string, TId>,
  assertSameSlot?: SlotGuard
) {
  async function lpopScalar(id: TId): Promise<TOutput | null> {
    return decodeStringOrNull(
      await client.send(["LPOP", schema.key(id)]),
      "LPOP",
      schema
    );
  }
  async function lpopMany(id: TId, count: number): Promise<TOutput[]> {
    const reply = await client.send([
      "LPOP",
      schema.key(id),
      listPopCount(count)
    ]);
    if (reply === null) return [];
    return decodeStringArrayReply(reply, "LPOP", schema);
  }
  async function rpopScalar(id: TId): Promise<TOutput | null> {
    return decodeStringOrNull(
      await client.send(["RPOP", schema.key(id)]),
      "RPOP",
      schema
    );
  }
  async function rpopMany(id: TId, count: number): Promise<TOutput[]> {
    const reply = await client.send([
      "RPOP",
      schema.key(id),
      listPopCount(count)
    ]);
    if (reply === null) return [];
    return decodeStringArrayReply(reply, "RPOP", schema);
  }

  async function lmpopFrom<TPick extends TId>(
    ids: readonly TPick[],
    options: ListMultiPopOptions
  ): Promise<{ id: TPick; values: TOutput[] } | null> {
    const idsByKey = requestedKeyIds(
      ids,
      (id) => schema.key(id),
      "LMPOP",
      assertSameSlot,
      schema
    );
    const command: [string, ...RedisCommandArgument[]] = [
      "LMPOP",
      ids.length,
      ...ids.map((id) => schema.key(id)),
      options.direction === "left" ? "LEFT" : "RIGHT"
    ];
    if (options.count !== undefined) {
      command.push("COUNT", listPopCount(options.count));
    }
    const reply = await client.send(command);
    if (reply === null) return null;
    if (!Array.isArray(reply) || reply.length !== 2) {
      throw replyShapeError("LMPOP", "key/values pair or null", reply);
    }
    return {
      id: attributeReplyKey(reply[0], "LMPOP", idsByKey),
      values: decodeStringArrayReply(reply[1], "LMPOP", schema)
    };
  }

  function lpop(id: TId): Promise<TOutput | null>;
  function lpop(id: TId, options: { count: number }): Promise<TOutput[]>;
  function lpop(
    id: TId,
    options?: ListPopOptions
  ): Promise<TOutput | null> | Promise<TOutput[]> {
    return options?.count === undefined
      ? lpopScalar(id)
      : lpopMany(id, options.count);
  }

  function rpop(id: TId): Promise<TOutput | null>;
  function rpop(id: TId, options: { count: number }): Promise<TOutput[]>;
  function rpop(
    id: TId,
    options?: ListPopOptions
  ): Promise<TOutput | null> | Promise<TOutput[]> {
    return options?.count === undefined
      ? rpopScalar(id)
      : rpopMany(id, options.count);
  }

  async function lposScalar(
    id: TId,
    value: TInput,
    options?: { rank?: number }
  ): Promise<number | null> {
    const command: [string, ...RedisCommandArgument[]] = [
      "LPOS",
      schema.key(id),
      schema.encode(value)
    ];
    if (options?.rank !== undefined) {
      command.push("RANK", listRank(options.rank));
    }
    const reply = await client.send(command);
    if (reply === null) return null;
    return expectNumber(reply, "LPOS");
  }
  async function lposMany(
    id: TId,
    value: TInput,
    count: number,
    options?: { rank?: number }
  ): Promise<number[]> {
    const command: [string, ...RedisCommandArgument[]] = [
      "LPOS",
      schema.key(id),
      schema.encode(value),
      "COUNT",
      listMatchCount(count)
    ];
    if (options?.rank !== undefined) {
      command.push("RANK", listRank(options.rank));
    }
    return decodePositionsReply(await client.send(command));
  }

  // `count?: undefined` rather than an absent property: `{ rank?: number }` is
  // a weak type, so only a fresh object literal carrying `count` is rejected,
  // and any variable (an options bag typed `ListPosOptions`, say) matched this
  // overload while the implementation returned the array form.
  function lpos(
    id: TId,
    value: TInput,
    options?: { readonly rank?: number; readonly count?: undefined }
  ): Promise<number | null>;
  function lpos(
    id: TId,
    value: TInput,
    options: { count: number; rank?: number }
  ): Promise<number[]>;
  function lpos(
    id: TId,
    value: TInput,
    options?: ListPosOptions
  ): Promise<number | null> | Promise<number[]> {
    return options?.count === undefined
      ? lposScalar(id, value, options)
      : lposMany(id, value, options.count, options);
  }

  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    /**
     * LPUSH — prepend `values` (last first, so they end up in given order at
     * the head). No-op returning 0 when `values` is empty.
     */
    async lpush(id: TId, values: readonly TInput[]): Promise<number> {
      if (values.length === 0) return 0;
      return expectNumber(
        await client.send([
          "LPUSH",
          schema.key(id),
          ...values.map((value) => schema.encode(value))
        ]),
        "LPUSH"
      );
    },
    /**
     * RPUSH — append `values` to the tail. No-op returning 0 when `values` is
     * empty.
     */
    async rpush(id: TId, values: readonly TInput[]): Promise<number> {
      if (values.length === 0) return 0;
      return expectNumber(
        await client.send([
          "RPUSH",
          schema.key(id),
          ...values.map((value) => schema.encode(value))
        ]),
        "RPUSH"
      );
    },
    /**
     * LPUSHX — prepend `values` only if the key already exists. No-op
     * returning 0 when `values` is empty.
     */
    async lpushx(id: TId, values: readonly TInput[]): Promise<number> {
      if (values.length === 0) return 0;
      return expectNumber(
        await client.send([
          "LPUSHX",
          schema.key(id),
          ...values.map((value) => schema.encode(value))
        ]),
        "LPUSHX"
      );
    },
    /**
     * RPUSHX — append `values` only if the key already exists. No-op
     * returning 0 when `values` is empty.
     */
    async rpushx(id: TId, values: readonly TInput[]): Promise<number> {
      if (values.length === 0) return 0;
      return expectNumber(
        await client.send([
          "RPUSHX",
          schema.key(id),
          ...values.map((value) => schema.encode(value))
        ]),
        "RPUSHX"
      );
    },
    /**
     * LPOP — pop from the head. Without `count`, returns the single element or
     * `null`; with `count`, returns up to `count` elements as an array.
     */
    lpop,
    /**
     * RPOP — pop from the tail. Without `count`, returns the single element or
     * `null`; with `count`, returns up to `count` elements as an array.
     */
    rpop,
    /** LRANGE — return the elements in `[start, stop]` (inclusive). */
    async lrange(id: TId, start: number, stop: number): Promise<TOutput[]> {
      return decodeStringArrayReply(
        await client.send(["LRANGE", schema.key(id), start, stop]),
        "LRANGE",
        schema
      );
    },
    /** LLEN — return the list length. */
    async llen(id: TId): Promise<number> {
      return expectNumber(await client.send(["LLEN", schema.key(id)]), "LLEN");
    },
    /** LINDEX — return the element at `index`, or `null` if out of range. */
    async lindex(id: TId, index: number): Promise<TOutput | null> {
      return decodeStringOrNull(
        await client.send(["LINDEX", schema.key(id), index]),
        "LINDEX",
        schema
      );
    },
    /**
     * LPOS — find `value` in the list. Without `count`, returns the first
     * matching index or `null`; with `count`, returns up to `count` matching
     * indices. `rank` chooses which match to start from (negative scans from
     * the tail).
     */
    lpos,
    /**
     * LINSERT — insert `value` before or after the first occurrence of
     * `pivot` (`options.position`). Returns the new length, 0 if the key is
     * missing, or -1 if `pivot` was not found.
     */
    async linsert(
      id: TId,
      pivot: TInput,
      value: TInput,
      options: ListInsertOptions
    ): Promise<number> {
      return expectNumber(
        await client.send([
          "LINSERT",
          schema.key(id),
          options.position === "before" ? "BEFORE" : "AFTER",
          schema.encode(pivot),
          schema.encode(value)
        ]),
        "LINSERT"
      );
    },
    /** LSET — set the element at `index`. Throws if the reply is not OK. */
    async lset(id: TId, index: number, value: TInput): Promise<void> {
      const reply = await client.send([
        "LSET",
        schema.key(id),
        index,
        schema.encode(value)
      ]);
      if (reply !== "OK") throw replyShapeError("LSET", "OK", reply);
    },
    /** LTRIM — keep only the elements in `[start, stop]`. */
    async ltrim(id: TId, start: number, stop: number): Promise<void> {
      const reply = await client.send(["LTRIM", schema.key(id), start, stop]);
      if (reply !== "OK") throw replyShapeError("LTRIM", "OK", reply);
    },
    /**
     * LREM — remove elements equal to `value`. `count > 0` removes from head,
     * `count < 0` from tail, `0` removes all. Returns the number removed.
     */
    async lrem(id: TId, count: number, value: TInput): Promise<number> {
      return expectNumber(
        await client.send([
          "LREM",
          schema.key(id),
          count,
          schema.encode(value)
        ]),
        "LREM"
      );
    },
    /**
     * LMOVE — atomically pop from `source` (`from` end) and push to
     * `destination` (`to` end). Returns the moved element, or `null` if
     * `source` was empty.
     */
    async lmove(
      source: TId,
      destination: TId,
      from: ListEnd,
      to: ListEnd
    ): Promise<TOutput | null> {
      const sourceKey = schema.key(source);
      const destKey = schema.key(destination);
      assertSameSlot?.("LMOVE", [sourceKey, destKey], schema);
      return decodeStringOrNull(
        await client.send([
          "LMOVE",
          sourceKey,
          destKey,
          from.toUpperCase(),
          to.toUpperCase()
        ]),
        "LMOVE",
        schema
      );
    },
    /**
     * LMPOP — pop up to `options.count` elements (default 1) from the
     * `options.direction` end of the first non-empty key in `ids`. Returns the
     * answering id with its values, or `null` if all keys were empty.
     */
    lmpop<const TPick extends TId>(
      ids: readonly TPick[],
      options: ListMultiPopOptions
    ): Promise<{ id: TPick; values: TOutput[] } | null> {
      return lmpopFrom(ids, options);
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", schema.key(id)]), "DEL");
    }
  };
}

/**
 * Blocking list operations (BLPOP/BRPOP/BLMOVE/BLMPOP). Session-only: the
 * factory takes the session's gated RedisClient facade and is spread over the
 * base list store by the session accessors, so these methods are structurally
 * absent from shared-client store types.
 */
export function createBlockingListOps<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: ListSchema<TInput, TOutput, string, TId>,
  assertSameSlot?: SlotGuard
) {
  function decodeBlockingPopPair(
    reply: RedisReply,
    command: string
  ): readonly [RedisReply, string] {
    if (
      !Array.isArray(reply) ||
      reply.length !== 2 ||
      typeof reply[1] !== "string"
    ) {
      throw replyShapeError(command, "key/value pair or null", reply);
    }
    return [reply[0], reply[1]];
  }

  async function popBlocking(
    command: "BLPOP" | "BRPOP",
    id: TId,
    options: BlockingWait
  ): Promise<TOutput | null> {
    const timeout = blockingTimeoutSeconds(options.timeoutSeconds);
    const reply = await client.send([command, schema.key(id), timeout]);
    if (reply === null) return null;
    const [, value] = decodeBlockingPopPair(reply, command);
    return schema.decode(value);
  }

  async function popBlockingFrom<TPick extends TId>(
    command: "BLPOP" | "BRPOP",
    ids: readonly TPick[],
    options: BlockingWait
  ): Promise<{ id: TPick; value: TOutput } | null> {
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
    const [key, value] = decodeBlockingPopPair(reply, command);
    return {
      id: attributeReplyKey(key, command, idsByKey),
      value: schema.decode(value)
    };
  }

  /**
   * BLPOP — blocking pop from the head. A single id resolves to the element
   * (or `null` on timeout); an array of ids resolves to `{ id, value }` for
   * the answering key (or `null` on timeout). `{ timeoutSeconds: "forever" }` blocks
   * indefinitely and narrows out the `null`.
   */
  function blpop(
    id: TId,
    options: { timeoutSeconds: "forever" }
  ): Promise<TOutput>;
  function blpop(id: TId, options: BlockingWait): Promise<TOutput | null>;
  function blpop<const TPick extends TId>(
    ids: readonly TPick[],
    options: { timeoutSeconds: "forever" }
  ): Promise<{ id: TPick; value: TOutput }>;
  function blpop<const TPick extends TId>(
    ids: readonly TPick[],
    options: BlockingWait
  ): Promise<{ id: TPick; value: TOutput } | null>;
  function blpop(
    idOrIds: TId | readonly TId[],
    options: BlockingWait
  ): Promise<TOutput | null> | Promise<{ id: TId; value: TOutput } | null> {
    return Array.isArray(idOrIds)
      ? popBlockingFrom("BLPOP", idOrIds, options)
      : popBlocking("BLPOP", idOrIds as TId, options);
  }

  /**
   * BRPOP — blocking pop from the tail. A single id resolves to the element
   * (or `null` on timeout); an array of ids resolves to `{ id, value }` for
   * the answering key (or `null` on timeout). `{ timeoutSeconds: "forever" }` blocks
   * indefinitely and narrows out the `null`.
   */
  function brpop(
    id: TId,
    options: { timeoutSeconds: "forever" }
  ): Promise<TOutput>;
  function brpop(id: TId, options: BlockingWait): Promise<TOutput | null>;
  function brpop<const TPick extends TId>(
    ids: readonly TPick[],
    options: { timeoutSeconds: "forever" }
  ): Promise<{ id: TPick; value: TOutput }>;
  function brpop<const TPick extends TId>(
    ids: readonly TPick[],
    options: BlockingWait
  ): Promise<{ id: TPick; value: TOutput } | null>;
  function brpop(
    idOrIds: TId | readonly TId[],
    options: BlockingWait
  ): Promise<TOutput | null> | Promise<{ id: TId; value: TOutput } | null> {
    return Array.isArray(idOrIds)
      ? popBlockingFrom("BRPOP", idOrIds, options)
      : popBlocking("BRPOP", idOrIds as TId, options);
  }

  /**
   * BLMOVE — blocking form of LMOVE: atomically pop from `source` (`from` end)
   * and push to `destination` (`to` end), blocking until `source` has an
   * element. `{ timeoutSeconds: "forever" }` blocks indefinitely and narrows out the
   * `null`.
   */
  function blmove(
    source: TId,
    destination: TId,
    from: ListEnd,
    to: ListEnd,
    options: { timeoutSeconds: "forever" }
  ): Promise<TOutput>;
  function blmove(
    source: TId,
    destination: TId,
    from: ListEnd,
    to: ListEnd,
    options: BlockingWait
  ): Promise<TOutput | null>;
  async function blmove(
    source: TId,
    destination: TId,
    from: ListEnd,
    to: ListEnd,
    options: BlockingWait
  ): Promise<TOutput | null> {
    const sourceKey = schema.key(source);
    const destKey = schema.key(destination);
    assertSameSlot?.("BLMOVE", [sourceKey, destKey], schema);
    const timeout = blockingTimeoutSeconds(options.timeoutSeconds);
    return decodeStringOrNull(
      await client.send([
        "BLMOVE",
        sourceKey,
        destKey,
        from.toUpperCase(),
        to.toUpperCase(),
        timeout
      ]),
      "BLMOVE",
      schema
    );
  }

  async function popManyBlockingFrom<TPick extends TId>(
    ids: readonly TPick[],
    options: ListBlockingMultiPopOptions
  ): Promise<{ id: TPick; values: TOutput[] } | null> {
    const idsByKey = requestedKeyIds(
      ids,
      (id) => schema.key(id),
      "BLMPOP",
      assertSameSlot,
      schema
    );
    const timeout = blockingTimeoutSeconds(options.timeoutSeconds);
    const command: [string, ...RedisCommandArgument[]] = [
      "BLMPOP",
      timeout,
      ids.length,
      ...ids.map((id) => schema.key(id)),
      options.direction === "left" ? "LEFT" : "RIGHT"
    ];
    if (options.count !== undefined) {
      command.push("COUNT", listPopCount(options.count));
    }
    const reply = await client.send(command);
    if (reply === null) return null;
    if (!Array.isArray(reply) || reply.length !== 2) {
      throw replyShapeError("BLMPOP", "key/values pair or null", reply);
    }
    return {
      id: attributeReplyKey(reply[0], "BLMPOP", idsByKey),
      values: decodeStringArrayReply(reply[1], "BLMPOP", schema)
    };
  }

  // BLMPOP — the blocking form of lmpop (LMPOP): pops up to COUNT items from
  // the first non-empty of several keys, blocking until one has data. Distinct
  // from blpop/brpop over an id array (BLPOP/BRPOP), which pop one item.
  /**
   * BLMPOP — pop up to `options.count` elements (default 1) from the
   * `options.direction` end of the first non-empty key in `ids`, blocking
   * until one has data. `{ timeoutSeconds: "forever" }` blocks indefinitely and
   * narrows out the `null`.
   */
  function blmpop<const TPick extends TId>(
    ids: readonly TPick[],
    options: { timeoutSeconds: "forever" } & {
      direction: "left" | "right";
      count?: number;
    }
  ): Promise<{ id: TPick; values: TOutput[] }>;
  function blmpop<const TPick extends TId>(
    ids: readonly TPick[],
    options: ListBlockingMultiPopOptions
  ): Promise<{ id: TPick; values: TOutput[] } | null>;
  function blmpop<const TPick extends TId>(
    ids: readonly TPick[],
    options: ListBlockingMultiPopOptions
  ): Promise<{ id: TPick; values: TOutput[] } | null> {
    return popManyBlockingFrom(ids, options);
  }

  return {
    blpop,
    brpop,
    blmove,
    blmpop
  };
}

/** The list resource: the base (non-blocking) store plus the typed `key()`. */
export function createListResource<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(
  ctx: StoreContext,
  schema: ListSchema<TInput, TOutput, TPrefix, TId, THashTag>
) {
  return withKey(
    schema,
    createListStore(ctx.client, schema, ctx.assertSameSlot)
  );
}

/**
 * Session list accessor: the base store spread with the blocking pops. Its
 * inferred return type drives BeniSession["list"], so leftPopBlocking &
 * friends are structurally present on a session and absent on the shared
 * Beni handle.
 */
export function createListSessionAccessor<
  TInput,
  TOutput,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(
  ctx: StoreContext,
  schema: ListSchema<TInput, TOutput, TPrefix, TId, THashTag>
) {
  const store = createListStore(ctx.client, schema, ctx.assertSameSlot);
  return {
    ...withKey(schema, store),
    ...createBlockingListOps(ctx.client, schema, ctx.assertSameSlot)
  };
}

const listBinding: StoreBinding = {
  resource: createListResource,
  session: createListSessionAccessor
};

export function defineList<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  const THashTag extends HashTagLayout | undefined = undefined
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  options?: KeyOptions<TIds, THashTag>
): ListSchema<TInput, TOutput, TPrefix, TIds[number], THashTag> {
  const hashTag = options?.hashTag as THashTag;
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    kind: "list",
    prefix,
    // Spread so the property is absent, not `undefined`, on the default
    // layout: a schema still enumerates as the plain data it looks like.
    ...(hashTag === undefined ? {} : { hashTag }),
    key: keyBuilder(prefix, hashTag),
    encode(value) {
      return codec.encode(value);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as ListSchema<TInput, TOutput, TPrefix, TIds[number], THashTag>;
  return withStore(schema, listBinding);
}

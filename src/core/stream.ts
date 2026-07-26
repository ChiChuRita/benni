import { replyShapeError, ValidationError } from "./errors.js";
import { createKeyLifecycleOps, expectNumber } from "./helpers.js";
import { type BlockingWait, blockingTimeoutMilliseconds } from "./session.js";
import type {
  FieldCodecs,
  InferAnchors,
  InferHashInput,
  InferHashOutput,
  RedisClient,
  RedisCommandArgument,
  RedisKey,
  RedisKeyPart,
  RedisReply
} from "./types.js";

export type StreamSchema<
  TFields extends FieldCodecs,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
> = InferAnchors<InferHashInput<TFields>, Partial<InferHashOutput<TFields>>> & {
  readonly kind: "stream";
  readonly prefix: TPrefix;
  readonly fields: TFields;
  key<TActualId extends TId>(id: TActualId): RedisKey<TPrefix, TActualId>;
};

export type StreamEntry<TFields extends FieldCodecs> = {
  id: string;
  value: Partial<InferHashOutput<TFields>>;
};

export type StreamAddOptions = {
  readonly entryId?: string;
  /** `NOMKSTREAM` — don't create the stream if it is missing (xadd returns null). */
  readonly nomkstream?: boolean;
  /** `MAXLEN [~] count` — trim while adding; same shape as xtrim's `maxLen`. */
  readonly maxLen?: {
    readonly count: number;
    readonly approximate?: boolean;
  };
};

export type StreamRangeOptions = {
  readonly start?: string;
  readonly end?: string;
  readonly count?: number;
};

export type StreamReadOptions = {
  readonly count?: number;
};

/**
 * XTRIM strategy. Exactly one of `maxLen`/`minId` selects the trim mode;
 * `approximate` maps to the `~` modifier on either.
 */
export type StreamTrimOptions =
  | {
      readonly maxLen: {
        readonly count: number;
        readonly approximate?: boolean;
      };
      readonly minId?: undefined;
    }
  | {
      readonly minId: {
        readonly value: string;
        readonly approximate?: boolean;
      };
      readonly maxLen?: undefined;
    };

export type StreamBlockingReadOptions = StreamReadOptions & BlockingWait;

export function defineStream<
  TPrefix extends string,
  TFields extends FieldCodecs,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  fields: TFields,
  _options?: { readonly ids?: TIds }
): StreamSchema<TFields, TPrefix, TIds[number]> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  return {
    kind: "stream",
    prefix,
    fields,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    }
  } as StreamSchema<TFields, TPrefix, TIds[number]>;
}

function positiveCount(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new ValidationError(`${name} must be a positive safe integer`);
  }
  return value;
}

export function decodeStreamEntry<TFields extends FieldCodecs>(
  entry: RedisReply,
  command: string,
  fields: TFields
): StreamEntry<TFields> {
  if (
    !Array.isArray(entry) ||
    entry.length !== 2 ||
    typeof entry[0] !== "string"
  ) {
    throw replyShapeError(command, "id/fields pairs", entry);
  }
  const id = entry[0];
  const rawFields = entry[1];
  if (!Array.isArray(rawFields) || rawFields.length % 2 !== 0) {
    throw replyShapeError(command, "field/value pairs", rawFields);
  }
  const value: Partial<InferHashOutput<TFields>> = {};
  for (let index = 0; index < rawFields.length; index += 2) {
    const field = rawFields[index];
    const stored = rawFields[index + 1];
    if (typeof field !== "string" || typeof stored !== "string") {
      throw replyShapeError(command, "field/value pairs", entry);
    }
    const codec = fields[field];
    if (!codec) continue;
    value[field as keyof TFields] = codec.decode(
      stored
    ) as InferHashOutput<TFields>[keyof TFields];
  }
  return { id, value };
}

export function decodeStreamEntries<TFields extends FieldCodecs>(
  reply: RedisReply,
  command: string,
  fields: TFields
): Array<StreamEntry<TFields>> {
  if (!Array.isArray(reply)) {
    throw replyShapeError(command, "array", reply);
  }
  return reply.map((entry) => decodeStreamEntry(entry, command, fields));
}

export function xreadStreamPairs(
  reply: RedisReply
): Array<readonly [RedisReply, RedisReply]> {
  if (reply instanceof Map) {
    return [...reply.entries()];
  }
  if (!Array.isArray(reply)) {
    throw replyShapeError("XREAD", "array or null", reply);
  }
  return reply.map((stream) => {
    if (!Array.isArray(stream) || stream.length !== 2) {
      throw replyShapeError("XREAD", "key/entries pairs", stream);
    }
    return [stream[0], stream[1]] as const;
  });
}

function decodeSingleStreamXread<TFields extends FieldCodecs>(
  reply: RedisReply,
  fields: TFields
): Array<StreamEntry<TFields>> {
  if (reply === null) return [];
  const pairs = xreadStreamPairs(reply);
  if (pairs.length !== 1) {
    throw replyShapeError("XREAD", "one stream", reply);
  }
  const [key, entries] = pairs[0];
  if (typeof key !== "string") {
    throw replyShapeError("XREAD", "key/entries pairs", key);
  }
  return decodeStreamEntries(entries, "XREAD", fields);
}

export function createStreamStore<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(client: RedisClient, schema: StreamSchema<TFields, string, TId>) {
  type Input = InferHashInput<TFields>;
  const declaredFields = Object.keys(schema.fields) as Array<
    keyof TFields & string
  >;

  /**
   * `XADD`. Appends an entry; returns its id. Only the `nomkstream` form can
   * return `null` (missing stream), so the plain form's type has no null.
   *
   * @example const entryId = await redis.stream(events).xadd("42", { kind: "click" });
   */
  function xadd(
    id: TId,
    value: Input,
    options: StreamAddOptions & { nomkstream: true }
  ): Promise<string | null>;
  function xadd(
    id: TId,
    value: Input,
    options?: StreamAddOptions
  ): Promise<string>;
  async function xadd(
    id: TId,
    value: Input,
    options: StreamAddOptions = {}
  ): Promise<string | null> {
    const args: RedisCommandArgument[] = [schema.key(id)];
    if (options.nomkstream) args.push("NOMKSTREAM");
    if (options.maxLen !== undefined) {
      args.push("MAXLEN");
      if (options.maxLen.approximate) args.push("~");
      args.push(positiveCount(options.maxLen.count, "maxLen.count"));
    }
    args.push(options.entryId ?? "*");
    for (const field of declaredFields) {
      args.push(field, schema.fields[field].encode(value[field]));
    }
    const reply = await client.send(["XADD", ...args]);
    if (options.nomkstream && reply === null) return null;
    if (typeof reply !== "string") {
      throw replyShapeError("XADD", "string", reply);
    }
    return reply;
  }

  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    xadd,
    /** XLEN — number of entries in the stream. */
    async xlen(id: TId): Promise<number> {
      return expectNumber(await client.send(["XLEN", schema.key(id)]), "XLEN");
    },
    async xrange(
      id: TId,
      options: StreamRangeOptions = {}
    ): Promise<Array<StreamEntry<TFields>>> {
      const args: RedisCommandArgument[] = [
        schema.key(id),
        options.start ?? "-",
        options.end ?? "+"
      ];
      if (options.count !== undefined) {
        args.push("COUNT", positiveCount(options.count, "count"));
      }
      return decodeStreamEntries(
        await client.send(["XRANGE", ...args]),
        "XRANGE",
        schema.fields
      );
    },
    async xrevrange(
      id: TId,
      options: StreamRangeOptions = {}
    ): Promise<Array<StreamEntry<TFields>>> {
      const args: RedisCommandArgument[] = [
        schema.key(id),
        options.start ?? "+",
        options.end ?? "-"
      ];
      if (options.count !== undefined) {
        args.push("COUNT", positiveCount(options.count, "count"));
      }
      return decodeStreamEntries(
        await client.send(["XREVRANGE", ...args]),
        "XREVRANGE",
        schema.fields
      );
    },
    async xdel(id: TId, entryIds: readonly string[]): Promise<number> {
      if (entryIds.length === 0) return 0;
      return expectNumber(
        await client.send(["XDEL", schema.key(id), ...entryIds]),
        "XDEL"
      );
    },
    /**
     * XTRIM by MAXLEN or MINID. Pass `{ maxLen: { count, approximate? } }`
     * to cap length or `{ minId: { value, approximate? } }` to drop entries
     * older than an id; `approximate` maps to the `~` modifier.
     */
    async xtrim(id: TId, options: StreamTrimOptions): Promise<number> {
      const args: RedisCommandArgument[] = [schema.key(id)];
      if (options.maxLen !== undefined) {
        args.push("MAXLEN");
        if (options.maxLen.approximate) args.push("~");
        args.push(positiveCount(options.maxLen.count, "maxLen.count"));
      } else if (options.minId !== undefined) {
        args.push("MINID");
        if (options.minId.approximate) args.push("~");
        args.push(options.minId.value);
      } else {
        throw new ValidationError("xtrim requires either maxLen or minId");
      }
      return expectNumber(await client.send(["XTRIM", ...args]), "XTRIM");
    },
    /**
     * XREAD after `afterEntryId` (an entry id or `"$"`). Non-blocking on the
     * shared store; the session store adds a `{ timeoutSeconds }` overload that
     * issues XREAD BLOCK.
     */
    async xread(
      id: TId,
      afterEntryId: string,
      options: StreamReadOptions = {}
    ): Promise<Array<StreamEntry<TFields>>> {
      const args: RedisCommandArgument[] = [];
      if (options.count !== undefined) {
        args.push("COUNT", positiveCount(options.count, "count"));
      }
      args.push("STREAMS", schema.key(id), afterEntryId);
      const reply = await client.send(["XREAD", ...args]);
      return decodeSingleStreamXread(reply, schema.fields);
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", schema.key(id)]), "DEL");
    }
  };
}

/**
 * Blocking stream operations (XREAD BLOCK). Session-only: the factory takes
 * the session's gated RedisClient facade and is spread over the base stream
 * store by the session accessors, so these methods are structurally absent
 * from shared-client store types. Timeouts are seconds like every other
 * blocking surface; the XREAD BLOCK millisecond conversion happens here.
 */
export function createBlockingStreamOps<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(client: RedisClient, schema: StreamSchema<TFields, string, TId>) {
  return {
    /**
     * Blocking XREAD (XREAD BLOCK). `afterEntryId` accepts an entry id or
     * `"$"`. Track the last seen id across iterations; `"$"` re-arms "from
     * now" each call and can miss entries between calls. Resolves `[]` on
     * timeout, matching the non-blocking `xread` null-to-[] convention.
     */
    async xread(
      id: TId,
      afterEntryId: string,
      options: StreamBlockingReadOptions
    ): Promise<Array<StreamEntry<TFields>>> {
      const args: RedisCommandArgument[] = [];
      if (options.count !== undefined) {
        args.push("COUNT", positiveCount(options.count, "count"));
      }
      args.push("BLOCK", blockingTimeoutMilliseconds(options.timeoutSeconds));
      args.push("STREAMS", schema.key(id), afterEntryId);
      const reply = await client.send(["XREAD", ...args]);
      return decodeSingleStreamXread(reply, schema.fields);
    }
  };
}

import { ReplyShapeError, replyShapeError, ValidationError } from "./errors.js";
import { expectNumber, expectNumberLike } from "./helpers.js";
import { blockingTimeoutMilliseconds } from "./session.js";
import {
  decodeStreamEntries,
  decodeStreamEntry,
  type StreamBlockingReadOptions,
  type StreamEntry,
  type StreamReadOptions,
  type StreamSchema,
  xreadStreamPairs
} from "./stream.js";
import type {
  FieldCodecs,
  InferHashOutput,
  RedisClient,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply
} from "./types.js";

/**
 * History/claim paths can return tombstones: entries XDELed from the stream
 * while still in the PEL decode as `[id, nil]` and must still be acked to
 * clear the PEL. Live reads (`>`) never see tombstones and keep the plain
 * StreamEntry type.
 */
export type PendingStreamEntry<TFields extends FieldCodecs> = {
  readonly id: string;
  /** `null` = deleted upstream; ack and move on. */
  readonly value: Partial<InferHashOutput<TFields>> | null;
};

export type StreamPendingSummary = {
  readonly count: number;
  readonly minEntryId: string | null;
  readonly maxEntryId: string | null;
  readonly consumers: ReadonlyArray<{
    readonly consumer: string;
    readonly count: number;
  }>;
};

export type StreamPendingEntry = {
  readonly entryId: string;
  readonly consumer: string;
  readonly idleMs: number;
  /** The poison-pill counter. */
  readonly deliveries: number;
};

/**
 * Where a new group starts reading. Required on create() — defaulting would
 * silently choose between replaying history and skipping it.
 */
export type StreamGroupFrom = "start" | "end" | { readonly entryId: string };

export type StreamGroupCreateOptions = {
  readonly from: StreamGroupFrom;
  /** XGROUP CREATE MKSTREAM; defaults true (bootstrap-friendly). */
  readonly mkstream?: boolean;
};

export type StreamPendingRangeOptions = {
  readonly start?: string;
  readonly end?: string;
  /** Required — Redis requires an explicit count on the extended form. */
  readonly count: number;
  readonly consumer?: string;
  readonly minIdleMs?: number;
};

export type StreamPendingReadOptions = {
  /**
   * Read this consumer's PEL after this entry id. `undefined` is the live
   * `>` read, matching what the no-`after` overload promises; pass `"0"` for
   * the whole PEL.
   */
  readonly after?: string;
  readonly count?: number;
};

export type StreamClaimOptions = {
  readonly minIdleMs: number;
};

export type StreamAutoClaimOptions = {
  readonly minIdleMs: number;
  /** Scan cursor; defaults to "0-0" (scan from the beginning). */
  readonly start?: string;
  readonly count?: number;
};

export type StreamAutoClaimResult<TFields extends FieldCodecs> = {
  /** `"0-0"` = scan complete; otherwise pass back as `start`. */
  readonly cursor: string;
  readonly entries: Array<PendingStreamEntry<TFields>>;
  /**
   * Entry ids XAUTOCLAIM found deleted from the stream. Redis 7+ removes
   * them from the PEL itself — no ack needed. Always `[]` on Redis 6.2,
   * which replies with the 2-element form.
   */
  readonly deletedIds: string[];
};

export type StreamGroupConsumer<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
> = {
  /**
   * XREADGROUP `>` — new deliveries for this consumer; never contains
   * tombstones. `[]` when nothing is available.
   */
  xreadgroup(
    id: TId,
    // `after?: undefined` is what keeps this overload from swallowing the one
    // below. A *variable* typed StreamPendingReadOptions is assignable to a
    // bare StreamReadOptions (no excess-property check outside a literal), so
    // it matched here first and a tombstone-bearing history read came back
    // typed as new deliveries with non-nullable values.
    options?: StreamReadOptions & { readonly after?: undefined }
  ): Promise<Array<StreamEntry<TFields>>>;
  /**
   * XREADGROUP with an explicit id via `{ after }` (default "0") — this
   * consumer's unacked history; tombstone-tolerant. Non-blocking everywhere
   * (BLOCK with an explicit id returns immediately anyway).
   */
  xreadgroup(
    id: TId,
    options: StreamPendingReadOptions
  ): Promise<Array<PendingStreamEntry<TFields>>>;
  /** Convenience mirror of group.xack so worker code never reaches back up. */
  xack(id: TId, entryIds: readonly string[]): Promise<number>;
  /** XCLAIM — take ownership of specific pending entries. */
  xclaim(
    id: TId,
    entryIds: readonly string[],
    options: StreamClaimOptions
  ): Promise<Array<PendingStreamEntry<TFields>>>;
  /** XAUTOCLAIM — cursor scan stealing entries idle longer than minIdleMs. */
  xautoclaim(
    id: TId,
    options: StreamAutoClaimOptions
  ): Promise<StreamAutoClaimResult<TFields>>;
};

export type BlockingStreamGroupConsumer<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
> = StreamGroupConsumer<TFields, TId> & {
  /**
   * Blocking XREADGROUP via `{ timeoutSeconds }`. Always reads `>` — Redis only
   * honors BLOCK for new deliveries — so `{ after }` is not accepted here.
   * `[]` on timeout.
   */
  xreadgroup(
    id: TId,
    options: StreamBlockingReadOptions
  ): Promise<Array<StreamEntry<TFields>>>;
};

export type StreamGroup<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
> = {
  /**
   * XGROUP CREATE [MKSTREAM]. `true` = created, `false` = already existed
   * (BUSYGROUP).
   */
  create(id: TId, options: StreamGroupCreateOptions): Promise<boolean>;
  /** XGROUP DESTROY. `true` = the group existed and was destroyed. */
  destroy(id: TId): Promise<boolean>;
  /**
   * XGROUP DELCONSUMER — returns the number of pending entries destroyed
   * with the consumer. Destructive: the consumer's PEL is dropped.
   */
  deleteConsumer(id: TId, consumer: string): Promise<number>;
  /** XACK — entries acknowledged; an empty id list short-circuits to 0. */
  xack(id: TId, entryIds: readonly string[]): Promise<number>;
  /** XPENDING summary form (no options). */
  xpending(id: TId): Promise<StreamPendingSummary>;
  /** XPENDING extended form; `count` is required (Redis requires it). */
  xpending(
    id: TId,
    options: StreamPendingRangeOptions
  ): Promise<StreamPendingEntry[]>;
  consumer(name: string): StreamGroupConsumer<TFields, TId>;
};

export type BlockingStreamGroup<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
> = Omit<StreamGroup<TFields, TId>, "consumer"> & {
  consumer(name: string): BlockingStreamGroupConsumer<TFields, TId>;
};

function positiveCount(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new ValidationError(`${name} must be a positive safe integer`);
  }
  return value;
}

function minIdleMilliseconds(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ValidationError("minIdleMs must be a non-negative safe integer");
  }
  return value;
}

function validGroupName(name: string): string {
  if (typeof name !== "string" || name.length === 0) {
    throw new ValidationError("group name must be a non-empty string");
  }
  return name;
}

function validConsumerName(name: string): string {
  if (typeof name !== "string" || name.length === 0) {
    throw new ValidationError("consumer name must be a non-empty string");
  }
  return name;
}

function startEntryId(from: StreamGroupFrom): string {
  if (from === "start") return "0";
  if (from === "end") return "$";
  if (
    typeof from === "object" &&
    from !== null &&
    typeof from.entryId === "string" &&
    from.entryId.length > 0
  ) {
    return from.entryId;
  }
  throw new ValidationError('from must be "start", "end", or { entryId }');
}

/**
 * RESP error prefixes are a stable Redis convention, but this is message
 * matching; the contract test pins the sniff per adapter. Verified text:
 * "BUSYGROUP Consumer Group name already exists".
 */
function isBusyGroupError(error: unknown): boolean {
  return error instanceof Error && error.message.includes("BUSYGROUP");
}

/**
 * xreadStreamPairs hardcodes "XREAD" in its TypeError text; relabel so
 * XREADGROUP failures name the command that actually ran.
 */
function xreadGroupStreamPairs(
  reply: RedisReply
): Array<readonly [RedisReply, RedisReply]> {
  try {
    return xreadStreamPairs(reply);
  } catch (error) {
    if (error instanceof ReplyShapeError) {
      throw new ReplyShapeError(
        error.message.replace("Redis XREAD ", "Redis XREADGROUP "),
        error.reply
      );
    }
    throw error;
  }
}

/** Unwrap the single requested stream's raw entries; null = null reply. */
function singleStreamEntries(reply: RedisReply): RedisReply | null {
  if (reply === null) return null;
  const pairs = xreadGroupStreamPairs(reply);
  if (pairs.length !== 1) {
    throw replyShapeError("XREADGROUP", "one stream", reply);
  }
  const [key, entries] = pairs[0];
  if (typeof key !== "string") {
    throw replyShapeError("XREADGROUP", "key/entries pairs", key);
  }
  return entries;
}

/** Tombstone-tolerant entry decode: `[id, nil]` becomes `{ id, value: null }`. */
function decodePendingStreamEntry<TFields extends FieldCodecs>(
  entry: RedisReply,
  command: string,
  fields: TFields
): PendingStreamEntry<TFields> {
  if (
    Array.isArray(entry) &&
    entry.length === 2 &&
    typeof entry[0] === "string" &&
    entry[1] === null
  ) {
    return { id: entry[0], value: null };
  }
  return decodeStreamEntry(entry, command, fields);
}

function decodePendingStreamEntries<TFields extends FieldCodecs>(
  reply: RedisReply,
  command: string,
  fields: TFields
): Array<PendingStreamEntry<TFields>> {
  if (!Array.isArray(reply)) {
    throw replyShapeError(command, "array", reply);
  }
  return reply.map((entry) => decodePendingStreamEntry(entry, command, fields));
}

function decodeEntryIdOrNull(reply: RedisReply): string | null {
  if (reply === null) return null;
  if (typeof reply !== "string") {
    throw replyShapeError("XPENDING", "entry id or null", reply);
  }
  return reply;
}

function decodePendingSummary(reply: RedisReply): StreamPendingSummary {
  if (!Array.isArray(reply) || reply.length !== 4) {
    throw replyShapeError("XPENDING", "count/min/max/consumers summary", reply);
  }
  const rawConsumers = reply[3];
  let consumers: Array<{ consumer: string; count: number }> = [];
  if (rawConsumers !== null) {
    if (!Array.isArray(rawConsumers)) {
      throw replyShapeError("XPENDING", "consumer/count pairs", rawConsumers);
    }
    consumers = rawConsumers.map((row) => {
      if (
        !Array.isArray(row) ||
        row.length !== 2 ||
        typeof row[0] !== "string"
      ) {
        throw replyShapeError("XPENDING", "consumer/count pairs", row);
      }
      return { consumer: row[0], count: expectNumberLike(row[1], "XPENDING") };
    });
  }
  return {
    count: expectNumber(reply[0], "XPENDING"),
    minEntryId: decodeEntryIdOrNull(reply[1]),
    maxEntryId: decodeEntryIdOrNull(reply[2]),
    consumers
  };
}

function decodePendingRows(reply: RedisReply): StreamPendingEntry[] {
  if (!Array.isArray(reply)) {
    throw replyShapeError("XPENDING", "array", reply);
  }
  return reply.map((row) => {
    if (
      !Array.isArray(row) ||
      row.length !== 4 ||
      typeof row[0] !== "string" ||
      typeof row[1] !== "string"
    ) {
      throw replyShapeError(
        "XPENDING",
        "id/consumer/idle/deliveries rows",
        row
      );
    }
    return {
      entryId: row[0],
      consumer: row[1],
      idleMs: expectNumberLike(row[2], "XPENDING"),
      deliveries: expectNumberLike(row[3], "XPENDING")
    };
  });
}

function decodeAutoClaim<TFields extends FieldCodecs>(
  reply: RedisReply,
  fields: TFields
): StreamAutoClaimResult<TFields> {
  if (
    !Array.isArray(reply) ||
    (reply.length !== 2 && reply.length !== 3) ||
    typeof reply[0] !== "string"
  ) {
    throw replyShapeError("XAUTOCLAIM", "cursor/entries reply", reply);
  }
  const entries = decodePendingStreamEntries(reply[1], "XAUTOCLAIM", fields);
  // Redis 6.2 replies with the 2-element form (no deleted-ids list).
  let deletedIds: string[] = [];
  if (reply.length === 3) {
    const rawDeleted = reply[2];
    if (!Array.isArray(rawDeleted)) {
      throw replyShapeError("XAUTOCLAIM", "deleted entry ids", rawDeleted);
    }
    deletedIds = rawDeleted.map((id) => {
      if (typeof id !== "string") {
        throw replyShapeError("XAUTOCLAIM", "deleted entry ids", id);
      }
      return id;
    });
  }
  return { cursor: reply[0], entries, deletedIds };
}

async function sendAck(
  client: RedisClient,
  key: string,
  group: string,
  entryIds: readonly string[]
): Promise<number> {
  if (entryIds.length === 0) return 0;
  return expectNumber(
    await client.send(["XACK", key, group, ...entryIds]),
    "XACK"
  );
}

function createConsumer<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: StreamSchema<TFields, string, TId>,
  group: string,
  consumer: string
): StreamGroupConsumer<TFields, TId> {
  function xreadgroup(
    id: TId,
    options?: StreamReadOptions
  ): Promise<Array<StreamEntry<TFields>>>;
  function xreadgroup(
    id: TId,
    options: StreamPendingReadOptions
  ): Promise<Array<PendingStreamEntry<TFields>>>;
  async function xreadgroup(
    id: TId,
    options: StreamReadOptions | StreamPendingReadOptions = {}
  ): Promise<Array<StreamEntry<TFields>> | Array<PendingStreamEntry<TFields>>> {
    // An `after` *value* selects the history read, not the presence of the
    // key: `{ after: undefined }` matches the `>` overload, which promises
    // non-nullable values, and a history read can return tombstones.
    const after = (options as StreamPendingReadOptions).after;
    const args: RedisCommandArgument[] = ["GROUP", group, consumer];
    if (options.count !== undefined) {
      args.push("COUNT", positiveCount(options.count, "count"));
    }
    if (after !== undefined) {
      if (typeof after !== "string" || after.length === 0 || after === ">") {
        throw new ValidationError(
          "after must be an entry id; new deliveries come from xreadgroup() without { after }"
        );
      }
      args.push("STREAMS", schema.key(id), after);
      const entries = singleStreamEntries(
        await client.send(["XREADGROUP", ...args])
      );
      if (entries === null) return [];
      return decodePendingStreamEntries(entries, "XREADGROUP", schema.fields);
    }
    args.push("STREAMS", schema.key(id), ">");
    const entries = singleStreamEntries(
      await client.send(["XREADGROUP", ...args])
    );
    if (entries === null) return [];
    return decodeStreamEntries(entries, "XREADGROUP", schema.fields);
  }
  return {
    xreadgroup,
    async xack(id: TId, entryIds: readonly string[]): Promise<number> {
      return sendAck(client, schema.key(id), group, entryIds);
    },
    async xclaim(
      id: TId,
      entryIds: readonly string[],
      options: StreamClaimOptions
    ): Promise<Array<PendingStreamEntry<TFields>>> {
      const idle = minIdleMilliseconds(options.minIdleMs);
      if (entryIds.length === 0) return [];
      return decodePendingStreamEntries(
        await client.send([
          "XCLAIM",
          schema.key(id),
          group,
          consumer,
          idle,
          ...entryIds
        ]),
        "XCLAIM",
        schema.fields
      );
    },
    async xautoclaim(
      id: TId,
      options: StreamAutoClaimOptions
    ): Promise<StreamAutoClaimResult<TFields>> {
      const args: RedisCommandArgument[] = [
        schema.key(id),
        group,
        consumer,
        minIdleMilliseconds(options.minIdleMs),
        options.start ?? "0-0"
      ];
      if (options.count !== undefined) {
        args.push("COUNT", positiveCount(options.count, "count"));
      }
      return decodeAutoClaim(
        await client.send(["XAUTOCLAIM", ...args]),
        schema.fields
      );
    }
  };
}

function createGroup<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: StreamSchema<TFields, string, TId>,
  group: string
): StreamGroup<TFields, TId> {
  function xpending(id: TId): Promise<StreamPendingSummary>;
  function xpending(
    id: TId,
    options: StreamPendingRangeOptions
  ): Promise<StreamPendingEntry[]>;
  async function xpending(
    id: TId,
    options?: StreamPendingRangeOptions
  ): Promise<StreamPendingSummary | StreamPendingEntry[]> {
    if (options === undefined) {
      return decodePendingSummary(
        await client.send(["XPENDING", schema.key(id), group])
      );
    }
    const args: RedisCommandArgument[] = [schema.key(id), group];
    if (options.minIdleMs !== undefined) {
      // IDLE goes before start/end (Redis argument order).
      args.push("IDLE", minIdleMilliseconds(options.minIdleMs));
    }
    args.push(
      options.start ?? "-",
      options.end ?? "+",
      positiveCount(options.count, "count")
    );
    if (options.consumer !== undefined) {
      args.push(validConsumerName(options.consumer));
    }
    return decodePendingRows(await client.send(["XPENDING", ...args]));
  }
  return {
    xpending,
    async create(id: TId, options: StreamGroupCreateOptions): Promise<boolean> {
      const from = startEntryId(options.from);
      const args: RedisCommandArgument[] = [
        "CREATE",
        schema.key(id),
        group,
        from
      ];
      if (options.mkstream ?? true) args.push("MKSTREAM");
      let reply: RedisReply;
      try {
        reply = await client.send(["XGROUP", ...args]);
      } catch (error) {
        if (isBusyGroupError(error)) return false;
        throw error;
      }
      if (reply !== "OK") {
        throw replyShapeError("XGROUP", "OK", reply);
      }
      return true;
    },
    async destroy(id: TId): Promise<boolean> {
      const reply = await client.send([
        "XGROUP",
        "DESTROY",
        schema.key(id),
        group
      ]);
      return expectNumber(reply, "XGROUP") === 1;
    },
    async deleteConsumer(id: TId, consumer: string): Promise<number> {
      const reply = await client.send([
        "XGROUP",
        "DELCONSUMER",
        schema.key(id),
        group,
        validConsumerName(consumer)
      ]);
      return expectNumber(reply, "XGROUP");
    },
    async xack(id: TId, entryIds: readonly string[]): Promise<number> {
      return sendAck(client, schema.key(id), group, entryIds);
    },
    consumer(name: string): StreamGroupConsumer<TFields, TId> {
      return createConsumer(client, schema, group, validConsumerName(name));
    }
  };
}

/**
 * Consumer-group operations the Beni layer hangs off the shared stream store:
 * `redis.stream(events).group("workers")` and `group.consumer("w-1")`. Groups
 * are operational runtime resources addressed by name — group topology
 * changes at deploy time, so they hang off the store, not the schema. The
 * stream id (schema key id) stays the first argument of every call.
 *
 * Millisecond options are suffixed `...Ms` (`minIdleMs`, `idleMs`) because
 * XPENDING/XCLAIM speak ms while blocking timeouts speak seconds — the
 * suffix prevents unit confusion at call sites; the asymmetry is deliberate.
 */
export function createStreamGroupOps<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: StreamSchema<TFields, string, TId>
): { group(name: string): StreamGroup<TFields, TId> } {
  return {
    group(name: string): StreamGroup<TFields, TId> {
      return createGroup(client, schema, validGroupName(name));
    }
  };
}

/**
 * Session-only superset: identical group surface, but consumers additionally
 * accept a `{ timeoutSeconds }` xreadgroup (XREADGROUP BLOCK). Takes the session's
 * gated RedisClient facade and is spread over the shared group ops by the
 * session accessors, so the blocking overload is structurally absent from
 * shared-client types.
 */
export function createBlockingStreamGroupOps<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: StreamSchema<TFields, string, TId>
): { group(name: string): BlockingStreamGroup<TFields, TId> } {
  return {
    group(name: string): BlockingStreamGroup<TFields, TId> {
      const group = validGroupName(name);
      const base = createGroup(client, schema, group);
      return {
        ...base,
        consumer(consumer: string): BlockingStreamGroupConsumer<TFields, TId> {
          const consumerName = validConsumerName(consumer);
          const base = createConsumer(client, schema, group, consumerName);
          async function blockingXreadgroup(
            id: TId,
            options: StreamBlockingReadOptions
          ): Promise<Array<StreamEntry<TFields>>> {
            const args: RedisCommandArgument[] = ["GROUP", group, consumerName];
            if (options.count !== undefined) {
              args.push("COUNT", positiveCount(options.count, "count"));
            }
            args.push(
              "BLOCK",
              blockingTimeoutMilliseconds(options.timeoutSeconds)
            );
            args.push("STREAMS", schema.key(id), ">");
            const entries = singleStreamEntries(
              await client.send(["XREADGROUP", ...args])
            );
            if (entries === null) return [];
            return decodeStreamEntries(entries, "XREADGROUP", schema.fields);
          }
          return {
            ...base,
            // Dispatch: `{ timeoutSeconds }` blocks (`>`), everything else delegates
            // to the base overloads (new deliveries / `{ after }` history).
            xreadgroup(
              id: TId,
              options?:
                | StreamReadOptions
                | StreamPendingReadOptions
                | StreamBlockingReadOptions
            ) {
              if (options !== undefined && "timeoutSeconds" in options) {
                return blockingXreadgroup(
                  id,
                  options as StreamBlockingReadOptions
                );
              }
              return base.xreadgroup(id, options as StreamPendingReadOptions);
            }
          } as BlockingStreamGroupConsumer<TFields, TId>;
        }
      };
    }
  };
}

import { replyShapeError, ValidationError } from "./errors.js";
import { createKeyLifecycleOps, expectNumber } from "./helpers.js";
import type {
  RedisClient,
  RedisCommandArgument,
  RedisKey,
  RedisKeyPart,
  RedisReply
} from "./types.js";

export type BitmapSchema<
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
> = {
  readonly kind: "bitmap";
  readonly prefix: TPrefix;
  key<TActualId extends TId>(id: TActualId): RedisKey<TPrefix, TActualId>;
};

export type BitmapRangeUnit = "BYTE" | "BIT";

export type BitmapRange = {
  readonly start: number;
  readonly end: number;
  readonly unit?: BitmapRangeUnit;
};

export type BitmapPositionOptions = {
  readonly start?: number;
  readonly end?: number;
  readonly unit?: BitmapRangeUnit;
};

export type BitmapOperation = "AND" | "OR" | "XOR" | "NOT";

/** A bitfield encoding: `u<1-63>` (unsigned) or `i<1-64>` (signed). */
export type BitfieldType = `u${number}` | `i${number}`;

/**
 * Where a bitfield operation reads or writes: an absolute bit offset, or
 * `#<n>` to address the nth field of the operation's width.
 */
export type BitfieldOffset = number | `#${number}`;

/** Overflow behavior for subsequent `set`/`incrby` operations. */
export type BitfieldOverflow = "wrap" | "sat" | "fail";

/**
 * A chainable BITFIELD builder. Each operation appends to the result tuple:
 * `get` yields a `number`, while `set`/`incrby` yield `number | null` (null
 * when an `overflow("fail")` operation overflows). `overflow` sets the mode
 * for the operations that follow it and does not add a result.
 */
export interface BitfieldBuilder<T extends readonly (number | null)[]> {
  get(
    type: BitfieldType,
    offset: BitfieldOffset
  ): BitfieldBuilder<[...T, number]>;
  set(
    type: BitfieldType,
    offset: BitfieldOffset,
    value: number
  ): BitfieldBuilder<[...T, number | null]>;
  incrby(
    type: BitfieldType,
    offset: BitfieldOffset,
    increment: number
  ): BitfieldBuilder<[...T, number | null]>;
  overflow(mode: BitfieldOverflow): BitfieldBuilder<T>;
  exec(): Promise<T>;
}

export function defineBitmap<
  TPrefix extends string,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  _options?: { readonly ids?: TIds }
): BitmapSchema<TPrefix, TIds[number]> {
  return {
    kind: "bitmap",
    prefix,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    }
  };
}

function bitOffset(offset: number): number {
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new ValidationError("offset must be a non-negative safe integer");
  }
  return offset;
}

function rangeIndex(value: number, name: "start" | "end"): number {
  if (!Number.isSafeInteger(value)) {
    throw new ValidationError(`${name} must be a safe integer`);
  }
  return value;
}

const BITFIELD_TYPE_PATTERN = /^([iu])([1-9][0-9]*)$/;

function bitfieldType(type: BitfieldType): BitfieldType {
  const match = BITFIELD_TYPE_PATTERN.exec(type);
  if (match === null) {
    throw new ValidationError(
      `bitfield type must be "u<width>" or "i<width>", received ${JSON.stringify(type)}`
    );
  }
  const signed = match[1] === "i";
  const max = signed ? 64 : 63;
  const width = Number(match[2]);
  if (width > max) {
    throw new ValidationError(
      `bitfield ${signed ? "signed" : "unsigned"} width must be 1-${max}, received ${width}`
    );
  }
  return type;
}

function bitfieldOffset(offset: BitfieldOffset): RedisCommandArgument {
  if (typeof offset === "number") return bitOffset(offset);
  if (!/^#(0|[1-9][0-9]*)$/.test(offset)) {
    throw new ValidationError(
      `bitfield offset must be a non-negative integer or "#<n>", received ${JSON.stringify(offset)}`
    );
  }
  return offset;
}

function bitfieldValue(value: number, name: "value" | "increment"): number {
  if (!Number.isSafeInteger(value)) {
    throw new ValidationError(
      `bitfield ${name} must be a safe integer, received ${String(value)}`
    );
  }
  return value;
}

function overflowToken(mode: BitfieldOverflow): "WRAP" | "SAT" | "FAIL" {
  switch (mode) {
    case "wrap":
      return "WRAP";
    case "sat":
      return "SAT";
    case "fail":
      return "FAIL";
    default:
      throw new ValidationError(
        `bitfield overflow must be "wrap", "sat", or "fail", received ${JSON.stringify(mode)}`
      );
  }
}

function decodeBitfieldReply(reply: RedisReply): (number | null)[] {
  if (!Array.isArray(reply)) {
    throw replyShapeError("BITFIELD", "an array", reply);
  }
  return reply.map((entry) =>
    entry === null ? null : expectNumber(entry, "BITFIELD")
  );
}

function makeBitfieldBuilder<T extends readonly (number | null)[]>(
  client: RedisClient,
  key: string,
  args: readonly RedisCommandArgument[]
): BitfieldBuilder<T> {
  return {
    get(type, offset) {
      return makeBitfieldBuilder<[...T, number]>(client, key, [
        ...args,
        "GET",
        bitfieldType(type),
        bitfieldOffset(offset)
      ]);
    },
    set(type, offset, value) {
      return makeBitfieldBuilder<[...T, number | null]>(client, key, [
        ...args,
        "SET",
        bitfieldType(type),
        bitfieldOffset(offset),
        bitfieldValue(value, "value")
      ]);
    },
    incrby(type, offset, increment) {
      return makeBitfieldBuilder<[...T, number | null]>(client, key, [
        ...args,
        "INCRBY",
        bitfieldType(type),
        bitfieldOffset(offset),
        bitfieldValue(increment, "increment")
      ]);
    },
    overflow(mode) {
      return makeBitfieldBuilder<T>(client, key, [
        ...args,
        "OVERFLOW",
        overflowToken(mode)
      ]);
    },
    async exec() {
      return decodeBitfieldReply(
        await client.send(["BITFIELD", key, ...args])
      ) as unknown as T;
    }
  };
}

export function createBitmapStore<TId extends RedisKeyPart = RedisKeyPart>(
  client: RedisClient,
  schema: BitmapSchema<string, TId>
) {
  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    /**
     * SETBIT — set the bit at `offset`; returns the previous bit.
     * @example await redis.bitmap(seen).setbit("2026-07-11", userId, true);
     */
    async setbit(id: TId, offset: number, value: boolean): Promise<boolean> {
      return (
        expectNumber(
          await client.send([
            "SETBIT",
            schema.key(id),
            bitOffset(offset),
            value ? 1 : 0
          ]),
          "SETBIT"
        ) === 1
      );
    },
    /** GETBIT — the bit at `offset` (`false` past the end of the value). */
    async getbit(id: TId, offset: number): Promise<boolean> {
      return (
        expectNumber(
          await client.send(["GETBIT", schema.key(id), bitOffset(offset)]),
          "GETBIT"
        ) === 1
      );
    },
    /**
     * BITCOUNT — number of set bits, optionally within a `BYTE`/`BIT` range.
     * @example const active = await redis.bitmap(seen).bitcount("2026-07-11");
     */
    async bitcount(id: TId, range?: BitmapRange): Promise<number> {
      const args: RedisCommandArgument[] = [];
      if (range !== undefined) {
        args.push(
          rangeIndex(range.start, "start"),
          rangeIndex(range.end, "end")
        );
        if (range.unit !== undefined) args.push(range.unit);
      }
      return expectNumber(
        await client.send(["BITCOUNT", schema.key(id), ...args]),
        "BITCOUNT"
      );
    },
    /**
     * BITPOS — index of the first bit equal to `bit`, optionally within a
     * range; `null` if not found.
     */
    async bitpos(
      id: TId,
      bit: boolean,
      options?: BitmapPositionOptions
    ): Promise<number | null> {
      if (options?.end !== undefined && options.start === undefined) {
        throw new ValidationError("bitpos end requires start");
      }
      if (options?.unit !== undefined && options.end === undefined) {
        throw new ValidationError("bitpos unit requires start and end");
      }
      const args: RedisCommandArgument[] = [];
      if (options?.start !== undefined) {
        args.push(rangeIndex(options.start, "start"));
        if (options.end !== undefined) {
          args.push(rangeIndex(options.end, "end"));
          if (options.unit !== undefined) args.push(options.unit);
        }
      }
      const found = expectNumber(
        await client.send(["BITPOS", schema.key(id), bit ? 1 : 0, ...args]),
        "BITPOS"
      );
      return found === -1 ? null : found;
    },
    /** BITOP — combine `sources` with `operation` into `destination`. */
    async bitop(
      destination: TId,
      operation: BitmapOperation,
      sources: readonly TId[]
    ): Promise<number> {
      if (sources.length === 0) {
        throw new ValidationError("bitop requires at least one source id");
      }
      if (operation === "NOT" && sources.length !== 1) {
        throw new ValidationError(
          "bitop with NOT requires exactly one source id"
        );
      }
      return expectNumber(
        await client.send([
          "BITOP",
          operation,
          schema.key(destination),
          ...sources.map((source) => schema.key(source))
        ]),
        "BITOP"
      );
    },
    /**
     * BITFIELD — chainable builder for `get`/`set`/`incrby`/`overflow`
     * operations; `exec()` sends one command and returns the result tuple.
     */
    bitfield(id: TId): BitfieldBuilder<[]> {
      return makeBitfieldBuilder<[]>(client, schema.key(id), []);
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", schema.key(id)]), "DEL");
    }
  };
}

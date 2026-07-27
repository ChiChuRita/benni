import { ReplyShapeError, replyShapeError, ValidationError } from "./errors.js";
import { decodeSortedSetEntries } from "./helpers.js";
import { keyspaceGlob } from "./keys.js";
import type {
  FieldCodecs,
  HashSchema,
  InferHashOutput,
  Keyspace,
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply,
  SetSchema,
  SortedSetEntry,
  SortedSetSchema
} from "./types.js";

export type ScanOptions = {
  readonly match?: string;
  readonly count?: number;
  readonly type?: string;
};

export type ScanMemberOptions = {
  readonly match?: string;
  readonly count?: number;
};

export type HashScanEntry<TFields extends FieldCodecs> = {
  [K in keyof TFields & string]: {
    readonly field: K;
    readonly value: InferHashOutput<TFields>[K];
  };
}[keyof TFields & string];

function scanCount(count: number): number {
  if (!Number.isSafeInteger(count) || count < 1) {
    throw new ValidationError("count must be a positive safe integer");
  }
  return count;
}

function scanArguments(options: ScanOptions): RedisCommandArgument[] {
  const args: RedisCommandArgument[] = [];
  if (options.match !== undefined) args.push("MATCH", options.match);
  if (options.count !== undefined) args.push("COUNT", scanCount(options.count));
  if (options.type !== undefined) args.push("TYPE", options.type);
  return args;
}

function parseScanPage(
  reply: RedisReply,
  command: string
): { cursor: string; items: readonly RedisReply[] } {
  if (!Array.isArray(reply) || reply.length !== 2) {
    throw replyShapeError(command, "[cursor, items]", reply);
  }
  const [cursor, items] = reply;
  if (typeof cursor !== "string" && typeof cursor !== "number") {
    throw replyShapeError(command, "string or number cursor", cursor);
  }
  // SCAN cursors are unsigned 64-bit; a numeric cursor past 2^53 has already
  // lost precision, and stringifying it would silently corrupt the scan
  // (missed or repeated keys). Fail loudly instead.
  if (typeof cursor === "number" && !Number.isSafeInteger(cursor)) {
    throw new ReplyShapeError(
      `Expected Redis ${command} cursor to be a safe integer or string`,
      cursor
    );
  }
  if (!Array.isArray(items)) {
    throw replyShapeError(command, "items array", items);
  }
  return { cursor: String(cursor), items };
}

function expectStringItem(item: RedisReply, command: string): string {
  if (typeof item !== "string") {
    throw replyShapeError(`${command} item`, "string", item);
  }
  return item;
}

async function* scanPages(
  client: RedisClient,
  command: string,
  commandFor: (cursor: string) => RedisCommand
): AsyncGenerator<readonly RedisReply[], void, undefined> {
  let cursor = "0";
  do {
    const page = parseScanPage(await client.send(commandFor(cursor)), command);
    cursor = page.cursor;
    yield page.items;
  } while (cursor !== "0");
}

/**
 * SCAN — lazily iterate every key in the database matching `match`/`type`,
 * one cursor page per round trip.
 */
export async function* scanKeys(
  client: RedisClient,
  options: ScanOptions = {}
): AsyncIterable<string> {
  const args = scanArguments(options);
  const pages = scanPages(client, "SCAN", (cursor) => [
    "SCAN",
    cursor,
    ...args
  ]);
  for await (const items of pages) {
    for (const item of items) {
      yield expectStringItem(item, "SCAN");
    }
  }
}

/**
 * SCAN scoped to a keyspace — iterates keys under the schema's prefix
 * (glob-escaped, in the schema's hash-tag layout) unless an explicit `match`
 * overrides it.
 */
export async function* scanKeyspace<TInput, TOutput>(
  client: RedisClient,
  keyspace: Keyspace<TInput, TOutput>,
  options: ScanOptions = {}
): AsyncIterable<string> {
  const match =
    options.match ?? keyspaceGlob(keyspace.prefix, keyspace.hashTag);
  yield* scanKeys(client, { ...options, match });
}

/** SSCAN — lazily iterate a set's members, decoded, one page per round trip. */
export async function* scanSet<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: SetSchema<TInput, TOutput, string, TId>,
  id: NoInfer<TId>,
  options: ScanMemberOptions = {}
): AsyncIterable<TOutput> {
  const key = schema.key(id);
  const args = scanArguments(options);
  const pages = scanPages(client, "SSCAN", (cursor) => [
    "SSCAN",
    key,
    cursor,
    ...args
  ]);
  for await (const items of pages) {
    for (const item of items) {
      yield schema.decode(expectStringItem(item, "SSCAN"));
    }
  }
}

/**
 * HSCAN — lazily iterate a hash's field/value entries, decoded; fields not
 * declared in the schema are skipped.
 */
export async function* scanHash<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: HashSchema<TFields, string, TId>,
  id: NoInfer<TId>,
  options: ScanMemberOptions = {}
): AsyncIterable<HashScanEntry<TFields>> {
  const key = schema.key(id);
  const args = scanArguments(options);
  const pages = scanPages(client, "HSCAN", (cursor) => [
    "HSCAN",
    key,
    cursor,
    ...args
  ]);
  for await (const items of pages) {
    if (items.length % 2 !== 0) {
      throw replyShapeError("HSCAN", "field/value pairs", items);
    }
    for (let index = 0; index < items.length; index += 2) {
      const field = expectStringItem(items[index], "HSCAN");
      const stored = expectStringItem(items[index + 1], "HSCAN");
      if (!Object.hasOwn(schema.fields, field)) continue;
      yield {
        field,
        value: schema.fields[field].decode(stored)
      } as HashScanEntry<TFields>;
    }
  }
}

/** ZSCAN — lazily iterate a sorted set's member/score entries, decoded. */
export async function* scanSortedSet<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(
  client: RedisClient,
  schema: SortedSetSchema<TInput, TOutput, string, TId>,
  id: NoInfer<TId>,
  options: ScanMemberOptions = {}
): AsyncIterable<SortedSetEntry<TOutput>> {
  const key = schema.key(id);
  const args = scanArguments(options);
  const pages = scanPages(client, "ZSCAN", (cursor) => [
    "ZSCAN",
    key,
    cursor,
    ...args
  ]);
  for await (const items of pages) {
    yield* decodeSortedSetEntries(items, "ZSCAN", schema);
  }
}

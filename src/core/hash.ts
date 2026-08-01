import {
  describeReply,
  PartialRecordError,
  ReplyShapeError,
  replyShapeError,
  ValidationError
} from "./errors.js";
import {
  createKeyLifecycleOps,
  type ExpiryOptions,
  expectNumber,
  expectNumberLike,
  expiryArgs,
  positiveSafeInteger,
  ttlSeconds
} from "./helpers.js";
import { type HashTagLayout, type KeyOptions, keyBuilder } from "./keys.js";
import {
  type StoreBinding,
  type StoreContext,
  withKey,
  withStore
} from "./store.js";
import type {
  Codec,
  FieldCodecs,
  HashSchema,
  InferHashInput,
  InferHashOutput,
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisKeyPart,
  RedisReply,
  StoreSetOptions
} from "./types.js";

/**
 * Field-expiry vocabulary for `hexpire`. A bare `number` is a relative
 * TTL in seconds (HEXPIRE) — the original, backward-compatible form. The
 * object forms select the millisecond and absolute-Unix variants, each
 * mapping to a distinct Redis command.
 */
export type HashFieldExpiry =
  | number
  | { readonly ttlSeconds: number }
  | { readonly ttlMilliseconds: number }
  | { readonly expireAtSeconds: number }
  | { readonly expireAtMilliseconds: number };

/**
 * Options for `hsetex` (HSETEX). At most one expiry mode, and `fnx` (write
 * only if no field exists) is exclusive with `fxx` (only if all exist) — both
 * constraints are modeled in the type, so invalid combinations don't compile.
 */
type HashSetExExpiry =
  | {
      readonly ttlSeconds?: number;
      readonly ttlMilliseconds?: never;
      readonly expireAtSeconds?: never;
      readonly expireAtMilliseconds?: never;
      readonly keepTtl?: never;
    }
  | {
      readonly ttlMilliseconds?: number;
      readonly ttlSeconds?: never;
      readonly expireAtSeconds?: never;
      readonly expireAtMilliseconds?: never;
      readonly keepTtl?: never;
    }
  | {
      readonly expireAtSeconds?: number;
      readonly ttlSeconds?: never;
      readonly ttlMilliseconds?: never;
      readonly expireAtMilliseconds?: never;
      readonly keepTtl?: never;
    }
  | {
      readonly expireAtMilliseconds?: number;
      readonly ttlSeconds?: never;
      readonly ttlMilliseconds?: never;
      readonly expireAtSeconds?: never;
      readonly keepTtl?: never;
    }
  | {
      readonly keepTtl?: true;
      readonly ttlSeconds?: never;
      readonly ttlMilliseconds?: never;
      readonly expireAtSeconds?: never;
      readonly expireAtMilliseconds?: never;
    };

type HashSetExCondition =
  | { readonly fnx?: boolean; readonly fxx?: never }
  | { readonly fxx?: boolean; readonly fnx?: never };

export type HashSetExOptions = HashSetExExpiry & HashSetExCondition;

/** Options for the field-TTL / field-expire-time reads. */
export type HashFieldTtlOptions = {
  readonly milliseconds?: boolean;
};

/**
 * Maps HashFieldExpiry to the `[command, value]` pair for HEXPIRE/HPEXPIRE/
 * HEXPIREAT/HPEXPIREAT. The command name encodes the unit and relative-vs-
 * absolute choice, so each mode is its own command rather than an arg token.
 */
function hashExpireCommand(expiry: HashFieldExpiry): readonly [string, number] {
  if (typeof expiry === "number") {
    return ["HEXPIRE", ttlSeconds(expiry)];
  }
  const modes = expiry as {
    readonly ttlSeconds?: number;
    readonly ttlMilliseconds?: number;
    readonly expireAtSeconds?: number;
    readonly expireAtMilliseconds?: number;
  };
  const provided = [
    modes.ttlSeconds,
    modes.ttlMilliseconds,
    modes.expireAtSeconds,
    modes.expireAtMilliseconds
  ].filter((mode) => mode !== undefined);
  if (provided.length !== 1) {
    throw new ValidationError(
      "field expiry must set exactly one of ttlSeconds, ttlMilliseconds, expireAtSeconds, or expireAtMilliseconds"
    );
  }
  if (modes.ttlSeconds !== undefined) {
    return ["HEXPIRE", positiveSafeInteger(modes.ttlSeconds, "ttlSeconds")];
  }
  if (modes.ttlMilliseconds !== undefined) {
    return [
      "HPEXPIRE",
      positiveSafeInteger(modes.ttlMilliseconds, "ttlMilliseconds")
    ];
  }
  if (modes.expireAtSeconds !== undefined) {
    return [
      "HEXPIREAT",
      positiveSafeInteger(modes.expireAtSeconds, "expireAtSeconds")
    ];
  }
  return [
    "HPEXPIREAT",
    positiveSafeInteger(
      modes.expireAtMilliseconds as number,
      "expireAtMilliseconds"
    )
  ];
}

/** Maps HashSetExOptions to the HSETEX condition + expiry argument tokens. */
function hashSetExArgs(options: HashSetExOptions): RedisCommandArgument[] {
  // The types forbid fnx+fxx, but untyped JS callers can still pass both.
  if (options.fxx && options.fnx) {
    throw new ValidationError("hsetex cannot set both fnx and fxx");
  }
  const args: RedisCommandArgument[] = [];
  if (options.fnx) args.push("FNX");
  else if (options.fxx) args.push("FXX");
  const provided = [
    options.ttlSeconds,
    options.ttlMilliseconds,
    options.expireAtSeconds,
    options.expireAtMilliseconds,
    options.keepTtl
  ].filter((mode) => mode !== undefined);
  if (provided.length > 1) {
    throw new ValidationError(
      "hsetex expiry must set at most one of ttlSeconds, ttlMilliseconds, expireAtSeconds, expireAtMilliseconds, or keepTtl"
    );
  }
  if (options.ttlSeconds !== undefined) {
    args.push("EX", positiveSafeInteger(options.ttlSeconds, "ttlSeconds"));
  } else if (options.ttlMilliseconds !== undefined) {
    args.push(
      "PX",
      positiveSafeInteger(options.ttlMilliseconds, "ttlMilliseconds")
    );
  } else if (options.expireAtSeconds !== undefined) {
    args.push(
      "EXAT",
      positiveSafeInteger(options.expireAtSeconds, "expireAtSeconds")
    );
  } else if (options.expireAtMilliseconds !== undefined) {
    args.push(
      "PXAT",
      positiveSafeInteger(options.expireAtMilliseconds, "expireAtMilliseconds")
    );
  } else if (options.keepTtl) {
    args.push("KEEPTTL");
  }
  return args;
}

type NumberHashField<TFields extends FieldCodecs> = {
  [K in keyof TFields]: TFields[K] extends Codec<number, number> ? K : never;
}[keyof TFields] &
  string;

export type PartialHashOutput<TFields extends FieldCodecs> = {
  [K in keyof TFields]?: InferHashOutput<TFields>[K];
};

// Optional, not required: HMGET/HGETEX/HGETDEL fill only the field names the
// call actually asked for, and a caller can pass a narrowed subset of the union
// at runtime. Declaring every member of TField as a present key promised data
// the reply need not contain.
export type PickedHashOutput<
  TFields extends FieldCodecs,
  TField extends keyof TFields & string
> = {
  [K in TField]?: InferHashOutput<TFields>[K] | null;
};

function expectNumberArray(reply: RedisReply, command: string): number[] {
  if (!Array.isArray(reply)) {
    throw replyShapeError(command, "array", reply);
  }
  return reply.map((value) => {
    if (typeof value !== "number") {
      throw new ReplyShapeError(
        `Expected Redis ${command} item to return number, got ${describeReply(value)}`,
        value
      );
    }
    return value;
  });
}

async function readSingleFieldExpiry(
  client: RedisClient,
  key: string,
  command: string,
  field: string
): Promise<number> {
  const reply = await client.send([command, key, "FIELDS", 1, field]);
  const values = expectNumberArray(reply, command);
  if (values.length !== 1) {
    throw replyShapeError(command, "one number", reply);
  }
  return values[0];
}

function hashEntries(
  reply: RedisReply,
  command: string
): Array<[string, string]> {
  const entries: Array<[string, string]> = [];
  if (reply instanceof Map) {
    for (const [field, value] of reply) {
      entries.push(hashEntry(field, value, command));
    }
    return entries;
  }
  if (!Array.isArray(reply)) {
    throw replyShapeError(command, "array or map", reply);
  }
  if (reply.length % 2 !== 0) {
    throw replyShapeError(command, "field/value pairs", reply);
  }
  for (let index = 0; index < reply.length; index += 2) {
    entries.push(hashEntry(reply[index], reply[index + 1], command));
  }
  return entries;
}

function hashEntry(
  field: RedisReply,
  value: RedisReply,
  command: string
): [string, string] {
  if (typeof field !== "string" || typeof value !== "string") {
    throw new ReplyShapeError(
      `Expected Redis ${command} to return field/value strings, got ${describeReply(field)}/${describeReply(value)}`,
      [field, value]
    );
  }
  return [field, value];
}

export function createHashStore<
  TFields extends FieldCodecs,
  TId extends RedisKeyPart = RedisKeyPart
>(client: RedisClient, schema: HashSchema<TFields, string, TId>) {
  type Input = InferHashInput<TFields>;
  type Output = InferHashOutput<TFields>;
  const declaredFields = Object.keys(schema.fields) as Array<
    keyof TFields & string
  >;
  const fieldCodec = <TField extends keyof TFields & string>(field: TField) => {
    // Own-property only: a plain index read walks the prototype chain, so
    // "toString" or "constructor" resolves to an Object.prototype member,
    // passes the truthiness check, and then blows up on .decode.
    const codec = Object.hasOwn(schema.fields, field)
      ? schema.fields[field]
      : undefined;
    if (!codec) {
      throw new ValidationError(
        `Unknown hash field '${field}'; declared fields: ${declaredFields.join(", ")}`
      );
    }
    return codec;
  };

  // Positional decode shared by HMGET / HGETEX / HGETDEL: each returns the
  // requested field values in order, with nil (null/absent) for missing ones.
  const decodePicked = <TField extends keyof TFields & string>(
    reply: RedisReply,
    codecsByField: ReadonlyArray<readonly [TField, TFields[TField]]>,
    command: string
  ): PickedHashOutput<TFields, TField> => {
    if (!Array.isArray(reply)) {
      throw replyShapeError(command, "array", reply);
    }
    const output = {} as PickedHashOutput<TFields, TField>;
    for (const [index, [field, codec]] of codecsByField.entries()) {
      const value = reply[index];
      if (value === null) {
        output[field] = null;
        continue;
      }
      if (typeof value !== "string") {
        throw new ReplyShapeError(
          `Expected Redis ${command} item to return string or null, got ${describeReply(value)}`,
          value
        );
      }
      output[field] = codec.decode(value) as Output[TField];
    }
    return output;
  };

  // Object-literal methods can't carry overload signatures, so the collapsed
  // commands (HSET / HGET / HRANDFIELD) are declared as overloaded standalone
  // functions and spread onto the returned object below.

  /**
   * HSET. Two forms:
   * - `hset(id, value, options?)` writes the whole record (every declared
   *   field), with an optional TTL; resolves to `void`.
   * - `hset(id, field, value)` writes one field; resolves to the number of
   *   newly-added fields (0 or 1).
   */
  function hset(
    id: TId,
    value: Input,
    options?: StoreSetOptions
  ): Promise<void>;
  function hset<TField extends keyof TFields & string>(
    id: TId,
    field: TField,
    value: Input[TField]
  ): Promise<number>;
  async function hset(
    id: TId,
    valueOrField: Input | (keyof TFields & string),
    optionsOrValue?: StoreSetOptions | Input[keyof TFields & string]
  ): Promise<void | number> {
    if (typeof valueOrField === "string") {
      const field = valueOrField as keyof TFields & string;
      const reply = await client.send([
        "HSET",
        schema.key(id),
        field,
        fieldCodec(field).encode(
          optionsOrValue as Input[keyof TFields & string]
        )
      ]);
      return expectNumber(reply, "HSET");
    }
    const value = valueOrField;
    const options = (optionsOrValue as StoreSetOptions | undefined) ?? {};
    const key = schema.key(id);
    // One variadic HSET writes the whole record atomically — no torn record
    // if the process dies mid-write, no interleaving with other clients.
    const commands: RedisCommand[] = [
      [
        "HSET",
        key,
        ...declaredFields.flatMap((field) => [
          field,
          schema.fields[field].encode(value[field])
        ])
      ]
    ];
    if (options.ttlSeconds !== undefined) {
      commands.push(["EXPIRE", key, ttlSeconds(options.ttlSeconds)]);
    }
    // A pipeline only batches; it does not make the pair atomic. With a TTL
    // that matters: another client reading between the HSET and the EXPIRE
    // sees a record with no expiry, and a connection lost in the same window
    // leaves one that never expires at all. MULTI/EXEC closes both. Every
    // adapter implements transaction(); the fallback is for a custom client
    // that does not, which is no worse off than before. On a session holding
    // a WATCH the facade degrades this back to a pipeline rather than let an
    // EXEC clear the caller's watch set (see createBeniSession).
    const replies =
      options.ttlSeconds === undefined
        ? await client.pipeline(commands)
        : await (client.transaction?.(commands) ?? client.pipeline(commands));

    for (const reply of replies) {
      if (typeof reply !== "number") {
        throw new ReplyShapeError(
          `Expected Redis HSET/EXPIRE to return number, got ${describeReply(reply)}`,
          reply
        );
      }
    }
  }

  /**
   * HGET. Two forms:
   * - `hget(id)` reads the whole record; resolves to the record or null.
   * - `hget(id, field)` reads one field; resolves to the decoded value or
   *   null.
   */
  function hget(id: TId): Promise<Output | null>;
  function hget<TField extends keyof TFields & string>(
    id: TId,
    field: TField
  ): Promise<Output[TField] | null>;
  async function hget(
    id: TId,
    field?: keyof TFields & string
  ): Promise<Output | Output[keyof TFields & string] | null> {
    if (field !== undefined) {
      const codec = fieldCodec(field);
      const reply = await client.send(["HGET", schema.key(id), field]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("HGET", "string or null", reply);
      }
      return codec.decode(reply) as Output[keyof TFields & string];
    }
    // One HMGET reads the whole record atomically — a pipeline of HGETs can
    // interleave with other clients' writes and produce a torn read.
    const reply = await client.send([
      "HMGET",
      schema.key(id),
      ...declaredFields
    ]);
    if (!Array.isArray(reply)) {
      throw replyShapeError("HMGET", "array", reply);
    }

    if (reply.every((value) => value === null)) return null;

    const missing = declaredFields.filter(
      (_, index) => typeof reply[index] !== "string"
    );
    if (missing.length > 0) {
      // Not a shape violation: the reply is well formed and the record is
      // simply incomplete, which per-field TTLs make an ordinary outcome. A
      // dedicated class lets a caller tell the two apart, and it still extends
      // ReplyShapeError so existing handling keeps working.
      throw new PartialRecordError(
        `Hash ${schema.key(id)} is missing declared field(s): ${missing.join(", ")}`,
        reply,
        missing
      );
    }

    const output: Partial<Output> = {};
    for (const [index, f] of declaredFields.entries()) {
      output[f as keyof Output] = schema.fields[f].decode(
        reply[index] as string
      ) as Output[keyof Output];
    }
    return output as Output;
  }

  /**
   * HRANDFIELD. Without `count` returns a single random field name (or null
   * for an empty/missing hash); with `count` returns an array of field names
   * (negative `count` allows repeats).
   */
  function hrandfield(id: TId): Promise<string | null>;
  function hrandfield(
    id: TId,
    options: { readonly count: number }
  ): Promise<string[]>;
  async function hrandfield(
    id: TId,
    options?: { readonly count: number }
  ): Promise<string | null | string[]> {
    if (options === undefined) {
      const reply = await client.send(["HRANDFIELD", schema.key(id)]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("HRANDFIELD", "string or null", reply);
      }
      return reply;
    }
    const { count } = options;
    if (!Number.isSafeInteger(count) || count === 0) {
      throw new ValidationError("count must be a nonzero safe integer");
    }
    const reply = await client.send(["HRANDFIELD", schema.key(id), count]);
    if (!Array.isArray(reply)) {
      throw replyShapeError("HRANDFIELD", "array", reply);
    }
    return reply.map((field) => {
      if (typeof field !== "string") {
        throw new ReplyShapeError(
          `Expected Redis HRANDFIELD item to return string, got ${describeReply(field)}`,
          field
        );
      }
      return field;
    });
  }

  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    hset,
    hget,
    hrandfield,
    /** HGETALL — tolerant/partial read; unknown fields are ignored. */
    async hgetall(id: TId): Promise<PartialHashOutput<TFields> | null> {
      const reply = await client.send(["HGETALL", schema.key(id)]);
      const entries = hashEntries(reply, "HGETALL");
      if (entries.length === 0) return null;

      const output: PartialHashOutput<TFields> = {};
      for (const [field, value] of entries) {
        // Field names come from Redis, so the lookup has to be own-property:
        // an undeclared field named after an Object.prototype member
        // ("toString", "constructor", "__proto__") otherwise resolves to a
        // truthy non-codec and throws instead of being ignored. scanHash
        // already does it this way.
        if (!Object.hasOwn(schema.fields, field)) continue;
        const codec = schema.fields[field];
        if (!codec) continue;
        output[field as keyof TFields] = codec.decode(
          value
        ) as Output[keyof TFields];
      }
      return output;
    },
    /** HMGET — read the requested fields in order (null for missing). */
    async hmget<TField extends keyof TFields & string>(
      id: TId,
      fields: readonly TField[]
    ): Promise<PickedHashOutput<TFields, TField>> {
      if (fields.length === 0) return {} as PickedHashOutput<TFields, TField>;
      const codecsByField = fields.map(
        (field) => [field, fieldCodec(field)] as const
      );
      const reply = await client.send(["HMGET", schema.key(id), ...fields]);
      return decodePicked(reply, codecsByField, "HMGET");
    },
    /** HGETEX — read fields, optionally (re)setting their expiry. */
    async hgetex<TField extends keyof TFields & string>(
      id: TId,
      fields: readonly TField[],
      expiry?: ExpiryOptions
    ): Promise<PickedHashOutput<TFields, TField>> {
      if (fields.length === 0) {
        // HGETEX ... FIELDS 0 is a server error, so an empty read short
        // circuits. An expiry is a write, though: dropping it silently would
        // leave a computed-field-list caller believing the TTLs moved.
        if (expiry !== undefined) {
          throw new ValidationError(
            "hgetex was given an expiry but no fields; the expiry would be silently dropped"
          );
        }
        return {} as PickedHashOutput<TFields, TField>;
      }
      const codecsByField = fields.map(
        (field) => [field, fieldCodec(field)] as const
      );
      const command: [string, ...RedisCommandArgument[]] = [
        "HGETEX",
        schema.key(id),
        ...(expiry !== undefined ? expiryArgs(expiry) : []),
        "FIELDS",
        fields.length,
        ...fields
      ];
      return decodePicked(await client.send(command), codecsByField, "HGETEX");
    },
    /** HGETDEL — read the requested fields and delete them. */
    async hgetdel<TField extends keyof TFields & string>(
      id: TId,
      fields: readonly TField[]
    ): Promise<PickedHashOutput<TFields, TField>> {
      if (fields.length === 0) return {} as PickedHashOutput<TFields, TField>;
      const codecsByField = fields.map(
        (field) => [field, fieldCodec(field)] as const
      );
      const reply = await client.send([
        "HGETDEL",
        schema.key(id),
        "FIELDS",
        fields.length,
        ...fields
      ]);
      return decodePicked(reply, codecsByField, "HGETDEL");
    },
    /** HKEYS — all field names. */
    async hkeys(id: TId): Promise<string[]> {
      const reply = await client.send(["HKEYS", schema.key(id)]);
      if (!Array.isArray(reply)) {
        throw replyShapeError("HKEYS", "array", reply);
      }
      return reply.map((value) => {
        if (typeof value !== "string") {
          throw new ReplyShapeError(
            `Expected Redis HKEYS item to return string, got ${describeReply(value)}`,
            value
          );
        }
        return value;
      });
    },
    /** HLEN — number of fields. */
    async hlen(id: TId): Promise<number> {
      const reply = await client.send(["HLEN", schema.key(id)]);
      return expectNumber(reply, "HLEN");
    },
    /** HSETNX — set a field only if it does not already exist. */
    async hsetnx<TField extends keyof TFields & string>(
      id: TId,
      field: TField,
      value: Input[TField]
    ): Promise<boolean> {
      const reply = await client.send([
        "HSETNX",
        schema.key(id),
        field,
        fieldCodec(field).encode(value)
      ]);
      return expectNumber(reply, "HSETNX") === 1;
    },
    /** HSETEX — set fields with an expiry/condition in one command. */
    async hsetex(
      id: TId,
      values: Partial<Input>,
      options: HashSetExOptions = {}
    ): Promise<boolean> {
      const fields = Object.keys(values) as Array<keyof TFields & string>;
      if (fields.length === 0) {
        throw new ValidationError("hsetex requires at least one field");
      }
      const pairs: RedisCommandArgument[] = [];
      for (const field of fields) {
        const value = values[field];
        // Partial<Input> admits an explicit undefined unless the caller runs
        // exactOptionalPropertyTypes, and the permissive codecs encode it:
        // string()/enumOf() write "undefined", boolean() writes "0". Refuse
        // rather than corrupt the field with `{ active: form.active }`.
        if (value === undefined) {
          throw new ValidationError(
            `hsetex received undefined for field '${field}'; omit the key to leave the field unchanged, or hdel to remove it`
          );
        }
        pairs.push(field, fieldCodec(field).encode(value));
      }
      const reply = await client.send([
        "HSETEX",
        schema.key(id),
        ...hashSetExArgs(options),
        "FIELDS",
        fields.length,
        ...pairs
      ]);
      return expectNumber(reply, "HSETEX") === 1;
    },
    /** HSTRLEN — length of a field's string value (0 if absent). */
    async hstrlen<TField extends keyof TFields & string>(
      id: TId,
      field: TField
    ): Promise<number> {
      fieldCodec(field);
      const reply = await client.send(["HSTRLEN", schema.key(id), field]);
      return expectNumber(reply, "HSTRLEN");
    },
    /** HINCRBYFLOAT — increment a numeric field by a float amount. */
    async hincrbyfloat<TField extends NumberHashField<TFields>>(
      id: TId,
      field: TField,
      amount: number
    ): Promise<number> {
      if (!Number.isFinite(amount)) {
        throw new ValidationError("amount must be a finite number");
      }
      fieldCodec(field);
      const reply = await client.send([
        "HINCRBYFLOAT",
        schema.key(id),
        field,
        amount
      ]);
      return expectNumberLike(reply, "HINCRBYFLOAT");
    },
    /** HEXPIRE family — set per-field expiry (unit/mode from `expiry`). */
    async hexpire<TField extends keyof TFields & string>(
      id: TId,
      fields: readonly TField[],
      expiry: HashFieldExpiry
    ): Promise<number[]> {
      const [command, value] = hashExpireCommand(expiry);
      if (fields.length === 0) return [];
      for (const field of fields) fieldCodec(field);
      const reply = await client.send([
        command,
        schema.key(id),
        value,
        "FIELDS",
        fields.length,
        ...fields
      ]);
      return expectNumberArray(reply, command);
    },
    /** HTTL / HPTTL — remaining TTL of a field (`milliseconds` for HPTTL). */
    async httl<TField extends keyof TFields & string>(
      id: TId,
      field: TField,
      options?: HashFieldTtlOptions
    ): Promise<number> {
      fieldCodec(field);
      return readSingleFieldExpiry(
        client,
        schema.key(id),
        options?.milliseconds ? "HPTTL" : "HTTL",
        field
      );
    },
    /**
     * HEXPIRETIME / HPEXPIRETIME — absolute expiry of a field (`milliseconds`
     * for HPEXPIRETIME).
     */
    async hexpiretime<TField extends keyof TFields & string>(
      id: TId,
      field: TField,
      options?: HashFieldTtlOptions
    ): Promise<number> {
      fieldCodec(field);
      return readSingleFieldExpiry(
        client,
        schema.key(id),
        options?.milliseconds ? "HPEXPIRETIME" : "HEXPIRETIME",
        field
      );
    },
    /** HPERSIST — remove per-field expiry. */
    async hpersist<TField extends keyof TFields & string>(
      id: TId,
      fields: readonly TField[]
    ): Promise<number[]> {
      if (fields.length === 0) return [];
      for (const field of fields) fieldCodec(field);
      const reply = await client.send([
        "HPERSIST",
        schema.key(id),
        "FIELDS",
        fields.length,
        ...fields
      ]);
      return expectNumberArray(reply, "HPERSIST");
    },
    /**
     * HDEL — delete one field or several, returning the count removed.
     * @example await redis.hash(users).hdel("42", ["name", "score"]);
     */
    async hdel<TField extends keyof TFields & string>(
      id: TId,
      fields: TField | readonly TField[]
    ): Promise<number> {
      const list = Array.isArray(fields)
        ? (fields as readonly TField[])
        : [fields as TField];
      if (list.length === 0) return 0;
      for (const field of list) fieldCodec(field);
      const reply = await client.send(["HDEL", schema.key(id), ...list]);
      return expectNumber(reply, "HDEL");
    },
    /** HEXISTS — whether a field is present. */
    async hexists<TField extends keyof TFields & string>(
      id: TId,
      field: TField
    ): Promise<boolean> {
      fieldCodec(field);
      const reply = await client.send(["HEXISTS", schema.key(id), field]);
      return expectNumber(reply, "HEXISTS") === 1;
    },
    /** HINCRBY — increment a numeric field by an integer amount. */
    async hincrby<TField extends NumberHashField<TFields>>(
      id: TId,
      field: TField,
      amount: number
    ): Promise<number> {
      if (!Number.isSafeInteger(amount)) {
        throw new ValidationError("amount must be a safe integer");
      }
      fieldCodec(field);
      const reply = await client.send([
        "HINCRBY",
        schema.key(id),
        field,
        amount
      ]);
      return expectNumber(reply, "HINCRBY");
    },
    /** DEL — delete the whole hash. */
    async del(id: TId): Promise<number> {
      const reply = await client.send(["DEL", schema.key(id)]);
      return expectNumber(reply, "DEL");
    }
  };
}

/** The hash resource: the store plus the schema's own typed `key()`. */
export function createHashResource<
  TFields extends FieldCodecs,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(ctx: StoreContext, schema: HashSchema<TFields, TPrefix, TId, THashTag>) {
  return withKey(schema, createHashStore(ctx.client, schema));
}

const hashBinding: StoreBinding = { resource: createHashResource };

export function defineHash<
  TPrefix extends string,
  TFields extends FieldCodecs,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  const THashTag extends HashTagLayout | undefined = undefined
>(
  prefix: TPrefix,
  fields: TFields,
  options?: KeyOptions<TIds, THashTag>
): HashSchema<TFields, TPrefix, TIds[number], THashTag> {
  const hashTag = options?.hashTag as THashTag;
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    kind: "hash",
    prefix,
    // Spread so the property is absent, not `undefined`, on the default
    // layout: a schema still enumerates as the plain data it looks like.
    ...(hashTag === undefined ? {} : { hashTag }),
    fields,
    key: keyBuilder(prefix, hashTag)
  } as HashSchema<TFields, TPrefix, TIds[number], THashTag>;
  return withStore(schema, hashBinding);
}

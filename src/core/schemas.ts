import type {
  Codec,
  FieldCodecs,
  HashSchema,
  Keyspace,
  ListSchema,
  RedisKeyPart,
  SetSchema,
  SortedSetSchema
} from "./types.js";

export function defineKeyspace<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  _options?: { readonly ids?: TIds }
): Keyspace<TInput, TOutput, TPrefix, TIds[number]> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  return {
    kind: "kv",
    prefix,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    },
    encode(value) {
      return codec.encode(value);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as Keyspace<TInput, TOutput, TPrefix, TIds[number]>;
}

export function defineHash<
  TPrefix extends string,
  TFields extends FieldCodecs,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  fields: TFields,
  _options?: { readonly ids?: TIds }
): HashSchema<TFields, TPrefix, TIds[number]> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  return {
    kind: "hash",
    prefix,
    fields,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    }
  } as HashSchema<TFields, TPrefix, TIds[number]>;
}

export function defineSet<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  _options?: { readonly ids?: TIds }
): SetSchema<TInput, TOutput, TPrefix, TIds[number]> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  return {
    kind: "set",
    prefix,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    },
    encode(member) {
      return codec.encode(member);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as SetSchema<TInput, TOutput, TPrefix, TIds[number]>;
}

export function defineList<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  _options?: { readonly ids?: TIds }
): ListSchema<TInput, TOutput, TPrefix, TIds[number]> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  return {
    kind: "list",
    prefix,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    },
    encode(value) {
      return codec.encode(value);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as ListSchema<TInput, TOutput, TPrefix, TIds[number]>;
}

export function defineSortedSet<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  _options?: { readonly ids?: TIds }
): SortedSetSchema<TInput, TOutput, TPrefix, TIds[number]> {
  // The $infer* anchors are type-only phantoms — cast the literal.
  return {
    kind: "zset",
    prefix,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    },
    encode(member) {
      return codec.encode(member);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  } as SortedSetSchema<TInput, TOutput, TPrefix, TIds[number]>;
}

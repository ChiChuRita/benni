import { type HashTagLayout, type KeyOptions, keyBuilder } from "./keys.js";
import {
  type StoreBinding,
  type StoreContext,
  withKey,
  withStore
} from "./store.js";
import {
  createBlockingStreamOps,
  createStreamStore,
  type StreamSchema
} from "./stream.js";
import {
  createBlockingStreamGroupOps,
  createStreamGroupOps
} from "./stream-group.js";
import type { FieldCodecs, RedisKeyPart } from "./types.js";

/**
 * `defineStream` and the stream resources live here rather than in stream.ts
 * because a stream resource is the base store *plus* the consumer-group ops —
 * and stream-group.ts already imports stream.ts. Binding them from a third
 * module keeps the graph acyclic, which is what lets a bundler reason about
 * the pair cleanly.
 */
export function defineStream<
  TPrefix extends string,
  TFields extends FieldCodecs,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  const THashTag extends HashTagLayout | undefined = undefined
>(
  prefix: TPrefix,
  fields: TFields,
  options?: KeyOptions<TIds, THashTag>
): StreamSchema<TFields, TPrefix, TIds[number], THashTag> {
  const hashTag = options?.hashTag as THashTag;
  // The $infer* anchors are type-only phantoms — cast the literal.
  const schema = {
    kind: "stream",
    prefix,
    // Spread so the property is absent, not `undefined`, on the default
    // layout: a schema still enumerates as the plain data it looks like.
    ...(hashTag === undefined ? {} : { hashTag }),
    fields,
    key: keyBuilder(prefix, hashTag)
  } as StreamSchema<TFields, TPrefix, TIds[number], THashTag>;
  return withStore(schema, streamBinding);
}

/**
 * The shared stream resource: the base store plus the non-blocking consumer
 * group ops, which bind to whatever connection they are given.
 */
export function createStreamResource<
  TFields extends FieldCodecs,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(ctx: StoreContext, schema: StreamSchema<TFields, TPrefix, TId, THashTag>) {
  const store = createStreamStore(ctx.client, schema);
  return {
    ...withKey(schema, store),
    ...createStreamGroupOps(ctx.client, schema)
  };
}

/**
 * Session stream accessor: base store + blocking XREAD + the blocking-consumer
 * group superset. createBlockingStreamGroupOps is spread last so its group()
 * (returning the full BlockingStreamGroup) wins the `group` key.
 */
export function createStreamSessionAccessor<
  TFields extends FieldCodecs,
  TPrefix extends string,
  TId extends RedisKeyPart,
  THashTag extends HashTagLayout | undefined
>(ctx: StoreContext, schema: StreamSchema<TFields, TPrefix, TId, THashTag>) {
  const store = createStreamStore(ctx.client, schema);
  return {
    ...withKey(schema, store),
    ...createBlockingStreamOps(ctx.client, schema),
    ...createBlockingStreamGroupOps(ctx.client, schema)
  };
}

const streamBinding: StoreBinding = {
  resource: createStreamResource,
  session: createStreamSessionAccessor
};

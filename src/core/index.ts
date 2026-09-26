// The adapter-author and integration surface: the client contract adapters
// implement, the helpers they share, and the store builders `benni()` is made
// of. Apps import from `benni`, `benni/schema`, and an adapter instead.
//
// The schema builders are not repeated here under their `define*` names:
// `benni/schema` is the one place to declare a schema (`hash`, not
// `defineHash`).

export {
  type BitfieldBuilder,
  type BitfieldOffset,
  type BitfieldOverflow,
  type BitfieldType,
  type BitmapOperation,
  type BitmapPositionOptions,
  type BitmapRange,
  type BitmapRangeUnit,
  type BitmapSchema,
  createBitmapResource,
  createBitmapStore
} from "./bitmap.js";
export {
  type ClientProvider,
  type ClientSource,
  resolveClient
} from "./client-source.js";
export * from "./codecs.js";
export type { ConnectionEvents } from "./connection.js";
export * from "./counter.js";
export * from "./errors.js";
export {
  createGeoResource,
  createGeoStore,
  type GeoAddOptions,
  type GeoCoordinates,
  type GeoEntry,
  type GeoSearchBy,
  type GeoSearchCount,
  type GeoSearchFrom,
  type GeoSearchQuery,
  type GeoSearchResult,
  type GeoSearchStoreOptions,
  type GeoSearchStoreQuery,
  type GeoSetSchema,
  type GeoUnit
} from "./geo.js";
export {
  createHashResource,
  createHashStore,
  type HashFieldExpiry,
  type HashFieldTtlOptions,
  type HashSetExOptions,
  type PickedHashOutput
} from "./hash.js";
export {
  createHllResource,
  createHyperLogLogStore,
  type HyperLogLogSchema
} from "./hyperloglog.js";
export {
  createKeyValueStore,
  createKvResource,
  type KeyValueSetOptions
} from "./key-value.js";
export {
  createBlockingListOps,
  createListResource,
  createListSessionAccessor,
  createListStore,
  type ListBlockingMultiPopOptions,
  type ListEnd,
  type ListInsertOptions,
  type ListMultiPopOptions,
  type ListPopOptions,
  type ListPosOptions
} from "./list.js";
export {
  type ChannelName,
  createChannelResource,
  createPatternResource,
  createPubSubHub,
  createPubSubPublisher,
  hubFor,
  type InferPubSubId,
  type InferPubSubInput,
  type PubSubChannel,
  type PubSubChannelOptions,
  type PubSubChannelResource,
  type PubSubHandler,
  type PubSubHub,
  type PubSubPattern,
  type PubSubPatternHandler,
  type PubSubPatternMessage,
  type PubSubStreamOptions,
  type PubSubSubscription
} from "./pubsub.js";
export * from "./scan.js";
export * from "./script.js";
export * from "./session.js";
export {
  createSetResource,
  createSetStore
} from "./set.js";
export {
  createBlockingSortedSetOps,
  createSortedSetStore,
  createZsetResource,
  createZsetSessionAccessor,
  type SortedSetAddOptions,
  type SortedSetAggregate,
  type SortedSetCombineOptions,
  type SortedSetIntersectionSizeOptions,
  type SortedSetLexBound,
  type SortedSetLimit,
  type SortedSetMultiPopOptions,
  type SortedSetPopEnd,
  type SortedSetPopOptions,
  type SortedSetRandomMemberOptions,
  type SortedSetRangeByLexOptions,
  type SortedSetRangeByScoreOptions,
  type SortedSetRangeOptions,
  type SortedSetRangeStoreOptions,
  type SortedSetScoreBound
} from "./sorted-set.js";
export * from "./standard-schema.js";
export * from "./store.js";
export * from "./stream.js";
export * from "./stream-group.js";
export {
  createStreamResource,
  createStreamSessionAccessor
} from "./stream-resource.js";
export * from "./string.js";
export * from "./transaction.js";
export * from "./types.js";

// Redis Cluster slot checking, kept out of the root entry on purpose.
//
// `beni()` must reference the guard to install it, so anything the root entry
// names lands in every bundle — including the ones that never turn the check
// on. Shipping the CRC16 table and the error's fix-hint prose from a separate
// entry keeps the default baseline free of both: `beni/cluster` is only ever
// pulled in by an app that imports it.
//
// Beni checks slot CO-LOCATION, not topology. Routing (MOVED/ASK, per-node
// pools, failover) stays with the cluster-aware client underneath.

export type { HashTagLayout, KeyOptions } from "./core/keys.js";
export {
  assertSameSlot,
  CrossSlotError,
  hashTagOf,
  type SlotGuard,
  type SlotHint,
  slotOf
} from "./core/slot.js";

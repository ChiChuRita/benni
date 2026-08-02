import { ValidationError } from "./errors.js";
import type { HashTagLayout } from "./keys.js";

/**
 * CRC16/XMODEM lookup table (polynomial 0x1021, init 0), built on first use.
 *
 * Generated rather than written out as a literal: a 256-entry source table is
 * roughly 900 bytes gzipped in every bundle, including the ones that never
 * enable the cluster guard, while this loop is a few dozen bytes of source and
 * runs 2048 iterations exactly once.
 */
let table: Uint16Array | undefined;

function crcTable(): Uint16Array {
  if (table !== undefined) return table;
  const built = new Uint16Array(256);
  for (let index = 0; index < 256; index++) {
    let value = index << 8;
    for (let bit = 0; bit < 8; bit++) {
      value =
        (value & 0x8000) !== 0
          ? ((value << 1) ^ 0x1021) & 0xffff
          : (value << 1) & 0xffff;
    }
    built[index] = value;
  }
  table = built;
  return built;
}

/**
 * The substring Redis hashes for `key`, mirroring `keyHashSlot()` in cluster.c:
 *
 * 1. Find the first `{`. Without one, hash the whole key.
 * 2. From the next character, find the first `}`. Without one, hash the whole key.
 * 3. If that `}` sits immediately after the `{`, the tag is empty: hash the whole key.
 * 4. Otherwise hash exactly the characters between them.
 *
 * Redis does not rescan after an empty pair, so `"a{}{b}"` hashes whole rather
 * than on `b`. That is the case hand-rolled implementations usually get wrong.
 *
 * Scanning the JS string rather than its bytes is safe: `{` and `}` are ASCII,
 * and UTF-8 is self-synchronizing, so no brace byte can hide inside a
 * multi-byte sequence.
 */
export function hashTagOf(key: string): string {
  const open = key.indexOf("{");
  if (open === -1) return key;
  const close = key.indexOf("}", open + 1);
  if (close === -1 || close === open + 1) return key;
  return key.slice(open + 1, close);
}

const encoder = /* @__PURE__ */ new TextEncoder();

/** The Redis Cluster slot for `key`: CRC16/XMODEM of its hash tag, mod 16384. */
export function slotOf(key: string): number {
  const tag = hashTagOf(key);
  const lookup = crcTable();
  let crc = 0;
  for (let index = 0; index < tag.length; index++) {
    const code = tag.charCodeAt(index);
    // Redis hashes bytes. The ASCII fast path walks the string with zero
    // allocation; the moment a code unit needs more than one UTF-8 byte we
    // restart over the encoded form, which also gets surrogate pairs right.
    if (code > 0x7f) return crcBytes(encoder.encode(tag), lookup) & 0x3fff;
    crc = ((crc << 8) & 0xffff) ^ lookup[((crc >> 8) ^ code) & 0xff];
  }
  return crc & 0x3fff;
}

function crcBytes(bytes: Uint8Array, lookup: Uint16Array): number {
  let crc = 0;
  for (let index = 0; index < bytes.length; index++) {
    crc = ((crc << 8) & 0xffff) ^ lookup[((crc >> 8) ^ bytes[index]) & 0xff];
  }
  return crc;
}

/** What the failing command knew about its schema, for the fix hint. */
export type SlotHint = {
  readonly prefix?: string;
  readonly hashTag?: HashTagLayout;
};

/**
 * Throws {@link CrossSlotError} unless every key hashes to one Cluster slot.
 *
 * Installed only under `benni(client, { cluster: assertSameSlot })`. Call sites invoke it
 * as `assertSameSlot?.(…)` so that when it is absent the optional call
 * short-circuits argument evaluation too, and the key array is never built.
 */
export type SlotGuard = (
  command: string,
  keys: readonly string[],
  hint?: SlotHint
) => void;

export const assertSameSlot: SlotGuard = (command, keys, hint) => {
  if (keys.length < 2) return;
  const firstTag = hashTagOf(keys[0]);
  let firstSlot = -1;
  for (let index = 1; index < keys.length; index++) {
    // Identical tags are identical slots by construction, so a correctly
    // configured schema never reaches the CRC at all.
    if (hashTagOf(keys[index]) === firstTag) continue;
    if (firstSlot === -1) firstSlot = slotOf(keys[0]);
    const slot = slotOf(keys[index]);
    // Distinct tags can still collide onto one slot, and that is legal.
    if (slot === firstSlot) continue;
    throw new CrossSlotError(
      command,
      keys[0],
      firstSlot,
      keys[index],
      slot,
      hint
    );
  }
};

/**
 * A command whose keys span two Cluster slots, caught before it is sent.
 *
 * Extends ValidationError because this is a pre-send caller mistake, which is
 * the repo-wide contract for that class. `instanceof TypeError` still holds.
 */
export class CrossSlotError extends ValidationError {
  readonly command: string;
  readonly keys: readonly [string, string];
  readonly slots: readonly [number, number];

  constructor(
    command: string,
    firstKey: string,
    firstSlot: number,
    otherKey: string,
    otherSlot: number,
    hint?: SlotHint
  ) {
    super(
      crossSlotMessage(command, firstKey, firstSlot, otherKey, otherSlot, hint)
    );
    this.name = "CrossSlotError";
    this.command = command;
    this.keys = [firstKey, otherKey];
    this.slots = [firstSlot, otherSlot];
  }
}

function crossSlotMessage(
  command: string,
  firstKey: string,
  firstSlot: number,
  otherKey: string,
  otherSlot: number,
  hint?: SlotHint
): string {
  return [
    `${command} spans two Redis Cluster hash slots, which the server rejects with CROSSSLOT.`,
    `  ${JSON.stringify(firstKey)} hashes to slot ${firstSlot}`,
    `  ${JSON.stringify(otherKey)} hashes to slot ${otherSlot}`,
    `Every key in one command must hash to the same slot. ${fixFor(hint)}`,
    "This check is opt-in: drop `cluster` from benni(client, options) to allow cross-slot commands, which are legal on a single-node Redis."
  ].join("\n");
}

function fixFor(hint?: SlotHint): string {
  const prefix = hint?.prefix ?? "prefix";
  if (hint?.hashTag === undefined) {
    return (
      `Declare the schema with hashTag: "prefix" so its keys become ` +
      `"{${prefix}}:<id>" and the whole keyspace shares one slot, or with ` +
      `hashTag: "id" so its keys become "${prefix}:{<id>}" and stay spread ` +
      "while co-locating one id across schemas."
    );
  }
  if (hint.hashTag === "id") {
    return (
      'This schema uses hashTag: "id", which co-locates a single id across ' +
      "schemas but still spreads different ids across slots. Switch it to " +
      'hashTag: "prefix" to put the whole keyspace in one slot, or call this ' +
      "command with keys that share one id."
    );
  }
  return (
    'This schema already uses hashTag: "prefix", so these keys come from two ' +
    'different keyspaces, which "prefix" can never co-locate. Give both ' +
    'schemas hashTag: "id" and pass the same id to reach them in one command.'
  );
}

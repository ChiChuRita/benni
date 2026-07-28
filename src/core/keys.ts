import { ValidationError } from "./errors.js";
import type { RedisKey, RedisKeyPart } from "./types.js";

/**
 * True when wrapping `part` in braces would produce a tag Redis ignores.
 *
 * Redis reads the tag as the text between the first `{` and the next `}`, and
 * falls back to hashing the whole key when that text is empty. So `""` gives
 * `{}`, and anything starting with `}` closes the brace immediately: both
 * silently opt the key out of co-location rather than failing.
 */
function isVoidHashTag(part: string): boolean {
  return part === "" || part.startsWith("}");
}

function voidHashTagMessage(layout: "prefix" | "id", part: string): string {
  return (
    `hashTag: "${layout}" needs a non-empty Redis hash tag, but ${layout} ` +
    `${JSON.stringify(part)} yields an empty one. Redis ignores "{}" and ` +
    "hashes the whole key instead, so these keys would scatter across slots " +
    "rather than sharing one, and the multi-key commands the layout exists " +
    "for would fail with CROSSSLOT."
  );
}

/**
 * Where a schema puts its Redis Cluster hash tag, if anywhere.
 *
 * A cluster routes a key by CRC16 of the substring between the first `{` and
 * the first `}` after it, falling back to the whole key when there is no such
 * (non-empty) substring. Every key in one command must land on one slot, so
 * where the tag goes decides which multi-key commands are legal.
 *
 * - omitted: `profile:42`. Today's layout. Keys spread across all 16384 slots,
 *   so two ids from the same schema are almost never in one slot and every
 *   multi-key command over them is CROSSSLOT on a cluster.
 * - `"prefix"`: `{profile}:42`. The whole keyspace pins to one slot, so every
 *   multi-key command over this schema is legal. The cost is distribution:
 *   one node owns the entire keyspace.
 * - `"id"`: `cart:{u42}`. Keys stay spread, but every schema tagged this way
 *   co-locates the same id, so a command touching `cart:{u42}` and
 *   `orders:{u42}` is legal while one touching `cart:{u42}` and `cart:{u43}`
 *   is not.
 *
 * This is a co-location decision, not a compatibility flag. Reaching for
 * `"prefix"` to silence a CROSSSLOT error puts a whole keyspace on one node.
 */
export type HashTagLayout = "prefix" | "id";

/** The options bag every keyed schema factory accepts. */
export type KeyOptions<
  TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[],
  THashTag extends HashTagLayout | undefined = undefined
> = {
  /** Known ids, for autocomplete and a narrowed id parameter. */
  readonly ids?: TIds;
  /** Where to put the Cluster hash tag. See {@link HashTagLayout}. */
  readonly hashTag?: THashTag;
};

/**
 * The one `key()` implementation, shared by every schema factory.
 *
 * The layout is decided once, here, when the schema is defined, and baked into
 * the returned closure. `key()` itself stays a single template concatenation in
 * every layout, so the default path does exactly the work the hand-rolled
 * per-module copies used to do.
 */
export function keyBuilder<
  TPrefix extends string,
  THashTag extends HashTagLayout | undefined
>(
  prefix: TPrefix,
  hashTag: THashTag
): <TId extends RedisKeyPart>(id: TId) => RedisKey<TPrefix, TId, THashTag> {
  if (hashTag === "prefix" && isVoidHashTag(prefix)) {
    throw new ValidationError(voidHashTagMessage("prefix", prefix));
  }
  const build =
    hashTag === "prefix"
      ? (id: RedisKeyPart) => `{${prefix}}:${String(id)}`
      : hashTag === "id"
        ? (id: RedisKeyPart) => {
            const part = String(id);
            // Checked per id, because the id is runtime data. An empty tag is
            // not a smaller tag, it is *no* tag: the whole key gets hashed
            // instead, so this id's keys scatter across slots while every
            // other id's co-locate. The multi-key commands and Lua scripts
            // that "id" exists to enable then fail with CROSSSLOT for this
            // one id and no other, which is a miserable thing to debug.
            if (isVoidHashTag(part)) {
              throw new ValidationError(voidHashTagMessage("id", part));
            }
            return `${prefix}:{${part}}`;
          }
        : (id: RedisKeyPart) => `${prefix}:${String(id)}`;
  return build as <TId extends RedisKeyPart>(
    id: TId
  ) => RedisKey<TPrefix, TId, THashTag>;
}

// The keyspace prefix is data, but MATCH is a glob pattern — escape the
// metacharacters so a prefix like "user[1]" or "a\\" matches literally.
// `[` is escaped, so no character class can open, which keeps `^`/`-` inert.
// `{` and `}` are deliberately NOT escaped: Redis's stringmatchlen knows only
// `*`, `?`, `[...]` and `\`, so braces are already literal in a MATCH pattern.
export function escapeGlob(text: string): string {
  return text.replace(/[\\*?[\]]/g, "\\$&");
}

/** The SCAN MATCH pattern covering every key a schema's layout can build. */
export function keyspaceGlob(
  prefix: string,
  hashTag: HashTagLayout | undefined
): string {
  const escaped = escapeGlob(prefix);
  if (hashTag === "prefix") return `{${escaped}}:*`;
  if (hashTag === "id") return `${escaped}:{*}`;
  return `${escaped}:*`;
}

// ---------------------------------------------------------------------------
// Type-level slot reasoning. Everything below is erased at build time.
// ---------------------------------------------------------------------------

/**
 * The substring Redis actually hashes for `K`, mirroring `keyHashSlot()`: the
 * text between the first `{` and the first `}` after it, but only when that
 * text is non-empty. Otherwise the whole key hashes, so this yields `K`.
 *
 * Redis does not rescan, so `"a{}{b}"` hashes whole rather than on `b`.
 */
export type HashTag<K extends string> = string extends K
  ? string
  : K extends `${string}{${infer TRest}`
    ? TRest extends `${infer TTag}}${string}`
      ? TTag extends ""
        ? K
        : TTag
      : K
    : K;

/**
 * The hash tag when, and only when, it is provable from the type alone.
 * `never` means "no proof", which every same-slot check treats as inert.
 *
 * Three ways to get `never`: the key carries no tag (`"profile:42"`), the tag
 * is empty (`"a{}b"`), or the tag is a dynamic placeholder
 * (`` `cart:{${string}}` ``, which is what `cart.key(runtimeId)` produces).
 *
 * That last case is the load-bearing one. If it yielded `string`, then
 * `` `cart:{${string}}` `` and `` `orders:{${string}}` `` would compare equal
 * and the checker would affirm co-location it cannot possibly know. `never`
 * makes the key invisible to the checker instead.
 *
 * The governing invariant: **a passing check means "no provable conflict", not
 * "provably co-located."** Only pairs whose hash tags are distinct string
 * literals are rejected. Untagged keys, empty tags, and runtime ids all pass.
 */
export type ProvableTag<K extends string> = string extends K
  ? never
  : K extends `${string}{${infer TRest}`
    ? TRest extends `${infer TTag}}${string}`
      ? TTag extends ""
        ? never
        : string extends TTag
          ? never
          : TTag
      : never
    : never;

declare const CROSS_SLOT: unique symbol;

/**
 * The type Beni substitutes for a key that cannot share a slot with the others
 * in the same call. It is deliberately un-constructible: assigning a string to
 * it always fails, and the failure names the offending key and the tag it had
 * to match.
 *
 * TypeScript has no custom error messages, so this alias name IS the message.
 * See the cluster docs page for what to do about it.
 */
export type KeysMustShareOneHashSlot<
  TOffendingKey extends string,
  TExpectedHashTag extends string
> = {
  readonly [CROSS_SLOT]: unique symbol;
  readonly offendingKey: TOffendingKey;
  readonly expectedHashTag: TExpectedHashTag;
};

/**
 * The first provable tag in declaration order: the anchor every other key is
 * checked against. Anchoring (rather than comparing every pair) is what keeps
 * one mistake to one diagnostic, on the actual offender.
 */
export type FirstTag<TKeys extends readonly string[]> = TKeys extends readonly [
  infer THead extends string,
  ...infer TRest extends readonly string[]
]
  ? [ProvableTag<THead>] extends [never]
    ? FirstTag<TRest>
    : ProvableTag<THead>
  : never;

/**
 * Identity when every provable tag agrees (or none is provable); otherwise the
 * offending elements, and only those, become {@link KeysMustShareOneHashSlot}.
 *
 * Always intersect this with the naked type parameter — `TKeys &
 * SameSlotList<TKeys>`, never `SameSlotList<TKeys>` alone. TypeScript cannot
 * infer through a conditional type, so without the naked member `TKeys` falls
 * back to its constraint, every `ProvableTag` becomes `never`, and the check
 * silently vanishes with zero diagnostics.
 */
export type SameSlotList<
  TKeys extends readonly string[],
  TAnchor extends string = FirstTag<TKeys>
> = [TAnchor] extends [never]
  ? TKeys
  : {
      [I in keyof TKeys]: [ProvableTag<TKeys[I] & string>] extends [never]
        ? TKeys[I]
        : [ProvableTag<TKeys[I] & string>] extends [TAnchor]
          ? TKeys[I]
          : KeysMustShareOneHashSlot<TKeys[I] & string, TAnchor>;
    };

/** {@link SameSlotList} for a parameter that also accepts a single key. */
export type SameSlotArg<TKeys extends string | readonly string[]> =
  TKeys extends readonly string[] ? SameSlotList<TKeys> : TKeys;

/**
 * {@link FirstTag} over a record, walking an explicit name order. Records have
 * no reliable type-level order, so scripts anchor on their declared key tuple.
 */
export type FirstTagOfNames<
  TNames extends readonly string[],
  TValues
> = TNames extends readonly [
  infer THead,
  ...infer TRest extends readonly string[]
]
  ? THead extends keyof TValues
    ? [ProvableTag<TValues[THead] & string>] extends [never]
      ? FirstTagOfNames<TRest, TValues>
      : ProvableTag<TValues[THead] & string>
    : FirstTagOfNames<TRest, TValues>
  : never;

/** {@link SameSlotList} for a record of named keys, anchored on `TNames` order. */
export type SameSlotRecord<
  TNames extends readonly string[],
  TValues extends Record<string, string>,
  TAnchor extends string = FirstTagOfNames<TNames, TValues>
> = [TAnchor] extends [never]
  ? TValues
  : {
      [K in keyof TValues]: [ProvableTag<TValues[K] & string>] extends [never]
        ? TValues[K]
        : [ProvableTag<TValues[K] & string>] extends [TAnchor]
          ? TValues[K]
          : KeysMustShareOneHashSlot<TValues[K] & string, TAnchor>;
    };

/**
 * The `keys` parameter of `script().run()`: the caller's own record, checked
 * for a shared hash tag, with unknown key names rejected.
 *
 * Named as one alias rather than spelled inline so hovers and `Parameters<>`
 * print `SameSlotScriptKeys<…>` instead of a three-member intersection.
 *
 * The naked `TValues` member is mandatory. TypeScript cannot infer through a
 * conditional type, so without it `TValues` falls back to its constraint,
 * every {@link ProvableTag} becomes `never`, and the check silently never
 * fires. The `Record<Exclude<…>, never>` member restores the excess-property
 * checking that a `const` type parameter otherwise loses.
 */
export type SameSlotScriptKeys<
  TNames extends readonly string[],
  TValues extends Record<string, string>
> = TValues &
  SameSlotRecord<TNames, TValues> &
  Record<Exclude<keyof TValues, TNames[number]>, never>;

/**
 * The running anchor for a builder that accumulates keys across calls: keep the
 * one already established, or adopt the first provable tag in the new batch.
 */
export type SlotAnchor<
  TCurrent extends string,
  TNew extends readonly string[]
> = [TCurrent] extends [never] ? FirstTag<TNew> : TCurrent;

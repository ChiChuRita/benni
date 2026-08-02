import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import type { HashTag, ProvableTag } from "../src/core/keys.js";
import { numberReply } from "../src/core/transaction.js";
import { benni } from "../src/database.js";
import { kv, number as schemaNumber, script, zset } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;

// --- HashTag mirrors Redis's keyHashSlot() exactly ------------------------
type _NoBraces = Expect<Equal<HashTag<"profile:42">, "profile:42">>;
type _TaggedPrefix = Expect<Equal<HashTag<"{profile}:42">, "profile">>;
type _TaggedId = Expect<Equal<HashTag<"cart:{u42}">, "u42">>;
// An empty first pair wins; Redis does not rescan for a later one.
type _EmptyTag = Expect<Equal<HashTag<"a{}b">, "a{}b">>;
type _EmptyThenTagged = Expect<Equal<HashTag<"a{}{b}">, "a{}{b}">>;
type _FirstPairOnly = Expect<Equal<HashTag<"a{b}c{d}">, "b">>;
// A `}` before the first `{` is ignored.
type _CloseBeforeOpen = Expect<Equal<HashTag<"a}b{c}">, "c">>;
type _Unclosed = Expect<Equal<HashTag<"a{b">, "a{b">>;

// --- ProvableTag is deliberately weaker: `never` means "no proof" ---------
type _UntaggedUnprovable = Expect<Equal<ProvableTag<"profile:42">, never>>;
type _EmptyUnprovable = Expect<Equal<ProvableTag<"a{}b">, never>>;
type _StaticPrefixProvable = Expect<
  Equal<ProvableTag<`{profile}:${string}`>, "profile">
>;
// The load-bearing case. If this were `string`, two different runtime ids
// would compare equal and the checker would affirm co-location it cannot
// know. `never` makes the key invisible instead: a false negative, never a
// false positive.
type _DynamicIdUnprovable = Expect<
  Equal<ProvableTag<`cart:{${string}}`>, never>
>;

const carts = kv("cart", codecs.string(), { hashTag: "id" });
const orders = kv("order", codecs.string(), { hashTag: "id" });
const pinned = kv("pinned", codecs.string(), { hashTag: "prefix" });
const plain = kv("plain", codecs.string());

const move = script("move", {
  keys: ["from", "to"],
  args: { amount: schemaNumber() },
  returns: schemaNumber(),
  lua: "return 1"
});

const redis = benni(fakeClient([], []));
declare const body: (session: never) => Promise<null>;

/**
 * Every constraint here depends on inference reaching a naked type parameter.
 * If that breaks, the check does not error loudly, it silently stops firing
 * and everything compiles. These `@ts-expect-error`s are the only thing that
 * would notice, so they are load-bearing rather than decorative.
 */
function typeLevelChecks() {
  // --- script().run({ keys }) --------------------------------------------
  void redis.script(move).run({
    keys: { from: carts.key("u1"), to: orders.key("u1") },
    args: { amount: 1 }
  });
  void redis.script(move).run({
    keys: { from: plain.key("a"), to: plain.key("b") }, // untagged: inert
    args: { amount: 1 }
  });
  void redis.script(move).run({
    keys: {
      from: carts.key("u1"),
      // @ts-expect-error keys from two different hash tags cannot share a slot
      to: orders.key("u2")
    },
    args: { amount: 1 }
  });

  // --- redis.watch() ------------------------------------------------------
  void redis.watch([carts.key("u1"), orders.key("u1")], body as never);
  void redis.watch([plain.key("a"), plain.key("b")], body as never);
  void redis.watch("a-single-key", body as never);
  void redis.watch(
    [
      carts.key("u1"),
      // @ts-expect-error keys from two different hash tags cannot share a slot
      orders.key("u2")
    ],
    body as never
  );

  // --- multi().keys() -----------------------------------------------------
  void redis.multi().keys([carts.key("u1"), orders.key("u1")]);
  void redis.multi().keys([
    carts.key("u1"),
    // @ts-expect-error keys from two different hash tags cannot share a slot
    orders.key("u2")
  ]);
  // The anchor accumulates across calls, and across an intervening add().
  void redis
    .multi()
    .keys([carts.key("u1")])
    .add(["INCR", carts.key("u1")], () => 1)
    // @ts-expect-error the anchor "u1" carries past add(); "u2" conflicts
    .keys([orders.key("u2")]);
}

void typeLevelChecks;

describe("cluster-safe key types", () => {
  it("builds each layout's key at runtime as the type says", () => {
    expect(plain.key("a")).toBe("plain:a");
    expect(pinned.key("a")).toBe("{pinned}:a");
    expect(carts.key("u1")).toBe("cart:{u1}");
  });
});

/**
 * The snippets from `docs/advanced/cluster.md`, typechecked against src so the
 * page cannot drift from the API (and so its deliberate compile error stays
 * one).
 */
function docsSnippets() {
  const featureFlags = zset("flags", codecs.string(), { hashTag: "prefix" });
  const moveItem = script("moveItem", {
    keys: ["from", "to"],
    args: { amount: schemaNumber() },
    returns: schemaNumber(),
    lua: "return 1"
  });

  void redis.zset(featureFlags).zunionstore("all", "beta", ["internal"]);
  void redis.script(moveItem).run({
    // @ts-expect-error keys from two different hash tags cannot share a slot
    keys: { from: carts.key("u1"), to: orders.key("u2") },
    args: { amount: 1 }
  });
  void redis
    .multi()
    .keys([carts.key("u1"), orders.key("u1")])
    .add(["INCR", carts.key("u1")], numberReply)
    .add(["SADD", orders.key("u1"), "x"], numberReply)
    .exec();
}

void docsSnippets;

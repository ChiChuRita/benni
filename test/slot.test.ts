import { describe, expect, it } from "vitest";
import { hashTagOf, slotOf } from "../src/core/slot.js";

/**
 * Vectors computed from the CRC16/XMODEM definition Redis uses (polynomial
 * 0x1021, init 0, mod 16384). `{user1000}.following` / `.followers` both
 * landing on 3443 is the co-location example from the Redis Cluster spec.
 */
describe("slotOf", () => {
  it.each([
    ["foo", 12182],
    ["bar", 5061],
    ["hello", 866],
    ["user:1", 10778],
    ["{user}:1", 5474],
    ["user:{1}", 9842]
  ])("hashes %j to slot %i", (key, slot) => {
    expect(slotOf(key)).toBe(slot);
  });

  it("co-locates keys that share a hash tag", () => {
    expect(slotOf("{user1000}.following")).toBe(3443);
    expect(slotOf("{user1000}.followers")).toBe(3443);
  });

  it("hashes bytes, not code units, for non-ASCII keys", () => {
    expect(slotOf("café")).toBe(5735);
    expect(slotOf("キー")).toBe(8582);
  });

  it("stays in range for every key", () => {
    for (const key of ["", "{}", "a".repeat(500), "\u{1F600}", "\ud800"]) {
      const slot = slotOf(key);
      expect(slot).toBeGreaterThanOrEqual(0);
      expect(slot).toBeLessThan(16384);
    }
  });
});

describe("hashTagOf", () => {
  it.each([
    // No brace pair: the whole key hashes.
    ["profile:42", "profile:42"],
    ["a{b", "a{b"],
    ["", ""],
    ["{", "{"],
    // A non-empty first pair wins.
    ["{profile}:42", "profile"],
    ["cart:{u42}", "u42"],
    ["a{b}c{d}", "b"],
    // The inner `{` is data: Redis takes the first `}` after the first `{`.
    ["a{b{c}d}", "b{c"],
    // A `}` before the first `{` is ignored.
    ["a}b{c}", "c"],
    // An EMPTY first pair means the whole key hashes. Redis does not rescan,
    // so "a{}{b}" does NOT hash on "b". This is the case hand-rolled
    // implementations usually get wrong.
    ["a{}b", "a{}b"],
    ["a{}{b}", "a{}{b}"],
    ["foo{}", "foo{}"],
    ["{}foo", "{}foo"]
  ])("extracts %j -> %j", (key, tag) => {
    expect(hashTagOf(key)).toBe(tag);
  });

  it("agrees with the empty-pair fallback at the slot level", () => {
    // If the empty pair were skipped, this would equal slotOf("bar").
    expect(slotOf("foo{}{bar}")).toBe(8363);
    expect(slotOf("foo{}{bar}")).not.toBe(slotOf("bar"));
  });
});

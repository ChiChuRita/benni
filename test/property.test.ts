import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { RedisCommand } from "../src/core/index.js";
import {
  codecs,
  createHashStore,
  createSortedSetStore,
  defineHash,
  defineKeyspace,
  defineList,
  defineSet,
  defineSortedSet,
  ReplyShapeError,
  ValidationError
} from "../src/core/index.js";
import { bytes } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// Property-based tests: fast-check generates the inputs, so these pin
// *laws* (round-trips, differential equality against the platform) instead
// of hand-picked examples.

const finiteDouble = fc.double({ noNaN: true, noDefaultInfinity: true });
const anyText = fc.string({ unit: "grapheme" });

describe("string codec properties", () => {
  const strings = codecs.string();

  it("round-trips every string", () => {
    fc.assert(
      fc.property(anyText, (value) => {
        expect(strings.decode(strings.encode(value))).toBe(value);
      })
    );
  });
});

describe("number codec properties", () => {
  const numbers = codecs.number();

  it("round-trips every finite double exactly", () => {
    fc.assert(
      fc.property(finiteDouble, (value) => {
        // === (not Object.is) on purpose: String(-0) is "0", so -0 reads
        // back as +0 — the one lossy case, and === treats them as equal.
        expect(numbers.decode(numbers.encode(value)) === value).toBe(true);
      })
    );
  });

  it("rejects NaN and infinities on encode", () => {
    for (const bad of [
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY
    ]) {
      expect(() => numbers.encode(bad)).toThrow(ValidationError);
    }
  });

  it("rejects every string outside the decimal format", () => {
    const decimal = /^-?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?$/i;
    fc.assert(
      fc.property(
        anyText.filter((text) => !decimal.test(text.trim())),
        (garbage) => {
          expect(() => numbers.decode(garbage)).toThrow(ReplyShapeError);
        }
      )
    );
  });

  it("never fabricates a value: accepted strings decode to Number(input)", () => {
    fc.assert(
      fc.property(anyText, (text) => {
        let decoded: number;
        try {
          decoded = numbers.decode(text);
        } catch {
          return; // rejection is always allowed
        }
        expect(Number.isFinite(decoded)).toBe(true);
        expect(decoded).toBe(Number(text));
      })
    );
  });
});

describe("boolean codec properties", () => {
  const booleans = codecs.boolean();

  it("round-trips both values and rejects everything else", () => {
    expect(booleans.decode(booleans.encode(true))).toBe(true);
    expect(booleans.decode(booleans.encode(false))).toBe(false);
    fc.assert(
      fc.property(
        anyText.filter((s) => !["1", "0", "true", "false"].includes(s)),
        (garbage) => {
          expect(() => booleans.decode(garbage)).toThrow(ReplyShapeError);
        }
      )
    );
  });
});

describe("json codec properties", () => {
  const values = codecs.json<unknown>();
  const jsonArb = fc.jsonValue({ maxDepth: 3, stringUnit: "grapheme" });

  it("matches the platform JSON round-trip (differential)", () => {
    fc.assert(
      fc.property(jsonArb, (value) => {
        expect(values.decode(values.encode(value))).toEqual(
          JSON.parse(JSON.stringify(value))
        );
      })
    );
  });

  it("is stable: encode∘decode∘encode is encode", () => {
    fc.assert(
      fc.property(jsonArb, (value) => {
        const encoded = values.encode(value);
        expect(values.encode(values.decode(encoded))).toBe(encoded);
      })
    );
  });

  it("rejects undefined on encode", () => {
    expect(() => values.encode(undefined)).toThrow(ValidationError);
  });
});

describe("enumOf codec properties", () => {
  it("round-trips members and rejects non-members", () => {
    fc.assert(
      fc.property(
        fc.uniqueArray(fc.string(), { minLength: 1, maxLength: 8 }),
        fc.nat(),
        anyText,
        (rawValues, pick, outsider) => {
          const values = rawValues as [string, ...string[]];
          const codec = codecs.enumOf(values);
          const member = values[pick % values.length];
          expect(codec.decode(codec.encode(member))).toBe(member);
          if (!values.includes(outsider)) {
            expect(() => codec.decode(outsider)).toThrow(ReplyShapeError);
          }
        }
      )
    );
  });
});

describe("bytes codec properties", () => {
  const binary = bytes();
  const byteArrays = fc.uint8Array({ maxLength: 256 });

  it("encodes exactly like Node's Buffer base64 (differential)", () => {
    fc.assert(
      fc.property(byteArrays, (input) => {
        expect(binary.encode(input)).toBe(
          Buffer.from(input).toString("base64")
        );
      })
    );
  });

  it("decodes Buffer-produced base64 back to the original bytes", () => {
    fc.assert(
      fc.property(byteArrays, (input) => {
        const viaBuffer = Buffer.from(input).toString("base64");
        expect(binary.decode(viaBuffer)).toEqual(input);
        expect(binary.decode(binary.encode(input))).toEqual(input);
      })
    );
  });

  it("rejects truncated and alphabet-violating strings", () => {
    fc.assert(
      fc.property(fc.uint8Array({ minLength: 1, maxLength: 64 }), (input) => {
        const valid = binary.encode(input);
        expect(() => binary.decode(valid.slice(0, -1))).toThrow(TypeError);
        expect(() => binary.decode(`!${valid.slice(1)}`)).toThrow(TypeError);
      })
    );
  });
});

describe("key prefixing properties", () => {
  const idArb = fc.oneof(
    anyText,
    fc.integer(),
    finiteDouble,
    fc.bigInt({ min: -(2n ** 72n), max: 2n ** 72n })
  );

  it("every schema kind builds keys as `prefix:id`", () => {
    fc.assert(
      fc.property(anyText, idArb, (prefix, id) => {
        const expected = `${prefix}:${String(id)}`;
        expect(defineKeyspace(prefix, codecs.string()).key(id)).toBe(expected);
        expect(defineHash(prefix, { f: codecs.string() }).key(id)).toBe(
          expected
        );
        expect(defineSet(prefix, codecs.string()).key(id)).toBe(expected);
        expect(defineList(prefix, codecs.string()).key(id)).toBe(expected);
        expect(defineSortedSet(prefix, codecs.string()).key(id)).toBe(expected);
      })
    );
  });
});

describe("zadd wire encoding properties", () => {
  const board = defineSortedSet("board", codecs.string());
  const scoreArb = fc.oneof(
    { weight: 5, arbitrary: finiteDouble },
    {
      weight: 1,
      arbitrary: fc.constantFrom(
        Number.POSITIVE_INFINITY,
        Number.NEGATIVE_INFINITY
      )
    }
  );

  it("every score reaches the wire losslessly", () => {
    fc.assert(
      fc.asyncProperty(
        fc.array(fc.record({ score: scoreArb, member: anyText }), {
          minLength: 1,
          maxLength: 6
        }),
        async (entries) => {
          const commands: RedisCommand[] = [];
          const store = createSortedSetStore(
            fakeClient(commands, [entries.length]),
            board
          );
          await store.zadd("d", entries);
          const [name, key, ...args] = commands[0];
          expect(name).toBe("ZADD");
          expect(key).toBe("board:d");
          expect(args).toHaveLength(entries.length * 2);
          for (const [index, entry] of entries.entries()) {
            const scoreArg = args[index * 2];
            if (entry.score === Number.POSITIVE_INFINITY) {
              expect(scoreArg).toBe("+inf");
            } else if (entry.score === Number.NEGATIVE_INFINITY) {
              expect(scoreArg).toBe("-inf");
            } else {
              // Whatever spelling goes on the wire must parse back to the
              // exact same double — that is what Redis will store.
              expect(Number(String(scoreArg)) === entry.score).toBe(true);
            }
            expect(args[index * 2 + 1]).toBe(entry.member);
          }
        }
      )
    );
  });
});

describe("hash record wire symmetry", () => {
  const users = defineHash("user", {
    name: codecs.string(),
    score: codecs.number(),
    active: codecs.boolean()
  });
  const recordArb = fc.record({
    name: anyText,
    score: finiteDouble,
    active: fc.boolean()
  });

  it("a written record read back through the wire shape is the same record", async () => {
    await fc.assert(
      fc.asyncProperty(recordArb, async (record) => {
        // Write: capture the variadic HSET this record produces.
        const commands: RedisCommand[] = [];
        const writer = createHashStore(fakeClient(commands, [3]), users);
        await writer.hset("42", record);
        const [, , ...pairs] = commands[0];

        // Read: answer the whole-record HMGET with exactly what was written.
        const written = new Map<string, string>();
        for (let index = 0; index < pairs.length; index += 2) {
          written.set(String(pairs[index]), String(pairs[index + 1]));
        }
        const reply = ["name", "score", "active"].map(
          (field) => written.get(field) ?? null
        );
        const reader = createHashStore(fakeClient([], [reply]), users);
        const decoded = await reader.hget("42");

        expect(decoded).not.toBeNull();
        expect(decoded?.name).toBe(record.name);
        expect(decoded?.score === record.score).toBe(true);
        expect(decoded?.active).toBe(record.active);
      })
    );
  });
});

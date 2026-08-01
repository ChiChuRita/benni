import { describeReply, ReplyShapeError, ValidationError } from "./errors.js";
import {
  formatStandardIssues,
  type InferStandardInput,
  type InferStandardOutput,
  type StandardSchemaV1
} from "./standard-schema.js";
import type { Codec } from "./types.js";

// Shared with the zod bridge's zodJson() so both JSON codecs refuse the same
// unrepresentable values; `label` names the offending codec in the message.
export function encodeJson(input: unknown, label = "JSON codec"): string {
  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(input, (_key, value) => {
      // JSON.stringify writes NaN/Infinity as the literal `null`, which reads
      // back indistinguishable from "the key does not exist" — the sentinel
      // kv.get() and friends return for a missing key. Fail at the write, the
      // way the number() codec already does, rather than poisoning a read.
      if (typeof value === "number" && !Number.isFinite(value)) {
        throw new ValidationError(
          `${label} cannot encode the non-finite number ${value}; it would be stored as null and read back as a missing value`
        );
      }
      return value;
    });
  } catch (error) {
    if (error instanceof ValidationError) throw error;
    // A BigInt or a circular structure otherwise escapes as a raw TypeError,
    // where every other pre-send failure in the library is a ValidationError.
    throw new ValidationError(
      `${label} could not encode value: ${(error as Error).message}`
    );
  }
  if (encoded === undefined) {
    throw new ValidationError(`${label} could not encode value`);
  }
  return encoded;
}

function decodeJson(stored: string): unknown {
  try {
    return JSON.parse(stored);
  } catch (error) {
    throw new ReplyShapeError(
      `JSON codec failed to decode stored value: ${(error as Error).message}`,
      stored
    );
  }
}

/**
 * `json()` — store a value as JSON, trusting the type parameter.
 * `json(schema)` — same storage, but every read is validated by the given
 * [Standard Schema](https://standardschema.dev) validator (Zod, Valibot,
 * ArkType, …) and the value type is inferred from it.
 */
function json<T>(): Codec<T>;
function json<S extends StandardSchemaV1>(
  schema: S
): Codec<InferStandardInput<S>, InferStandardOutput<S>>;
function json(schema?: StandardSchemaV1): Codec<unknown, unknown> {
  // Wrapped rather than handed over by reference: a caller doing
  // `values.map(codec.encode)` would otherwise pass the array index as `label`.
  const encode = (input: unknown) => encodeJson(input);
  if (schema === undefined) {
    return { encode, decode: decodeJson };
  }
  const standard = schema["~standard"];
  return {
    encode,
    decode(stored) {
      const result = standard.validate(decodeJson(stored));
      if (result instanceof Promise) {
        // Codec.decode is synchronous by contract; an async validator can
        // never produce a value here, so fail with the reason. Claim the
        // promise first: nothing else will await it, and a validator that
        // rejects would otherwise surface as an unhandled rejection, which
        // is fatal under --unhandled-rejections=strict. That only covers
        // vendors whose validate() hands back the promise it made; zod runs a
        // synchronous pass first and drops a rejecting promise inside it,
        // where no caller can reach it.
        result.catch(() => undefined);
        throw new ValidationError(
          `json(schema) requires a synchronous validator, but the ${standard.vendor} schema returned a Promise. Use a schema without async refinements.`
        );
      }
      if (result.issues) {
        throw new ReplyShapeError(
          `json(schema) validation failed (${standard.vendor}): ${formatStandardIssues(result.issues)}`,
          stored
        );
      }
      return result.value;
    }
  };
}

export const codecs = {
  /** Store and read a value as-is. */
  string(): Codec<string> {
    return {
      // Not `String`: that stringifies anything, so a value the types said was
      // a string but was not (an undefined field, a forwarded optional) landed
      // in Redis as "undefined" or "[object Object]" instead of failing. The
      // number codec already refuses input it cannot represent.
      encode(input) {
        if (typeof input !== "string") {
          throw new ValidationError(
            `string codec cannot encode ${describeReply(input)}`
          );
        }
        return input;
      },
      decode: String
    };
  },
  /**
   * Store a JS number as its decimal string. Rejects non-finite input on
   * encode (a symmetric guard to decode's finite-number check) so `NaN` /
   * `Infinity` fail at the write rather than poisoning a later read.
   */
  number(): Codec<number> {
    return {
      encode(input) {
        if (!Number.isFinite(input)) {
          throw new ValidationError(
            "number codec cannot encode a non-finite value"
          );
        }
        return String(input);
      },
      decode(stored) {
        // Require the decimal format encode produces. Bare Number() coercion
        // silently fabricates values encode never wrote: Number("") and
        // Number("  ") are 0, Number("0x1A") is 26.
        if (!/^-?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?$/i.test(stored.trim())) {
          throw new ReplyShapeError(
            `number codec expected a decimal number, got ${describeReply(stored)}`,
            stored
          );
        }
        const decoded = Number(stored);
        if (!Number.isFinite(decoded)) {
          throw new ReplyShapeError(
            `number codec expected a finite number, got ${describeReply(stored)}`,
            stored
          );
        }
        return decoded;
      }
    };
  },
  /** Store a boolean as `"1"`/`"0"`; decodes `"1"`/`"true"` and `"0"`/`"false"`. */
  boolean(): Codec<boolean> {
    return {
      encode(input) {
        // A bare truthiness test silently accepted undefined as false, which
        // wrote a real "0" for a field that was never set.
        if (typeof input !== "boolean") {
          throw new ValidationError(
            `boolean codec cannot encode ${describeReply(input)}`
          );
        }
        return input ? "1" : "0";
      },
      decode(stored) {
        if (stored === "1" || stored === "true") return true;
        if (stored === "0" || stored === "false") return false;
        throw new ReplyShapeError(
          `boolean codec expected "1"/"0"/"true"/"false", got ${describeReply(stored)}`,
          stored
        );
      }
    };
  },
  /**
   * Store a value as JSON. Two forms:
   * - `json<T>()` — `T` is trusted, not validated at runtime.
   * - `json(schema)` — pass any Standard Schema validator (Zod, Valibot,
   *   ArkType, …); reads are validated and the type is inferred from it.
   *   Validation failures surface as `ReplyShapeError` naming the issues.
   *
   * @example
   * ```ts
   * const profiles = kv("profile", json<Profile>());        // trusted
   * const users = kv("user", json(z.object({ name: z.string() }))); // validated
   * ```
   */
  json,
  /**
   * A string field constrained to a fixed set of literals, stored as the plain
   * string (no JSON overhead) and validated on decode. The inferred type is the
   * union of `values`.
   *
   * @example
   * ```ts
   * const status = enumOf(["pending", "active", "done"]);
   * //    ^? Codec<"pending" | "active" | "done">
   * ```
   */
  enumOf<const T extends readonly [string, ...string[]]>(
    values: T
  ): Codec<T[number]> {
    const allowed = new Set<string>(values);
    return {
      // Refuse on the way in as well as out: an out-of-set value that is
      // written unchecked only fails later, on read, at a point that gives no
      // clue which write produced it.
      encode(input) {
        if (typeof input !== "string" || !allowed.has(input)) {
          throw new ValidationError(
            `enum codec expected one of ${values.join(", ")}, got ${describeReply(input)}`
          );
        }
        return input;
      },
      decode(stored) {
        if (!allowed.has(stored)) {
          throw new ReplyShapeError(
            `enum codec expected one of ${values.join(", ")}, got ${describeReply(stored)}`,
            stored
          );
        }
        return stored as T[number];
      }
    };
  }
};

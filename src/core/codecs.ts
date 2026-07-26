import { describeReply, ReplyShapeError, ValidationError } from "./errors.js";
import {
  formatStandardIssues,
  type InferStandardInput,
  type InferStandardOutput,
  type StandardSchemaV1
} from "./standard-schema.js";
import type { Codec } from "./types.js";

function encodeJson(input: unknown): string {
  const encoded = JSON.stringify(input);
  if (encoded === undefined) {
    throw new ValidationError("JSON codec could not encode value");
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
  if (schema === undefined) {
    return { encode: encodeJson, decode: decodeJson };
  }
  const standard = schema["~standard"];
  return {
    encode: encodeJson,
    decode(stored) {
      const result = standard.validate(decodeJson(stored));
      if (result instanceof Promise) {
        // Codec.decode is synchronous by contract; an async validator can
        // never produce a value here, so fail with the reason.
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
      encode: String,
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
      encode: String,
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

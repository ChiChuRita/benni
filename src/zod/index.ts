// beni/zod — Zod codecs (https://zod.dev/codecs) as beni field codecs.
// Zod is an optional peer dependency; only this subpath imports it. Built
// against `zod/v4/core` (Zod's stated interface for libraries), so schemas
// from both `zod` and `zod/mini` work.
import type { $ZodType, output as InferZodOutput } from "zod/v4/core";
import { $ZodAsyncError, safeDecode, safeEncode } from "zod/v4/core";
import { ReplyShapeError, ValidationError } from "../core/errors.js";
import { formatStandardIssues } from "../core/standard-schema.js";
import type { Codec } from "../core/types.js";

function syncOnly(label: string): ValidationError {
  return new ValidationError(
    `${label} requires a synchronous zod schema. Use a schema without async refinements or transforms.`
  );
}

function runEncode<S extends $ZodType>(
  label: string,
  schema: S,
  value: InferZodOutput<S>
): unknown {
  let result: ReturnType<typeof safeEncode<S>>;
  try {
    result = safeEncode(schema, value);
  } catch (error) {
    if (error instanceof $ZodAsyncError) throw syncOnly(label);
    throw error;
  }
  if (!result.success) {
    throw new ValidationError(
      `${label} could not encode value: ${formatStandardIssues(result.error.issues)}`
    );
  }
  return result.data;
}

function runDecode<S extends $ZodType>(
  label: string,
  schema: S,
  value: unknown,
  stored: string
): InferZodOutput<S> {
  let result: ReturnType<typeof safeDecode<S>>;
  try {
    result = safeDecode(schema, value as never);
  } catch (error) {
    if (error instanceof $ZodAsyncError) throw syncOnly(label);
    throw error;
  }
  if (!result.success) {
    throw new ReplyShapeError(
      `${label} validation failed: ${formatStandardIssues(result.error.issues)}`,
      stored
    );
  }
  return result.data;
}

/**
 * Codec: a zod schema or [zod codec](https://zod.dev/codecs) whose encoded
 * (input) side is a string, run in **both directions** — writes are encoded
 * and validated with `z.encode`, reads decoded and validated with `z.decode`.
 * This is what plain Standard Schema interop (`json(schema)`) cannot do:
 * Standard Schema v1 has no encode direction, so it validates reads only.
 *
 * Use it anywhere a codec is accepted — kv values, hash fields, list items,
 * set / sorted-set members, stream fields, pub/sub messages — to store rich
 * types (`Date`, `bigint`, `URL`, …) that genuinely round-trip.
 *
 * Encode failures throw `ValidationError` (caller mistake, nothing is sent);
 * decode failures throw `ReplyShapeError` with the stored string attached.
 * The schema must be synchronous — async refinements throw `ValidationError`.
 *
 * @example
 * ```ts
 * const isoDate = z.codec(z.iso.datetime(), z.date(), {
 *   decode: (iso) => new Date(iso),
 *   encode: (date) => date.toISOString()
 * });
 * const lastSeen = kv("last-seen", zodCodec(isoDate));
 * //    ^ writes take a Date, Redis stores an ISO string, reads return a Date
 * ```
 */
export function zodCodec<S extends $ZodType<unknown, string>>(
  schema: S
): Codec<InferZodOutput<S>> {
  return {
    encode(input) {
      // The schema's input side is a string schema, so zod has already
      // validated that the encoded value is a string.
      return runEncode("zodCodec(schema)", schema, input) as string;
    },
    decode(stored) {
      return runDecode("zodCodec(schema)", schema, stored, stored);
    }
  };
}

/**
 * Codec: store a value as JSON with a zod schema validating **both
 * directions** — a stronger `json(schema)`. Writes run `z.encode` (validated,
 * with codec fields converted to their JSON-safe form) then `JSON.stringify`;
 * reads run `JSON.parse` then `z.decode` (validated, codec fields revived).
 *
 * Fields that aren't JSON-safe (`Date`, `bigint`, `Map`, …) need a zod codec
 * to a JSON-safe form — see {@link zodCodec} for the `Date` example. A plain
 * `z.date()` field would stringify on write but fail loudly on read.
 *
 * Encode failures throw `ValidationError` (caller mistake, nothing is sent);
 * decode failures throw `ReplyShapeError` with the stored string attached.
 * The schema must be synchronous — async refinements throw `ValidationError`.
 *
 * @example
 * ```ts
 * const user = z.object({ name: z.string(), created: isoDate });
 * const users = kv("user", zodJson(user));
 * await redis.kv(users).set("u1", { name: "ada", created: new Date() });
 * const found = await redis.kv(users).get("u1");
 * //    ^? { name: string; created: Date } | null
 * ```
 */
export function zodJson<S extends $ZodType>(
  schema: S
): Codec<InferZodOutput<S>> {
  return {
    encode(input) {
      const encoded = JSON.stringify(
        runEncode("zodJson(schema)", schema, input)
      );
      if (encoded === undefined) {
        throw new ValidationError("zodJson(schema) could not encode value");
      }
      return encoded;
    },
    decode(stored) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(stored);
      } catch (error) {
        throw new ReplyShapeError(
          `zodJson(schema) failed to decode stored value: ${(error as Error).message}`,
          stored
        );
      }
      return runDecode("zodJson(schema)", schema, parsed, stored);
    }
  };
}

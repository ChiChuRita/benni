/**
 * Thrown when caller-supplied input fails validation before any command is
 * sent to Redis — an out-of-range count, a non-finite number, a bad option
 * combination. Extends `TypeError`, so existing `catch` / `instanceof
 * TypeError` handling keeps working; catch `ValidationError` specifically to
 * tell "I passed bad input" apart from a protocol-level failure.
 */
export class ValidationError extends TypeError {
  constructor(message: string) {
    super(message);
    this.name = "ValidationError";
  }
}

/**
 * Thrown when a Redis reply — or a stored value handed to a codec — does not
 * match the shape a decoder expected. `reply` is the raw value received, so a
 * caller can inspect it programmatically. Extends `TypeError` for
 * backward-compatible catching.
 */
export class ReplyShapeError extends TypeError {
  readonly reply: unknown;

  constructor(message: string, reply: unknown) {
    super(message);
    this.name = "ReplyShapeError";
    this.reply = reply;
  }
}

/**
 * Thrown when a whole-record hash read finds some, but not all, of the fields
 * the schema declares. The reply is well formed, so this is not a protocol or
 * adapter fault: it means the stored record is incomplete, most often because
 * individual fields were given their own TTLs with `hexpire` and some have
 * since lapsed. `missing` names the absent fields.
 *
 * Extends {@link ReplyShapeError} so code that already catches that keeps
 * working; catch `PartialRecordError` specifically to tell an ordinary
 * incomplete record apart from a genuine shape violation.
 */
export class PartialRecordError extends ReplyShapeError {
  readonly missing: readonly string[];

  constructor(message: string, reply: unknown, missing: readonly string[]) {
    super(message, reply);
    this.name = "PartialRecordError";
    this.missing = missing;
  }
}

/** A compact, safe one-line rendering of a reply value for error messages. */
export function describeReply(reply: unknown): string {
  if (reply === null) return "null";
  if (reply === undefined) return "undefined";
  if (typeof reply === "string") {
    const shown = reply.length > 60 ? `${reply.slice(0, 60)}…` : reply;
    return `string ${JSON.stringify(shown)}`;
  }
  if (
    typeof reply === "number" ||
    typeof reply === "bigint" ||
    typeof reply === "boolean"
  ) {
    return `${typeof reply} ${String(reply)}`;
  }
  if (Array.isArray(reply)) return `array(length ${reply.length})`;
  if (reply instanceof Map) return `map(size ${reply.size})`;
  if (reply instanceof Set) return `set(size ${reply.size})`;
  if (reply instanceof Uint8Array) return `bytes(length ${reply.length})`;
  return typeof reply;
}

/**
 * Builds the standard `Expected Redis <command> to return <expected>, got
 * <actual>` error. `expected` keeps the historical wording (e.g. "number",
 * "array", "string or null") so the message prefix is stable.
 */
export function replyShapeError(
  command: string,
  expected: string,
  reply: unknown
): ReplyShapeError {
  return new ReplyShapeError(
    `Expected Redis ${command} to return ${expected}, got ${describeReply(reply)}`,
    reply
  );
}

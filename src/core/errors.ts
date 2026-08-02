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

/**
 * A Redis error reply opens with an uppercase code — `WRONGTYPE`, `NOSCRIPT`,
 * `OOM`, `MOVED`, … — followed by a space and the human text. Three characters
 * minimum on purpose: the shortest real codes are `ERR`, `OOM`, and `ASK`, and
 * requiring three keeps a Lua `redis.error_reply("A bad thing")` from reporting
 * `A` as an error code. Leading whitespace is tolerated for the same reason
 * `isNoScriptError` tolerates it: cheap, and a proxy that pads the reply should
 * not cost the caller its code.
 */
const REDIS_ERROR_CODE = /^\s*([A-Z][A-Z0-9_]{2,})(?:\s|$)/;

/**
 * The leading error code of a Redis error reply, or `undefined` when the text
 * carries none (a Lua script's own `redis.error_reply("no code here")`).
 * Exported because a caller that already holds a raw message — from a nested
 * reply, a log line — can classify it the same way {@link RedisServerError}
 * does.
 */
export function redisErrorCode(message: string): string | undefined {
  return REDIS_ERROR_CODE.exec(message)?.[1];
}

/** Extra structure {@link RedisServerError} carries when the throw site has it. */
export type RedisServerErrorOptions = {
  /**
   * Name of the command that drew the error, uppercased (`"ZADD"`). Only set
   * where the throw site can attribute it: a single `send`, or a pipeline entry
   * an adapter reports per command. Undefined for an aggregate rejection whose
   * failing entry cannot be identified.
   */
  readonly command?: string;
  /** The adapter-native error (or raw payload) this was normalized from. */
  readonly cause?: unknown;
};

/**
 * Thrown when the Redis *server* answered with an error reply: `WRONGTYPE` on a
 * key holding another type, `NOSCRIPT`, `OOM`, `READONLY`, `NOAUTH`, a script's
 * own `redis.error_reply(...)`. This is the one error type every adapter
 * normalizes to, so `catch (error) { if (error instanceof RedisServerError) }`
 * means the same thing on `benni/node`, `benni/ioredis`, `benni/bun`, and
 * `benni/upstash` — without it, callers were left matching against node-redis's
 * `SimpleError`, ioredis's `ReplyError`, Bun's `RedisError`, or a bare `Error`
 * built from an Upstash REST payload, one taxonomy per runtime.
 *
 * How it differs from its siblings: {@link ValidationError} means benni refused
 * the input before anything was sent, and {@link ReplyShapeError} means a
 * *successful* reply did not match the shape a decoder expected. This one means
 * the command reached Redis and Redis said no.
 *
 * `message` is the server's text verbatim, code and all, so message matching
 * that predates this class keeps working. Branch on {@link code} rather than the
 * message. {@link cause} holds the adapter-native error, so nothing the
 * underlying client attached is lost.
 *
 * Cluster redirections (`MOVED`, `ASK`) are followed by the cluster-aware client
 * underneath and normally never surface; one that does reach here is a real
 * failure, and its code is parsed like any other.
 *
 * @example
 * ```ts
 * try {
 *   await leaderboard.zadd("global", { member: "ada", score: 1 });
 * } catch (error) {
 *   if (error instanceof RedisServerError && error.code === "WRONGTYPE") {
 *     // that key holds a hash, not a sorted set
 *   }
 * }
 * ```
 */
export class RedisServerError extends Error {
  /**
   * The uppercase code the reply opens with (`"WRONGTYPE"`, `"NOSCRIPT"`,
   * `"OOM"`, …), parsed from the message so callers never have to. `undefined`
   * when the server's text carries no code, which in practice means a Lua
   * script returned a bare `redis.error_reply(...)`.
   */
  readonly code: string | undefined;

  /**
   * Uppercased name of the command that drew the error, when the throw site
   * could attribute it. See {@link RedisServerErrorOptions.command}.
   */
  readonly command: string | undefined;

  constructor(message: string, options?: RedisServerErrorOptions) {
    // Only pass the options bag when there is a cause: `{ cause: undefined }`
    // still installs an own `cause` property, which makes a normalized error
    // look like it wrapped something when it did not.
    super(
      message,
      options?.cause === undefined ? undefined : { cause: options.cause }
    );
    this.name = "RedisServerError";
    this.code = redisErrorCode(message);
    this.command = options?.command;
  }
}

/**
 * Normalizes whatever an adapter's underlying client reported for a server error
 * reply into a {@link RedisServerError}: node-redis's `ErrorReply`, ioredis's
 * `ReplyError`, Bun's `RedisError`, or the plain string an Upstash REST payload
 * carries in `{ error }`. The message is preserved verbatim and the original is
 * kept as `cause`.
 *
 * Deciding *whether* something is a server error reply stays with each adapter,
 * which knows its client's taxonomy; this only does the conversion. Already
 * normalized errors pass through untouched, so re-wrapping on the way out of a
 * nested call cannot double-wrap or break identity comparisons.
 */
export function redisServerError(
  source: unknown,
  command?: string
): RedisServerError {
  if (source instanceof RedisServerError) return source;
  const message = source instanceof Error ? source.message : String(source);
  return new RedisServerError(message, { command, cause: source });
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

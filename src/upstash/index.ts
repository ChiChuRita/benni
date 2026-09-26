import { decodeBase64 } from "../core/base64.js";
import { redisServerError } from "../core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisCommandArgument,
  RedisReply
} from "../core/index.js";

/**
 * Options for {@link upstash}. `url` and `token` are the Upstash REST endpoint
 * and bearer token (or any Upstash-REST-compatible server, e.g.
 * `serverless-redis-http`). Pass `fetch` to override the global (for tests or a
 * custom edge fetch); it defaults to `globalThis.fetch`.
 */
export type UpstashOptions = {
  readonly url: string;
  readonly token: string;
  readonly fetch?: typeof fetch;
  /**
   * Abort any request, body included, that has not completed within this
   * many milliseconds; it rejects with a `DOMException` named
   * `"TimeoutError"`. Unset by default, which leaves a hung request to
   * whatever limit the runtime imposes.
   */
  readonly timeoutMs?: number;
  /**
   * Aborting this signal rejects every in-flight request and every later one
   * with the signal's reason, e.g. to tie the client to one request's
   * lifetime on an edge runtime.
   */
  readonly signal?: AbortSignal;
};

/** Every string in a result is base64, see {@link decodeResult}. */
const utf8 = new TextDecoder();

/**
 * A {@link RedisClient} that speaks the Upstash REST protocol over HTTP, so the
 * same typed Benni API runs on serverless/edge runtimes (Cloudflare Workers,
 * Vercel Edge, Fastly, …) with nothing but `fetch` — zero dependencies.
 *
 * The REST endpoints map 1:1 onto the client contract: a command array is
 * `POST`ed to `/` (`send`), `/pipeline` (`pipeline`), or `/multi-exec`
 * (`transaction`, atomic MULTI/EXEC).
 *
 * HTTP is stateless, so this adapter deliberately omits `session` and
 * `subscriber`: blocking commands (`BLPOP`, `XREAD BLOCK`, …), `WATCH`-based
 * optimistic transactions, and Pub/Sub *subscribing* all need a persistent
 * exclusive connection, so they are only available through the TCP adapters
 * (`benni/node`, `benni/ioredis`, `benni/bun`). `redis.session()` /
 * `redis.watch()` and subscribing throw a clear `TypeError` when used with
 * this client. Publishing is a plain stateless command and works here.
 *
 * Responses are requested base64-encoded (`Upstash-Encoding: base64`) and
 * decoded here, the way Upstash's own client does by default: a value that is
 * not valid UTF-8 would otherwise break the server's JSON encoding
 * (serverless-redis-http answers such a `GET` with an empty body). Bulk
 * strings still reach the caller as UTF-8 decoded strings, like every
 * adapter's. The endpoint must honour that header; Upstash and
 * serverless-redis-http both do.
 *
 * Binary (`Uint8Array`) command arguments are not supported over REST; use the
 * `bytes()` codec (which stores base64 strings) or a TCP adapter.
 */
export function upstash(options: UpstashOptions): RedisClient {
  const doFetch = options.fetch ?? globalThis.fetch;
  if (typeof doFetch !== "function") {
    throw new TypeError(
      "upstash() requires a global fetch or an explicit fetch option"
    );
  }
  // An unset environment variable is the usual way these arrive empty, and
  // the request it produces fails with a 401 that no longer names the cause.
  if (typeof options.url !== "string" || options.url === "") {
    throw new TypeError("upstash() requires a url (the REST endpoint)");
  }
  if (typeof options.token !== "string" || options.token === "") {
    throw new TypeError("upstash() requires a token (the REST bearer token)");
  }
  const { timeoutMs, signal } = options;
  if (
    timeoutMs !== undefined &&
    !(Number.isFinite(timeoutMs) && timeoutMs > 0)
  ) {
    throw new TypeError(
      `upstash() timeoutMs must be a positive number of milliseconds, got ${String(timeoutMs)}`
    );
  }
  const base = options.url.replace(/\/+$/, "");
  const authorization = `Bearer ${options.token}`;
  // HTTP holds no connection, so close() has nothing to tear down. It still
  // has to be final, per the client contract: a command after close() is a
  // shutdown-ordering bug and must fail the same way it does on the TCP
  // adapters rather than quietly succeed. Requests already in flight finish.
  let closed = false;

  async function exchange(
    path: string,
    body: unknown,
    abortSignal: AbortSignal
  ): Promise<unknown> {
    const response = await doFetch(`${base}${path}`, {
      method: "POST",
      headers: {
        Authorization: authorization,
        "Content-Type": "application/json",
        "Upstash-Encoding": "base64"
      },
      body: JSON.stringify(body),
      signal: abortSignal
    });
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new Error(`Upstash HTTP ${response.status}: non-JSON response`);
    }
    // `{ "error": ... }` is two different things on the wire. Redis refusing
    // a command arrives as 400 (or 200, per element, in a pipeline): that is
    // an error reply and becomes a `RedisServerError` in unwrapOne. Every
    // other failure status is the service in front of Redis refusing or
    // failing the *request* — 401 for a bad token, 403, 413, 429, any 5xx —
    // with the same envelope, and none of it came from Redis. Reporting a 401
    // `{ "error": "Unauthorized" }` as a server error reply put a bogus
    // `.code` on it and told the caller Redis had said no. Those stay plain
    // transport errors, the boundary the errors reference documents:
    // `RedisServerError` means Redis said no.
    if (!response.ok && !(response.status === 400 && isErrorPayload(payload))) {
      const detail =
        isErrorPayload(payload) && payload.error !== undefined
          ? `: ${String(payload.error)}`
          : "";
      throw new Error(`Upstash HTTP ${response.status}${detail}`);
    }
    return payload;
  }

  async function post(path: string, body: unknown): Promise<unknown> {
    if (closed) throw new Error("benni/upstash client is closed");
    const controller = new AbortController();
    // Raced as well as handed to fetch, so a custom `fetch` that ignores its
    // signal still cannot outlive the timeout, and the body read is covered.
    const aborted = new Promise<never>((_, reject) => {
      controller.signal.addEventListener(
        "abort",
        () => reject(controller.signal.reason),
        { once: true }
      );
    });
    aborted.catch(() => {
      // Observed here; the race below is what reports it.
    });
    const forward = () => controller.abort(signal?.reason);
    if (signal?.aborted) forward();
    else signal?.addEventListener("abort", forward, { once: true });
    const timer =
      timeoutMs === undefined
        ? undefined
        : setTimeout(() => {
            controller.abort(
              new DOMException(
                `Upstash request timed out after ${timeoutMs}ms`,
                "TimeoutError"
              )
            );
          }, timeoutMs);
    try {
      return await Promise.race([
        exchange(path, body, controller.signal),
        aborted
      ]);
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", forward);
    }
  }

  return {
    async send(command: RedisCommand): Promise<RedisReply> {
      return unwrapOne(
        await post("", command.map(toRestArgument)),
        commandName(command)
      );
    },
    async pipeline(commands: readonly RedisCommand[]): Promise<RedisReply[]> {
      if (commands.length === 0) return [];
      return unwrapMany(
        await post(
          "/pipeline",
          commands.map((command) => command.map(toRestArgument))
        ),
        commands
      );
    },
    async transaction(
      commands: readonly RedisCommand[]
    ): Promise<RedisReply[]> {
      if (commands.length === 0) return [];
      return unwrapMany(
        await post(
          "/multi-exec",
          commands.map((command) => command.map(toRestArgument))
        ),
        commands
      );
    },
    // session is intentionally omitted — HTTP has no persistent connection.
    async close() {
      closed = true;
    }
  };
}

function isErrorPayload(value: unknown): value is { error: unknown } {
  return typeof value === "object" && value !== null && "error" in value;
}

function toRestArgument(argument: RedisCommandArgument): string {
  if (argument instanceof Uint8Array) {
    throw new TypeError(
      "The Upstash HTTP adapter does not support binary (Uint8Array) command arguments; use the bytes() codec (base64 strings) or a TCP adapter (benni/node, benni/bun)."
    );
  }
  return typeof argument === "string" ? argument : String(argument);
}

/** The command name for error attribution, matching Redis's own casing. */
function commandName(command: RedisCommand): string {
  return String(command[0]).toUpperCase();
}

/**
 * A REST result with its base64 strings decoded. Every string at any depth is
 * base64 under `Upstash-Encoding: base64`, simple strings included (`OK`
 * arrives as `T0s=` from serverless-redis-http); integers and nil are not
 * encoded. Text that is not well-formed base64 is kept as is, which covers a
 * plain `OK` (Upstash's own client special-cases it) and cannot misfire,
 * since padded base64 is never two characters long.
 */
function decodeResult(value: unknown): RedisReply {
  if (typeof value === "string") {
    const bytes = decodeBase64(value);
    return bytes === undefined ? value : utf8.decode(bytes);
  }
  if (Array.isArray(value)) return value.map(decodeResult);
  return (value ?? null) as RedisReply;
}

/**
 * Unwrap one `{ result }` / `{ error }` REST reply. Upstash's JSON mirrors
 * RESP2 flat shapes (integers as numbers, arrays not maps, nil as null) —
 * exactly what the adapter contract asks for and the typed stores decode — so
 * beyond the result/error unwrap only the base64 layer comes off.
 *
 * `{ error }` is the REST protocol's rendering of a Redis error reply, so it
 * becomes a `RedisServerError` like every other adapter's server error, with the
 * text kept verbatim (code included) and the raw payload string as `cause`.
 * Transport failures — a 401, a 5xx, a non-JSON body, a timeout — stay plain
 * `Error`s: nothing about them came from Redis.
 */
function unwrapOne(payload: unknown, command?: string): RedisReply {
  if (isErrorPayload(payload) && payload.error) {
    throw redisServerError(String(payload.error), command);
  }
  if (
    typeof payload !== "object" ||
    payload === null ||
    !("result" in payload)
  ) {
    throw new TypeError("Expected an Upstash { result } response");
  }
  return decodeResult((payload as { result: unknown }).result);
}

function unwrapMany(
  payload: unknown,
  commands: readonly RedisCommand[]
): RedisReply[] {
  // A failed transaction comes back as one top-level { error } object, not an
  // array — surface the Redis error text instead of a shape complaint. Which
  // command failed is not recoverable from that shape, so it goes unattributed.
  if (isErrorPayload(payload) && payload.error) {
    throw redisServerError(String(payload.error));
  }
  if (!Array.isArray(payload)) {
    throw new TypeError(
      "Expected an array response from an Upstash pipeline/multi-exec"
    );
  }
  // Element order matches the request, so a failing element can name its own
  // command.
  return payload.map((element, index) => {
    const command = commands[index];
    return unwrapOne(element, command && commandName(command));
  });
}

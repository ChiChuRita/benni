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
};

/**
 * A {@link RedisClient} that speaks the Upstash REST protocol over HTTP, so the
 * same typed Beni API runs on serverless/edge runtimes (Cloudflare Workers,
 * Vercel Edge, Fastly, …) with nothing but `fetch` — zero dependencies.
 *
 * The REST endpoints map 1:1 onto the client contract: a command array is
 * `POST`ed to `/` (`send`), `/pipeline` (`pipeline`), or `/multi-exec`
 * (`transaction`, atomic MULTI/EXEC).
 *
 * HTTP is stateless, so this adapter deliberately omits `session`: blocking
 * commands (`BLPOP`, `XREAD BLOCK`, …), `WATCH`-based optimistic transactions,
 * and Pub/Sub all require a persistent exclusive connection and are only
 * available through the TCP adapters (`beni/node`, `beni/bun`).
 * `redis.session()` / `redis.watch()` throw a clear `TypeError` when used
 * with this client.
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
  const base = options.url.replace(/\/+$/, "");
  const authorization = `Bearer ${options.token}`;

  async function post(path: string, body: unknown): Promise<unknown> {
    const response = await doFetch(`${base}${path}`, {
      method: "POST",
      headers: {
        Authorization: authorization,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new Error(`Upstash HTTP ${response.status}: non-JSON response`);
    }
    // A Redis-level error arrives as `{ "error": "..." }` (with 200 in a
    // pipeline element, or 4xx for a single command); surface it as thrown.
    if (!response.ok && !isErrorPayload(payload)) {
      throw new Error(`Upstash HTTP ${response.status}`);
    }
    return payload;
  }

  return {
    async send(command: RedisCommand): Promise<RedisReply> {
      return unwrapOne(await post("", command.map(toRestArgument)));
    },
    async pipeline(commands: readonly RedisCommand[]): Promise<RedisReply[]> {
      if (commands.length === 0) return [];
      return unwrapMany(
        await post(
          "/pipeline",
          commands.map((command) => command.map(toRestArgument))
        )
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
        )
      );
    },
    // session is intentionally omitted — HTTP has no persistent connection.
    async close() {}
  };
}

function isErrorPayload(value: unknown): value is { error: unknown } {
  return typeof value === "object" && value !== null && "error" in value;
}

function toRestArgument(argument: RedisCommandArgument): string {
  if (argument instanceof Uint8Array) {
    throw new TypeError(
      "The Upstash HTTP adapter does not support binary (Uint8Array) command arguments; use the bytes() codec (base64 strings) or a TCP adapter (beni/node, beni/bun)."
    );
  }
  return typeof argument === "string" ? argument : String(argument);
}

/**
 * Unwrap one `{ result }` / `{ error }` REST reply. Upstash's default JSON mode
 * mirrors RESP2 flat shapes (integers as numbers, arrays not maps, nil as
 * null) — exactly what the Node adapter forces and the typed stores decode — so
 * no reply normalization beyond the result/error unwrap is needed.
 */
function unwrapOne(payload: unknown): RedisReply {
  if (isErrorPayload(payload) && payload.error) {
    throw new Error(String(payload.error));
  }
  if (
    typeof payload !== "object" ||
    payload === null ||
    !("result" in payload)
  ) {
    throw new TypeError("Expected an Upstash { result } response");
  }
  return ((payload as { result: RedisReply }).result ?? null) as RedisReply;
}

function unwrapMany(payload: unknown): RedisReply[] {
  // A failed transaction comes back as one top-level { error } object, not an
  // array — surface the Redis error text instead of a shape complaint.
  if (isErrorPayload(payload) && payload.error) {
    throw new Error(String(payload.error));
  }
  if (!Array.isArray(payload)) {
    throw new TypeError(
      "Expected an array response from an Upstash pipeline/multi-exec"
    );
  }
  return payload.map(unwrapOne);
}

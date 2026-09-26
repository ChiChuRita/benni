import type { RedisClient, RedisCommand, RedisReply } from "./types.js";

/**
 * Anything that carries a bound client on `raw` — in practice the handle
 * `benni()` returns. Accepting it is what lets `benni({ client: redis })` and
 * the Hono and Next.js integrations share a handle's client instead of forcing
 * `redis.raw` on callers who already hold a handle.
 */
export type ClientProvider<TClient extends RedisClient = RedisClient> = {
  readonly raw: TClient;
};

/**
 * What every entry point that needs a client takes: an adapter's client, or a
 * benni handle whose client to share.
 *
 * ```ts
 * export const redis = benni({ client: node({ url }), schema });
 * ```
 *
 * Adapters return their client synchronously and connect on first use, so
 * there is nothing to await at module scope and no promise or factory form to
 * accept. A handle built over another handle shares its client and does not
 * own it: closing the second handle leaves the first one's client open.
 */
export type ClientSource<TClient extends RedisClient = RedisClient> =
  | TClient
  | ClientProvider<TClient>;

/**
 * The capability messages, shared by every guard that raises one. They live
 * here, the one module both `benni()` and the stores can import without
 * pulling anything else in (`core/pubsub.ts` imports the subscriber message
 * from here rather than the reverse, which would pin the whole Pub/Sub hub
 * into every bundle that binds a client).
 */
export const SESSION_UNSUPPORTED = "Redis client does not support sessions";
export const TRANSACTION_UNSUPPORTED =
  "Redis client does not support transactions";
export const SUBSCRIBER_UNSUPPORTED =
  "Pub/Sub subscribe requires a client that can hold a connection; this adapter provides none (HTTP is stateless). Publishing still works — subscribe through benni/node, benni/ioredis, or benni/bun.";

/**
 * Both required halves of the client contract, not just `send`.
 *
 * A `RedisSession` also has `send`, so a `send`-only check accepted one (or a
 * handle's `session.raw`) as a client and handed back something whose
 * `pipeline` was undefined — the failure then surfaced commands later as
 * "pipeline is not a function" rather than here, where the message can say what
 * was expected. TypeScript already rejects a session, since `RedisSession` is
 * not assignable to `RedisClient`; this is what makes the runtime guard agree
 * with the type for callers who are not type-checked. Every adapter provides
 * both, and the contract has `pipeline` non-optional, so nothing legitimate is
 * excluded.
 */
function isClient(value: object): value is RedisClient {
  return (
    typeof (value as RedisClient).send === "function" &&
    typeof (value as RedisClient).pipeline === "function"
  );
}

function isProvider(value: object): value is ClientProvider {
  const raw = (value as Partial<ClientProvider>).raw;
  return typeof raw === "object" && raw !== null && isClient(raw);
}

/**
 * Why `source` is not a client, in terms of what to write instead. The common
 * mistakes each get their own answer: handing over the driver's client
 * unwrapped, and the promise and factory forms 0.1 accepted.
 */
function refusal(source: unknown): string {
  const fn = (name: string) =>
    typeof (source as Record<string, unknown> | null)?.[name] === "function";
  if (typeof source === "function") {
    return "benni no longer takes a client factory. Adapters return their client synchronously and connect on the first command, so pass the client itself: `client: node({ url })`.";
  }
  if (fn("then")) {
    return "benni no longer takes a promise of a client. Adapters return their client synchronously and connect on the first command, so pass `node({ url })` itself, without awaiting it; if the promise is your own, await it first.";
  }
  // ioredis first: it has sendCommand too, but only it has call().
  if (fn("call") && fn("duplicate")) {
    return 'benni was handed an ioredis client directly. Wrap it: `client: ioredis(instance)` from "benni/ioredis" adopts it, and benni never closes a client it adopted.';
  }
  if (fn("sendCommand") && fn("duplicate")) {
    return 'benni was handed a node-redis client directly. benni/node cannot adopt an existing node-redis client; let it create one: `client: node({ url })` from "benni/node" takes every node-redis option.';
  }
  return "Expected a Redis client from a benni adapter (with send() and pipeline()) or a benni handle.";
}

/**
 * Narrow a {@link ClientSource} to the `RedisClient` the internals speak,
 * keeping the adapter's own client type.
 *
 * A client is returned as-is and a handle yields the client it carries, so
 * the common path adds no wrapper and no indirection.
 *
 * @throws TypeError for anything else, saying what to pass instead.
 */
export function resolveClient<TClient extends RedisClient>(
  source: ClientSource<TClient>
): TClient {
  if (typeof source === "object" && source !== null) {
    if (isClient(source)) return source as TClient;
    if (isProvider(source)) return source.raw as TClient;
  }
  throw new TypeError(refusal(source));
}

/**
 * MULTI/EXEC when the client has it, one pipeline when it does not.
 *
 * For a caller that only wants the atomicity as an upgrade, and is correct
 * (just weaker) without it. Callers whose whole point is atomicity
 * (`redis.multi()`, via `core/transaction.ts`) must not use this — degrading
 * those to a pipeline would drop the atomicity silently, which is worse than
 * refusing.
 */
export async function transactionOrPipeline(
  client: RedisClient,
  commands: readonly RedisCommand[]
): Promise<RedisReply[]> {
  if (client.transaction === undefined) return client.pipeline(commands);
  return client.transaction(commands);
}

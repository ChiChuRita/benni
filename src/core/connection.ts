/**
 * Connection hooks every adapter accepts, with the same meaning on each. The
 * callbacks only observe: a command a failure affects still rejects as it
 * would without them.
 */
export type ConnectionEvents = {
  /**
   * Called with every connection-level failure the adapter sees: a connect
   * that failed, a socket error, a dropped connection, or (on `benni/upstash`)
   * a request that failed in transit. Error replies from Redis are not
   * reported here; they reject the command that drew them.
   *
   * Without it those failures still reach any command they affect, and the
   * background ones (a drop while idle, a failed reconnect) are dropped so an
   * idle blip cannot crash the process. If this callback throws, the error is
   * rethrown asynchronously rather than into the Redis client's internals.
   */
  readonly onError?: (error: unknown) => void;
  /**
   * Called when a connection that was ready comes back after a drop:
   * `"client"` for the shared command connection, `"subscriber"` for the
   * Pub/Sub connection. Messages published while the subscriber was down are
   * lost (Redis Pub/Sub is at-most-once), so this is the signal to refetch
   * whatever the subscription was keeping current. Never called by
   * `benni/upstash`, which holds no connection.
   */
  readonly onReconnect?: (connection: "client" | "subscriber") => void;
};

/**
 * `onError` made safe to call from inside a client's event emitter: a throw
 * from the caller's callback would otherwise unwind through the client's own
 * socket handling. Rethrown on a microtask instead, like a failing Pub/Sub
 * handler, so it is loud without corrupting the connection.
 */
export function reporter(
  onError: ((error: unknown) => void) | undefined
): (error: unknown) => void {
  if (onError === undefined) return () => {};
  return (error) => {
    try {
      onError(error);
    } catch (thrown) {
      queueMicrotask(() => {
        throw thrown;
      });
    }
  };
}

/** First retry after a failed connect, doubling per failure up to the cap. */
const FIRST_RETRY_MS = 100;
const MAX_RETRY_MS = 5_000;

export type LazyConnection = {
  /**
   * Resolve once connected, starting a connect when none is in flight. After
   * a failure, calls inside the backoff window reject with that failure
   * without dialing; the first call after it dials again.
   */
  ready(): Promise<void>;
};

/**
 * Connect on first use, and again on a later use after a failure.
 *
 * This is what lets an adapter hand back its client synchronously. It connects
 * only when a command needs it, so importing the module that builds a handle
 * opens nothing, and a connect that fails (a pod booting while Redis restarts)
 * fails the commands waiting on it without poisoning the client: once the
 * backoff has passed, the next command dials again.
 *
 * Retries are driven by commands, not a timer. A background timer would keep
 * dialing a server nobody is asking for and hold the process open.
 *
 * The adapter decides when a connection is needed (its client is not open) and
 * keeps its own reconnect-after-drop behaviour; this only covers establishing
 * a connection that is not there.
 */
export function lazyConnection(
  adapter: string,
  connect: () => Promise<void>
): LazyConnection {
  let pending: Promise<void> | undefined;
  let failure: Error | undefined;
  let retryAt = 0;
  let failures = 0;

  return {
    ready() {
      if (pending !== undefined) return pending;
      if (failure !== undefined && Date.now() < retryAt) {
        return Promise.reject(failure);
      }
      pending = connect().then(
        () => {
          pending = undefined;
          failure = undefined;
          failures = 0;
        },
        (cause: unknown) => {
          pending = undefined;
          failures += 1;
          retryAt =
            Date.now() +
            Math.min(FIRST_RETRY_MS * 2 ** (failures - 1), MAX_RETRY_MS);
          failure = new Error(
            `${adapter} could not connect to Redis: ${messageOf(cause)}. A later command retries the connection.`,
            { cause }
          );
          throw failure;
        }
      );
      return pending;
    }
  };
}

function messageOf(cause: unknown): string {
  if (cause instanceof Error) {
    // Node reports a refused connection to a host with several addresses as
    // an AggregateError with an empty message; the addresses are inside.
    if (cause.message !== "") return cause.message;
    const inner = (cause as { errors?: unknown }).errors;
    if (Array.isArray(inner) && inner[0] instanceof Error) {
      return inner[0].message;
    }
    return cause.name;
  }
  return String(cause);
}

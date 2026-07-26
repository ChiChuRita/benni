import { describe, expect, it, vi } from "vitest";
import { ValidationError } from "../src/core/errors.js";
import {
  type BlockingWait,
  blockingTimeoutMilliseconds,
  blockingTimeoutSeconds,
  createBeniSession,
  runWatch,
  SessionClosedError,
  WatchRetriesExceededError
} from "../src/core/session.js";
import {
  createWatchedTransaction,
  numberReply,
  okReply,
  stringOrNullReply
} from "../src/core/transaction.js";
import type {
  RedisCommand,
  RedisReply,
  RedisSession
} from "../src/core/types.js";
import { type FakeWatchedResult, fakeSession } from "./fake-client.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

describe("blocking timeout helpers", () => {
  it("renders fractional seconds as decimal strings", () => {
    expect(blockingTimeoutSeconds(5)).toBe("5");
    expect(blockingTimeoutSeconds(0.1)).toBe("0.1");
    expect(blockingTimeoutSeconds(2.5)).toBe("2.5");
  });

  it('maps "forever" to the Redis block-forever sentinel "0"', () => {
    expect(blockingTimeoutSeconds("forever")).toBe("0");
    expect(blockingTimeoutMilliseconds("forever")).toBe("0");
  });

  it("converts seconds to integer milliseconds without hitting 0", () => {
    expect(blockingTimeoutMilliseconds(5)).toBe("5000");
    expect(blockingTimeoutMilliseconds(0.1)).toBe("100");
    expect(blockingTimeoutMilliseconds(0.0001)).toBe("1");
  });

  it.each([
    0,
    -1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY
  ])("rejects %s with TypeError", (timeout) => {
    const expected = new ValidationError(
      'Blocking timeoutSeconds must be a positive finite number of seconds or "forever"'
    );
    expect(() => blockingTimeoutSeconds(timeout)).toThrow(expected);
    expect(() => blockingTimeoutMilliseconds(timeout)).toThrow(expected);
  });
});

describe("createWatchedTransaction", () => {
  it("decodes the tuple from a committed EXEC", async () => {
    const commands: RedisCommand[] = [];
    const raw = fakeSession(commands, [], [["OK", "beni", 2]]);

    const results = await createWatchedTransaction(raw)
      .add(["SET", "user:42", "beni"], okReply)
      .add(["GET", "user:42"], stringOrNullReply)
      .add(["INCR", "user:42:hits"], numberReply)
      .exec();

    expect(results).toEqual([undefined, "beni", 2]);
    expect(commands).toEqual([
      ["SET", "user:42", "beni"],
      ["GET", "user:42"],
      ["INCR", "user:42:hits"]
    ]);
  });

  it("maps a null watchedTransaction result to null (abort)", async () => {
    const raw = fakeSession([], [], [null]);

    await expect(
      createWatchedTransaction(raw).add(["INCR", "hits"], numberReply).exec()
    ).resolves.toBeNull();
  });

  it("throws on an empty watched transaction without sending anything", async () => {
    const commands: RedisCommand[] = [];
    const raw = fakeSession(commands, [], []);

    await expect(createWatchedTransaction(raw).exec()).rejects.toThrow(
      "Cannot exec an empty watched transaction; queue at least one command"
    );
    expect(commands).toEqual([]);
  });

  it("rejects with a scripted per-command Error from a committed EXEC", async () => {
    const wrongType = new Error(
      "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    const raw = fakeSession([], [], [wrongType]);

    await expect(
      createWatchedTransaction(raw).add(["INCR", "hits"], numberReply).exec()
    ).rejects.toBe(wrongType);
  });

  it("throws when EXEC returns the wrong number of replies", async () => {
    const raw = fakeSession([], [], [["OK"]]);

    await expect(
      createWatchedTransaction(raw)
        .add(["SET", "a", "1"], okReply)
        .add(["SET", "b", "2"], okReply)
        .exec()
    ).rejects.toThrow("Expected Redis EXEC to return 2 replies");
  });

  it("throws when EXEC returns neither array nor null", async () => {
    const raw: RedisSession = {
      ...fakeSession([], [], []),
      async watchedTransaction() {
        return "OK" as unknown as RedisReply[];
      }
    };

    await expect(
      createWatchedTransaction(raw).add(["PING"], stringOrNullReply).exec()
    ).rejects.toThrow("Expected Redis EXEC to return array or null");
  });
});

describe("createBeniSession", () => {
  it("sends WATCH with the given keys and validates OK", async () => {
    const commands: RedisCommand[] = [];
    const session = createBeniSession(fakeSession(commands, ["OK", "OK"]));

    await session.watch(["beni:views:home", "beni:views:about"]);
    await session.unwatch();

    expect(commands).toEqual([
      ["WATCH", "beni:views:home", "beni:views:about"],
      ["UNWATCH"]
    ]);
  });

  it("rejects a WATCH with no keys before sending anything", async () => {
    const commands: RedisCommand[] = [];
    const session = createBeniSession(fakeSession(commands, []));

    await expect(session.watch([])).rejects.toThrow(
      "watch requires at least one key"
    );
    expect(commands).toEqual([]);
  });

  it("throws when WATCH or UNWATCH does not return OK", async () => {
    const session = createBeniSession(fakeSession([], [null, null]));

    await expect(session.watch(["k"])).rejects.toThrow(
      "Expected Redis WATCH to return OK"
    );
    await expect(session.unwatch()).rejects.toThrow(
      "Expected Redis UNWATCH to return OK"
    );
  });

  it("multi() builds a watched transaction over the gated connection", async () => {
    const commands: RedisCommand[] = [];
    const raw = fakeSession(commands, ["OK"], [null, [3]]);
    const session = createBeniSession(raw);

    await session.watch(["beni:views:home"]);
    const first = await session
      .multi()
      .add(["INCR", "beni:views:home"], numberReply)
      .exec();
    const second = await session
      .multi()
      .add(["INCR", "beni:views:home"], numberReply)
      .exec();

    expect(first).toBeNull();
    expect(second).toEqual([3]);
    expect(commands).toEqual([
      ["WATCH", "beni:views:home"],
      ["INCR", "beni:views:home"],
      ["INCR", "beni:views:home"]
    ]);
  });

  it("facade send and pipeline flow through the raw session in order", async () => {
    const commands: RedisCommand[] = [];
    const session = createBeniSession(
      fakeSession(commands, ["PONG", "1", "2"])
    );

    await expect(session.client.send(["PING"])).resolves.toBe("PONG");
    await expect(
      session.client.pipeline([
        ["GET", "a"],
        ["GET", "b"]
      ])
    ).resolves.toEqual(["1", "2"]);
    expect(commands).toEqual([["PING"], ["GET", "a"], ["GET", "b"]]);
  });

  it("facade transaction throws TypeError when a bare multi aborts", async () => {
    const session = createBeniSession(fakeSession([], [], [null]));

    await expect(
      session.client.transaction!([["INCR", "hits"]])
    ).rejects.toThrow("Expected Redis EXEC to return array");
  });

  it("facade transaction short-circuits an empty batch without sending", async () => {
    const watched = vi.fn();
    const raw: RedisSession = {
      ...fakeSession([], []),
      watchedTransaction: watched
    };
    const session = createBeniSession(raw);

    await expect(session.client.transaction!([])).resolves.toEqual([]);
    expect(watched).not.toHaveBeenCalled();
  });

  it("gates a send fired during an in-flight watched exec until after EXEC", async () => {
    const order: string[] = [];
    const execGate = deferred<void>();
    const raw: RedisSession = {
      async send(command) {
        order.push(`send:${command[0]}`);
        return "OK";
      },
      async watchedTransaction() {
        order.push("exec:start");
        await execGate.promise;
        order.push("exec:end");
        return [1];
      },
      closed: false,
      async close() {}
    };
    const session = createBeniSession(raw);

    const exec = session.multi().add(["INCR", "hits"], numberReply).exec();
    const stray = session.client.send(["GET", "hits"]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(order).toEqual(["exec:start"]);

    execGate.resolve();
    await expect(exec).resolves.toEqual([1]);
    await expect(stray).resolves.toBe("OK");
    expect(order).toEqual(["exec:start", "exec:end", "send:GET"]);
  });

  it("rejects everything with SessionClosedError after close()", async () => {
    const session = createBeniSession(fakeSession([], ["OK"]));

    await session.close();

    expect(session.closed).toBe(true);
    await expect(session.client.send(["PING"])).rejects.toThrow(
      SessionClosedError
    );
    await expect(session.watch(["k"])).rejects.toThrow(SessionClosedError);
    await expect(session.unwatch()).rejects.toThrow(SessionClosedError);
    await expect(
      session.multi().add(["INCR", "hits"], numberReply).exec()
    ).rejects.toThrow(SessionClosedError);
    await expect(session.client.pipeline([["GET", "a"]])).rejects.toThrow(
      SessionClosedError
    );
  });

  it("close() is idempotent and also closes the raw session", async () => {
    const raw = fakeSession([], []);
    const session = createBeniSession(raw);

    await session.close();
    await session.close();

    expect(raw.closed).toBe(true);
    expect(session.closed).toBe(true);
  });

  it("Symbol.asyncDispose closes the session for await using", async () => {
    const raw = fakeSession([], []);
    const session = createBeniSession(raw);

    await session[Symbol.asyncDispose]();

    expect(session.closed).toBe(true);
    expect(raw.closed).toBe(true);
  });

  it("reflects a dropped raw connection through closed", async () => {
    const raw = fakeSession([], []);
    const session = createBeniSession(raw);

    expect(session.closed).toBe(false);
    await raw.close();
    expect(session.closed).toBe(true);
  });
});

describe("runWatch", () => {
  function watchedKernel(
    commands: RedisCommand[],
    replies: RedisReply[],
    watchedResults: FakeWatchedResult[]
  ) {
    return createBeniSession(fakeSession(commands, replies, watchedResults));
  }

  it("retries on abort with a fresh WATCH, then commits", async () => {
    const commands: RedisCommand[] = [];
    const kernel = watchedKernel(commands, ["OK", "OK"], [null, [1]]);
    const aborts: number[] = [];

    const result = await runWatch(
      async () => kernel,
      "beni:views:home",
      async (session) =>
        session.multi().add(["INCR", "beni:views:home"], numberReply),
      { onAbort: ({ attempt }) => aborts.push(attempt) }
    );

    expect(result).toEqual([1]);
    expect(aborts).toEqual([1]);
    expect(commands).toEqual([
      ["WATCH", "beni:views:home"],
      ["INCR", "beni:views:home"],
      ["WATCH", "beni:views:home"],
      ["INCR", "beni:views:home"]
    ]);
    expect(kernel.closed).toBe(true);
  });

  it("invokes backoff with the attempt number between retries", async () => {
    const commands: RedisCommand[] = [];
    const kernel = watchedKernel(
      commands,
      ["OK", "OK", "OK"],
      [null, null, [1]]
    );
    const backoffCalls: number[] = [];

    const result = await runWatch(
      async () => kernel,
      ["beni:views:home"],
      async (session) =>
        session.multi().add(["INCR", "beni:views:home"], numberReply),
      {
        backoff: (attempt) => {
          backoffCalls.push(attempt);
          return 1;
        }
      }
    );

    expect(result).toEqual([1]);
    expect(backoffCalls).toEqual([1, 2]);
  });

  it("resolves null and UNWATCHes when the body opts out", async () => {
    const commands: RedisCommand[] = [];
    const kernel = watchedKernel(commands, ["OK", "OK"], []);

    const result = await runWatch(
      async () => kernel,
      "beni:views:home",
      async () => null
    );

    expect(result).toBeNull();
    expect(commands).toEqual([["WATCH", "beni:views:home"], ["UNWATCH"]]);
    expect(kernel.closed).toBe(true);
  });

  it("throws WatchRetriesExceededError with .attempts when exhausted", async () => {
    const commands: RedisCommand[] = [];
    const kernel = watchedKernel(commands, ["OK", "OK"], [null, null]);
    const aborts: number[] = [];

    const attempt = runWatch(
      async () => kernel,
      "beni:views:home",
      async (session) =>
        session.multi().add(["INCR", "beni:views:home"], numberReply),
      { attempts: 2, onAbort: ({ attempt }) => aborts.push(attempt) }
    );

    await expect(attempt).rejects.toThrow(WatchRetriesExceededError);
    await attempt.catch((error: unknown) => {
      expect(error).toBeInstanceOf(WatchRetriesExceededError);
      expect((error as WatchRetriesExceededError).attempts).toBe(2);
    });
    expect(aborts).toEqual([1, 2]);
    expect(kernel.closed).toBe(true);
  });

  it("UNWATCHes best-effort and rethrows when the body throws", async () => {
    const commands: RedisCommand[] = [];
    const kernel = watchedKernel(commands, ["OK", "OK"], []);
    const boom = new Error("boom");

    await expect(
      runWatch(
        async () => kernel,
        "beni:views:home",
        async () => {
          throw boom;
        }
      )
    ).rejects.toBe(boom);

    expect(commands).toEqual([["WATCH", "beni:views:home"], ["UNWATCH"]]);
    expect(kernel.closed).toBe(true);
  });

  it("rethrows the body error even when UNWATCH itself fails", async () => {
    const kernel = watchedKernel([], ["OK"], []);
    const boom = new Error("boom");

    await expect(
      runWatch(
        async () => kernel,
        "beni:views:home",
        async () => {
          throw boom;
        }
      )
    ).rejects.toBe(boom);
  });

  it("never closes a borrowed session", async () => {
    const commands: RedisCommand[] = [];
    const kernel = watchedKernel(commands, ["OK"], [[1]]);
    const openSession = vi.fn();

    const result = await runWatch(
      openSession as unknown as () => Promise<typeof kernel>,
      "beni:views:home",
      async (session) =>
        session.multi().add(["INCR", "beni:views:home"], numberReply),
      { session: kernel }
    );

    expect(result).toEqual([1]);
    expect(openSession).not.toHaveBeenCalled();
    expect(kernel.closed).toBe(false);
  });

  it.each([
    0,
    -1,
    1.5,
    Number.NaN
  ])("rejects attempts: %s before opening a session", async (attempts) => {
    const openSession = vi.fn();

    await expect(
      runWatch(openSession, "beni:views:home", async () => null, {
        attempts
      })
    ).rejects.toThrow("attempts must be a safe integer >= 1");
    expect(openSession).not.toHaveBeenCalled();
  });

  it("rejects an empty key list before opening a session", async () => {
    const openSession = vi.fn();

    await expect(runWatch(openSession, [], async () => null)).rejects.toThrow(
      "watch requires at least one key"
    );
    expect(openSession).not.toHaveBeenCalled();
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typedSession = null as unknown as RedisSession;

const typedWatched = createWatchedTransaction(typedSession)
  .add(["SET", "user:42", "beni"], okReply)
  .add(["GET", "user:42"], stringOrNullReply)
  .add(["INCR", "hits"], numberReply);

type WatchedResults = Awaited<ReturnType<typeof typedWatched.exec>>;
type _WatchedResults = Expect<
  Equal<WatchedResults, [void, string | null, number] | null>
>;

type _BlockingWaitTimeout = Expect<
  Equal<BlockingWait["timeoutSeconds"], number | "forever">
>;

async function expectTypeErrorsOnly() {
  // @ts-expect-error a watched exec result may be null and must be narrowed.
  const forced: [void, string | null, number] = await typedWatched.exec();
  void forced;

  // @ts-expect-error a RedisSession is not assignable to RedisClient.
  const asClient: import("../src/core/types.js").RedisClient = typedSession;
  void asClient;

  // @ts-expect-error blocking timeouts are seconds or the "forever" literal.
  const wait: BlockingWait = { timeoutSeconds: "never" };
  void wait;

  // @ts-expect-error exec accepts no arguments.
  void typedWatched.exec("now");
}

void expectTypeErrorsOnly;

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { lazyConnection, reporter } from "../src/core/connection.js";

// The connect-on-first-use gate the TCP adapters share. The review finding it
// answers: a pod that booted while Redis was restarting kept one rejected
// connect promise forever, so every command failed for the process lifetime.

describe("lazyConnection", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not connect until asked, then shares one attempt", async () => {
    const connect = vi.fn(async () => {});
    const connection = lazyConnection("benni/test", connect);
    expect(connect).not.toHaveBeenCalled();

    await Promise.all([connection.ready(), connection.ready()]);
    expect(connect).toHaveBeenCalledTimes(1);
  });

  it("fails the waiting callers, then dials again once the backoff passed", async () => {
    let up = false;
    const connect = vi.fn(async () => {
      if (!up) throw new Error("connect ECONNREFUSED 127.0.0.1:6379");
    });
    const connection = lazyConnection("benni/test", connect);

    const waiting = [connection.ready(), connection.ready()];
    for (const attempt of waiting) {
      await expect(attempt).rejects.toThrow(
        "benni/test could not connect to Redis: connect ECONNREFUSED 127.0.0.1:6379. A later command retries the connection."
      );
    }
    expect(connect).toHaveBeenCalledTimes(1);

    // Inside the backoff window: the same failure, without dialing.
    await expect(connection.ready()).rejects.toThrow(/ECONNREFUSED/);
    expect(connect).toHaveBeenCalledTimes(1);

    up = true;
    vi.advanceTimersByTime(100);
    await expect(connection.ready()).resolves.toBeUndefined();
    expect(connect).toHaveBeenCalledTimes(2);
  });

  it("doubles the backoff per failure up to five seconds, and resets on success", async () => {
    let up = false;
    const connect = vi.fn(async () => {
      if (!up) throw new Error("down");
    });
    const connection = lazyConnection("benni/test", connect);
    const windows: number[] = [];

    for (let failure = 0; failure < 8; failure += 1) {
      await connection.ready().catch(() => {});
      const calls = connect.mock.calls.length;
      let waited = 0;
      while (connect.mock.calls.length === calls) {
        vi.advanceTimersByTime(50);
        waited += 50;
        await connection.ready().catch(() => {});
      }
      windows.push(waited);
    }
    expect(windows).toEqual([100, 200, 400, 800, 1600, 3200, 5000, 5000]);

    up = true;
    vi.advanceTimersByTime(5000);
    await connection.ready();
    up = false;
    const calls = connect.mock.calls.length;
    await connection.ready().catch(() => {});
    vi.advanceTimersByTime(100);
    await connection.ready().catch(() => {});
    expect(connect.mock.calls.length).toBe(calls + 2);
  });

  it("names the first address of an AggregateError with no message", async () => {
    const refused = new AggregateError(
      [new Error("connect ECONNREFUSED ::1:6379")],
      ""
    );
    const connection = lazyConnection("benni/test", async () => {
      throw refused;
    });
    const error = await connection.ready().catch((caught: Error) => caught);
    expect((error as Error).message).toContain("connect ECONNREFUSED ::1:6379");
    expect((error as Error).cause).toBe(refused);
  });
});

describe("reporter", () => {
  it("is a no-op without a callback", () => {
    expect(() => reporter(undefined)(new Error("x"))).not.toThrow();
  });

  it("rethrows a throwing callback on a microtask, not into the caller", async () => {
    const thrown = new Error("callback bug");
    const report = reporter(() => {
      throw thrown;
    });
    const seen = new Promise<unknown>((resolve) => {
      const original = globalThis.queueMicrotask;
      globalThis.queueMicrotask = (task) => {
        globalThis.queueMicrotask = original;
        try {
          task();
        } catch (error) {
          resolve(error);
        }
      };
    });
    expect(() => report(new Error("socket"))).not.toThrow();
    await expect(seen).resolves.toBe(thrown);
  });
});

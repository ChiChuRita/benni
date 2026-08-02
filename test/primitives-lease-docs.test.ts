import { describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/types.js";
import {
  LockLeaseLostError,
  LockNotAcquiredError,
  lock,
  SemaphoreLeaseLostError,
  SemaphoreNotAcquiredError,
  semaphore
} from "../src/primitives/index.js";
import { fakeClient } from "./fake-client.js";

const client: RedisClient = fakeClient([], []);

type Receipt = { id: string };
declare const prompt: string;
declare const url: string;
declare const model: unknown;
declare const logger: {
  warn(fields: Record<string, unknown>, message: string): void;
  error(fields: Record<string, unknown>, message: string): void;
};
declare function callModel(p?: string): Promise<string>;
declare function doWork(): Promise<void>;
declare function work(): Promise<void>;
declare function processOrder(): Promise<void>;
declare function chargeCard(): Promise<Receipt>;
declare function reconcile(key: string): Receipt;
declare function generateReport(): Promise<void>;
declare function generateText(input: {
  model: unknown;
  prompt: string;
  abortSignal?: AbortSignal;
}): Promise<{ text: string }>;

/**
 * The snippets from the lock and semaphore pages, typechecked against src so a
 * page cannot drift from the lease API it documents. Nothing here runs against a
 * server; the behaviour is proved in `semaphore-lease.integration.test.ts` and
 * `lock-lease.integration.test.ts`.
 */
function docsSnippets() {
  // --- primitives/lock -----------------------------------------------------
  const locks = lock(client, { ttlMs: 10_000 });

  void (async () => {
    await locks.run("order:42", async () => {
      // critical section
    });

    try {
      await locks.run("order:42", processOrder);
    } catch (error) {
      if (error instanceof LockNotAcquiredError) {
        void error.key;
        return new Response("Already processing", { status: 409 });
      }
      throw error;
    }

    await locks.run("order:42", processOrder, {
      retries: 100,
      retryDelayMs: 50
    });

    await locks.run("report:nightly", async () => {
      await generateReport();
    });

    await locks.run("order:42", processOrder, { heartbeatMs: 1_000 });
    await locks.run("order:42", processOrder, { heartbeatMs: false });

    await locks.run("order:42", processOrder, {
      onRenewError: (error) => {
        logger.warn({ error }, "lock renewal round trip failed");
      }
    });

    await locks.run("order:42", async (handle) => {
      const res = await fetch(url, { signal: handle.signal });
      const { text } = await generateText({
        model,
        prompt,
        abortSignal: handle.signal
      });
      return { res, text };
    });

    return undefined;
  });

  void (async () => {
    try {
      const receipt = await locks.run("order:42", chargeCard);
      return receipt;
    } catch (error) {
      if (error instanceof LockLeaseLostError) {
        return reconcile(error.key);
      }
      throw error;
    }
  });

  void (async () => {
    const handle = await locks.acquire("order:42");
    if (handle) {
      try {
        await doWork();
      } finally {
        await handle.release();
      }
    }
  });

  void (async () => {
    const handle = await locks.acquire("report", { ttlMs: 30_000 });
    const stillOurs = await handle?.extend(30_000);
    if (stillOurs === false) {
      void handle?.signal.aborted;
    }
  });

  // --- primitives/semaphore ------------------------------------------------
  const slots = semaphore(client, { limit: 20, leaseMs: 60_000 });

  void (async () => {
    const answer = await slots.run("openai", async () => callModel(prompt));
    void answer;

    const held = await slots.acquire("openai");
    if (!held) return new Response("Busy, try again", { status: 503 });
    try {
      await doWork();
    } finally {
      await held.release();
    }

    await slots.run("openai", work, { retries: 100, retryDelayMs: 50 });

    await slots.run("openai", async () => callModel(prompt));

    await slots.run("openai", work, { heartbeatMs: 5_000 });
    await slots.run("openai", work, { heartbeatMs: false });

    await slots.run("openai", work, {
      onRenewError: (error) => {
        logger.warn({ error }, "semaphore renewal round trip failed");
      }
    });

    void (await slots.count("openai"));
    return undefined;
  });

  void (async () => {
    try {
      return await slots.run("openai", () => callModel(prompt));
    } catch (error) {
      if (error instanceof SemaphoreLeaseLostError) {
        logger.error(
          { key: error.key, limit: error.limit },
          "semaphore overran"
        );
      }
      throw error;
    }
  });

  void (async () => {
    await slots.run("openai", async (held) => {
      const { text } = await generateText({
        model,
        prompt,
        abortSignal: held.signal
      });
      return text;
    });

    await slots.run("openai", async (held) => {
      const res = await fetch(url, { signal: held.signal });
      return res.json();
    });
  });

  void (async () => {
    const held = await slots.acquire("openai", { leaseMs: 5_000 });
    const stillOurs = await held?.extend();
    if (stillOurs === false) {
      void held?.signal.aborted;
    }
  });

  // The pages name these errors in prose; keep the imports honest.
  void SemaphoreNotAcquiredError;
}

void docsSnippets;

describe("lease docs snippets", () => {
  it("reference the lease surface both pages document", () => {
    expect(typeof lock).toBe("function");
    expect(typeof semaphore).toBe("function");
    expect(new LockLeaseLostError("lock:x").key).toBe("lock:x");
    expect(new SemaphoreLeaseLostError("semaphore:x", 3).limit).toBe(3);
  });
});

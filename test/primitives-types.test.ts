import { describe, expect, it } from "vitest";
import { benni } from "../src/index.js";
import { budget, cache, idempotency, json, semaphore } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

type Receipt = { id: string };
type Order = { total: number };

// The pages declare these in the schema module and reach them by name.
const redis = benni({
  client: fakeClient([], []),
  schema: {
    budgets: budget("budget", { limit: 2_000_000, windowMs: 86_400_000 }),
    slots: semaphore("semaphore", { limit: 20, leaseMs: 60_000 }),
    once: idempotency("idem", { codec: json<Receipt>() }),
    receipts: cache("receipt", { ttlMs: 60_000, codec: json<Receipt>() })
  }
});
declare const userId: string;
declare const promptTokens: number;
declare const prompt: string;
declare const order: Order;
declare const request: Request;
declare function callModel(p?: string): Promise<{
  usage: { totalTokens: number };
}>;
declare function chargeCard(o: Order): Promise<Receipt>;
declare function doWork(): Promise<void>;
declare function handler(): Promise<Receipt>;

/**
 * The snippets from the three new primitive pages, typechecked against src so
 * a page cannot drift from the API it documents. Nothing here runs against a
 * server; the behaviour is proved in `primitives.integration.test.ts`.
 */
function docsSnippets() {
  // --- primitives/budget ---------------------------------------------------
  const budgets = redis.query.budgets;

  void (async () => {
    const { ok, remaining, retryAfterMs } = await budgets.charge(
      userId,
      promptTokens
    );
    if (!ok) {
      return Response.json(
        { error: "Daily token budget exhausted", remaining },
        {
          status: 429,
          headers: { "Retry-After": String(Math.ceil(retryAfterMs / 1000)) }
        }
      );
    }

    const hold = await budgets.reserve(userId, 8_000);
    if (!hold) return new Response("Budget exhausted", { status: 429 });
    try {
      const result = await callModel(prompt);
      await hold.settle(result.usage.totalTokens);
    } catch {
      await hold.release();
    }
    await hold.extend();

    const checked = await budgets.check(userId);
    void checked.remaining;
    void checked.retryAfterMs;
    await budgets.reset(userId);
    return undefined;
  });

  // --- primitives/semaphore ------------------------------------------------
  const slots = redis.query.slots;

  void (async () => {
    // run() renews the lease on its own, so a body no longer heartbeats by
    // hand; it watches `signal` instead, which aborts if the slot is lost.
    const answer = await slots.run("openai", async (held) => {
      void held.signal.aborted;
      return callModel(prompt);
    });
    void answer;

    await slots.run("openai", doWork, {
      heartbeatMs: 5_000,
      onRenewError: (error) => void error
    });
    await slots.run("openai", doWork, { heartbeatMs: false });

    const held = await slots.acquire("openai");
    if (!held) return new Response("Busy, try again", { status: 503 });
    try {
      await doWork();
    } finally {
      await held.release();
    }

    await slots.run("openai", doWork, { retries: 100, retryDelayMs: 50 });
    const short = await slots.acquire("openai", { leaseMs: 5_000 });
    void (await short?.extend());
    void (await slots.count("openai"));
    return undefined;
  });

  // --- primitives/idempotency ----------------------------------------------
  const once = redis.query.once;

  void (async () => {
    const { value, replayed } = await once.run(
      request.headers.get("Idempotency-Key"),
      () => chargeCard(order)
    );
    void Response.json(value, {
      headers: { "Idempotent-Replay": String(replayed) }
    });

    const [a, b] = await Promise.all([
      once.run("key-1", () => chargeCard(order)),
      once.run("key-1", () => chargeCard(order))
    ]);
    void a.value.id;
    void b.replayed;

    await once.run(request.headers.get("Idempotency-Key"), handler);
    void (await once.peek("key-1"));
    void (await once.forget("key-1"));
  });

  // The pages cross-link to cache; keep that call shape honest too.
  void redis.query.receipts.get("r-1", async () => ({ id: "r-1" }));
}

void docsSnippets;

describe("new primitives", () => {
  it("are all reachable through redis.query", () => {
    expect(typeof redis.query.budgets.charge).toBe("function");
    expect(typeof redis.query.slots.run).toBe("function");
    expect(typeof redis.query.once.run).toBe("function");
  });
});

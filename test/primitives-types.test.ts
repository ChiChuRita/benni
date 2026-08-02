import { describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/types.js";
import {
  budget,
  cache,
  idempotency,
  semaphore
} from "../src/primitives/index.js";
import { fakeClient } from "./fake-client.js";

const client: RedisClient = fakeClient([], []);

type Receipt = { id: string };
type Order = { total: number };
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
  const budgets = budget(client, {
    limit: 2_000_000,
    windowMs: 86_400_000
  });

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
  const slots = semaphore(client, { limit: 20, leaseMs: 60_000 });

  void (async () => {
    const answer = await slots.run("openai", async (held) => {
      await held.extend();
      return callModel(prompt);
    });
    void answer;

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
  const once = idempotency<Receipt>(client);

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
  void cache<Receipt>(client, { ttlMs: 60_000 });
}

void docsSnippets;

describe("new primitives", () => {
  it("are all reachable from benni/primitives", () => {
    expect(typeof budget).toBe("function");
    expect(typeof semaphore).toBe("function");
    expect(typeof idempotency).toBe("function");
  });
});

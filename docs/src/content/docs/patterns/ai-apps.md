---
title: "AI Apps"
description: "Redis recipes for LLM apps: chat memory on streams, token budgets, response caching by prompt hash, and resumable generations, fully typed."
---

LLM backends are state-heavy: conversation history, per-user budgets, response caches, in-flight generation tracking. Redis is the natural home for all of it, and every recipe on this page is fully typed end to end. They all assume the client binding from the [Quick Start](/benni/getting-started/quick-start/), and every one of them (streams, counters, and all three primitives) runs unchanged on the [edge adapter](/benni/runtime/edge/).

## Chat Memory On A Stream

A conversation is an append-only log, which is exactly what a Redis stream is: ordered entries with stable IDs, one stream per conversation id. Trimming on write with `maxLen` keeps memory bounded per conversation: no cron job, no unbounded keys.

```ts
import { enumOf, stream, string } from "benni/schema";

export const chat = stream("chat", {
  role: enumOf(["user", "assistant", "system"]),
  content: string()
});
```

Append each turn, trimming to the last ~200 as you write:

```ts
await redis.stream(chat).xadd(
  conversationId,
  { role: "user", content },
  { maxLen: { count: 200, approximate: true } }
);
```

Load the prompt window with `xrevrange` (newest first, so `count` caps the read), reverse back into chronological order, and map entries straight onto an AI SDK `messages` array:

```ts
import { generateText } from "ai";

const recent = await redis
  .stream(chat)
  .xrevrange(conversationId, { count: 20 });

const messages = recent
  .reverse()
  .flatMap(({ value }) =>
    value.role !== undefined && value.content !== undefined
      ? [{ role: value.role, content: value.content }]
      : []
  );
//    ^? Array<{ role: "user" | "assistant" | "system"; content: string }>

const { text } = await generateText({ model: openai("gpt-4o-mini"), messages });
```

Entry values are `Partial` because Redis does not enforce stream entry shapes; the `flatMap` guard both narrows the types and skips malformed entries. For abandoned conversations, arm a per-conversation TTL after writing:

```ts
await redis.stream(chat).expire(conversationId, 60 * 60 * 24 * 30); // 30 days
```

In production, size `maxLen` to your model's context budget, not your UI's history length, and remember `approximate: true` trims in whole macro nodes, so the stream may briefly hold a few more entries than the count. See [Streams](/benni/data-structures/streams/) for the full store API.

## Token Budgets For LLM Endpoints

Requests-per-minute alone does not protect an LLM endpoint: twenty small requests and twenty 100k-token requests cost wildly different amounts. Layer two checks: a sliding-window request limit via the [`ratelimit` primitive](/benni/primitives/ratelimit/), and a daily token budget in a plain counter keyed by user and date.

```ts
import { kv, number } from "benni/schema";
import { ratelimit } from "benni/primitives";

export const dailyTokens = kv("tokens", number());

const limiter = ratelimit(client, { limit: 20, windowMs: 60_000, prefix: "llm" });
const DAILY_TOKEN_BUDGET = 200_000;

export async function POST(request: Request): Promise<Response> {
  const { userId, prompt } = (await request.json()) as {
    userId: string;
    prompt: string;
  };

  // Layer 1: requests per minute, sliding window.
  const { success, resetMs } = await limiter.check(userId);
  if (!success) {
    return new Response("Too Many Requests", {
      status: 429,
      headers: { "Retry-After": String(Math.ceil((resetMs - Date.now()) / 1000)) }
    });
  }

  // Layer 2: tokens per day.
  const day = new Date().toISOString().slice(0, 10); // "2026-07-12"
  const budgetId = `${userId}:${day}`;
  const used = (await redis.kv(dailyTokens).get(budgetId)) ?? 0;
  if (used >= DAILY_TOKEN_BUDGET) {
    return new Response("Daily token budget exhausted", { status: 429 });
  }

  const result = await generateText({ model: openai("gpt-4o-mini"), prompt });

  // Record usage; the increment that creates the key arms its TTL.
  const total = await redis
    .counter(dailyTokens)
    .incrby(budgetId, result.usage.totalTokens);
  if (total === result.usage.totalTokens) {
    await redis.counter(dailyTokens).expire(budgetId, 60 * 60 * 24 * 2);
  }

  return Response.json({ text: result.text, tokensUsedToday: total });
}
```

The sliding window matters here: a fixed window resets all at once, so a caller can burn a full limit at 11:59 and again at 12:00, a 2x burst exactly when abuse scripts hammer the boundary. The sliding-window log admits at most `limit` requests in *any* 60-second span. The date in the budget key does the real expiry work; the TTL is just cleanup, so its precision never affects correctness. Note the budget check runs *before* the call but records *after*, so concurrent requests can overshoot the budget by one generation each, which is the usual, acceptable trade for not holding a reservation across a model call.

## Cache Responses By Prompt Hash

Identical prompts arrive in bursts (the same trending question, the same retried classification), and every duplicate model call costs real money and seconds of latency. The [`cache` primitive](/benni/primitives/cache/) is single-flight: on a miss, exactly one caller runs the loader while concurrent identical prompts wait for the filled value, so a burst of the same prompt becomes one model call. Key it by a SHA-256 over everything that determines the output: model, system prompt, and user input.

```ts
import { cache } from "benni/primitives";

const responses = cache<string>(client, {
  ttlMs: 24 * 60 * 60 * 1000,
  prefix: "llm-response"
});

// Web Crypto: works on Node, Bun, Deno, and every edge runtime.
async function promptHash(model: string, system: string, input: string): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify([model, system, input]));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

const id = await promptHash("gpt-4o-mini", SYSTEM_PROMPT, userInput);
const text = await responses.get(id, async () => {
  const result = await generateText({
    model: openai("gpt-4o-mini"),
    system: SYSTEM_PROMPT,
    prompt: userInput
  });
  return result.text;
});
```

Only cache calls that are deterministic enough to reuse: classification, extraction, and RAG-style answers at low temperature, not open-ended chat. Anything that changes the output belongs in the hash (temperature, retrieval context, output schema version), and set `lockTtlMs` above your slowest generation so waiters don't fail open into a duplicate model call mid-load.

## Resumable Generation State

Streamed responses die with the connection: a mobile client drops mid-generation and has to start (and you have to pay) from scratch. Append chunks to a stream as the model produces them, and a reconnecting client replays everything after the last entry ID it saw.

If the generation should also survive the *server* going away (a deploy, a crash, a serverless invocation timing out), reach for the [`queue` primitive](/benni/primitives/queue/) instead. It runs the generation on a worker, gives every job this same resumable stream, and adds the lifecycle this recipe leaves to you: retries, cancellation, and dead-lettering.

```ts
export const generation = stream("generation", { chunk: string() });

// Producer: append chunks as the model streams them.
for await (const delta of textStream) {
  await redis.stream(generation).xadd(generationId, { chunk: delta });
}
await redis.stream(generation).expire(generationId, 60 * 60);

// Reconnecting client: replay everything after the last seen entry ID.
const missed = await redis.stream(generation).xread(generationId, lastSeenEntryId);
```

`xread` returns entries newer than the given ID (use `"0"` for a full replay). On a long-lived server, a [session](/benni/advanced/sessions/)'s blocking `xread` with `{ timeoutSeconds }` turns the replay loop into a live tail. Writing one entry per token is chatty, so batch a few chunks per `xadd` under load. See [Streams](/benni/data-structures/streams/) for ranges, trimming, and consumer groups.

Use a stream, not [Pub/Sub](/benni/data-structures/pubsub/), for anything a client might reconnect to: Pub/Sub is fire-and-forget, so chunks published while the client was offline are simply gone. Pub/Sub earns its place when you need *live fan-out* to several watchers of the same generation (a shared session, an ops dashboard), and each one only cares about what arrives from now on. That subscriber side needs a held connection, so it runs on Node or Bun rather than the edge, and `stream()` makes it a plain loop you can pipe into an SSE response:

```ts
import { channel, json } from "benni/schema";

export const generationFeed = channel("feed:generation", json<{ chunk: string }>());

// Long-lived server: one watcher per connected viewer.
const controller = new AbortController();
for await (const { chunk } of redis.pubsub
  .channel(generationFeed)
  .stream({ signal: controller.signal })) {
  writeSse(chunk);
}
```

Publishing is one stateless `PUBLISH`, so the producer side of that fan-out still runs anywhere: an edge handler can publish chunks that long-lived servers subscribe to.

## Deduplicate In-Flight Generations

Retries and double-clicks are the other way to pay twice for one answer. Wrap the generation in the [`lock` primitive](/benni/primitives/lock/) keyed by a client-supplied request ID: the first request generates, and any duplicate that arrives while it is running gets a `409` instead of a second model call.

For a queued generation, prefer the [`queue` primitive](/benni/primitives/queue/)'s `idempotencyKey`, which returns the *original job* rather than a `409`, so the duplicate request can watch or await the answer the first one is already producing.

```ts
import { lock, LockNotAcquiredError } from "benni/primitives";

const generating = lock(client, { ttlMs: 60_000, prefix: "generating" });

try {
  return await generating.run(requestId, async () => {
    const { text } = await generateText({ model: openai("gpt-4o-mini"), prompt });
    return Response.json({ text });
  });
} catch (error) {
  if (error instanceof LockNotAcquiredError) {
    return new Response("Generation already in flight", { status: 409 });
  }
  throw error;
}
```

Set `ttlMs` above your worst-case generation time (or `extend()` the handle for long jobs); if the holder crashes, the TTL frees the lock instead of deadlocking the request ID. The lock deduplicates *in-flight* work; pair it with the response cache above so a retry that lands *after* completion gets the finished answer instead of a 409.

## Works Everywhere

Everything on this page (streams, counters, `ratelimit`, `cache`, and `lock`) runs on the same typed API across Node, Bun, and Deno, and over [`benni/upstash`](/benni/runtime/edge/) on Cloudflare Workers, Vercel Edge, and Deno Deploy. The one exception is blocking stream reads, which need a persistent connection; the polling `xread` shown here works on every adapter.

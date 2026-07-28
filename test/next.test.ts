import { afterEach, describe, expect, it, vi } from "vitest";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { cacheHandler, rateLimit } from "../src/next/index.js";
import { fakeClient } from "./fake-client.js";

describe("cacheHandler", () => {
  it("stores an entry with EX from revalidate and one SADD per tag", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK", 1, 1]);
    const Handler = cacheHandler({ client });
    const handler = new Handler();

    await handler.set(
      "/blog",
      { kind: "PAGE" },
      { revalidate: 60, tags: ["posts", "layout"] }
    );

    expect(commands).toHaveLength(3);
    const [set, ...sadds] = commands;
    expect(set?.slice(0, 2)).toEqual(["SET", "{next-cache}:entry:/blog"]);
    expect(set?.slice(3)).toEqual(["EX", 60]);
    const entry = JSON.parse(set?.[2] as string);
    expect(entry.value).toEqual({ kind: "PAGE" });
    expect(entry.tags).toEqual(["posts", "layout"]);
    expect(typeof entry.lastModified).toBe("number");
    expect(sadds).toEqual([
      ["SADD", "{next-cache}:tag:posts", "/blog"],
      ["SADD", "{next-cache}:tag:layout", "/blog"]
    ]);
  });

  it("stores without EX when revalidate is false", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK"]);
    const Handler = cacheHandler({ client });

    await new Handler().set("/page", { kind: "PAGE" }, { revalidate: false });

    expect(commands).toHaveLength(1);
    expect(commands[0]).toHaveLength(3); // SET key payload — no EX
    expect(commands[0]?.[0]).toBe("SET");
  });

  it("applies defaultTtlSeconds when revalidate is absent", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK"]);
    const Handler = cacheHandler({ client, defaultTtlSeconds: 300 });

    await new Handler().set("/page", { kind: "PAGE" });

    expect(commands[0]?.slice(3)).toEqual(["EX", 300]);
  });

  it("returns the decoded entry on a hit", async () => {
    const commands: RedisCommand[] = [];
    const stored = {
      value: { kind: "FETCH", data: [1, 2] },
      lastModified: 1_700_000_000_000,
      tags: ["posts"]
    };
    const client = fakeClient(commands, [JSON.stringify(stored)]);
    const Handler = cacheHandler({ client });

    const entry = await new Handler().get("/blog");

    expect(commands).toEqual([["GET", "{next-cache}:entry:/blog"]]);
    expect(entry).toEqual(stored);
  });

  it("returns null on a miss", async () => {
    const client = fakeClient([], [null]);
    const Handler = cacheHandler({ client });

    expect(await new Handler().get("/missing")).toBeNull();
  });

  it("fails open on corrupt JSON", async () => {
    const client = fakeClient([], ["{not json"]);
    const Handler = cacheHandler({ client });

    expect(await new Handler().get("/corrupt")).toBeNull();
  });

  it("revalidateTag reads members and deletes entries plus tag sets", async () => {
    const commands: RedisCommand[] = [];
    // pipeline: SMEMBERS -> members; send: DEL -> deleted count
    const client = fakeClient(commands, [["/blog", "/blog/post-1"], 3]);
    const Handler = cacheHandler({ client });

    await new Handler().revalidateTag("posts");

    expect(commands).toEqual([
      ["SMEMBERS", "{next-cache}:tag:posts"],
      [
        "DEL",
        "{next-cache}:entry:/blog",
        "{next-cache}:entry:/blog/post-1",
        "{next-cache}:tag:posts"
      ]
    ]);
  });

  it("revalidateTag accepts an array of tags", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["/a"], ["/b"], 4]);
    const Handler = cacheHandler({ client });

    await new Handler().revalidateTag(["one", "two"]);

    expect(commands).toEqual([
      ["SMEMBERS", "{next-cache}:tag:one"],
      ["SMEMBERS", "{next-cache}:tag:two"],
      [
        "DEL",
        "{next-cache}:entry:/a",
        "{next-cache}:entry:/b",
        "{next-cache}:tag:one",
        "{next-cache}:tag:two"
      ]
    ]);
  });

  it("awaits a lazy client factory exactly once across operations", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK", null]);
    let calls = 0;
    const factory = async (): Promise<RedisClient> => {
      calls += 1;
      return client;
    };
    const Handler = cacheHandler({ client: factory });
    const handler = new Handler();

    await handler.set("/page", { kind: "PAGE" }, { revalidate: false });
    await handler.get("/page");

    expect(calls).toBe(1);
    expect(commands.map((command) => command[0])).toEqual(["SET", "GET"]);
  });

  it("resetRequestCache is a no-op", () => {
    const Handler = cacheHandler({ client: fakeClient([], []) });
    expect(new Handler().resetRequestCache()).toBeUndefined();
  });
});

describe("rateLimit", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("resolves null when the request is allowed", async () => {
    const commands: RedisCommand[] = [];
    // SCRIPT LOAD -> sha, EVALSHA -> [allowed, remaining, reset]
    const client = fakeClient(commands, ["sha1", [1, 9, Date.now() + 60_000]]);
    const limiter = rateLimit({ client, limit: 10, windowMs: 60_000 });

    const request = new Request("https://example.com/api", {
      headers: { "x-forwarded-for": "203.0.113.7, 10.0.0.1" }
    });

    expect(await limiter(request)).toBeNull();
    // Default identify: first x-forwarded-for hop under the next prefix.
    const evalsha = commands.at(-1);
    expect(evalsha?.slice(0, 4)).toEqual([
      "EVALSHA",
      "sha1",
      1,
      "next-ratelimit:203.0.113.7"
    ]);
  });

  it("returns a 429 with Retry-After and X-RateLimit headers when denied", async () => {
    const resetMs = 1_700_000_030_000;
    // No fake timers needed any more: Retry-After comes from the server's own
    // duration, so the local clock is irrelevant to it.
    const client = fakeClient([], ["sha1", [0, 0, resetMs, 30_000]]);
    const limiter = rateLimit({ client, limit: 5, windowMs: 60_000 });

    const response = await limiter(new Request("https://example.com/api"));

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(429);
    expect(response?.headers.get("Retry-After")).toBe("30");
    expect(response?.headers.get("X-RateLimit-Limit")).toBe("5");
    expect(response?.headers.get("X-RateLimit-Remaining")).toBe("0");
    expect(response?.headers.get("X-RateLimit-Reset")).toBe(
      String(Math.ceil(resetMs / 1000))
    );
  });

  it('falls back to "anonymous" without x-forwarded-for', async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["sha1", [1, 4, Date.now() + 1_000]]);
    const limiter = rateLimit({ client, limit: 5, windowMs: 1_000 });

    await limiter(new Request("https://example.com"));

    expect(commands.at(-1)?.[3]).toBe("next-ratelimit:anonymous");
  });

  it("supports a custom identify", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["sha1", [1, 4, Date.now() + 1_000]]);
    const limiter = rateLimit({
      client,
      limit: 5,
      windowMs: 1_000,
      identify: (request) => request.headers.get("x-api-key") ?? "anonymous"
    });

    await limiter(
      new Request("https://example.com", {
        headers: { "x-api-key": "key-1" }
      })
    );

    expect(commands.at(-1)?.[3]).toBe("next-ratelimit:key-1");
  });

  it(".check works without a Request (Server Actions)", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [
      "sha1",
      [0, 0, 1_700_000_099_000, 900]
    ]);
    const limiter = rateLimit({ client, limit: 3, windowMs: 10_000 });

    const result = await limiter.check("user:42");

    expect(result).toEqual({
      success: false,
      limit: 3,
      remaining: 0,
      resetMs: 1_700_000_099_000,
      retryAfterMs: 900
    });
    expect(commands.at(-1)?.[3]).toBe("next-ratelimit:user:42");
  });

  it("awaits a lazy client factory exactly once across checks", async () => {
    const client = fakeClient(
      [],
      ["sha1", [1, 1, 1], [1, 0, 2]] // one SCRIPT LOAD, two EVALSHAs
    );
    let calls = 0;
    const limiter = rateLimit({
      client: async () => {
        calls += 1;
        return client;
      },
      limit: 2,
      windowMs: 1_000
    });

    await limiter.check("a");
    await limiter.check("b");

    expect(calls).toBe(1);
  });
});

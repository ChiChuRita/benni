import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi
} from "vitest";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { benni } from "../src/index.js";
import { cacheHandler, rateLimitMiddleware } from "../src/next/index.js";
import { node } from "../src/node/index.js";
import { ratelimit } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";
import {
  appPageSet,
  appRouteSet,
  fetchGet,
  fetchSet
} from "./fixtures/next-16-payloads.js";

/** Runs one set() against a fake client; returns what it sent. */
async function recordSet(
  key: string,
  data: unknown,
  ctx?: Parameters<InstanceType<ReturnType<typeof cacheHandler>>["set"]>[2],
  options: { defaultTtlSeconds?: number } = {}
) {
  const commands: RedisCommand[] = [];
  const Handler = cacheHandler({
    client: fakeClient(commands, []),
    ...options
  });
  await new Handler().set(key, data, ctx);
  const [set, ...tagCalls] = commands;
  return { set, tagCalls, payload: set?.[2] as string };
}

/** Feeds a stored payload back through get() and returns the entry. */
async function readBack(payload: string) {
  const Handler = cacheHandler({ client: fakeClient([], [payload]) });
  return new Handler().get("any");
}

/** Buffers compared as bytes, so a Uint8Array that merely looks right fails. */
function expectBuffer(actual: unknown, expected: Uint8Array) {
  expect(Buffer.isBuffer(actual)).toBe(true);
  expect(Buffer.compare(actual as Buffer, expected)).toBe(0);
}

describe("cacheHandler: values Next.js 16 really sends", () => {
  it("round-trips an APP_PAGE with rscData and segmentData byte for byte", async () => {
    const { payload } = await recordSet(
      appPageSet.key,
      appPageSet.data,
      appPageSet.ctx
    );
    const entry = await readBack(payload);
    const value = entry?.value as typeof appPageSet.data;

    expect(value.kind).toBe("APP_PAGE");
    expect(value.html).toBe(appPageSet.data.html);
    expect(value.headers).toEqual(appPageSet.data.headers);
    expectBuffer(value.rscData, appPageSet.data.rscData);
    // Plain JSON turned this Map into {} and Next.js lost every prefetch
    // segment; it has to come back a Map of Buffers.
    expect(value.segmentData).toBeInstanceOf(Map);
    expect([...value.segmentData.keys()]).toEqual([
      ...appPageSet.data.segmentData.keys()
    ]);
    for (const [segment, bytes] of appPageSet.data.segmentData) {
      expectBuffer(value.segmentData.get(segment), bytes);
    }
  });

  it("round-trips an APP_ROUTE body that is not valid UTF-8", async () => {
    const { payload } = await recordSet(
      appRouteSet.key,
      appRouteSet.data,
      appRouteSet.ctx
    );
    const value = (await readBack(payload))?.value as typeof appRouteSet.data;

    expect(value.status).toBe(200);
    expect(value.headers).toEqual(appRouteSet.data.headers);
    expectBuffer(value.body, appRouteSet.data.body);
  });

  it("round-trips a FETCH entry unchanged", async () => {
    const { payload } = await recordSet(
      fetchSet.key,
      fetchSet.data,
      fetchSet.ctx
    );

    expect((await readBack(payload))?.value).toEqual(fetchSet.data);
  });

  it("gives a page its cacheControl.expire as TTL, so Next.js can serve it stale", async () => {
    // Next.js only re-renders before responding once `expire` has passed;
    // between `revalidate` and `expire` it serves the entry stale and
    // regenerates in the background. Expiring at `revalidate` would turn
    // every revalidation into a blocking render.
    const { set } = await recordSet(
      appPageSet.key,
      appPageSet.data,
      appPageSet.ctx
    );

    expect(set?.slice(0, 2)).toEqual(["SET", "{next-cache}:entry:/static"]);
    expect(set?.slice(3)).toEqual(["EX", 31_536_000]);
  });

  it("indexes a page under the tags in its x-next-cache-tags header", async () => {
    // A page's set() context carries no tags at all; without the header the
    // page was unreachable by revalidateTag, and by revalidatePath, whose
    // implicit `_N_T_` tag lives only there.
    const { tagCalls } = await recordSet(
      appPageSet.key,
      appPageSet.data,
      appPageSet.ctx
    );

    expect(tagCalls.map((command) => command[0])).toEqual(
      Array(5).fill("EVAL")
    );
    expect(tagCalls.map((command) => command.slice(3))).toEqual(
      [
        "_N_T_/layout",
        "_N_T_/static/layout",
        "_N_T_/static/page",
        "_N_T_/static",
        "posts"
      ].map((tag) => [`{next-cache}:tag:${tag}`, "/static", 31_536_000])
    );
  });

  it("gives a fetch entry its data.revalidate as TTL and indexes ctx.tags", async () => {
    const { set, tagCalls } = await recordSet(
      fetchSet.key,
      fetchSet.data,
      fetchSet.ctx
    );

    expect(set?.slice(3)).toEqual(["EX", 600]);
    expect(tagCalls.map((command) => command.slice(3))).toEqual([
      ["{next-cache}:tag:posts", fetchSet.key, 600]
    ]);
  });

  it("caps a fetch entry at one year, the longest a revalidation is remembered", async () => {
    const { set } = await recordSet(
      "k",
      { ...fetchSet.data, revalidate: 10 * 31_536_000 },
      fetchSet.ctx
    );

    expect(set?.slice(3)).toEqual(["EX", 31_536_000]);
  });
});

describe("cacheHandler: Next.js 15 shapes", () => {
  it("reads ctx.revalidate, which 15.0-15.2 send instead of cacheControl", async () => {
    const { set } = await recordSet(
      "/blog",
      { kind: "APP_PAGE", html: "<p/>", headers: {} },
      { revalidate: 60, isFallback: false } as never
    );

    expect(set?.slice(3)).toEqual(["EX", 60]);
  });

  it("falls back to cacheControl.revalidate when there is no expire (15.3-15.5)", async () => {
    // Captured from Next.js 15.5.26 for the same /static page that 16 sends
    // `expire: 31536000` for: 15.x keeps expire in its prerender manifest
    // and never hands it to the cache handler.
    const { set } = await recordSet("/static", appPageSet.data, {
      cacheControl: { revalidate: 600, expire: undefined },
      isRoutePPREnabled: false,
      isFallback: false
    } as never);

    expect(set?.slice(3)).toEqual(["EX", 600]);
  });

  it("round-trips 15.0's segmentData, a record of strings", async () => {
    const value = {
      kind: "APP_PAGE",
      html: "<p/>",
      rscData: Buffer.from([0, 255, 128]),
      segmentData: { "/_tree": "0:tree", "/blog/__PAGE__": "1:page" }
    };
    const { payload } = await recordSet("/blog", value, { revalidate: 60 });

    expect((await readBack(payload))?.value).toEqual(value);
  });
});

describe("cacheHandler: lifetimes", () => {
  it("stores without EX when Next.js gives no lifetime", async () => {
    const { set } = await recordSet(
      "/page",
      { kind: "APP_PAGE", html: "" },
      { cacheControl: { revalidate: false, expire: undefined } }
    );

    expect(set).toHaveLength(3); // SET key payload, no EX
  });

  it("applies defaultTtlSeconds only when Next.js gives no lifetime", async () => {
    const forever = await recordSet(
      "/page",
      { kind: "APP_PAGE", html: "" },
      { cacheControl: { revalidate: false, expire: undefined } },
      { defaultTtlSeconds: 300 }
    );
    const timed = await recordSet("/page", appPageSet.data, appPageSet.ctx, {
      defaultTtlSeconds: 300
    });

    expect(forever.set?.slice(3)).toEqual(["EX", 300]);
    expect(timed.set?.slice(3)).toEqual(["EX", 31_536_000]);
  });

  it("makes a tag set permanent when its entry never expires", async () => {
    // GT never clears an existing expiry, so a permanent entry needs an
    // explicit PERSIST: otherwise a tag set that earlier held a short-lived
    // entry would expire out from under this one, and the page could never be
    // revalidated by tag again. ttl 0 is the script's signal to PERSIST.
    const { tagCalls } = await recordSet(
      "/page",
      { kind: "APP_PAGE", html: "", headers: { "x-next-cache-tags": "a" } },
      { cacheControl: { revalidate: false, expire: undefined } }
    );

    expect(tagCalls[0]?.slice(3)).toEqual(["{next-cache}:tag:a", "/page", 0]);
  });
});

describe("cacheHandler: encoding", () => {
  it("keeps caller data that looks like its own tags intact", async () => {
    // Pages Router pageData is whatever getStaticProps returned, so it can
    // hold any shape, including the encoding's own markers.
    const pageData = {
      a: { $b: "AAEC" },
      m: { $m: [["k", 1]] },
      o: { $o: { $b: "x" } },
      nested: [{ $b: "not bytes" }, { $b: "x", other: 1 }],
      own: JSON.parse('{"__proto__": {"polluted": true}}') as object
    };
    const { payload } = await recordSet(
      "/p",
      { kind: "PAGES", html: "", pageData },
      { revalidate: 60 }
    );
    const value = (await readBack(payload))?.value as { pageData: unknown };

    expect(value.pageData).toEqual(pageData);
    const own = (value.pageData as { own: object }).own;
    expect(Object.hasOwn(own, "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(own)).toBe(Object.prototype);
  });

  it("drops undefined members the way JSON does, without miscounting keys", async () => {
    // { $b, other: undefined } lands in Redis as { $b }; it must be escaped
    // on that basis or it would come back as bytes.
    const value = {
      kind: "PAGES",
      pageData: { x: { $b: "AAEC", other: undefined } }
    };
    const { payload } = await recordSet("/p", value, { revalidate: 60 });

    expect((await readBack(payload))?.value).toEqual({
      kind: "PAGES",
      pageData: { x: { $b: "AAEC" } }
    });
  });

  it("stores and returns a null value (a notFound route)", async () => {
    const { payload } = await recordSet("/gone", null, {
      cacheControl: { revalidate: 60, expire: 600 }
    });

    const entry = await readBack(payload);
    expect(entry).not.toBeNull();
    expect(entry?.value).toBeNull();
  });

  it("skips caching a value JSON cannot hold", async () => {
    const { set } = await recordSet("/p", { kind: "FETCH", big: 1n });
    expect(set).toBeUndefined();
  });

  it("treats a 0.1.0 entry as a miss rather than a corrupt hit", async () => {
    // 0.1.0 stored plain JSON: rscData as { type: "Buffer", data } and
    // segmentData as {}. Handing that back would break the page for good.
    const legacy = JSON.stringify({
      value: { kind: "APP_PAGE", rscData: { type: "Buffer", data: [1] } },
      lastModified: 1,
      tags: []
    });

    expect(await readBack(legacy)).toBeNull();
  });

  it("returns null on a miss and on corrupt JSON", async () => {
    expect(await readBack(null as never)).toBeNull();
    expect(await readBack("{not json")).toBeNull();
  });
});

describe("cacheHandler: revalidation", () => {
  it("checks a fetch's own and implicit tags in the same round trip as the GET", async () => {
    const { payload } = await recordSet(
      fetchSet.key,
      fetchSet.data,
      fetchSet.ctx
    );
    const commands: RedisCommand[] = [];
    const Handler = cacheHandler({
      client: fakeClient(commands, [payload, [null, null, null, null, null]])
    });

    const entry = await new Handler().get(fetchGet.key, fetchGet.ctx);

    expect(entry?.value).toEqual(fetchSet.data);
    expect(commands).toEqual([
      ["GET", `{next-cache}:entry:${fetchSet.key}`],
      [
        "MGET",
        "{next-cache}:revalidated:posts",
        "{next-cache}:revalidated:_N_T_/layout",
        "{next-cache}:revalidated:_N_T_/static/layout",
        "{next-cache}:revalidated:_N_T_/static/page",
        "{next-cache}:revalidated:_N_T_/static"
      ]
    ]);
  });

  it("misses a fetch whose route was revalidated after it was written", async () => {
    // revalidatePath("/static") is revalidateTag("_N_T_/static"). A fetch
    // entry is only ever indexed under its explicit tags, so the implicit
    // one reaches it through this check, on every instance.
    const { payload } = await recordSet(
      fetchSet.key,
      fetchSet.data,
      fetchSet.ctx
    );
    const { lastModified } = JSON.parse(payload) as { lastModified: number };
    const at = (offset: number) => String(lastModified + offset);
    const get = (markers: (string | null)[]) =>
      new (cacheHandler({ client: fakeClient([], [payload, markers]) }))().get(
        fetchGet.key,
        fetchGet.ctx
      );

    expect(await get([null, null, null, null, at(5)])).toBeNull();
    expect(await get([at(0), null, null, null, null])).toBeNull();
    expect(await get([at(-5), null, null, null, null])).not.toBeNull();
  });

  it("revalidateTag stamps each tag, then deletes its entries and SREMs only the members it saw", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["/blog", "/blog/post-1"], "OK"]);

    await new (cacheHandler({ client }))().revalidateTag("posts");

    expect(commands[0]).toEqual(["SMEMBERS", "{next-cache}:tag:posts"]);
    expect(commands[1]?.slice(0, 2)).toEqual([
      "SET",
      "{next-cache}:revalidated:posts"
    ]);
    expect(commands[1]?.slice(3)).toEqual(["EX", 31_536_000]);
    // SREM of the observed members, never DEL of the tag set: a set() racing
    // between the SMEMBERS and this point would otherwise lose its tag
    // membership while its entry survived, leaving a page that can never be
    // revalidated again.
    expect(commands.slice(2)).toEqual([
      ["DEL", "{next-cache}:entry:/blog", "{next-cache}:entry:/blog/post-1"],
      ["SREM", "{next-cache}:tag:posts", "/blog", "/blog/post-1"]
    ]);
  });

  it("revalidateTag takes two round trips however many tags and keys", async () => {
    const members = Array.from({ length: 1_200 }, (_, index) => `/p/${index}`);
    const pipelines: (readonly RedisCommand[])[] = [];
    const client: RedisClient = {
      async send() {
        throw new Error("revalidateTag should only pipeline");
      },
      async pipeline(commands) {
        pipelines.push(commands);
        return pipelines.length === 1 ? [members, ["/x"], "OK", "OK"] : [];
      },
      async close() {}
    };

    await new (cacheHandler({ client }))().revalidateTag(["one", "two"], {
      expire: 3600
    });

    expect(pipelines).toHaveLength(2);
    // Chunked so no single command blocks the server; still one flight.
    expect(pipelines[1]?.map((command) => command[0])).toEqual([
      "DEL",
      "DEL",
      "DEL",
      "SREM",
      "SREM",
      "SREM",
      "SREM"
    ]);
    expect(pipelines[1]?.[0]).toHaveLength(501);
    expect(pipelines[1]?.[2]).toHaveLength(202);
  });

  it("revalidateTag keeps a member added while it was running", async () => {
    // The concrete race: /b is SADD-ed to the tag after SMEMBERS returned
    // ["/a"]. DEL-ing the tag set dropped /b's membership while /b itself
    // stayed cached, so no later revalidateTag could ever reach it.
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["/a"], "OK"]);

    await new (cacheHandler({ client }))().revalidateTag("posts");

    expect(commands.find((command) => command[0] === "SREM")).toEqual([
      "SREM",
      "{next-cache}:tag:posts",
      "/a"
    ]);
    expect(
      commands.some(
        (command) =>
          command[0] === "DEL" && command.includes("{next-cache}:tag:posts")
      )
    ).toBe(false);
  });

  it("revalidateTag of an empty tag set only stamps it", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [[], "OK"]);

    await new (cacheHandler({ client }))().revalidateTag(["gone"]);
    await new (cacheHandler({ client }))().revalidateTag([]);

    expect(commands.map((command) => command[0])).toEqual(["SMEMBERS", "SET"]);
  });

  it("resetRequestCache is a no-op", () => {
    const Handler = cacheHandler({ client: fakeClient([], []) });
    expect(new Handler().resetRequestCache()).toBeUndefined();
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("cacheHandler (live)", () => {
  let client: RedisClient;
  const prefix = `next-live:${Date.now()}:${Math.random().toString(36).slice(2)}`;
  const key = (suffix: string) => `{${prefix}}:${suffix}`;

  beforeAll(async () => {
    client = node({ url: redisUrl });
  });
  afterAll(async () => {
    const keys = (await client.send(["KEYS", `{${prefix}}:*`])) as string[];
    if (keys.length > 0) await client.send(["DEL", ...keys]);
    await client.close();
  });

  it("stores real payloads with Next's TTLs and reads them back exactly", async () => {
    const handler = new (cacheHandler({ client, prefix }))();

    await handler.set(appPageSet.key, appPageSet.data, appPageSet.ctx);
    await handler.set(appRouteSet.key, appRouteSet.data, appRouteSet.ctx);
    await handler.set(fetchSet.key, fetchSet.data, fetchSet.ctx);

    const ttl = async (suffix: string) =>
      Number(await client.send(["TTL", key(suffix)]));
    expect(await ttl("entry:/static")).toBeGreaterThan(31_536_000 - 5);
    expect(await ttl("entry:/api/binary")).toBeGreaterThan(31_536_000 - 5);
    expect(await ttl(`entry:${fetchSet.key}`)).toBeGreaterThan(595);
    expect(await ttl(`entry:${fetchSet.key}`)).toBeLessThanOrEqual(600);
    expect(await ttl("tag:_N_T_/static")).toBeGreaterThan(0);

    const route = (await handler.get(appRouteSet.key))?.value as {
      body: Buffer;
    };
    expectBuffer(route.body, appRouteSet.data.body);
    const page = (await handler.get(appPageSet.key))?.value as {
      segmentData: Map<string, Buffer>;
    };
    expect(page.segmentData.size).toBe(appPageSet.data.segmentData.size);
    expect((await handler.get(fetchGet.key, fetchGet.ctx))?.value).toEqual(
      fetchSet.data
    );
  });

  it("revalidatePath's implicit tag drops the page and misses its fetch", async () => {
    const handler = new (cacheHandler({ client, prefix }))();
    await handler.set(appPageSet.key, appPageSet.data, appPageSet.ctx);
    await handler.set(fetchSet.key, fetchSet.data, fetchSet.ctx);

    await handler.revalidateTag("_N_T_/static");

    expect(await handler.get(appPageSet.key)).toBeNull();
    // The fetch entry is still stored (it is not indexed under the implicit
    // tag), but a lookup from the revalidated route no longer returns it.
    expect(await client.send(["EXISTS", key(`entry:${fetchSet.key}`)])).toBe(1);
    expect(await handler.get(fetchGet.key, fetchGet.ctx)).toBeNull();

    // A write after the revalidation is fresh again.
    await new Promise((resolve) => setTimeout(resolve, 2));
    await handler.set(fetchSet.key, fetchSet.data, fetchSet.ctx);
    expect(await handler.get(fetchGet.key, fetchGet.ctx)).not.toBeNull();
  });

  it("revalidateTag drops both the page and the fetch that share an explicit tag", async () => {
    const handler = new (cacheHandler({ client, prefix }))();
    await handler.set(appPageSet.key, appPageSet.data, appPageSet.ctx);
    await handler.set(fetchSet.key, fetchSet.data, fetchSet.ctx);

    await handler.revalidateTag("posts");

    expect(await client.send(["EXISTS", key("entry:/static")])).toBe(0);
    expect(await client.send(["EXISTS", key(`entry:${fetchSet.key}`)])).toBe(0);
    expect(await client.send(["EXISTS", key("tag:posts")])).toBe(0);
  });
});

describe("rateLimit", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("refuses the 0.1 { client, limit, windowMs } options, naming the fix", () => {
    expect(() =>
      rateLimitMiddleware({
        client: fakeClient([], []),
        limit: 5,
        windowMs: 60_000,
        identify: () => "tester"
      } as never)
    ).toThrow(/pass `limiter: redis\.query\.apiLimit`/);
  });

  it("resolves null when the request is allowed", async () => {
    const commands: RedisCommand[] = [];
    // SCRIPT LOAD -> sha, EVALSHA -> [allowed, remaining, reset]
    const client = fakeClient(commands, ["sha1", [1, 9, Date.now() + 60_000]]);
    const limiter = rateLimitMiddleware({
      limiter: benni({ client: client }).store(
        ratelimit("next-ratelimit", { limit: 10, windowMs: 60_000 })
      ),
      identify: (request) =>
        request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ??
        "anonymous"
    });

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
    const limiter = rateLimitMiddleware({
      limiter: benni({ client: client }).store(
        ratelimit("next-ratelimit", { limit: 5, windowMs: 60_000 })
      ),
      identify: () => "tester"
    });

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

  it("lets the caller's identify fall back when its header is absent", async () => {
    // identify is required: there is no header a limiter can trust without
    // knowing the deployment, so the fallback is the caller's to choose.
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["sha1", [1, 4, Date.now() + 1_000]]);
    const limiter = rateLimitMiddleware({
      limiter: benni({ client: client }).store(
        ratelimit("next-ratelimit", { limit: 5, windowMs: 1_000 })
      ),
      identify: (request) =>
        request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ??
        "anonymous"
    });

    await limiter(new Request("https://example.com"));

    expect(commands.at(-1)?.[3]).toBe("next-ratelimit:anonymous");
  });

  it("supports a custom identify", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["sha1", [1, 4, Date.now() + 1_000]]);
    const limiter = rateLimitMiddleware({
      limiter: benni({ client: client }).store(
        ratelimit("next-ratelimit", { limit: 5, windowMs: 1_000 })
      ),
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
    const limiter = rateLimitMiddleware({
      limiter: benni({ client: client }).store(
        ratelimit("next-ratelimit", { limit: 3, windowMs: 10_000 })
      ),
      identify: () => "tester"
    });

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
});

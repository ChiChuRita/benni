import { Hono } from "hono";
import { describe, expect, it } from "vitest";
import type { RedisCommand, RedisReply } from "../src/core/types.js";
import { cache, getSession, ratelimit, session } from "../src/hono/index.js";
import { fakeClient } from "./fake-client.js";

describe("hono ratelimit", () => {
  it("allows a request under the limit and sets the X-RateLimit headers", async () => {
    const commands: RedisCommand[] = [];
    // SCRIPT LOAD -> sha, EVALSHA -> [allowed, remaining, resetMs]
    const client = fakeClient(commands, ["sha1", [1, 4, 1_800_000_060_000]]);
    const app = new Hono();
    app.use("*", ratelimit({ client, limit: 5, windowMs: 60_000 }));
    app.get("/", (c) => c.text("hello"));

    const res = await app.request("/");
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("hello");
    expect(res.headers.get("X-RateLimit-Limit")).toBe("5");
    expect(res.headers.get("X-RateLimit-Remaining")).toBe("4");
    expect(res.headers.get("X-RateLimit-Reset")).toBe("1800000060");
  });

  it("denies with a JSON 429 and Retry-After once the limit is hit", async () => {
    const resetMs = Date.now() + 30_000;
    // The 4th element is the server-derived retry delay; the middleware uses
    // it directly rather than differencing resetMs against its own clock.
    const client = fakeClient([], ["sha1", [0, 0, resetMs, 30_000]]);
    const app = new Hono();
    app.use("*", ratelimit({ client, limit: 5, windowMs: 60_000 }));
    app.get("/", (c) => c.text("hello"));

    const res = await app.request("/");
    expect(res.status).toBe(429);
    expect(await res.json()).toEqual({ error: "rate limit exceeded" });
    expect(res.headers.get("Retry-After")).toBe("30");
    expect(res.headers.get("X-RateLimit-Limit")).toBeNull();
  });

  it("keys on the first x-forwarded-for hop by default", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["sha1", [1, 0, 1_800_000_000_000]]);
    const app = new Hono();
    app.use("*", ratelimit({ client, limit: 1, windowMs: 1_000 }));
    app.get("/", (c) => c.text("ok"));

    await app.request("/", {
      headers: { "x-forwarded-for": "1.2.3.4, 10.0.0.1" }
    });
    const evalsha = commands.at(-1);
    expect(evalsha?.[0]).toBe("EVALSHA");
    expect(evalsha?.[3]).toBe("ratelimit:1.2.3.4");
  });
});

describe("hono cache", () => {
  it("stores on a miss, then serves the stored body with a hit header", async () => {
    const commands: RedisCommand[] = [];
    const replies: RedisReply[] = [null, "OK"]; // GET miss, SET OK
    const client = fakeClient(commands, replies);
    const app = new Hono();
    let handlerCalls = 0;
    app.get("/data", cache({ client, ttlMs: 30_000 }), (c) => {
      handlerCalls++;
      return c.json({ n: 1 });
    });

    const miss = await app.request("/data?x=1");
    expect(miss.status).toBe(200);
    expect(await miss.json()).toEqual({ n: 1 });
    expect(miss.headers.get("X-Beni-Cache")).toBeNull();
    expect(handlerCalls).toBe(1);

    const set = commands.at(-1);
    expect(set?.[0]).toBe("SET");
    expect(set?.[1]).toBe("hono-cache:GET:/data?x=1");
    expect(set?.slice(3)).toEqual(["PX", 30_000]);
    const stored = JSON.parse(set?.[2] as string);
    expect(stored.status).toBe(200);
    expect(stored.body).toBe('{"n":1}');
    expect(stored.headers["content-type"]).toContain("application/json");

    // Second request: GET returns the stored entry.
    replies.push(set?.[2] as string);
    const hit = await app.request("/data?x=1");
    expect(hit.status).toBe(200);
    expect(await hit.json()).toEqual({ n: 1 });
    expect(hit.headers.get("X-Beni-Cache")).toBe("hit");
    expect(hit.headers.get("content-type")).toContain("application/json");
    expect(handlerCalls).toBe(1); // served from Redis, handler untouched
  });

  it("passes non-GET requests straight through", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, []);
    const app = new Hono();
    app.post("/data", cache({ client, ttlMs: 30_000 }), (c) =>
      c.text("created", 201)
    );

    const res = await app.request("/data", { method: "POST" });
    expect(res.status).toBe(201);
    expect(commands).toEqual([]);
  });

  it("does not cache responses that set cookies", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null]); // GET miss only
    const app = new Hono();
    app.get("/login", cache({ client, ttlMs: 30_000 }), (c) => {
      c.header("Set-Cookie", "sid=abc");
      return c.text("ok");
    });

    const res = await app.request("/login");
    expect(res.status).toBe(200);
    expect(commands.map((command) => command[0])).toEqual(["GET"]);
  });

  it("fails open when Redis errors — the request still succeeds", async () => {
    // An empty reply queue makes every send() throw.
    const client = fakeClient([], []);
    const app = new Hono();
    app.get("/data", cache({ client, ttlMs: 30_000 }), (c) => c.text("ok"));

    const res = await app.request("/data");
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("ok");
  });

  it("folds vary headers into the cache key", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null, "OK"]);
    const app = new Hono();
    app.get(
      "/greet",
      cache({ client, ttlMs: 1_000, vary: ["Accept-Language"] }),
      (c) => c.text("hi")
    );

    await app.request("/greet", { headers: { "Accept-Language": "de" } });
    // Length-prefixed so a value containing the separator cannot collide.
    expect(commands[0]?.[1]).toBe("hono-cache:GET:/greet|accept-language=2:de");
  });

  it("does not let a crafted query string collide with a vary header", async () => {
    // The vary suffix was appended by plain concatenation, so a query string
    // carrying "|a=b" produced the same key as a request whose A header
    // carried it instead: `/g?z=|a=b` with A="" and `/g?z=` with A="b|a="
    // both built "...GET:/g?z=|a=b|a=". An attacker who can pick either side
    // can poison the entry a victim's URL reads.
    const keyFor = async (path: string, a: string) => {
      const commands: RedisCommand[] = [];
      const client = fakeClient(commands, [null, "OK"]);
      const app = new Hono();
      app.get("/g", cache({ client, ttlMs: 1_000, vary: ["A"] }), (c) =>
        c.text("hi")
      );
      await app.request(path, { headers: { A: a } });
      return commands[0]?.[1];
    };

    expect(await keyFor("/g?z=|a=b", "")).not.toBe(
      await keyFor("/g?z=", "b|a=")
    );
  });

  it("does not cache a response the handler derived from the session", async () => {
    // The set-cookie guard misses the common case entirely: a returning
    // visitor already has their sid, so session() emits no Set-Cookie, and
    // with session() as the outer middleware its header would land after
    // cache() has already stored the body. Either way one user's
    // authenticated response was cached under a session-independent key and
    // served to everyone else.
    const commands: RedisCommand[] = [];
    // GET session record, GET cache (miss). No SET should follow.
    const client = fakeClient(commands, [
      JSON.stringify({ userId: "u1" }),
      null
    ]);
    const app = new Hono();
    app.use("*", session({ client }));
    app.get("/me", cache({ client, ttlMs: 30_000 }), (c) =>
      c.text(getSession(c).get<string>("userId") ?? "anonymous")
    );

    const res = await app.request("/me", { headers: { Cookie: "sid=abc" } });
    expect(await res.text()).toBe("u1");
    expect(commands.some((command) => command[0] === "SET")).toBe(false);
  });

  it("still caches a response that never touches the session", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null, null, "OK"]);
    const app = new Hono();
    app.use("*", session({ client }));
    app.get("/public", cache({ client, ttlMs: 30_000 }), (c) => c.text("hi"));

    await app.request("/public", { headers: { Cookie: "sid=abc" } });
    expect(commands.some((command) => command[0] === "SET")).toBe(true);
  });

  it("passes an Authorization-bearing request straight through", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, []);
    const app = new Hono();
    app.get("/private", cache({ client, ttlMs: 30_000 }), (c) => c.text("ok"));

    const res = await app.request("/private", {
      headers: { Authorization: "Bearer t" }
    });
    expect(await res.text()).toBe("ok");
    expect(commands).toEqual([]);
  });
});

describe("hono session", () => {
  it("sets a cookie and persists dirty data for a new session", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK"]); // SET
    const app = new Hono();
    app.use("*", session({ client }));
    app.post("/login", (c) => {
      const bag = getSession(c);
      expect(bag.isNew).toBe(true);
      bag.set("userId", "u1");
      return c.text("welcome");
    });

    const res = await app.request("/login", { method: "POST" });
    expect(res.status).toBe(200);

    const set = commands.at(-1);
    expect(set?.[0]).toBe("SET");
    expect(JSON.parse(set?.[2] as string)).toEqual({ userId: "u1" });
    expect(set?.slice(3)).toEqual(["EX", 86_400]);

    const id = String(set?.[1]).replace("hono-session:", "");
    expect(res.headers.get("Set-Cookie")).toBe(
      `sid=${id}; Path=/; SameSite=Lax; HttpOnly`
    );
  });

  it("loads an existing sid and writes nothing when untouched", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ['{"userId":"u1"}']); // GET
    const app = new Hono();
    app.use("*", session({ client }));
    app.get("/me", (c) => {
      const bag = getSession(c);
      return c.text(`${bag.get<string>("userId")}:${bag.isNew}:${bag.id}`);
    });

    const res = await app.request("/me", {
      headers: { Cookie: "sid=abc123" }
    });
    expect(await res.text()).toBe("u1:false:abc123");
    expect(res.headers.get("Set-Cookie")).toBeNull();
    expect(commands).toEqual([["GET", "hono-session:abc123"]]);
  });

  it("touches Redis not at all for an untouched cookieless request", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, []);
    const app = new Hono();
    app.use("*", session({ client }));
    app.get("/", (c) => c.text("ok"));

    const res = await app.request("/");
    expect(res.status).toBe(200);
    expect(res.headers.get("Set-Cookie")).toBeNull();
    expect(commands).toEqual([]);
  });

  it("clear() deletes the stored record", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ['{"userId":"u1"}', 1]); // GET, DEL
    const app = new Hono();
    app.use("*", session({ client }));
    app.post("/logout", (c) => {
      getSession(c).clear();
      return c.text("bye");
    });

    const res = await app.request("/logout", {
      method: "POST",
      headers: { Cookie: "sid=abc123" }
    });
    expect(res.status).toBe(200);
    expect(commands).toEqual([
      ["GET", "hono-session:abc123"],
      ["DEL", "hono-session:abc123"]
    ]);
  });
});

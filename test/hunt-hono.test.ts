import { Hono } from "hono";
import { describe, expect, it } from "vitest";
import type { RedisCommand } from "../src/core/types.js";
import type { Session } from "../src/hono/index.js";
import { cache, getSession, ratelimit, session } from "../src/hono/index.js";
import { fakeClient } from "./fake-client.js";

describe("hono cache session guard", () => {
  it("does not cache a response derived only from the session id", async () => {
    // The touched flag was set by get/set/delete/clear only, so a handler
    // that read the *identity* rather than the contents slipped past the
    // guard and a live sid was stored under a session-independent key and
    // replayed to strangers, who could then replay it as their own cookie.
    const commands: RedisCommand[] = [];
    // GET session record, GET cache (miss). No SET must follow.
    const client = fakeClient(commands, ['{"userId":"u1"}', null]);
    const app = new Hono();
    app.use("*", session({ client }));
    app.get("/whoami", cache({ client, ttlMs: 30_000 }), (c) =>
      c.json({ sid: getSession(c).id })
    );

    const res = await app.request("/whoami", {
      headers: { Cookie: "sid=abc123" }
    });
    expect(await res.json()).toEqual({ sid: "abc123" });
    expect(commands.some((command) => command[0] === "SET")).toBe(false);
  });

  it("does not cache a response derived from isNew read off c.get", async () => {
    // The same hole one layer down: the bag reached straight through
    // c.get("session") never goes past getSession(), so the accessors have
    // to mark the touch themselves.
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null, null]);
    const app = new Hono<{ Variables: { session: Session } }>();
    app.use("*", session({ client }));
    app.get("/greeting", cache({ client, ttlMs: 30_000 }), (c) => {
      const bag = c.get("session");
      return c.text(bag.isNew ? "welcome" : "welcome back");
    });

    const res = await app.request("/greeting", {
      headers: { Cookie: "sid=abc123" }
    });
    expect(await res.text()).toBe("welcome");
    expect(commands.some((command) => command[0] === "SET")).toBe(false);
  });
});

describe("hono session rotation", () => {
  it("regenerate() rotates the id, drops the old record, re-issues the cookie", async () => {
    // Without a rotation primitive a privilege change writes the victim's
    // auth into whatever sid the attacker planted, and Set-Cookie was gated
    // on isNew so nothing could ever re-issue it.
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ['{"visits":1}', 1, "OK"]);
    const app = new Hono();
    app.use("*", session({ client }));
    app.post("/login", (c) => {
      const bag = getSession(c);
      bag.regenerate();
      bag.set("userId", "u1");
      return c.text("welcome");
    });

    const res = await app.request("/login", {
      method: "POST",
      headers: { Cookie: "sid=planted" }
    });
    expect(res.status).toBe(200);
    expect(commands[0]).toEqual(["GET", "hono-session:planted"]);
    expect(commands[1]).toEqual(["DEL", "hono-session:planted"]);

    const set = commands[2];
    expect(set?.[0]).toBe("SET");
    const rotated = String(set?.[1]).replace("hono-session:", "");
    expect(rotated).not.toBe("planted");
    expect(JSON.parse(String(set?.[2]))).toEqual({ visits: 1, userId: "u1" });
    // A record under a brand new id cannot be conditional on already existing.
    expect(set?.slice(3)).toEqual(["EX", 86_400]);
    expect(res.headers.get("Set-Cookie")).toBe(
      `sid=${rotated}; Path=/; SameSite=Lax; HttpOnly`
    );
  });

  it("writes an existing record back with XX so a logout is not undone", async () => {
    // A request already in flight when logout DELs the record used to re-SET
    // its stale snapshot with a fresh full lifetime, re-authenticating the
    // sid the user just logged out of.
    const commands: RedisCommand[] = [];
    // GET the record, then SET returns nil: the key is gone, nothing written.
    const client = fakeClient(commands, ['{"userId":"u1"}', null]);
    const app = new Hono();
    app.use("*", session({ client }));
    app.get("/activity", (c) => {
      getSession(c).set("lastSeen", 1);
      return c.text("ok");
    });

    await app.request("/activity", { headers: { Cookie: "sid=abc123" } });
    expect(commands[1]).toEqual([
      "SET",
      "hono-session:abc123",
      '{"userId":"u1","lastSeen":1}',
      "EX",
      86_400,
      "XX"
    ]);
  });

  it("writes a brand new session unconditionally", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK"]);
    const app = new Hono();
    app.use("*", session({ client }));
    app.post("/login", (c) => {
      getSession(c).set("userId", "u1");
      return c.text("welcome");
    });

    await app.request("/login", { method: "POST" });
    expect(commands[0]?.slice(3)).toEqual(["EX", 86_400]);
  });
});

describe("hono ratelimit headers", () => {
  it("keeps the X-RateLimit headers when the handler returns a raw Response", async () => {
    const client = fakeClient([], ["sha1", [1, 4, 1_800_000_060_000]]);
    const app = new Hono();
    app.use(
      "*",
      ratelimit({ client, limit: 5, windowMs: 60_000, key: () => "tester" })
    );
    app.get("/raw", () => new Response("raw"));

    const res = await app.request("/raw");
    expect(await res.text()).toBe("raw");
    expect(res.headers.get("X-RateLimit-Limit")).toBe("5");
    expect(res.headers.get("X-RateLimit-Remaining")).toBe("4");
    expect(res.headers.get("X-RateLimit-Reset")).toBe("1800000060");
  });

  it("keeps the X-RateLimit headers on a cache() hit", async () => {
    // The documented "putting it together" composition: headers set before
    // next() landed in Hono's prepared-header bag, which cache() discards
    // when it replays a stored response.
    const stored = JSON.stringify({
      status: 200,
      headers: { "content-type": "text/plain" },
      body: "cached"
    });
    const client = fakeClient([], ["sha1", [1, 4, 1_800_000_060_000], stored]);
    const app = new Hono();
    app.use(
      "*",
      ratelimit({ client, limit: 5, windowMs: 60_000, key: () => "tester" })
    );
    app.get("/pricing", cache({ client, ttlMs: 30_000 }), (c) =>
      c.text("fresh")
    );

    const res = await app.request("/pricing");
    expect(res.headers.get("X-Benni-Cache")).toBe("hit");
    expect(await res.text()).toBe("cached");
    expect(res.headers.get("X-RateLimit-Limit")).toBe("5");
  });
});

describe("hono cache storability", () => {
  it("does not store a 206 partial response", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null]);
    const app = new Hono();
    app.get("/asset", cache({ client, ttlMs: 30_000 }), (c) =>
      c.body("AB", 206)
    );

    const res = await app.request("/asset");
    expect(res.status).toBe(206);
    expect(commands.map((command) => command[0])).toEqual(["GET"]);
  });

  it("passes a ranged request straight through", async () => {
    // Range is not part of the key, so one ranged request must not be able
    // to poison the entry every later unranged client reads.
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, []);
    const app = new Hono();
    app.get("/asset", cache({ client, ttlMs: 30_000 }), (c) =>
      c.body("AB", 206)
    );

    const res = await app.request("/asset", {
      headers: { Range: "bytes=0-1" }
    });
    expect(res.status).toBe(206);
    expect(commands).toEqual([]);
  });

  it("does not store a response the handler marked private", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null]);
    const app = new Hono();
    app.get("/balance", cache({ client, ttlMs: 30_000 }), (c) => {
      c.header("Cache-Control", 'private="set-cookie", no-store');
      return c.json({ balance: 1234 });
    });

    await app.request("/balance");
    expect(commands.map((command) => command[0])).toEqual(["GET"]);
  });

  it("does not store a response varying by a header the key ignores", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null]);
    const app = new Hono();
    app.get("/profile", cache({ client, ttlMs: 30_000 }), (c) => {
      c.header("Vary", "Cookie");
      return c.json({ user: "alice" });
    });

    await app.request("/profile");
    expect(commands.map((command) => command[0])).toEqual(["GET"]);
  });

  it("still stores a response whose Vary is covered by the key", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null, "OK"]);
    const app = new Hono();
    app.get(
      "/greet",
      cache({ client, ttlMs: 30_000, vary: ["Accept-Language"] }),
      (c) => {
        c.header("Vary", "accept-language");
        return c.text("hallo");
      }
    );

    await app.request("/greet", { headers: { "Accept-Language": "de" } });
    expect(commands.map((command) => command[0])).toEqual(["GET", "SET"]);
  });

  it("carries cache-control, vary, and etag into the replayed entry", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null, "OK"]);
    const app = new Hono();
    app.get("/report", cache({ client, ttlMs: 30_000 }), (c) => {
      c.header("Cache-Control", "public, max-age=60");
      c.header("ETag", '"v1"');
      return c.json({ n: 1 });
    });

    await app.request("/report");
    const stored = JSON.parse(String(commands.at(-1)?.[2]));
    expect(stored.headers["cache-control"]).toBe("public, max-age=60");
    expect(stored.headers.etag).toBe('"v1"');
  });

  it("keys the cache on the origin, so two hosts do not share an entry", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [null, "OK", null, "OK"]);
    const app = new Hono();
    app.get("/report", cache({ client, ttlMs: 1_000 }), (c) =>
      c.json({ host: new URL(c.req.url).host })
    );

    await app.request("https://a.example/report?q=1");
    await app.request("https://b.example/report?q=1");
    const keys = commands
      .filter((command) => command[0] === "GET")
      .map((command) => command[1]);
    expect(keys).toEqual([
      "hono-cache:GET:https://a.example/report?q=1",
      "hono-cache:GET:https://b.example/report?q=1"
    ]);
  });
});

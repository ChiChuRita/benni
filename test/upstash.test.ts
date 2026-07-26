import { describe, expect, it } from "vitest";
import { codecs, createHashStore, defineHash } from "../src/core/index.js";
import { upstash } from "../src/upstash/index.js";

type FakeCall = { url: string; body: unknown; headers: Record<string, string> };

/**
 * A fake `fetch` that records each call and returns whatever `handler` maps the
 * (path-relative) command body to. `handler` returns `{ status?, body }`.
 */
function fakeFetch(
  handler: (url: string, body: unknown) => { status?: number; body: unknown }
) {
  const calls: FakeCall[] = [];
  const fn = (async (input: string | URL, init?: RequestInit) => {
    const url = String(input);
    const body = init?.body ? JSON.parse(init.body as string) : undefined;
    calls.push({
      url,
      body,
      headers: (init?.headers as Record<string, string>) ?? {}
    });
    const { status = 200, body: resBody } = handler(url, body);
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => resBody
    } as Response;
  }) as unknown as typeof fetch;
  return { fn, calls };
}

describe("upstash", () => {
  it("POSTs a command array to the base URL and unwraps the result", async () => {
    const { fn, calls } = fakeFetch(() => ({ body: { result: "OK" } }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });

    await expect(client.send(["SET", "k", "v"])).resolves.toBe("OK");
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe("https://x.upstash.io");
    expect(calls[0]?.body).toEqual(["SET", "k", "v"]);
    expect(calls[0]?.headers).toMatchObject({ Authorization: "Bearer tok" });
  });

  it("coerces number and bigint args to strings and rejects Uint8Array", async () => {
    const { fn, calls } = fakeFetch(() => ({ body: { result: 1 } }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });

    await client.send(["SET", "k", 5, 9007199254740993n]);
    expect(calls[0]?.body).toEqual(["SET", "k", "5", "9007199254740993"]);

    await expect(
      client.send(["SET", "k", new Uint8Array([1, 2, 3])])
    ).rejects.toThrow("does not support binary");
  });

  it("normalizes the base URL by trimming trailing slashes", async () => {
    const { fn, calls } = fakeFetch(() => ({ body: [{ result: null }] }));
    const client = upstash({
      url: "https://x.upstash.io/",
      token: "tok",
      fetch: fn
    });

    await client.pipeline([["GET", "a"]]);
    expect(calls[0]?.url).toBe("https://x.upstash.io/pipeline");
  });

  it("decodes a nil result as null", async () => {
    const { fn } = fakeFetch(() => ({ body: { result: null } }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });
    await expect(client.send(["GET", "missing"])).resolves.toBeNull();
  });

  it("throws on a Redis-level error payload", async () => {
    const { fn } = fakeFetch(() => ({
      status: 400,
      body: { error: "ERR value is not an integer or out of range" }
    }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });
    await expect(client.send(["INCR", "text"])).rejects.toThrow(
      "value is not an integer"
    );
  });

  it("maps a pipeline to /pipeline and unwraps each element", async () => {
    const { fn, calls } = fakeFetch(() => ({
      body: [{ result: "OK" }, { result: 2 }]
    }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });

    await expect(
      client.pipeline([
        ["SET", "k", "v"],
        ["INCR", "n"]
      ])
    ).resolves.toEqual(["OK", 2]);
    expect(calls[0]?.url).toBe("https://x.upstash.io/pipeline");
    expect(calls[0]?.body).toEqual([
      ["SET", "k", "v"],
      ["INCR", "n"]
    ]);
  });

  it("maps a transaction to /multi-exec and throws on a failed element", async () => {
    const { fn, calls } = fakeFetch(() => ({
      body: [{ result: "OK" }, { error: "ERR bad" }]
    }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });

    await expect(
      client.transaction?.([
        ["SET", "k", "v"],
        ["INCR", "text"]
      ])
    ).rejects.toThrow("bad");
    expect(calls[0]?.url).toBe("https://x.upstash.io/multi-exec");
  });

  it("resolves empty pipelines/transactions without contacting the server", async () => {
    const { fn, calls } = fakeFetch(() => ({ body: { result: null } }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });

    await expect(client.pipeline([])).resolves.toEqual([]);
    await expect(client.transaction?.([])).resolves.toEqual([]);
    expect(calls).toHaveLength(0);
  });

  it("omits session (blocking/WATCH are TCP-only) and closes as a no-op", async () => {
    const { fn } = fakeFetch(() => ({ body: { result: null } }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });
    expect(client.session).toBeUndefined();
    await expect(client.close()).resolves.toBeUndefined();
  });

  it("drives a typed store end to end over HTTP", async () => {
    // hset pipelines one HSET per field (array response); hgetall uses send and
    // returns a RESP2 flat array, which the hash store decodes into the object.
    const users = defineHash("user", {
      name: codecs.string(),
      score: codecs.number()
    });
    const { fn, calls } = fakeFetch((url) =>
      url.endsWith("/pipeline")
        ? { body: [{ result: 2 }] }
        : { body: { result: ["name", "Ada", "score", "10"] } }
    );
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });
    const store = createHashStore(client, users);

    await store.hset("42", { name: "Ada", score: 10 });
    expect(calls[0]?.url).toBe("https://x.upstash.io/pipeline");
    expect(calls[0]?.body).toEqual([
      ["HSET", "user:42", "name", "Ada", "score", "10"]
    ]);

    const user = await store.hgetall("42");
    expect(user).toEqual({ name: "Ada", score: 10 });
  });
});

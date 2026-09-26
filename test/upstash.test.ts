import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { defineHash } from "../src/core/hash.js";
import {
  codecs,
  createHashStore,
  RedisServerError
} from "../src/core/index.js";
import { upstash } from "../src/upstash/index.js";

type FakeCall = { url: string; body: unknown; headers: Record<string, string> };

/**
 * Encode every string result the way a server honouring
 * `Upstash-Encoding: base64` does: all strings at any depth, simple strings
 * included; integers, nil, and error texts untouched (verified against
 * hiett/serverless-redis-http).
 */
function encodeResults(body: unknown): unknown {
  const encode = (value: unknown): unknown =>
    typeof value === "string"
      ? Buffer.from(value, "utf8").toString("base64")
      : Array.isArray(value)
        ? value.map(encode)
        : value;
  const element = (entry: unknown) =>
    typeof entry === "object" && entry !== null && "result" in entry
      ? { ...entry, result: encode((entry as { result: unknown }).result) }
      : entry;
  return Array.isArray(body) ? body.map(element) : element(body);
}

/**
 * A fake `fetch` that records each call and returns whatever `handler` maps the
 * (path-relative) command body to. `handler` returns `{ status?, body }`, with
 * results written in plain text: they are base64-encoded on the way out when
 * the request asked for it, like a real endpoint.
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
    const headers = (init?.headers as Record<string, string>) ?? {};
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () =>
        headers["Upstash-Encoding"] === "base64"
          ? encodeResults(resBody)
          : resBody
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

  it("omits session (blocking/WATCH are TCP-only)", async () => {
    const { fn } = fakeFetch(() => ({ body: { result: null } }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });
    expect(client.session).toBeUndefined();
    expect(client.subscriber).toBeUndefined();
  });

  it("stays closed after close(): later commands reject without a request", async () => {
    const { fn, calls } = fakeFetch(() => ({ body: { result: "PONG" } }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });
    await expect(client.send(["PING"])).resolves.toBe("PONG");
    await expect(client.close()).resolves.toBeUndefined();
    // close() used to be a no-op, so commands kept succeeding after shutdown
    // where every TCP adapter rejects.
    await expect(client.send(["PING"])).rejects.toThrow(/client is closed/);
    await expect(client.pipeline([["PING"]])).rejects.toThrow(
      /client is closed/
    );
    await expect(client.transaction?.([["PING"]])).rejects.toThrow(
      /client is closed/
    );
    await expect(client.close()).resolves.toBeUndefined();
    expect(calls).toHaveLength(1);
  });

  it("asks for base64 responses and decodes every string, at any depth", async () => {
    const { fn, calls } = fakeFetch(() => ({
      body: { result: ["héllo 🎉", ["nested", 3, null], ""] }
    }));
    const client = upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fn
    });
    await expect(client.send(["XRANGE", "s", "-", "+"])).resolves.toEqual([
      "héllo 🎉",
      ["nested", 3, null],
      ""
    ]);
    expect(calls[0]?.headers).toMatchObject({ "Upstash-Encoding": "base64" });
  });

  it("decodes bytes that are not UTF-8 the way the TCP adapters do", async () => {
    // What serverless-redis-http sends for a value holding 0xff 0xfe 0x00
    // "bin". In plain JSON mode it cannot encode that value at all and
    // answers with an empty body.
    const fn = (async () =>
      ({
        ok: true,
        status: 200,
        json: async () => ({ result: "//4AYmlu" })
      }) as Response) as unknown as typeof fetch;
    const client = upstash({ url: "https://x", token: "tok", fetch: fn });
    await expect(client.send(["GET", "bin"])).resolves.toBe(
      "\uFFFD\uFFFD\u0000bin"
    );
  });

  it("keeps a result that is not base64 as sent, like a plain OK", async () => {
    const fn = (async () =>
      ({
        ok: true,
        status: 200,
        json: async () => ({ result: "OK" })
      }) as Response) as unknown as typeof fetch;
    const client = upstash({ url: "https://x", token: "tok", fetch: fn });
    await expect(client.send(["SET", "k", "v"])).resolves.toBe("OK");
  });

  it("rejects a missing url, token, or a bad timeoutMs at construction", () => {
    const fetch = fakeFetch(() => ({ body: { result: null } })).fn;
    expect(() => upstash({ url: "", token: "tok", fetch })).toThrow(
      /requires a url/
    );
    expect(() =>
      upstash({
        url: "https://x",
        token: undefined as unknown as string,
        fetch
      })
    ).toThrow(/requires a token/);
    for (const timeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() =>
        upstash({ url: "https://x", token: "tok", fetch, timeoutMs })
      ).toThrow(/timeoutMs/);
    }
  });

  it("times out a request that never answers, even if fetch ignores its signal", async () => {
    let seen: AbortSignal | undefined;
    const fn = ((_input: string, init?: RequestInit) => {
      seen = init?.signal ?? undefined;
      return new Promise<Response>(() => {});
    }) as unknown as typeof fetch;
    const client = upstash({
      url: "https://x",
      token: "tok",
      fetch: fn,
      timeoutMs: 20
    });
    const error = await client.send(["PING"]).then(
      () => undefined,
      (thrown: unknown) => thrown
    );
    expect((error as Error).name).toBe("TimeoutError");
    expect((error as Error).message).toContain("20ms");
    expect(error).not.toBeInstanceOf(RedisServerError);
    // The real fetch is told too, so it can release the socket.
    expect(seen?.aborted).toBe(true);
  });

  it("aborts in-flight and later requests when the caller's signal aborts", async () => {
    const fn = (() =>
      new Promise<Response>(() => {})) as unknown as typeof fetch;
    const controller = new AbortController();
    const client = upstash({
      url: "https://x",
      token: "tok",
      fetch: fn,
      signal: controller.signal
    });
    const pending = client.send(["PING"]);
    controller.abort(new Error("request finished"));
    await expect(pending).rejects.toThrow("request finished");
    await expect(client.send(["PING"])).rejects.toThrow("request finished");
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

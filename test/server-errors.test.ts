import { describe, expect, it } from "vitest";
import {
  RedisServerError,
  redisErrorCode,
  redisServerError
} from "../src/core/errors.js";
import { createScriptRunner, defineScript } from "../src/core/script.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { upstash } from "../src/upstash/index.js";

describe("redisErrorCode", () => {
  it("parses the leading code of a real error reply", () => {
    expect(
      redisErrorCode(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      )
    ).toBe("WRONGTYPE");
    expect(redisErrorCode("NOSCRIPT No matching script.")).toBe("NOSCRIPT");
    expect(redisErrorCode("ERR value is not an integer or out of range")).toBe(
      "ERR"
    );
    expect(
      redisErrorCode("OOM command not allowed when used memory > 'max'")
    ).toBe("OOM");
    expect(redisErrorCode("MOVED 3999 127.0.0.1:6381")).toBe("MOVED");
    expect(redisErrorCode("ASK 3999 127.0.0.1:6381")).toBe("ASK");
    expect(redisErrorCode("BUSYGROUP Consumer Group name already exists")).toBe(
      "BUSYGROUP"
    );
    // A code is allowed to be the whole reply.
    expect(redisErrorCode("EXECABORT")).toBe("EXECABORT");
  });

  it("reports no code for text a script wrote itself", () => {
    // redis.error_reply("...") reaches the client verbatim, code or not.
    expect(redisErrorCode("rate limit exceeded")).toBeUndefined();
    // One leading capital is a sentence, not a code — hence the 3-char floor.
    expect(redisErrorCode("A bad thing happened")).toBeUndefined();
    expect(redisErrorCode("")).toBeUndefined();
    // A code is uppercase; a capitalized word is not a code.
    expect(redisErrorCode("Wrongtype something")).toBeUndefined();
  });
});

describe("RedisServerError", () => {
  it("keeps the server text verbatim and exposes the parsed code", () => {
    const error = new RedisServerError(
      "WRONGTYPE Operation against a key holding the wrong kind of value"
    );

    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("RedisServerError");
    expect(error.message).toBe(
      "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    expect(error.code).toBe("WRONGTYPE");
    expect(error.command).toBeUndefined();
    // No cause was given, so none is installed — an error that wrapped nothing
    // must not look like it wrapped undefined.
    expect("cause" in error).toBe(false);
  });

  it("carries the originating command and the underlying error", () => {
    const native = new Error("NOSCRIPT No matching script. Please use EVAL.");
    const error = new RedisServerError(native.message, {
      command: "EVALSHA",
      cause: native
    });

    expect(error.code).toBe("NOSCRIPT");
    expect(error.command).toBe("EVALSHA");
    expect(error.cause).toBe(native);
  });
});

describe("redisServerError", () => {
  it("normalizes an adapter-native error, preserving message and cause", () => {
    const native = Object.assign(
      new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      ),
      { name: "ReplyError" }
    );

    const error = redisServerError(native, "ZADD");

    expect(error).toBeInstanceOf(RedisServerError);
    expect(error.message).toBe(native.message);
    expect(error.code).toBe("WRONGTYPE");
    expect(error.command).toBe("ZADD");
    expect(error.cause).toBe(native);
  });

  it("normalizes a raw error string (the Upstash REST shape)", () => {
    const error = redisServerError("ERR unknown command 'NOPE'", "NOPE");

    expect(error.code).toBe("ERR");
    expect(error.message).toBe("ERR unknown command 'NOPE'");
    expect(error.cause).toBe("ERR unknown command 'NOPE'");
  });

  it("passes an already normalized error through unchanged", () => {
    const first = redisServerError(new Error("OOM out of memory"), "SET");

    expect(redisServerError(first, "GET")).toBe(first);
  });
});

/**
 * Regression guard for the two places core classifies a server error by its
 * message. Normalization must not disturb either: `message` stays the server's
 * text, code first.
 */
describe("core message-based server error checks", () => {
  it("still retries a NOSCRIPT that arrives normalized", async () => {
    const commands: RedisCommand[] = [];
    let evalshaCalls = 0;
    const client: RedisClient = {
      async send(command) {
        commands.push(command);
        const name = String(command[0]).toUpperCase();
        if (name === "SCRIPT" && command[1] === "LOAD") return "sha-1";
        // The probe core issues to prove the server really forgot the script.
        if (name === "SCRIPT" && command[1] === "EXISTS") return [0];
        if (name === "EVALSHA") {
          evalshaCalls += 1;
          if (evalshaCalls === 1) {
            throw redisServerError(
              new Error("NOSCRIPT No matching script. Please use EVAL."),
              "EVALSHA"
            );
          }
          return 7;
        }
        throw new Error(`Unexpected command ${name}`);
      },
      async pipeline() {
        throw new Error("pipeline is not used by the script runner");
      },
      async close() {}
    };
    const runner = createScriptRunner(client);
    const script = defineScript<[], number>({
      lua: "return 7",
      keyCount: 0,
      decode: (reply) => reply as number
    });

    await expect(runner.run(script, [], [])).resolves.toBe(7);
    expect(evalshaCalls).toBe(2);
  });
});

describe("upstash server errors", () => {
  function client(
    handler: (body: unknown) => { status?: number; body: unknown }
  ) {
    const fetchImpl = (async (_input: string | URL, init?: RequestInit) => {
      const body = init?.body ? JSON.parse(init.body as string) : undefined;
      const { status = 200, body: responseBody } = handler(body);
      return {
        ok: status >= 200 && status < 300,
        status,
        json: async () => responseBody
      } as Response;
    }) as unknown as typeof fetch;
    return upstash({
      url: "https://x.upstash.io",
      token: "tok",
      fetch: fetchImpl
    });
  }

  it("normalizes a { error } payload, attributed to the command", async () => {
    const redis = client(() => ({
      status: 400,
      body: {
        error:
          "WRONGTYPE Operation against a key holding the wrong kind of value"
      }
    }));

    const error = await redis.send(["ZADD", "user:1", "1", "ada"]).then(
      () => undefined,
      (thrown: unknown) => thrown
    );

    expect(error).toBeInstanceOf(RedisServerError);
    expect((error as RedisServerError).code).toBe("WRONGTYPE");
    expect((error as RedisServerError).command).toBe("ZADD");
    expect((error as RedisServerError).cause).toBe(
      "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
  });

  it("normalizes a failing pipeline element and names its command", async () => {
    const redis = client(() => ({
      body: [{ result: "OK" }, { error: "ERR value is not an integer" }]
    }));

    const error = await redis
      .pipeline([
        ["SET", "k", "v"],
        ["INCR", "k"]
      ])
      .then(
        () => undefined,
        (thrown: unknown) => thrown
      );

    expect(error).toBeInstanceOf(RedisServerError);
    expect((error as RedisServerError).code).toBe("ERR");
    expect((error as RedisServerError).command).toBe("INCR");
  });

  it("leaves a transport failure a plain Error", async () => {
    // A 5xx with no Redis error payload never reached a Redis command, so
    // reporting it as a server error reply would be a lie.
    const redis = client(() => ({ status: 502, body: { nope: true } }));

    const error = await redis.send(["GET", "k"]).then(
      () => undefined,
      (thrown: unknown) => thrown
    );

    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(RedisServerError);
    expect((error as Error).message).toContain("Upstash HTTP 502");
  });
});

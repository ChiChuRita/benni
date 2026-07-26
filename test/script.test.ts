import { describe, expect, it } from "vitest";
import { createScriptRunner, defineScript } from "../src/core/script.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const rateLimitLua =
  "return redis.call('INCRBY', KEYS[1], ARGV[2]) <= tonumber(ARGV[1])";

function defineRateLimit() {
  return defineScript<[string, number], boolean>({
    lua: rateLimitLua,
    keyCount: 1,
    decode(reply) {
      if (typeof reply !== "number") {
        throw new TypeError("Expected Redis EVALSHA to return number");
      }
      return reply === 1;
    }
  });
}

function rejectingClient(
  commands: RedisCommand[],
  replies: Array<RedisReply | Error>
): RedisClient {
  return {
    async send(command) {
      commands.push(command);
      const reply = replies.shift();
      if (reply === undefined) throw new Error("No fake Redis reply queued");
      if (reply instanceof Error) throw reply;
      return reply;
    },
    async pipeline() {
      throw new Error("pipeline is not used by the script runner");
    },
    async close() {}
  };
}

describe("defineScript", () => {
  it("rejects negative, fractional, and unsafe keyCount values", () => {
    const options = { lua: "return 1", decode: (reply: RedisReply) => reply };

    expect(() => defineScript({ ...options, keyCount: -1 })).toThrow(
      "keyCount must be a non-negative safe integer"
    );
    expect(() => defineScript({ ...options, keyCount: 1.5 })).toThrow(
      "keyCount must be a non-negative safe integer"
    );
    expect(() =>
      defineScript({ ...options, keyCount: Number.MAX_SAFE_INTEGER + 1 })
    ).toThrow("keyCount must be a non-negative safe integer");
    expect(() => defineScript({ ...options, keyCount: Number.NaN })).toThrow(
      "keyCount must be a non-negative safe integer"
    );
  });
});

describe("createScriptRunner", () => {
  it("loads the script once and then evaluates by sha", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(fakeClient(commands, ["sha-1", 1]));
    const rateLimit = defineRateLimit();

    await expect(runner.run(rateLimit, ["rate:42"], ["100", 5])).resolves.toBe(
      true
    );

    expect(commands).toEqual([
      ["SCRIPT", "LOAD", rateLimitLua],
      ["EVALSHA", "sha-1", 1, "rate:42", "100", 5]
    ]);
  });

  it("reuses the cached sha across runs", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(fakeClient(commands, ["sha-1", 1, 0]));
    const rateLimit = defineRateLimit();

    await expect(runner.run(rateLimit, ["rate:42"], ["100", 5])).resolves.toBe(
      true
    );
    await expect(runner.run(rateLimit, ["rate:43"], ["100", 7])).resolves.toBe(
      false
    );

    expect(commands).toEqual([
      ["SCRIPT", "LOAD", rateLimitLua],
      ["EVALSHA", "sha-1", 1, "rate:42", "100", 5],
      ["EVALSHA", "sha-1", 1, "rate:43", "100", 7]
    ]);
  });

  it("loads each script object separately", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      fakeClient(commands, ["sha-1", 1, "sha-2", 0])
    );
    const first = defineRateLimit();
    const second = defineRateLimit();

    await runner.run(first, ["rate:42"], ["100", 5]);
    await runner.run(second, ["rate:42"], ["100", 5]);

    expect(commands).toEqual([
      ["SCRIPT", "LOAD", rateLimitLua],
      ["EVALSHA", "sha-1", 1, "rate:42", "100", 5],
      ["SCRIPT", "LOAD", rateLimitLua],
      ["EVALSHA", "sha-2", 1, "rate:42", "100", 5]
    ]);
  });

  it("supports scripts with zero keys and zero args", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(fakeClient(commands, ["sha-1", "pong"]));
    const ping = defineScript<[], string>({
      lua: "return 'pong'",
      keyCount: 0,
      decode(reply) {
        if (typeof reply !== "string") {
          throw new TypeError("Expected Redis EVALSHA to return string");
        }
        return reply;
      }
    });

    await expect(runner.run(ping, [], [])).resolves.toBe("pong");

    expect(commands).toEqual([
      ["SCRIPT", "LOAD", "return 'pong'"],
      ["EVALSHA", "sha-1", 0]
    ]);
  });

  it("reloads and retries once when EVALSHA rejects with NOSCRIPT", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        new Error("NOSCRIPT No matching script. Please use EVAL."),
        "sha-2",
        1,
        0
      ])
    );
    const rateLimit = defineRateLimit();

    await expect(runner.run(rateLimit, ["rate:42"], ["100", 5])).resolves.toBe(
      true
    );
    await expect(runner.run(rateLimit, ["rate:43"], ["100", 7])).resolves.toBe(
      false
    );

    expect(commands).toEqual([
      ["SCRIPT", "LOAD", rateLimitLua],
      ["EVALSHA", "sha-1", 1, "rate:42", "100", 5],
      ["SCRIPT", "LOAD", rateLimitLua],
      ["EVALSHA", "sha-2", 1, "rate:42", "100", 5],
      ["EVALSHA", "sha-2", 1, "rate:43", "100", 7]
    ]);
  });

  it("retries at most once when NOSCRIPT persists", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        new Error("NOSCRIPT No matching script. Please use EVAL."),
        "sha-2",
        new Error("NOSCRIPT No matching script. Please use EVAL.")
      ])
    );

    await expect(
      runner.run(defineRateLimit(), ["rate:42"], ["100", 5])
    ).rejects.toThrow("NOSCRIPT");
    expect(commands).toHaveLength(4);
  });

  it("propagates non-NOSCRIPT EVALSHA errors without retrying", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        new Error("ERR Error running script: oops")
      ])
    );

    await expect(
      runner.run(defineRateLimit(), ["rate:42"], ["100", 5])
    ).rejects.toThrow("ERR Error running script: oops");
    expect(commands).toHaveLength(2);
  });

  it("throws before sending anything when keys length mismatches keyCount", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(fakeClient(commands, []));

    await expect(
      runner.run(defineRateLimit(), ["rate:42", "rate:43"], ["100", 5])
    ).rejects.toThrow("Expected 1 script keys but received 2");
    expect(commands).toEqual([]);
  });

  it("throws when SCRIPT LOAD does not return a string", async () => {
    const runner = createScriptRunner(fakeClient([], [42]));

    await expect(
      runner.run(defineRateLimit(), ["rate:42"], ["100", 5])
    ).rejects.toThrow("Expected Redis SCRIPT LOAD to return string");
  });

  it("throws when the reloaded SCRIPT LOAD does not return a string", async () => {
    const runner = createScriptRunner(
      rejectingClient(
        [],
        [
          "sha-1",
          new Error("NOSCRIPT No matching script. Please use EVAL."),
          null
        ]
      )
    );

    await expect(
      runner.run(defineRateLimit(), ["rate:42"], ["100", 5])
    ).rejects.toThrow("Expected Redis SCRIPT LOAD to return string");
  });

  it("passes the raw reply through the script decoder", async () => {
    const runner = createScriptRunner(fakeClient([], ["sha-1", ["a", "b"]]));
    const collect = defineScript<[string], string[]>({
      lua: "return redis.call('LRANGE', KEYS[1], 0, -1)",
      keyCount: 1,
      decode(reply) {
        if (!Array.isArray(reply)) {
          throw new TypeError("Expected Redis EVALSHA to return array");
        }
        return reply.map(String);
      }
    });

    await expect(runner.run(collect, ["jobs:1"], ["all"])).resolves.toEqual([
      "a",
      "b"
    ]);
  });
});

const typedClient = null as unknown as RedisClient;
const typedRunner = createScriptRunner(typedClient);
const typedScript = defineScript<[string, number], boolean>({
  lua: rateLimitLua,
  keyCount: 1,
  decode: (reply) => reply === 1
});
const inferredScript = defineScript({
  lua: "return redis.status_reply('OK')",
  keyCount: 0,
  decode: (reply) => String(reply)
});

type TypedRunResult = Awaited<
  ReturnType<typeof typedRunner.run<[string, number], boolean>>
>;
type InferredDecodeResult = ReturnType<typeof inferredScript.decode>;
type TypedDecodeReply = Parameters<typeof typedScript.decode>[0];

type _TypedRunResult = Expect<Equal<TypedRunResult, boolean>>;
type _InferredDecodeResult = Expect<Equal<InferredDecodeResult, string>>;
type _TypedDecodeReply = Expect<Equal<TypedDecodeReply, RedisReply>>;

async function expectInferredResultTypes() {
  const flag = await typedRunner.run(typedScript, ["rate:42"], ["100", 5]);
  const status = await typedRunner.run(inferredScript, [], []);
  type _Flag = Expect<Equal<typeof flag, boolean>>;
  type _Status = Expect<Equal<typeof status, string>>;
}

function expectTypeErrorsOnly() {
  // @ts-expect-error script args tuple entries must match the declared types.
  void typedRunner.run(typedScript, ["rate:42"], ["100", "5"]);

  // @ts-expect-error script args tuple arity must match the declared tuple.
  void typedRunner.run(typedScript, ["rate:42"], ["100"]);

  // @ts-expect-error script args must be provided.
  void typedRunner.run(typedScript, ["rate:42"]);

  // @ts-expect-error keys must be strings.
  void typedRunner.run(typedScript, [42], ["100", 5]);

  // @ts-expect-error declared args must be valid Redis command arguments.
  void defineScript<[{ nested: true }], boolean>({
    lua: "return 1",
    keyCount: 0,
    decode: (reply) => reply === 1
  });

  void defineScript<[], number>({
    lua: "return 1",
    keyCount: 0,
    // @ts-expect-error decode return type must match the declared result type.
    decode: (reply) => String(reply)
  });
}

void expectInferredResultTypes;
void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

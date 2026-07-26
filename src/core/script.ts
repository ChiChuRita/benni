import { replyShapeError, ValidationError } from "./errors.js";
import type { RedisClient, RedisCommandArgument, RedisReply } from "./types.js";

declare const scriptArgsBrand: unique symbol;

export type RedisScript<
  TArgs extends
    readonly RedisCommandArgument[] = readonly RedisCommandArgument[],
  TResult = unknown
> = {
  readonly lua: string;
  readonly keyCount: number;
  decode(reply: RedisReply): TResult;
  readonly [scriptArgsBrand]?: (args: TArgs) => void;
};

export type DefineScriptOptions<TResult> = {
  readonly lua: string;
  readonly keyCount: number;
  decode(reply: RedisReply): TResult;
};

export type ScriptRunner = {
  run<TArgs extends readonly RedisCommandArgument[], TResult>(
    script: RedisScript<TArgs, TResult>,
    keys: readonly string[],
    args: NoInfer<TArgs>
  ): Promise<TResult>;
};

export function defineScript<
  TArgs extends readonly RedisCommandArgument[],
  TResult
>(options: DefineScriptOptions<TResult>): RedisScript<TArgs, TResult> {
  if (!Number.isSafeInteger(options.keyCount) || options.keyCount < 0) {
    throw new ValidationError("keyCount must be a non-negative safe integer");
  }
  return {
    lua: options.lua,
    keyCount: options.keyCount,
    decode(reply) {
      return options.decode(reply);
    }
  };
}

type AnyRedisScript = RedisScript<any, any>;

export function createScriptRunner(client: RedisClient): ScriptRunner {
  const shas = new Map<AnyRedisScript, string>();

  async function load(script: AnyRedisScript): Promise<string> {
    const reply = await client.send(["SCRIPT", "LOAD", script.lua]);
    if (typeof reply !== "string") {
      throw replyShapeError("SCRIPT LOAD", "string", reply);
    }
    shas.set(script, reply);
    return reply;
  }

  return {
    async run<TArgs extends readonly RedisCommandArgument[], TResult>(
      script: RedisScript<TArgs, TResult>,
      keys: readonly string[],
      args: NoInfer<TArgs>
    ): Promise<TResult> {
      if (keys.length !== script.keyCount) {
        throw new ValidationError(
          `Expected ${script.keyCount} script keys but received ${keys.length}`
        );
      }
      const sha = shas.get(script) ?? (await load(script));
      let reply: RedisReply;
      try {
        reply = await client.send([
          "EVALSHA",
          sha,
          script.keyCount,
          ...keys,
          ...args
        ]);
      } catch (error) {
        if (!isNoScriptError(error)) throw error;
        const reloaded = await load(script);
        reply = await client.send([
          "EVALSHA",
          reloaded,
          script.keyCount,
          ...keys,
          ...args
        ]);
      }
      return script.decode(reply);
    }
  };
}

function isNoScriptError(error: unknown): boolean {
  return error instanceof Error && error.message.includes("NOSCRIPT");
}

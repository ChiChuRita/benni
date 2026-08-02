import { ReplyShapeError, replyShapeError, ValidationError } from "./errors.js";
import type { SameSlotScriptKeys } from "./keys.js";
import type { SlotGuard } from "./slot.js";
import {
  SCRIPT_RUNNER_KEY,
  type StoreBinding,
  type StoreContext,
  withStore
} from "./store.js";
import type {
  Codec,
  FieldCodecs,
  InferHashInput,
  RedisClient,
  RedisCommandArgument,
  RedisReply
} from "./types.js";

declare const scriptArgsBrand: unique symbol;

/**
 * A prepared Lua script: its source, how many of the trailing arguments are
 * keys, and how to decode its reply. `TArgs` is a phantom brand, so a script
 * built for one argument tuple cannot be run with another.
 *
 * Most code should reach for `script()` from `benni/schema`, which adds named
 * keys and per-argument codecs on top of this.
 */
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

/** Options for {@link defineScript}. */
export type DefineScriptOptions<TResult> = {
  /** The Lua source, addressing keys as `KEYS[n]` and arguments as `ARGV[n]`. */
  readonly lua: string;
  /** How many leading arguments are keys — must match `run()`'s `keys.length`. */
  readonly keyCount: number;
  /** Turn the raw reply into the script's result type. */
  decode(reply: RedisReply): TResult;
};

/**
 * Runs {@link RedisScript}s against one client, loading each script once and
 * then executing cached `EVALSHA`. If the server has forgotten the script
 * (`NOSCRIPT`, e.g. after a restart or `SCRIPT FLUSH`), it reloads and retries
 * transparently. Built by {@link createScriptRunner}; `redis.script(schema)`
 * uses one internally, shared per client.
 */
export type ScriptRunner = {
  run<TArgs extends readonly RedisCommandArgument[], TResult>(
    script: RedisScript<TArgs, TResult>,
    keys: readonly string[],
    args: NoInfer<TArgs>
  ): Promise<TResult>;
};

/**
 * Prepares a Lua script for repeated execution. Declaring it once and reusing
 * the value is what lets the runner cache the `SHA` instead of re-sending the
 * source on every call.
 *
 * This is the low-level primitive. Application code should prefer {@link script}
 * from `benni/schema`, which adds named keys and per-argument codecs and is run
 * with `redis.script(schema).run({ keys, args })`.
 *
 * @example
 * ```ts
 * const incrBy = defineScript<[amount: string], number>({
 *   lua: `return redis.call("INCRBY", KEYS[1], ARGV[1])`,
 *   keyCount: 1,
 *   decode: (reply) => Number(reply)
 * });
 * ```
 * @throws ValidationError if `keyCount` is negative or not a safe integer.
 */
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

/**
 * Builds a {@link ScriptRunner} bound to one client. The `SHA` cache lives on
 * the returned runner, so keep it for the client's lifetime rather than making a
 * new one per call.
 */
export function createScriptRunner(
  client: RedisClient,
  assertSameSlot?: SlotGuard
): ScriptRunner {
  const shas = new Map<AnyRedisScript, string>();

  async function load(script: AnyRedisScript): Promise<string> {
    const reply = await client.send(["SCRIPT", "LOAD", script.lua]);
    if (typeof reply !== "string") {
      throw replyShapeError("SCRIPT LOAD", "string", reply);
    }
    shas.set(script, reply);
    return reply;
  }

  /**
   * Whether the server still holds `sha`. A reply we cannot read counts as
   * "still there", because not retrying is the safe side of the guess: the
   * caller gets the script's error instead of its side effects twice.
   */
  async function serverHasScript(sha: string): Promise<boolean> {
    let reply: RedisReply;
    try {
      reply = await client.send(["SCRIPT", "EXISTS", sha]);
    } catch {
      return true;
    }
    const found = Array.isArray(reply) ? reply[0] : undefined;
    return found !== 0 && found !== false;
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
      // A script whose keys span slots is broken on a cluster even when it
      // does not error: redis.call inside Lua can only reach the slot the
      // script was routed to.
      assertSameSlot?.("EVALSHA", keys);
      const cached = shas.get(script);
      const sha = cached ?? (await load(script));
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
        // The message alone cannot prove the server forgot the script: a
        // script's own `redis.error_reply("NOSCRIPT …")` reaches the client
        // byte for byte, indistinguishable from the server's. So ask, always.
        // Reloading and re-running a script that already applied its side
        // effects applies them twice, and the caller sees only the error.
        // The freshly-loaded sha needs the probe too, since a SCRIPT FLUSH or
        // a failover between the load and the call is exactly the case where
        // a retry is the right answer, and a script erroring on its first run
        // is exactly the case where it is not.
        if (await serverHasScript(sha)) {
          throw error;
        }
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

/**
 * Anchored, not a substring search. Redis prefixes the reply with the error
 * code, so a real NOSCRIPT always *starts* with it. A substring test also
 * matched a script's own failure whose text merely mentions NOSCRIPT — Redis
 * wraps those as "ERR Error running script ...: @user_script:N: <message>" —
 * and the retry then re-ran a script that had already applied its side
 * effects.
 *
 * Anchoring is necessary but not sufficient: a script that returns
 * `redis.error_reply("NOSCRIPT …")` produces the same bytes the server does,
 * so a match only means "may be a cache miss". `serverHasScript` decides.
 */
function isNoScriptError(error: unknown): boolean {
  return error instanceof Error && /^\s*NOSCRIPT\b/.test(error.message);
}

export type ScriptSchema<
  TName extends string,
  TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
> = ReturnType<
  typeof defineScript<readonly RedisCommandArgument[], TResult>
> & {
  readonly kind: "script";
  readonly name: TName;
  readonly keys: TKeys;
  readonly args: TArgs;
  encodeArgs(args: InferHashInput<TArgs>): RedisCommandArgument[];
};

export type ScriptOptions<
  TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
> = {
  readonly keys: TKeys;
  readonly args: TArgs;
  readonly returns: Codec<TResult, TResult>;
  readonly lua: string;
  /**
   * Allow the script to return nil, decoding it as `null` and widening the
   * result to `TResult | null`.
   *
   * Lua converts both `nil` and `false` to a RESP nil, so any script with a
   * `return nil` branch — or one that forwards a `GET` on a missing key, or a
   * `SET NX` that lost — needs this. Without it a nil reply is a
   * `ReplyShapeError`, because the alternative is inventing a value the codec
   * never produced.
   */
  readonly nullable?: boolean;
};

/**
 * The per-handle script runner, created on first use — so that
 * `createScriptRunner` is named only by this module.
 */
export function scriptRunnerFor(ctx: StoreContext): ScriptRunner {
  return ctx.shared(SCRIPT_RUNNER_KEY, () =>
    createScriptRunner(ctx.client, ctx.assertSameSlot)
  );
}

/** A typed Lua script resource: run with named keys and args. */
export function createScriptResource<
  TName extends string,
  TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
>(ctx: StoreContext, schema: ScriptSchema<TName, TKeys, TArgs, TResult>) {
  return {
    /**
     * Run the script. Keys are named, so they are checked for a shared Cluster
     * hash tag wherever that is provable from their types; see
     * {@link SameSlotRecord}. Keys built from runtime ids are not provable and
     * pass silently, which is what the `benni/cluster` runtime guard is for.
     */
    run<
      const TKeyValues extends { readonly [K in TKeys[number]]: string }
    >(input: {
      readonly keys: SameSlotScriptKeys<TKeys, TKeyValues>;
      readonly args: InferHashInput<TArgs>;
    }): Promise<TResult> {
      return scriptRunnerFor(ctx).run(
        schema,
        schema.keys.map((key) => input.keys[key as TKeys[number]]),
        schema.encodeArgs(input.args)
      );
    }
  };
}

const scriptBinding: StoreBinding = { resource: createScriptResource };

/**
 * A Lua script schema with named keys, typed args, and a scalar return codec.
 * Run it with `redis.script(schema).run({ keys, args })` — the runner loads the
 * script once and executes cached `EVALSHA`.
 * @example
 * ```ts
 * const rateLimit = script("rate-limit", {
 *   keys: ["counter"],
 *   args: { windowSeconds: number() },
 *   returns: number(),
 *   lua: `local n = redis.call("INCR", KEYS[1])
 *         if n == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
 *         return n`
 * });
 * ```
 */
export function script<
  TName extends string,
  const TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
>(
  name: TName,
  options: ScriptOptions<TKeys, TArgs, TResult> & { readonly nullable: true }
): ScriptSchema<TName, TKeys, TArgs, TResult | null>;
export function script<
  TName extends string,
  const TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
>(
  name: TName,
  // `nullable` is pinned to `false | undefined` rather than left to the bare
  // options type: a computed or forwarded boolean would otherwise select this
  // overload, and the decoder still returns null for a nil reply, so the
  // declared result type could not hold what run() resolves.
  options: ScriptOptions<TKeys, TArgs, TResult> & {
    readonly nullable?: false | undefined;
  }
): ScriptSchema<TName, TKeys, TArgs, TResult>;
export function script<
  TName extends string,
  const TKeys extends readonly string[],
  TArgs extends FieldCodecs,
  TResult
>(
  name: TName,
  options: ScriptOptions<TKeys, TArgs, TResult>
): ScriptSchema<TName, TKeys, TArgs, TResult | null> {
  const argNames = Object.keys(options.args) as Array<keyof TArgs & string>;
  const redisScript = defineScript<
    readonly RedisCommandArgument[],
    TResult | null
  >({
    lua: options.lua,
    keyCount: options.keys.length,
    decode(reply: RedisReply) {
      // Lua turns nil *and* false into a RESP nil, so this is the reply a
      // `return nil` branch or a losing `SET NX` produces — common enough that
      // it used to escape as a bare TypeError with the reply discarded.
      if (reply === null) {
        if (options.nullable) return null;
        throw new ReplyShapeError(
          `Script "${name}" returned nil, which its returns codec cannot decode. ` +
            "Lua converts both nil and false to a nil reply; declare " +
            "nullable: true to receive null, or return a sentinel the codec " +
            "understands (e.g. `... and 1 or 0` for a boolean).",
          reply
        );
      }
      if (typeof reply !== "string" && typeof reply !== "number") {
        throw replyShapeError(`script "${name}"`, "a scalar reply", reply);
      }
      return options.returns.decode(String(reply));
    }
  });
  const schema: ScriptSchema<TName, TKeys, TArgs, TResult | null> = {
    ...redisScript,
    kind: "script",
    name,
    keys: options.keys,
    args: options.args,
    encodeArgs(args: InferHashInput<TArgs>) {
      return argNames.map((argName) =>
        options.args[argName].encode(args[argName])
      );
    }
  };
  return withStore(schema, scriptBinding);
}

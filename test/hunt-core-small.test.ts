import { afterAll, describe, expect, it } from "vitest";
import { createBitmapStore, defineBitmap } from "../src/core/bitmap.js";
import { codecs } from "../src/core/codecs.js";
import { createCounterStore } from "../src/core/counter.js";
import { ReplyShapeError, ValidationError } from "../src/core/errors.js";
import { defineKeyspace } from "../src/core/key-value.js";
import type { ListPosOptions } from "../src/core/list.js";
import { createListStore, defineList } from "../src/core/list.js";
import type { ScriptOptions } from "../src/core/script.js";
import {
  createScriptRunner,
  defineScript,
  script
} from "../src/core/script.js";
import { createStringStore } from "../src/core/string.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { benni } from "../src/database.js";
import { node } from "../src/node/index.js";
import * as s from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

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
      throw new Error("pipeline is not used by these tests");
    },
    async close() {}
  };
}

describe("benni() binds a module that co-exports a foreign validator", () => {
  const users = s.hash("user", { name: s.string() });

  it("keeps an object that merely has a kind property out of the registry", () => {
    // Valibot stamps `kind: "schema"` on every schema and ArkType stamps one
    // on every type(); both are documented as `json(validator)` inputs, so a
    // schema module co-exporting one is the ordinary layout. It used to throw
    // at bind time, blaming a copy the user never made.
    const validator = { kind: "schema", type: "object", entries: {} };
    const db = benni(fakeClient([], []), { schema: { users, validator } });

    expect(Object.keys(db.query)).toEqual(["users"]);
  });

  it("still rejects a copied benni schema, naming the export", () => {
    expect(() =>
      benni(fakeClient([], []), { schema: { users: { ...users } } })
    ).toThrow(/schema\.users .*no store binding/s);
  });
});

describe('the hashTag: "id" layout validates its prefix', () => {
  it("rejects a prefix containing a brace", () => {
    // Redis reads the tag from the first `{` in the whole key, so `cart{v2}`
    // would tag on "v2" and every id would land in one slot instead of
    // co-locating with the other schemas tagged by id.
    expect(() =>
      defineKeyspace("cart{v2}", codecs.string(), { hashTag: "id" })
    ).toThrow(ValidationError);
    expect(() => s.hash("a{}b", { n: s.string() }, { hashTag: "id" })).toThrow(
      /first "\{"/
    );
  });

  it("leaves the other layouts alone", () => {
    expect(s.kv("cart{v2}", s.string()).key("u42")).toBe("cart{v2}:u42");
    expect(s.kv("cart", s.string(), { hashTag: "id" }).key("u42")).toBe(
      "cart:{u42}"
    );
  });
});

describe("the script runner does not re-run a script's own NOSCRIPT", () => {
  const lua = "return redis.call('INCR', KEYS[1])";

  function defineBump() {
    return defineScript<[], number>({
      lua,
      keyCount: 1,
      decode: (reply) => Number(reply)
    });
  }

  it("asks the server before reloading a cached sha", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        1,
        new Error("NOSCRIPT the script said so"),
        [1]
      ])
    );
    const bump = defineBump();

    await expect(runner.run(bump, ["n:1"], [])).resolves.toBe(1);
    await expect(runner.run(bump, ["n:1"], [])).rejects.toThrow(
      "NOSCRIPT the script said so"
    );
    // No second EVALSHA: the script had already applied its INCR.
    expect(commands).toEqual([
      ["SCRIPT", "LOAD", lua],
      ["EVALSHA", "sha-1", 1, "n:1"],
      ["EVALSHA", "sha-1", 1, "n:1"],
      ["SCRIPT", "EXISTS", "sha-1"]
    ]);
  });

  it("still reloads when the server really has forgotten the script", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        1,
        new Error("NOSCRIPT No matching script. Please use EVAL."),
        [0],
        "sha-2",
        2
      ])
    );
    const bump = defineBump();

    await expect(runner.run(bump, ["n:1"], [])).resolves.toBe(1);
    await expect(runner.run(bump, ["n:1"], [])).resolves.toBe(2);
    expect(commands).toEqual([
      ["SCRIPT", "LOAD", lua],
      ["EVALSHA", "sha-1", 1, "n:1"],
      ["EVALSHA", "sha-1", 1, "n:1"],
      ["SCRIPT", "EXISTS", "sha-1"],
      ["SCRIPT", "LOAD", lua],
      ["EVALSHA", "sha-2", 1, "n:1"]
    ]);
  });

  it("treats a boolean SCRIPT EXISTS reply as an answer too", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        1,
        new Error("NOSCRIPT No matching script. Please use EVAL."),
        [false],
        "sha-2",
        3
      ])
    );
    const bump = defineBump();

    await runner.run(bump, ["n:1"], []);
    await expect(runner.run(bump, ["n:1"], [])).resolves.toBe(3);
  });

  it("does not retry when the probe itself fails", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        1,
        new Error("NOSCRIPT No matching script. Please use EVAL."),
        new Error("Connection is closed")
      ])
    );
    const bump = defineBump();

    await runner.run(bump, ["n:1"], []);
    // The original error, not the probe's: a guess cannot justify re-running
    // side effects.
    await expect(runner.run(bump, ["n:1"], [])).rejects.toThrow("NOSCRIPT");
  });

  it("does not retry on a probe reply it cannot read", async () => {
    const commands: RedisCommand[] = [];
    const runner = createScriptRunner(
      rejectingClient(commands, [
        "sha-1",
        1,
        new Error("NOSCRIPT No matching script. Please use EVAL."),
        "surprise"
      ])
    );
    const bump = defineBump();

    await runner.run(bump, ["n:1"], []);
    await expect(runner.run(bump, ["n:1"], [])).rejects.toThrow("NOSCRIPT");
  });
});

describe("script() cannot be handed a forwarded nullable", () => {
  const nullableScript = script("hunt-nullable", {
    keys: ["k"],
    args: {},
    returns: codecs.string(),
    lua: "return nil",
    nullable: true
  });
  const plainScript = script("hunt-plain", {
    keys: ["k"],
    args: {},
    returns: codecs.string(),
    lua: "return 'ok'"
  });

  type NullableResult = Expect<
    Equal<ReturnType<typeof nullableScript.decode>, string | null>
  >;
  type PlainResult = Expect<
    Equal<ReturnType<typeof plainScript.decode>, string>
  >;

  it("keeps the literal forms typed as before", () => {
    const pinned: [NullableResult, PlainResult] = [true, true];
    expect(pinned).toEqual([true, true]);
    expect(nullableScript.decode(null)).toBeNull();
  });

  it("rejects an options bag whose nullable is a plain boolean", () => {
    const options: ScriptOptions<["k"], Record<string, never>, string> = {
      keys: ["k"],
      args: {},
      returns: codecs.string(),
      lua: "return nil",
      nullable: true
    };
    // The decoder honours a forwarded `nullable`, so the non-nullable overload
    // would hand back a schema whose result type cannot hold what it produces.
    // @ts-expect-error nullable must be a literal on the call itself.
    const forwarded = script("hunt-forwarded", options);

    expect(forwarded.decode(null)).toBeNull();
  });
});

describe("lpos overloads", () => {
  const jobs = defineList("hunt-lpos", codecs.string());

  it("rejects an options bag that could carry a count", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(fakeClient(commands, [[0, 2]]), jobs);
    const options: ListPosOptions = { count: 2, rank: 1 };

    // Before the fix this matched the scalar overload and resolved an array
    // typed `number | null`.
    // @ts-expect-error neither overload accepts `count?: number | undefined`.
    await store.lpos("q", "x", options);
    expect(commands).toEqual([
      ["LPOS", "hunt-lpos:q", "x", "COUNT", 2, "RANK", 1]
    ]);
  });

  it("still types and sends both literal forms", async () => {
    const commands: RedisCommand[] = [];
    const store = createListStore(fakeClient(commands, [1, [0, 2]]), jobs);

    const one = await store.lpos("q", "x", { rank: 1 });
    const many = await store.lpos("q", "x", { count: 2 });
    type OneIsScalar = Expect<Equal<typeof one, number | null>>;
    type ManyIsArray = Expect<Equal<typeof many, number[]>>;
    const pinned: [OneIsScalar, ManyIsArray] = [true, true];

    expect(pinned).toEqual([true, true]);
    expect(one).toBe(1);
    expect(many).toEqual([0, 2]);
  });
});

describe("64-bit replies refuse to round", () => {
  const beyondSafe = 2 ** 53 + 2;

  it("throws on a BITFIELD value past the safe range", async () => {
    const flags = defineBitmap("hunt-bitfield");
    const store = createBitmapStore(fakeClient([], [[beyondSafe]]), flags);

    await expect(
      store.bitfield("a").get("i64", 0).exec()
    ).rejects.toBeInstanceOf(ReplyShapeError);
  });

  it("throws on a counter that has run past the safe range", async () => {
    const seq = defineKeyspace("hunt-counter", codecs.number());
    const store = createCounterStore(
      fakeClient([], [beyondSafe, beyondSafe]),
      seq
    );

    await expect(store.incr("a")).rejects.toThrow(/MAX_SAFE_INTEGER/);
    const failure = await store.incrby("a", 2).catch((error) => error);
    expect(failure).toBeInstanceOf(ReplyShapeError);
    expect((failure as ReplyShapeError).reply).toBe(beyondSafe);
  });

  it("leaves representable values alone", async () => {
    const seq = defineKeyspace("hunt-counter", codecs.number());
    const store = createCounterStore(
      fakeClient([], [Number.MAX_SAFE_INTEGER, -1]),
      seq
    );

    await expect(store.incr("a")).resolves.toBe(Number.MAX_SAFE_INTEGER);
    await expect(store.decrby("a", 1)).resolves.toBe(-1);
  });
});

describe("bytes() reports a decode failure like every other codec", () => {
  it("throws ReplyShapeError carrying the offending value", () => {
    const codec = s.bytes();
    for (const stored of ["abc", "a=b=", "!!!!"]) {
      const failure = (() => {
        try {
          codec.decode(stored);
        } catch (error) {
          return error;
        }
      })();
      expect(failure).toBeInstanceOf(ReplyShapeError);
      expect((failure as ReplyShapeError).reply).toBe(stored);
    }
  });

  it("still round-trips a valid value", () => {
    const codec = s.bytes();
    expect(codec.decode(codec.encode(new Uint8Array([1, 2, 3])))).toEqual(
      new Uint8Array([1, 2, 3])
    );
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("GETRANGE and SETRANGE index bytes (live)", () => {
  const texts = defineKeyspace(
    `benni:hunt:string:${Date.now()}:${Math.random().toString(36).slice(2)}`,
    codecs.string()
  );
  let client: RedisClient;

  afterAll(async () => {
    if (client === undefined) return;
    try {
      await client.send(["DEL", texts.key("note")]);
    } finally {
      await client.close();
    }
  });

  it("counts and slices in bytes, as the JSDoc now says", async () => {
    client = await node({ url: redisUrl });
    const store = createStringStore(client, texts);
    const value = "café ☕ résumé";

    await client.send(["SET", texts.key("note"), value]);

    const byteLength = new TextEncoder().encode(value).length;
    await expect(store.strlen("note")).resolves.toBe(byteLength);
    expect(byteLength).not.toBe(value.length);

    // A boundary inside a multi-byte character loses it: this is the
    // corruption the JSDoc warns about, pinned so the warning stays true.
    await expect(store.getrange("note", 0, 3)).resolves.toBe("caf�");
    // Split on byte boundaries instead and the value survives.
    await expect(store.getrange("note", 0, -1)).resolves.toBe(value);
  });
});

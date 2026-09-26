import { describe, expect, it } from "vitest";
import {
  PartialRecordError,
  ReplyShapeError,
  ValidationError
} from "../src/core/errors.js";
import { createHashStore } from "../src/core/hash.js";
import type { Codec, RedisCommand, RedisReply } from "../src/core/types.js";
import { benni } from "../src/index.js";
import { node } from "../src/node/index.js";
import {
  boolean,
  hash,
  type InferInput,
  type InferOutput,
  kv,
  number,
  optional,
  script,
  stream,
  string
} from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// Optional fields (schema evolution), the hmset partial update, and one kind
// of "missing" on every hash read: absent, never null inside an object.

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;

const users = hash("user", {
  name: string(),
  score: number(),
  bio: optional(string()),
  verified: optional(boolean())
});

function store(commands: RedisCommand[], replies: RedisReply[]) {
  return createHashStore(fakeClient(commands, replies), users);
}

describe("optional() hash fields: types", () => {
  it("types optional fields as ?: on both sides, and per field without undefined", () => {
    const typed = store([], []);
    const whole = () => typed.hget("42");
    const one = () => typed.hget("42", "bio");
    const all = () => typed.hgetall("42");
    const picked = () => typed.hmget("42", ["name", "bio"]);

    type _In = Expect<
      Equal<
        InferInput<typeof users>,
        { name: string; score: number; bio?: string; verified?: boolean }
      >
    >;
    type _Out = Expect<
      Equal<
        InferOutput<typeof users>,
        { name: string; score: number; bio?: string; verified?: boolean }
      >
    >;
    type _Whole = Expect<
      Equal<
        Awaited<ReturnType<typeof whole>>,
        { name: string; score: number; bio?: string; verified?: boolean } | null
      >
    >;
    type _One = Expect<Equal<Awaited<ReturnType<typeof one>>, string | null>>;
    type _All = Expect<
      Equal<
        Awaited<ReturnType<typeof all>>,
        {
          name?: string;
          score?: number;
          bio?: string;
          verified?: boolean;
        } | null
      >
    >;
    type _Picked = Expect<
      Equal<Awaited<ReturnType<typeof picked>>, { name?: string; bio?: string }>
    >;
    type _HsetField = Expect<
      Equal<Parameters<typeof typed.hset<"verified">>[2], boolean>
    >;
    type _Hmset = Expect<
      Equal<
        Parameters<typeof typed.hmset>[1],
        { name?: string; score?: number; bio?: string; verified?: boolean }
      >
    >;
    type _Hsetex = Expect<
      Equal<
        Parameters<typeof typed.hsetex>[1],
        Parameters<typeof typed.hmset>[1]
      >
    >;
    expect(typeof whole).toBe("function");
  });

  it("keeps the missing-required-field write a compile error", () => {
    const typed = store([], []);
    const planted = () =>
      // @ts-expect-error score is required on the whole-record write
      typed.hset("42", { name: "Ada" });
    const wrongType = () =>
      // @ts-expect-error hmset values must match the field codecs
      typed.hmset("42", { score: "11" });
    const unknownField = () =>
      // @ts-expect-error hmset only takes declared fields
      typed.hmset("42", { nickname: "A" });
    const optionalOnly = () => typed.hset("42", { name: "Ada", score: 1 });
    expect(typeof planted).toBe("function");
    expect(typeof wrongType).toBe("function");
    expect(typeof unknownField).toBe("function");
    expect(typeof optionalOnly).toBe("function");
  });

  it("means nothing outside a hash: kv, stream, and script values stay required", () => {
    const counts = kv("count", optional(number()));
    const events = stream("event", {
      kind: string(),
      note: optional(string())
    });
    const bump = script("bump", {
      keys: ["counter"],
      args: { by: optional(number()) },
      returns: number(),
      lua: "return 1"
    });
    type _Kv = Expect<Equal<InferInput<typeof counts>, number>>;
    type _Stream = Expect<
      Equal<InferInput<typeof events>, { kind: string; note: string }>
    >;
    type _Script = Expect<
      Equal<Parameters<typeof bump.encodeArgs>[0], { by: number }>
    >;
    expect(bump.encodeArgs({ by: 2 })).toEqual(["2"]);
  });
});

describe("optional(): the codec", () => {
  it("marks the codec and delegates encode/decode with the codec as this", () => {
    class Prefixed implements Codec<string> {
      constructor(private readonly prefix: string) {}
      encode(input: string) {
        return `${this.prefix}${input}`;
      }
      decode(stored: string) {
        return stored.slice(this.prefix.length);
      }
    }
    const codec = optional(new Prefixed("p:"));
    expect(codec.optional).toBe(true);
    expect(codec.encode("x")).toBe("p:x");
    expect(codec.decode("p:x")).toBe("x");
  });
});

describe("optional() hash fields: whole-record reads", () => {
  it("leaves a missing optional field off the record", async () => {
    const commands: RedisCommand[] = [];
    const record = await store(commands, [["Ada", "10", null, "1"]]).hget("42");
    expect(record).toStrictEqual({ name: "Ada", score: 10, verified: true });
    expect(record !== null && "bio" in record).toBe(false);
    expect(commands).toEqual([
      ["HMGET", "user:42", "name", "score", "bio", "verified"]
    ]);
  });

  it("reads a record written before the optional fields existed", async () => {
    await expect(
      store([], [["Ada", "10", null, null]]).hget("42")
    ).resolves.toStrictEqual({ name: "Ada", score: 10 });
  });

  it("still throws PartialRecordError naming only the missing required fields", async () => {
    const failure = store([], [["Ada", null, null, "1"]]).hget("42");
    await expect(failure).rejects.toBeInstanceOf(PartialRecordError);
    await expect(failure).rejects.toMatchObject({ missing: ["score"] });
  });

  it("throws when only optional fields are stored", async () => {
    await expect(
      store([], [[null, null, "hi", null]]).hget("42")
    ).rejects.toMatchObject({ missing: ["name", "score"] });
  });

  it("returns null when no declared field is stored", async () => {
    await expect(
      store([], [[null, null, null, null]]).hget("42")
    ).resolves.toBeNull();
  });

  it("rejects an HMGET item that is neither a string nor nil", async () => {
    await expect(
      store([], [["Ada", 10, null, null]]).hget("42")
    ).rejects.toThrow(ReplyShapeError);
  });
});

describe("optional() hash fields: whole-record writes", () => {
  it("sends one HSET when every field is present", async () => {
    const commands: RedisCommand[] = [];
    await store(commands, [4]).hset("42", {
      name: "Ada",
      score: 10,
      bio: "hi",
      verified: false
    });
    expect(commands).toEqual([
      [
        "HSET",
        "user:42",
        "name",
        "Ada",
        "score",
        "10",
        "bio",
        "hi",
        "verified",
        "0"
      ]
    ]);
  });

  it("deletes omitted and undefined optional fields in the same transaction", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [2, 1]);
    let transactions = 0;
    const transaction = client.transaction?.bind(client);
    client.transaction = (batch) => {
      transactions += 1;
      return transaction?.(batch) ?? Promise.resolve([]);
    };
    await createHashStore(client, users).hset("42", {
      name: "Ada",
      score: 10,
      bio: undefined
    });
    expect(transactions).toBe(1);
    expect(commands).toEqual([
      ["HSET", "user:42", "name", "Ada", "score", "10"],
      ["HDEL", "user:42", "bio", "verified"]
    ]);
  });

  it("puts the EXPIRE in the same transaction", async () => {
    const commands: RedisCommand[] = [];
    await store(commands, [2, 0, 1]).hset(
      "42",
      { name: "Ada", score: 10 },
      { ttlSeconds: 60 }
    );
    expect(commands).toEqual([
      ["HSET", "user:42", "name", "Ada", "score", "10"],
      ["HDEL", "user:42", "bio", "verified"],
      ["EXPIRE", "user:42", 60]
    ]);
  });

  it("writes an all-optional record with nothing set as a delete of its fields", async () => {
    const notes = hash("note", { text: optional(string()) });
    const commands: RedisCommand[] = [];
    await createHashStore(fakeClient(commands, [1]), notes).hset("1", {});
    expect(commands).toEqual([["HDEL", "note:1", "text"]]);
  });

  it("refuses a required field that is undefined at runtime, before sending", async () => {
    const commands: RedisCommand[] = [];
    await expect(
      store(commands, []).hset("42", {
        name: "Ada",
        score: undefined as unknown as number
      })
    ).rejects.toThrow(ValidationError);
    expect(commands).toEqual([]);
  });
});

describe("hmset: the partial update", () => {
  it("sends one HSET with only the given fields and resolves the added count", async () => {
    const commands: RedisCommand[] = [];
    await expect(
      store(commands, [1]).hmset("42", { score: 11, bio: "hi" })
    ).resolves.toBe(1);
    expect(commands).toEqual([["HSET", "user:42", "score", "11", "bio", "hi"]]);
  });

  it("rejects an empty update, an undefined value, and an unknown field before sending", async () => {
    const commands: RedisCommand[] = [];
    const typed = store(commands, []);
    await expect(typed.hmset("42", {})).rejects.toThrow(
      "hmset requires at least one field"
    );
    await expect(typed.hmset("42", { bio: undefined })).rejects.toThrow(
      "hmset received undefined for field 'bio'"
    );
    await expect(typed.hmset("42", { nickname: "A" } as never)).rejects.toThrow(
      "Unknown hash field 'nickname'"
    );
    expect(commands).toEqual([]);
  });

  it("rejects a non-number HSET reply", async () => {
    await expect(store([], ["OK"]).hmset("42", { score: 1 })).rejects.toThrow(
      ReplyShapeError
    );
  });
});

describe("hsetex takes the same partial input", () => {
  it("counts FIELDS from the pairs it sends", async () => {
    const commands: RedisCommand[] = [];
    await store(commands, [1]).hsetex(
      "42",
      { bio: "hi", verified: true },
      { ttlSeconds: 5 }
    );
    expect(commands).toEqual([
      ["HSETEX", "user:42", "EX", 5, "FIELDS", 2, "bio", "hi", "verified", "1"]
    ]);
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("schema evolution against a live server", () => {
  const prefix = `evolve:${Date.now()}:${Math.random().toString(36).slice(2)}`;
  const v1 = hash(prefix, { name: string(), score: number() });
  const v2 = hash(prefix, {
    name: string(),
    score: number(),
    bio: optional(string())
  });

  it("reads old records through a schema that gained an optional field", async () => {
    const client = node({ url: redisUrl });
    const redis = benni({ client });
    try {
      await redis.store(v1).hset("old", { name: "Ada", score: 10 });
      await expect(redis.store(v2).hget("old")).resolves.toStrictEqual({
        name: "Ada",
        score: 10
      });

      await redis.store(v2).hmset("old", { bio: "hi", score: 11 });
      await expect(redis.store(v2).hget("old")).resolves.toStrictEqual({
        name: "Ada",
        score: 11,
        bio: "hi"
      });

      // The whole-record write replaces the record: the omitted bio goes.
      await redis
        .store(v2)
        .hset("old", { name: "Ada", score: 12 }, { ttlSeconds: 60 });
      await expect(redis.store(v2).hget("old")).resolves.toStrictEqual({
        name: "Ada",
        score: 12
      });
      await expect(client.send(["HLEN", v2.key("old")])).resolves.toBe(2);
      await expect(
        client.send(["TTL", v2.key("old")])
      ).resolves.toBeGreaterThan(0);

      await redis.store(v2).hdel("old", "score");
      await expect(redis.store(v2).hget("old")).rejects.toMatchObject({
        missing: ["score"]
      });
    } finally {
      await client.send(["DEL", v2.key("old")]);
      await client.close();
    }
  });
});

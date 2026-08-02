import { describe, expect, it } from "vitest";
import { benni } from "../src/index.js";
import { LockNotAcquiredError, lock } from "../src/primitives/index.js";
import type {
  InferInput,
  InferOutput,
  StandardSchemaV1
} from "../src/schema.js";
import {
  boolean,
  hash,
  json,
  kv,
  number,
  string,
  zset
} from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// Pins for the 2026-07-11 DX pass: schema type inference, template-literal
// key preservation through the handle, Standard Schema codecs, compile-time
// exclusive options, and the typed lock error.

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;

type Profile = { name: string; score: number };

const profiles = kv("profile", json<Profile>());
const users = hash("user", {
  name: string(),
  score: number(),
  active: boolean()
});
const board = zset("board", string());

describe("schema type inference ($infer / Infer*)", () => {
  it("derives value types from schemas without runtime cost", () => {
    type _KvOut = Expect<Equal<typeof profiles.$inferOutput, Profile>>;
    type _KvIn = Expect<Equal<typeof profiles.$inferInput, Profile>>;
    type _HashOut = Expect<
      Equal<
        typeof users.$inferOutput,
        { name: string; score: number; active: boolean }
      >
    >;
    type _UtilOut = Expect<
      Equal<InferOutput<typeof users>, typeof users.$inferOutput>
    >;
    type _UtilIn = Expect<Equal<InferInput<typeof profiles>, Profile>>;
    type _ZsetOut = Expect<Equal<InferOutput<typeof board>, string>>;
    // Codecs infer too (they expose decode).
    type _CodecOut = Expect<
      Equal<InferOutput<ReturnType<typeof number>>, number>
    >;

    // The anchors are phantoms: they must NOT exist at runtime.
    expect(Object.keys(profiles)).not.toContain("$inferInput");
    expect(Object.keys(users)).not.toContain("$inferOutput");
  });
});

describe("template-literal keys survive the handle", () => {
  it("redis.kv(schema).key and redis.query keep the literal prefix", () => {
    const redis = benni(fakeClient([], []), { schema: { profiles, users } });
    const direct = profiles.key("42");
    const viaAccessor = redis.kv(profiles).key("42");
    const viaQuery = redis.query.profiles.key("42");
    type _Direct = Expect<Equal<typeof direct, "profile:42">>;
    type _Accessor = Expect<Equal<typeof viaAccessor, "profile:42">>;
    type _Query = Expect<Equal<typeof viaQuery, "profile:42">>;
    expect(viaAccessor).toBe("profile:42");
    expect(viaQuery).toBe("profile:42");
  });
});

describe("json(standardSchema) validated codec", () => {
  const profileSchema: StandardSchemaV1<Profile> = {
    "~standard": {
      version: 1,
      vendor: "test",
      validate(value) {
        const candidate = value as Partial<Profile> | null;
        if (
          candidate === null ||
          typeof candidate.name !== "string" ||
          typeof candidate.score !== "number"
        ) {
          return {
            issues: [{ message: "expected a profile", path: ["name"] }]
          };
        }
        return { value: candidate as Profile };
      },
      types: undefined as unknown as { input: Profile; output: Profile }
    }
  };

  it("decodes valid values with the inferred type", () => {
    const codec = json(profileSchema);
    type _Out = Expect<Equal<ReturnType<typeof codec.decode>, Profile>>;
    expect(codec.decode('{"name":"Ada","score":10}')).toEqual({
      name: "Ada",
      score: 10
    });
  });

  it("rejects invalid stored values with the vendor and issue path", () => {
    const codec = json(profileSchema);
    expect(() => codec.decode('{"wrong":true}')).toThrow(
      /json\(schema\) validation failed \(test\): name: expected a profile/
    );
  });

  it("rejects async validators with a clear error", () => {
    const asyncSchema: StandardSchemaV1<Profile> = {
      "~standard": {
        version: 1,
        vendor: "async-test",
        validate: async () => ({ issues: [{ message: "nope" }] })
      }
    };
    const codec = json(asyncSchema);
    expect(() => codec.decode("{}")).toThrow(
      /requires a synchronous validator.*async-test/
    );
  });

  it("still supports the trusted json<T>() form", () => {
    const codec = json<Profile>();
    type _Out = Expect<Equal<ReturnType<typeof codec.decode>, Profile>>;
    expect(codec.decode('{"name":"Ada","score":1}')).toEqual({
      name: "Ada",
      score: 1
    });
  });
});

describe("compile-time exclusive options", () => {
  it("forbids the invalid combinations at the type level", async () => {
    const redis = benni(fakeClient([], []));
    const store = redis.kv(profiles);
    const nxXx = () =>
      // @ts-expect-error nx and xx are mutually exclusive
      store.set("1", { name: "a", score: 0 }, { nx: true, xx: true });
    const ttl = () =>
      // @ts-expect-error keepTtl and ttlSeconds are mutually exclusive
      store.set("1", { name: "a", score: 0 }, { keepTtl: true, ttlSeconds: 5 });
    const zs = redis.zset(board);
    const gtLt = () =>
      // @ts-expect-error gt and lt are mutually exclusive
      zs.zadd("g", { score: 1, member: "a" }, { gt: true, lt: true });
    const nxGt = () =>
      // @ts-expect-error nx cannot combine with gt
      zs.zadd("g", { score: 1, member: "a" }, { nx: true, gt: true });
    expect(typeof nxXx).toBe("function");
    expect(typeof ttl).toBe("function");
    expect(typeof gtLt).toBe("function");
    expect(typeof nxGt).toBe("function");
  });

  it("zadd accepts a single entry and emits condition tokens", async () => {
    const commands: import("../src/core/index.js").RedisCommand[] = [];
    const redis = benni(fakeClient(commands, [1, 1]));
    await redis.zset(board).zadd("g", { score: 1, member: "ada" });
    await redis
      .zset(board)
      .zadd("g", [{ score: 2, member: "bo" }], { gt: true, ch: true });
    expect(commands).toEqual([
      ["ZADD", "board:g", 1, "ada"],
      ["ZADD", "board:g", "GT", "CH", 2, "bo"]
    ]);
  });
});

describe("typed lock error", () => {
  it("run() throws LockNotAcquiredError carrying the key", async () => {
    // SET NX loses (null reply) and there are no retries.
    const locks = lock(fakeClient([], [null]));
    const failure = locks.run("order:1", async () => 1);
    await expect(failure).rejects.toBeInstanceOf(LockNotAcquiredError);
    await expect(failure).rejects.toMatchObject({ key: "lock:order:1" });
  });
});

import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import { createHashStore, defineHash } from "../src/core/hash.js";
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

const users = defineHash("user", {
  name: codecs.string(),
  score: codecs.number()
});

function userStore(commands: RedisCommand[], replies: RedisReply[]) {
  return createHashStore(fakeClient(commands, replies), users);
}

describe("createHashStore getAll", () => {
  it("decodes flat-array replies and ignores undeclared fields", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [
      ["name", "beni", "score", "7", "legacy", "ignored"]
    ]);

    await expect(store.hgetall("42")).resolves.toEqual({
      name: "beni",
      score: 7
    });

    expect(commands).toEqual([["HGETALL", "user:42"]]);
  });

  it("decodes map replies and ignores undeclared fields", async () => {
    const store = userStore(
      [],
      [
        new Map<RedisReply, RedisReply>([
          ["name", "beni"],
          ["score", "7"],
          ["legacy", "ignored"]
        ])
      ]
    );

    await expect(store.hgetall("42")).resolves.toEqual({
      name: "beni",
      score: 7
    });
  });

  it("returns a partial object when only some declared fields exist", async () => {
    const store = userStore([], [["score", "7"]]);

    await expect(store.hgetall("42")).resolves.toEqual({ score: 7 });
  });

  it("returns null for empty array and empty map replies", async () => {
    const store = userStore([], [[], new Map<RedisReply, RedisReply>()]);

    await expect(store.hgetall("42")).resolves.toBeNull();
    await expect(store.hgetall("42")).resolves.toBeNull();
  });

  it("returns an empty object when only undeclared fields exist", async () => {
    const store = userStore([], [["legacy", "ignored"]]);

    await expect(store.hgetall("42")).resolves.toEqual({});
  });

  it("rejects replies that are neither array nor map", async () => {
    const store = userStore([], ["oops"]);

    await expect(store.hgetall("42")).rejects.toThrow(
      "Expected Redis HGETALL to return array or map"
    );
  });

  it("rejects odd-length array replies", async () => {
    const store = userStore([], [["name"]]);

    await expect(store.hgetall("42")).rejects.toThrow(
      "Expected Redis HGETALL to return field/value pairs"
    );
  });

  it("rejects non-string fields or values in array replies", async () => {
    const store = userStore([], [["name", 1]]);

    await expect(store.hgetall("42")).rejects.toThrow(
      "Expected Redis HGETALL to return field/value strings"
    );
  });
});

describe("createHashStore getFields", () => {
  it("emits HMGET and decodes present fields, nulls for missing", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [["beni", null]]);

    await expect(store.hmget("42", ["name", "score"])).resolves.toEqual({
      name: "beni",
      score: null
    });

    expect(commands).toEqual([["HMGET", "user:42", "name", "score"]]);
  });

  it("short-circuits an empty field list without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(store.hmget("42", [])).resolves.toEqual({});

    expect(commands).toEqual([]);
  });

  it("rejects non-array replies", async () => {
    const store = userStore([], ["oops"]);

    await expect(store.hmget("42", ["name"])).rejects.toThrow(
      "Expected Redis HMGET to return array"
    );
  });

  it("rejects items that are neither string nor null", async () => {
    const store = userStore([], [[1]]);

    await expect(store.hmget("42", ["name"])).rejects.toThrow(
      "Expected Redis HMGET item to return string or null"
    );
  });

  it("rejects replies with fewer items than requested fields", async () => {
    const store = userStore([], [["beni"]]);

    await expect(store.hmget("42", ["name", "score"])).rejects.toThrow(
      "Expected Redis HMGET item to return string or null"
    );
  });
});

describe("createHashStore fieldNames and size", () => {
  it("emits HKEYS and returns names without filtering undeclared fields", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [["name", "score", "legacy"]]);

    await expect(store.hkeys("42")).resolves.toEqual([
      "name",
      "score",
      "legacy"
    ]);

    expect(commands).toEqual([["HKEYS", "user:42"]]);
  });

  it("rejects non-array HKEYS replies and non-string items", async () => {
    const store = userStore([], ["oops", [1]]);

    await expect(store.hkeys("42")).rejects.toThrow(
      "Expected Redis HKEYS to return array"
    );
    await expect(store.hkeys("42")).rejects.toThrow(
      "Expected Redis HKEYS item to return string"
    );
  });

  it("emits HLEN and returns the field count", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [2]);

    await expect(store.hlen("42")).resolves.toBe(2);

    expect(commands).toEqual([["HLEN", "user:42"]]);
  });
});

describe("createHashStore randomField and randomFields", () => {
  it("emits HRANDFIELD and returns one field name or null", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, ["score", null]);

    await expect(store.hrandfield("42")).resolves.toBe("score");
    await expect(store.hrandfield("42")).resolves.toBeNull();

    expect(commands).toEqual([
      ["HRANDFIELD", "user:42"],
      ["HRANDFIELD", "user:42"]
    ]);
  });

  it("emits HRANDFIELD with a count and returns field names", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [
      ["name", "score"],
      ["name", "name", "score"]
    ]);

    await expect(store.hrandfield("42", { count: 2 })).resolves.toEqual([
      "name",
      "score"
    ]);
    await expect(store.hrandfield("42", { count: -3 })).resolves.toEqual([
      "name",
      "name",
      "score"
    ]);

    expect(commands).toEqual([
      ["HRANDFIELD", "user:42", 2],
      ["HRANDFIELD", "user:42", -3]
    ]);
  });

  it("validates the count before sending", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(store.hrandfield("42", { count: 0 })).rejects.toThrow(
      "count must be a nonzero safe integer"
    );
    await expect(store.hrandfield("42", { count: 1.5 })).rejects.toThrow(
      TypeError
    );
    expect(commands).toEqual([]);
  });

  it("rejects unexpected HRANDFIELD replies", async () => {
    await expect(userStore([], [1]).hrandfield("42")).rejects.toThrow(
      "Expected Redis HRANDFIELD to return string or null"
    );
    await expect(
      userStore([], ["nope"]).hrandfield("42", { count: 2 })
    ).rejects.toThrow("Expected Redis HRANDFIELD to return array");
    await expect(
      userStore([], [[1]]).hrandfield("42", { count: 2 })
    ).rejects.toThrow("Expected Redis HRANDFIELD item to return string");
  });
});

describe("createHashStore setFieldIfAbsent and fieldLength", () => {
  it("emits HSETNX with the encoded value and maps 1/0 to boolean", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [1, 0]);

    await expect(store.hsetnx("42", "score", 7)).resolves.toBe(true);
    await expect(store.hsetnx("42", "name", "beni")).resolves.toBe(false);

    expect(commands).toEqual([
      ["HSETNX", "user:42", "score", "7"],
      ["HSETNX", "user:42", "name", "beni"]
    ]);
  });

  it("emits HSTRLEN and returns the stored length", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [5]);

    await expect(store.hstrlen("42", "name")).resolves.toBe(5);

    expect(commands).toEqual([["HSTRLEN", "user:42", "name"]]);
  });
});

describe("createHashStore incrementFieldByFloat", () => {
  it("emits HINCRBYFLOAT and parses the bulk-string reply", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, ["3.5"]);

    await expect(store.hincrbyfloat("42", "score", 1.5)).resolves.toBe(3.5);

    expect(commands).toEqual([["HINCRBYFLOAT", "user:42", "score", 1.5]]);
  });

  it("rejects non-finite amounts without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(store.hincrbyfloat("42", "score", Number.NaN)).rejects.toThrow(
      "amount must be a finite number"
    );
    await expect(
      store.hincrbyfloat("42", "score", Number.POSITIVE_INFINITY)
    ).rejects.toThrow("amount must be a finite number");

    expect(commands).toEqual([]);
  });

  it("rejects replies that are neither string nor number", async () => {
    const store = userStore([], [null]);

    await expect(store.hincrbyfloat("42", "score", 1)).rejects.toThrow(
      "Expected Redis HINCRBYFLOAT to return string or number"
    );
  });

  it("rejects replies that do not parse to a finite number", async () => {
    const store = userStore([], ["oops"]);

    await expect(store.hincrbyfloat("42", "score", 1)).rejects.toThrow(
      "Expected Redis HINCRBYFLOAT to return a number"
    );
  });
});

describe("createHashStore field expiration", () => {
  it("emits HEXPIRE with FIELDS layout and returns status codes", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [[1, -2]]);

    await expect(store.hexpire("42", ["name", "score"], 60)).resolves.toEqual([
      1, -2
    ]);

    expect(commands).toEqual([
      ["HEXPIRE", "user:42", 60, "FIELDS", 2, "name", "score"]
    ]);
  });

  it("short-circuits empty field lists after validating ttl", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(store.hexpire("42", [], 60)).resolves.toEqual([]);
    await expect(store.hexpire("42", [], 0)).rejects.toThrow(
      "ttlSeconds must be a positive safe integer"
    );

    expect(commands).toEqual([]);
  });

  it("rejects invalid ttl values", async () => {
    const store = userStore([], []);

    await expect(store.hexpire("42", ["name"], 1.5)).rejects.toThrow(
      "ttlSeconds must be a positive safe integer"
    );
  });

  it("emits HTTL for a single field and unwraps the reply", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [[60]]);

    await expect(store.httl("42", "name")).resolves.toBe(60);

    expect(commands).toEqual([["HTTL", "user:42", "FIELDS", 1, "name"]]);
  });

  it("rejects malformed HTTL replies", async () => {
    const store = userStore([], ["60", ["60"], [60, 60], []]);

    await expect(store.httl("42", "name")).rejects.toThrow(
      "Expected Redis HTTL to return array"
    );
    await expect(store.httl("42", "name")).rejects.toThrow(
      "Expected Redis HTTL item to return number"
    );
    await expect(store.httl("42", "name")).rejects.toThrow(
      "Expected Redis HTTL to return one number"
    );
    await expect(store.httl("42", "name")).rejects.toThrow(
      "Expected Redis HTTL to return one number"
    );
  });

  it("emits HPERSIST with FIELDS layout and returns status codes", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [[1, -1]]);

    await expect(store.hpersist("42", ["name", "score"])).resolves.toEqual([
      1, -1
    ]);

    expect(commands).toEqual([
      ["HPERSIST", "user:42", "FIELDS", 2, "name", "score"]
    ]);
  });

  it("short-circuits empty HPERSIST field lists", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(store.hpersist("42", [])).resolves.toEqual([]);

    expect(commands).toEqual([]);
  });
});

describe("createHashStore field expiration modes", () => {
  it("maps each expiry mode to its command and value", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [[1], [1], [1], [1]]);

    await store.hexpire("42", ["name"], { ttlSeconds: 60 });
    await store.hexpire("42", ["name"], { ttlMilliseconds: 60000 });
    await store.hexpire("42", ["name"], { expireAtSeconds: 1740470400 });
    await store.hexpire("42", ["name"], {
      expireAtMilliseconds: 1740470400000
    });

    expect(commands).toEqual([
      ["HEXPIRE", "user:42", 60, "FIELDS", 1, "name"],
      ["HPEXPIRE", "user:42", 60000, "FIELDS", 1, "name"],
      ["HEXPIREAT", "user:42", 1740470400, "FIELDS", 1, "name"],
      ["HPEXPIREAT", "user:42", 1740470400000, "FIELDS", 1, "name"]
    ]);
  });

  it("requires exactly one expiry mode", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(store.hexpire("42", ["name"], {} as never)).rejects.toThrow(
      /exactly one of ttlSeconds/
    );
    await expect(
      store.hexpire("42", ["name"], {
        ttlSeconds: 1,
        ttlMilliseconds: 1
      } as never)
    ).rejects.toThrow(/exactly one of ttlSeconds/);
    expect(commands).toEqual([]);
  });

  it("reads HPTTL, HEXPIRETIME and HPEXPIRETIME", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [[500], [1740470400], [1740470400000]]);

    await expect(
      store.httl("42", "name", { milliseconds: true })
    ).resolves.toBe(500);
    await expect(store.hexpiretime("42", "name")).resolves.toBe(1740470400);
    await expect(
      store.hexpiretime("42", "name", { milliseconds: true })
    ).resolves.toBe(1740470400000);

    expect(commands).toEqual([
      ["HPTTL", "user:42", "FIELDS", 1, "name"],
      ["HEXPIRETIME", "user:42", "FIELDS", 1, "name"],
      ["HPEXPIRETIME", "user:42", "FIELDS", 1, "name"]
    ]);
  });
});

describe("createHashStore getFieldsEx", () => {
  it("emits HGETEX without an expiry and decodes positionally", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [["beni", null]]);

    await expect(store.hgetex("42", ["name", "score"])).resolves.toEqual({
      name: "beni",
      score: null
    });

    expect(commands).toEqual([
      ["HGETEX", "user:42", "FIELDS", 2, "name", "score"]
    ]);
  });

  it("inserts the expiry tokens before FIELDS", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [["beni"], ["beni"], ["beni"]]);

    await store.hgetex("42", ["name"], { ttlSeconds: 60 });
    await store.hgetex("42", ["name"], {
      expireAtMilliseconds: 1740470400000
    });
    await store.hgetex("42", ["name"], { persist: true });

    expect(commands).toEqual([
      ["HGETEX", "user:42", "EX", 60, "FIELDS", 1, "name"],
      ["HGETEX", "user:42", "PXAT", 1740470400000, "FIELDS", 1, "name"],
      ["HGETEX", "user:42", "PERSIST", "FIELDS", 1, "name"]
    ]);
  });

  it("short-circuits an empty field list without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(store.hgetex("42", [])).resolves.toEqual({});
    expect(commands).toEqual([]);
  });
});

describe("createHashStore getDelFields", () => {
  it("emits HGETDEL and decodes positionally with nulls for missing", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [["beni", null]]);

    await expect(store.hgetdel("42", ["name", "score"])).resolves.toEqual({
      name: "beni",
      score: null
    });

    expect(commands).toEqual([
      ["HGETDEL", "user:42", "FIELDS", 2, "name", "score"]
    ]);
  });

  it("short-circuits an empty field list without sending a command", async () => {
    const store = userStore([], []);

    await expect(store.hgetdel("42", [])).resolves.toEqual({});
  });
});

describe("createHashStore setFieldsEx", () => {
  it("emits HSETEX with encoded field/value pairs and maps 1/0 to boolean", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [1, 0]);

    await expect(store.hsetex("42", { name: "beni", score: 7 })).resolves.toBe(
      true
    );
    await expect(store.hsetex("42", { score: 9 })).resolves.toBe(false);

    expect(commands).toEqual([
      ["HSETEX", "user:42", "FIELDS", 2, "name", "beni", "score", "7"],
      ["HSETEX", "user:42", "FIELDS", 1, "score", "9"]
    ]);
  });

  it("prepends condition and expiry tokens", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, [1, 1, 1]);

    await store.hsetex("42", { name: "a" }, { fnx: true, ttlSeconds: 60 });
    await store.hsetex("42", { name: "b" }, { fxx: true, keepTtl: true });
    await store.hsetex("42", { name: "c" }, { ttlMilliseconds: 1000 });

    expect(commands).toEqual([
      ["HSETEX", "user:42", "FNX", "EX", 60, "FIELDS", 1, "name", "a"],
      ["HSETEX", "user:42", "FXX", "KEEPTTL", "FIELDS", 1, "name", "b"],
      ["HSETEX", "user:42", "PX", 1000, "FIELDS", 1, "name", "c"]
    ]);
  });

  it("rejects conflicting conditions, multiple expiries, and empty input", async () => {
    const commands: RedisCommand[] = [];
    const store = userStore(commands, []);

    await expect(
      store.hsetex(
        "42",
        { name: "a" },
        // @ts-expect-error fnx+fxx no longer compiles; pin the runtime guard
        { fxx: true, fnx: true }
      )
    ).rejects.toThrow(/cannot set both/);
    await expect(
      store.hsetex(
        "42",
        { name: "a" },
        // @ts-expect-error two expiry modes no longer compile; pin the runtime guard
        { ttlSeconds: 1, keepTtl: true }
      )
    ).rejects.toThrow(/at most one/);
    await expect(store.hsetex("42", {})).rejects.toThrow(
      "hsetex requires at least one field"
    );
    expect(commands).toEqual([]);
  });
});

describe("createHashStore unknown field", () => {
  it("names the declared fields when given an unknown field", async () => {
    const store = userStore([], []);

    await expect(store.hget("42", "missing" as never)).rejects.toThrow(
      "Unknown hash field 'missing'; declared fields: name, score"
    );
  });
});

const typedClient = null as unknown as RedisClient;
const typedStore = createHashStore(typedClient, users);

type GetAllValue = Awaited<ReturnType<typeof typedStore.hgetall>>;
type GetNameValue = Awaited<ReturnType<typeof typedStore.hmget<"name">>>;
type GetBothValue = Awaited<
  ReturnType<typeof typedStore.hmget<"name" | "score">>
>;
type FieldNamesValue = Awaited<ReturnType<typeof typedStore.hkeys>>;
type SizeValue = Awaited<ReturnType<typeof typedStore.hlen>>;
type SetIfAbsentScoreValue = Parameters<typeof typedStore.hsetnx<"score">>[2];
type SetIfAbsentResult = Awaited<ReturnType<typeof typedStore.hsetnx>>;
type FieldLengthValue = Awaited<ReturnType<typeof typedStore.hstrlen>>;
type FloatIncrementFieldName = Parameters<typeof typedStore.hincrbyfloat>[1];
type FloatIncrementValue = Awaited<ReturnType<typeof typedStore.hincrbyfloat>>;
type ExpireFieldsValue = Awaited<ReturnType<typeof typedStore.hexpire>>;
type FieldTtlValue = Awaited<ReturnType<typeof typedStore.httl>>;
type PersistFieldsValue = Awaited<ReturnType<typeof typedStore.hpersist>>;

type _GetAllValue = Expect<
  Equal<GetAllValue, { name?: string; score?: number } | null>
>;
type _GetNameValue = Expect<Equal<GetNameValue, { name?: string | null }>>;
type _GetBothValue = Expect<
  Equal<GetBothValue, { name?: string | null; score?: number | null }>
>;
type _FieldNamesValue = Expect<Equal<FieldNamesValue, string[]>>;
type _SizeValue = Expect<Equal<SizeValue, number>>;
type _SetIfAbsentScoreValue = Expect<Equal<SetIfAbsentScoreValue, number>>;
type _SetIfAbsentResult = Expect<Equal<SetIfAbsentResult, boolean>>;
type _FieldLengthValue = Expect<Equal<FieldLengthValue, number>>;
type _FloatIncrementFieldName = Expect<Equal<FloatIncrementFieldName, "score">>;
type _FloatIncrementValue = Expect<Equal<FloatIncrementValue, number>>;
type _ExpireFieldsValue = Expect<Equal<ExpireFieldsValue, number[]>>;
type _FieldTtlValue = Expect<Equal<FieldTtlValue, number>>;
type _PersistFieldsValue = Expect<Equal<PersistFieldsValue, number[]>>;

type GetFieldsExValue = Awaited<ReturnType<typeof typedStore.hgetex<"name">>>;
type GetDelFieldsValue = Awaited<
  ReturnType<typeof typedStore.hgetdel<"name" | "score">>
>;
type FieldExpireTimeValue = Awaited<ReturnType<typeof typedStore.hexpiretime>>;
type SetFieldsExResult = Awaited<ReturnType<typeof typedStore.hsetex>>;
type SetFieldsExValues = Parameters<typeof typedStore.hsetex>[1];

type _GetFieldsExValue = Expect<
  Equal<GetFieldsExValue, { name?: string | null }>
>;
type _GetDelFieldsValue = Expect<
  Equal<GetDelFieldsValue, { name?: string | null; score?: number | null }>
>;
type _FieldExpireTimeValue = Expect<Equal<FieldExpireTimeValue, number>>;
type _SetFieldsExResult = Expect<Equal<SetFieldsExResult, boolean>>;
type _SetFieldsExValues = Expect<
  Equal<SetFieldsExValues, Partial<{ name: string; score: number }>>
>;

function expectTypeErrorsOnly() {
  // @ts-expect-error getFields only accepts declared field names.
  void typedStore.hmget("42", ["missing"]);

  // @ts-expect-error setFieldIfAbsent value must match the declared field codec.
  void typedStore.hsetnx("42", "score", "7");

  // @ts-expect-error setFieldIfAbsent only accepts declared field names.
  void typedStore.hsetnx("42", "missing", "7");

  // @ts-expect-error fieldLength only accepts declared field names.
  void typedStore.hstrlen("42", "missing");

  // @ts-expect-error only numeric hash fields can be float-incremented.
  void typedStore.hincrbyfloat("42", "name", 1.5);

  // @ts-expect-error expireFields only accepts declared field names.
  void typedStore.hexpire("42", ["missing"], 60);

  // @ts-expect-error fieldTtl only accepts declared field names.
  void typedStore.httl("42", "missing");

  // @ts-expect-error persistFields only accepts declared field names.
  void typedStore.hpersist("42", ["missing"]);

  // @ts-expect-error getFieldsEx only accepts declared field names.
  void typedStore.hgetex("42", ["missing"]);

  // @ts-expect-error getDelFields only accepts declared field names.
  void typedStore.hgetdel("42", ["missing"]);

  // @ts-expect-error fieldExpireTime only accepts declared field names.
  void typedStore.hexpiretime("42", "missing");

  // @ts-expect-error setFieldsEx values must match the declared field codecs.
  void typedStore.hsetex("42", { score: "7" });
}

void expectTypeErrorsOnly;

describe("hash-ext type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

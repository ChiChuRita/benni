import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import {
  createHyperLogLogStore,
  defineHyperLogLog,
  type HyperLogLogSchema
} from "../src/core/hyperloglog.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

describe("defineHyperLogLog", () => {
  it("formats keys with string, number, and bigint ids", () => {
    const visitors = defineHyperLogLog("visitors", codecs.string());

    expect(visitors.prefix).toBe("visitors");
    expect(visitors.key("42")).toBe("visitors:42");
    expect(visitors.key(42)).toBe("visitors:42");
    expect(visitors.key(42n)).toBe("visitors:42");
  });

  it("encodes values with the codec and exposes no decode", () => {
    const visitors = defineHyperLogLog(
      "visitors",
      codecs.json<{ ip: string }>()
    );

    expect(visitors.encode({ ip: "10.0.0.1" })).toBe('{"ip":"10.0.0.1"}');
    expect("decode" in visitors).toBe(false);
  });
});

describe("createHyperLogLogStore", () => {
  const visitors = defineHyperLogLog("visitors", codecs.string());

  it("emits PFADD and decodes 1/0 into booleans", async () => {
    const commands: RedisCommand[] = [];
    const store = createHyperLogLogStore(
      fakeClient(commands, [1, 0]),
      visitors
    );

    await expect(store.pfadd("page", ["alice", "bob"])).resolves.toBe(true);
    await expect(store.pfadd("page", ["alice"])).resolves.toBe(false);

    expect(commands).toEqual([
      ["PFADD", "visitors:page", "alice", "bob"],
      ["PFADD", "visitors:page", "alice"]
    ]);
  });

  it("treats empty pfadd as a no-op instead of creating the key", async () => {
    const commands: RedisCommand[] = [];
    const store = createHyperLogLogStore(fakeClient(commands, []), visitors);

    await expect(store.pfadd("page", [])).resolves.toBe(false);

    expect(commands).toEqual([]);
  });

  it("emits PFCOUNT for one key and for unions", async () => {
    const commands: RedisCommand[] = [];
    const store = createHyperLogLogStore(
      fakeClient(commands, [3, 5]),
      visitors
    );

    await expect(store.pfcount("page")).resolves.toBe(3);
    await expect(store.pfcount(["page", "blog"])).resolves.toBe(5);

    expect(commands).toEqual([
      ["PFCOUNT", "visitors:page"],
      ["PFCOUNT", "visitors:page", "visitors:blog"]
    ]);
  });

  it("emits PFMERGE and resolves void on OK", async () => {
    const commands: RedisCommand[] = [];
    const store = createHyperLogLogStore(
      fakeClient(commands, ["OK"]),
      visitors
    );

    await expect(
      store.pfmerge("total", ["page", "blog"])
    ).resolves.toBeUndefined();

    expect(commands).toEqual([
      ["PFMERGE", "visitors:total", "visitors:page", "visitors:blog"]
    ]);
  });

  it("emits DEL", async () => {
    const commands: RedisCommand[] = [];
    const store = createHyperLogLogStore(fakeClient(commands, [1]), visitors);

    await expect(store.del("page")).resolves.toBe(1);

    expect(commands).toEqual([["DEL", "visitors:page"]]);
  });

  it("rejects empty countMany ids without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createHyperLogLogStore(fakeClient(commands, []), visitors);

    await expect(store.pfcount([])).rejects.toThrow(
      "pfcount requires at least one id"
    );
    expect(commands).toEqual([]);
  });

  it("rejects empty merge sources without sending a command", async () => {
    const commands: RedisCommand[] = [];
    const store = createHyperLogLogStore(fakeClient(commands, []), visitors);

    await expect(store.pfmerge("total", [])).rejects.toThrow(
      "pfmerge requires at least one source id"
    );
    expect(commands).toEqual([]);
  });

  it("throws on unexpected PFMERGE replies", async () => {
    const store = createHyperLogLogStore(fakeClient([], [null]), visitors);

    await expect(store.pfmerge("total", ["page"])).rejects.toThrow(
      "Expected Redis PFMERGE to return OK"
    );
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const client = null as unknown as RedisClient;

const pageViews = defineHyperLogLog("views", codecs.json<{ userId: string }>());
const pageViewStore = createHyperLogLogStore(client, pageViews);

type PageViewKey = ReturnType<typeof pageViews.key<"home">>;
type PageViewSchemaHasDecode = "decode" extends keyof typeof pageViews
  ? true
  : false;
type PageViewAddValue = Parameters<typeof pageViewStore.pfadd>[1][number];
type PageViewAddResult = Awaited<ReturnType<typeof pageViewStore.pfadd>>;
type PageViewCount = Awaited<ReturnType<typeof pageViewStore.pfcount>>;
type PageViewCountManyIds = Parameters<typeof pageViewStore.pfcount>[0];
type PageViewMergeResult = Awaited<ReturnType<typeof pageViewStore.pfmerge>>;
type PageViewDelResult = Awaited<ReturnType<typeof pageViewStore.del>>;

type _PageViewKey = Expect<Equal<PageViewKey, "views:home">>;
type _PageViewSchemaHasDecode = Expect<Equal<PageViewSchemaHasDecode, false>>;
type _PageViewAddValue = Expect<Equal<PageViewAddValue, { userId: string }>>;
type _PageViewAddResult = Expect<Equal<PageViewAddResult, boolean>>;
type _PageViewCount = Expect<Equal<PageViewCount, number>>;
type _PageViewCountManyIds = Expect<
  Equal<
    PageViewCountManyIds,
    string | number | bigint | readonly (string | number | bigint)[]
  >
>;
type _PageViewMergeResult = Expect<Equal<PageViewMergeResult, void>>;
type _PageViewDelResult = Expect<Equal<PageViewDelResult, number>>;

const knownViews = defineHyperLogLog("known", codecs.string(), {
  ids: ["home", "blog"]
});
const knownViewStore = createHyperLogLogStore(client, knownViews);

type KnownViewId = Parameters<typeof knownViewStore.pfcount>[0];
type KnownViewSchema = typeof knownViews;
type _KnownViewId = Expect<
  Equal<KnownViewId, "home" | "blog" | readonly ("home" | "blog")[]>
>;
type _KnownViewSchema = Expect<
  Equal<
    KnownViewSchema,
    HyperLogLogSchema<string, "known", "home" | "blog", undefined>
  >
>;

function expectTypeErrorsOnly() {
  // @ts-expect-error hyperloglog values must match the codec input type.
  void pageViewStore.pfadd("home", ["alice"]);

  // @ts-expect-error hyperloglog schemas never expose decode.
  void pageViews.decode("{}");

  // @ts-expect-error known hyperloglogs only accept declared ids.
  void knownViewStore.pfcount("missing");

  // @ts-expect-error known hyperloglogs only accept declared ids in unions.
  void knownViewStore.pfcount(["home", "missing"]);

  // @ts-expect-error merge sources must use declared ids.
  void knownViewStore.pfmerge("home", ["missing"]);

  // @ts-expect-error merge requires a list of source ids.
  void knownViewStore.pfmerge("home", "blog");
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

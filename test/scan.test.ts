import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import {
  scanHash,
  scanKeys,
  scanKeyspace,
  scanSet,
  scanSortedSet
} from "../src/core/scan.js";
import {
  defineHash,
  defineKeyspace,
  defineSet,
  defineSortedSet
} from "../src/core/schemas.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) values.push(value);
  return values;
}

describe("scanKeys", () => {
  it("iterates pages until the cursor returns to 0", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [
      ["3", ["a", "b"]],
      ["0", ["c"]]
    ]);

    await expect(collect(scanKeys(client))).resolves.toEqual(["a", "b", "c"]);

    expect(commands).toEqual([
      ["SCAN", "0"],
      ["SCAN", "3"]
    ]);
  });

  it("accepts numeric cursors and resends them as strings", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [
      [3, ["a"]],
      [0, ["b"]]
    ]);

    await expect(collect(scanKeys(client))).resolves.toEqual(["a", "b"]);

    expect(commands).toEqual([
      ["SCAN", "0"],
      ["SCAN", "3"]
    ]);
  });

  it("emits MATCH, COUNT, and TYPE arguments", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["0", []]]);

    await expect(
      collect(scanKeys(client, { match: "user:*", count: 25, type: "string" }))
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["SCAN", "0", "MATCH", "user:*", "COUNT", 25, "TYPE", "string"]
    ]);
  });

  it("sends no command before iteration starts", () => {
    const commands: RedisCommand[] = [];
    void scanKeys(fakeClient(commands, []));

    expect(commands).toEqual([]);
  });

  it("sends no further command after the consumer breaks early", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["3", ["a", "b"]]]);
    const seen: string[] = [];

    for await (const key of scanKeys(client)) {
      seen.push(key);
      break;
    }

    expect(seen).toEqual(["a"]);
    expect(commands).toEqual([["SCAN", "0"]]);
  });

  it("rejects invalid counts before sending any command", async () => {
    const commands: RedisCommand[] = [];

    await expect(
      collect(scanKeys(fakeClient(commands, []), { count: 0 }))
    ).rejects.toThrow("count must be a positive safe integer");
    await expect(
      collect(scanKeys(fakeClient(commands, []), { count: 2.5 }))
    ).rejects.toThrow("count must be a positive safe integer");
    await expect(
      collect(scanKeys(fakeClient(commands, []), { count: Number.NaN }))
    ).rejects.toThrow("count must be a positive safe integer");

    expect(commands).toEqual([]);
  });

  it("throws on a non-array reply", async () => {
    await expect(collect(scanKeys(fakeClient([], ["nope"])))).rejects.toThrow(
      "Expected Redis SCAN to return [cursor, items]"
    );
  });

  it("throws on a wrong tuple length", async () => {
    await expect(collect(scanKeys(fakeClient([], [["0"]])))).rejects.toThrow(
      "Expected Redis SCAN to return [cursor, items]"
    );
    await expect(
      collect(scanKeys(fakeClient([], [["0", [], []]])))
    ).rejects.toThrow("Expected Redis SCAN to return [cursor, items]");
  });

  it("throws on a non-string non-number cursor", async () => {
    await expect(
      collect(scanKeys(fakeClient([], [[null, []]])))
    ).rejects.toThrow("Expected Redis SCAN to return string or number cursor");
  });

  it("throws when the items entry is not an array", async () => {
    await expect(
      collect(scanKeys(fakeClient([], [["0", "items"]])))
    ).rejects.toThrow("Expected Redis SCAN to return items array");
  });

  it("throws on non-string items", async () => {
    await expect(
      collect(scanKeys(fakeClient([], [["0", [1]]])))
    ).rejects.toThrow("Expected Redis SCAN item to return string");
  });
});

describe("scanKeyspace", () => {
  const users = defineKeyspace("user", codecs.json<{ name: string }>());

  it("defaults MATCH to the keyspace prefix and yields full keys", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["0", ["user:1", "user:2"]]]);

    await expect(collect(scanKeyspace(client, users))).resolves.toEqual([
      "user:1",
      "user:2"
    ]);

    expect(commands).toEqual([["SCAN", "0", "MATCH", "user:*"]]);
  });

  it("escapes glob metacharacters in the prefix", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["0", []]]);
    const hostile = defineKeyspace("user[1]*\\", codecs.string());

    await expect(collect(scanKeyspace(client, hostile))).resolves.toEqual([]);

    expect(commands).toEqual([["SCAN", "0", "MATCH", "user\\[1\\]\\*\\\\:*"]]);
  });

  it("lets callers override MATCH and forwards COUNT and TYPE", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [["0", []]]);

    await expect(
      collect(
        scanKeyspace(client, users, {
          match: "user:4*",
          count: 5,
          type: "string"
        })
      )
    ).resolves.toEqual([]);

    expect(commands).toEqual([
      ["SCAN", "0", "MATCH", "user:4*", "COUNT", 5, "TYPE", "string"]
    ]);
  });
});

describe("scanSet", () => {
  const tags = defineSet("tags", codecs.json<{ name: string }>());

  it("emits SSCAN and decodes members across pages", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [
      ["9", ['{"name":"a"}']],
      ["0", ['{"name":"b"}']]
    ]);

    await expect(
      collect(scanSet(client, tags, "42", { match: "*", count: 2 }))
    ).resolves.toEqual([{ name: "a" }, { name: "b" }]);

    expect(commands).toEqual([
      ["SSCAN", "tags:42", "0", "MATCH", "*", "COUNT", 2],
      ["SSCAN", "tags:42", "9", "MATCH", "*", "COUNT", 2]
    ]);
  });
});

describe("scanHash", () => {
  const profiles = defineHash("profile", {
    name: codecs.string(),
    score: codecs.number()
  });

  it("emits HSCAN, decodes declared fields, and skips undeclared ones", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [
      ["0", ["name", "beni", "legacy", "x", "score", "42"]]
    ]);

    await expect(
      collect(scanHash(client, profiles, "7", { match: "*", count: 10 }))
    ).resolves.toEqual([
      { field: "name", value: "beni" },
      { field: "score", value: 42 }
    ]);

    expect(commands).toEqual([
      ["HSCAN", "profile:7", "0", "MATCH", "*", "COUNT", 10]
    ]);
  });

  it("throws when the page holds an odd number of items", async () => {
    await expect(
      collect(scanHash(fakeClient([], [["0", ["name"]]]), profiles, "7"))
    ).rejects.toThrow("Expected Redis HSCAN to return field/value pairs");
  });
});

describe("scanSortedSet", () => {
  const board = defineSortedSet("board", codecs.string());

  it("emits ZSCAN and decodes flat member/score pairs", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [
      ["6", ["alice", "1"]],
      ["0", ["bob", "2.5"]]
    ]);

    await expect(
      collect(scanSortedSet(client, board, "1", { count: 2 }))
    ).resolves.toEqual([
      { member: "alice", score: 1 },
      { member: "bob", score: 2.5 }
    ]);

    expect(commands).toEqual([
      ["ZSCAN", "board:1", "0", "COUNT", 2],
      ["ZSCAN", "board:1", "6", "COUNT", 2]
    ]);
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

type AsyncItem<T> = T extends AsyncIterable<infer TItem> ? TItem : never;

const typeClient = null as unknown as RedisClient;
const typeUsers = defineKeyspace("user", codecs.string());
const typeTags = defineSet("tags", codecs.json<{ name: string }>());
const typeKnownTags = defineSet("known", codecs.string(), { ids: ["a", "b"] });
const typeProfiles = defineHash("profile", {
  name: codecs.string(),
  score: codecs.number()
});
const typeBoard = defineSortedSet("board", codecs.json<{ team: string }>());

const scanKeysIterable = scanKeys(typeClient);
const scanKeyspaceIterable = scanKeyspace(typeClient, typeUsers);
const scanSetIterable = scanSet(typeClient, typeTags, "42");
const scanHashIterable = scanHash(typeClient, typeProfiles, "42");
const scanSortedSetIterable = scanSortedSet(typeClient, typeBoard, "42");

type _ScanKeysItem = Expect<Equal<AsyncItem<typeof scanKeysIterable>, string>>;
type _ScanKeyspaceItem = Expect<
  Equal<AsyncItem<typeof scanKeyspaceIterable>, string>
>;
type _ScanSetItem = Expect<
  Equal<AsyncItem<typeof scanSetIterable>, { name: string }>
>;
type _ScanHashItem = Expect<
  Equal<
    AsyncItem<typeof scanHashIterable>,
    | { readonly field: "name"; readonly value: string }
    | { readonly field: "score"; readonly value: number }
  >
>;
type _ScanHashScoreEntry = Expect<
  Equal<
    Extract<AsyncItem<typeof scanHashIterable>, { field: "score" }>,
    { readonly field: "score"; readonly value: number }
  >
>;
type _ScanSortedSetItem = Expect<
  Equal<
    AsyncItem<typeof scanSortedSetIterable>,
    { readonly member: { team: string }; readonly score: number }
  >
>;

function expectTypeErrorsOnly() {
  // @ts-expect-error scan MATCH patterns must be strings.
  void scanKeys(typeClient, { match: 1 });

  // @ts-expect-error scan COUNT must be a number.
  void scanKeys(typeClient, { count: "10" });

  // @ts-expect-error member scans do not accept a TYPE filter.
  void scanSet(typeClient, typeTags, "42", { type: "string" });

  // @ts-expect-error known set schemas only accept declared ids.
  void scanSet(typeClient, typeKnownTags, "c");

  // @ts-expect-error member scans do not accept a TYPE filter.
  void scanHash(typeClient, typeProfiles, "42", { type: "hash" });

  // @ts-expect-error member scans do not accept a TYPE filter.
  void scanSortedSet(typeClient, typeBoard, "42", { type: "zset" });
}

void expectTypeErrorsOnly;

describe("scan type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

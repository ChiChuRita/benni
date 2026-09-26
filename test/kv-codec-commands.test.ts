import { describe, expect, it } from "vitest";
import { benni } from "../src/index.js";
import { json, kv, number, string } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

const COUNTER = ["incr", "incrby", "incrbyfloat", "decr", "decrby"];
const STRING = ["append", "getrange", "setrange", "strlen", "lcs"];

/**
 * A kv store carries its codec's commands: the counter commands for
 * `number()`, the string commands for `string()`. The type and the object
 * must agree exactly, so these check both halves: `@ts-expect-error` where a
 * command is absent, and the object's own keys at runtime.
 */
function expectTypes() {
  const views = kv("views", number());
  const texts = kv("text", string());
  const profiles = kv("profile", json<{ bio: string }>());
  const redis = benni({
    client: fakeClient([], []),
    schema: { views, texts, profiles }
  });

  void redis.query.views.incr("post-1");
  void redis.query.views.incr("post-1", { ttlMs: 60_000 });
  void redis.query.views.incrbyfloat("post-1", 0.5);
  void redis.query.texts.append("greeting", "hi");
  void redis.query.texts.lcs("a", "b", { len: true });

  // @ts-expect-error a json kv has no counter commands.
  void redis.query.profiles.incr("42");
  // @ts-expect-error a json kv has no string commands.
  void redis.query.profiles.append("42", "x");
  // @ts-expect-error a number() kv has no string commands.
  void redis.query.views.append("post-1", "x");
  // @ts-expect-error a string() kv has no counter commands.
  void redis.query.texts.incr("greeting");
  // @ts-expect-error ttlMs is a number of milliseconds.
  void redis.query.views.incr("post-1", { ttlMs: "1m" });
}
void expectTypes;

describe("kv codec commands", () => {
  const views = kv("views", number());
  const texts = kv("text", string());
  const profiles = kv("profile", json<{ bio: string }>());
  const redis = benni({
    client: fakeClient([], []),
    schema: { views, texts, profiles }
  });
  const keysOf = (store: object) => Object.keys(store);

  it("puts the counter commands on a number() kv, and only there", () => {
    expect(keysOf(redis.query.views)).toEqual(expect.arrayContaining(COUNTER));
    for (const command of STRING) {
      expect(keysOf(redis.query.views)).not.toContain(command);
    }
  });

  it("puts the string commands on a string() kv, and only there", () => {
    expect(keysOf(redis.query.texts)).toEqual(expect.arrayContaining(STRING));
    for (const command of COUNTER) {
      expect(keysOf(redis.query.texts)).not.toContain(command);
    }
  });

  it("adds neither to a kv over any other codec", () => {
    for (const command of [...COUNTER, ...STRING]) {
      expect(keysOf(redis.query.profiles)).not.toContain(command);
    }
  });

  it("gives every kv getex, decoded through its codec", async () => {
    const commands: Parameters<typeof fakeClient>[0] = [];
    const db = benni({
      client: fakeClient(commands, ['{"bio":"hi"}', "7", null]),
      schema: { views, profiles }
    });

    await expect(db.query.profiles.getex("42", 60)).resolves.toEqual({
      bio: "hi"
    });
    await expect(
      db.query.views.getex("post-1", { persist: true })
    ).resolves.toBe(7);
    await expect(db.query.views.getex("gone", 5)).resolves.toBeNull();
    expect(commands).toEqual([
      ["GETEX", "profile:42", "EX", 60],
      ["GETEX", "views:post-1", "PERSIST"],
      ["GETEX", "views:gone", "EX", 5]
    ]);
  });

  it("increments and sets a missing expiry in one script call", async () => {
    const commands: Parameters<typeof fakeClient>[0] = [];
    const db = benni({
      client: fakeClient(commands, ["sha", 1, 2]),
      schema: { views }
    });

    await expect(
      db.query.views.incr("post-1", { ttlMs: 60_000 })
    ).resolves.toBe(1);
    await expect(
      db.query.views.incr("post-1", { ttlMs: 60_000 })
    ).resolves.toBe(2);
    expect(commands[0]?.[0]).toBe("SCRIPT");
    expect(commands.slice(1)).toEqual([
      ["EVALSHA", "sha", 1, "views:post-1", 60_000],
      ["EVALSHA", "sha", 1, "views:post-1", 60_000]
    ]);
    // The script only expires a key that has no expiry, so a window started by
    // the first increment is not pushed back by the next one.
    expect(String(commands[0]?.[2])).toContain('"PTTL"');
  });

  it("refuses a ttlMs that is not a positive integer before sending", async () => {
    const commands: Parameters<typeof fakeClient>[0] = [];
    const db = benni({ client: fakeClient(commands, []), schema: { views } });

    for (const ttlMs of [0, -1, 1.5, Number.NaN]) {
      await expect(db.query.views.incr("post-1", { ttlMs })).rejects.toThrow(
        /ttlMs must be a positive safe integer/
      );
    }
    expect(commands).toEqual([]);
  });

  it("still serves the plain kv commands", async () => {
    const commands: Parameters<typeof fakeClient>[0] = [];
    const db = benni({
      client: fakeClient(commands, ["OK", "7"]),
      schema: { views }
    });

    await db.query.views.set("post-1", 7);
    expect(await db.query.views.get("post-1")).toBe(7);
    expect(commands).toEqual([
      ["SET", "views:post-1", "7"],
      ["GET", "views:post-1"]
    ]);
  });
});

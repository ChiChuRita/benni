import { describe, expect, it } from "vitest";
import { benni } from "../src/index.js";
import { kv, number, string } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

/**
 * `counter` and `string` are views over a kv keyspace, so their commands are not
 * on the kv store. The store carries a type-only hint per command whose error
 * text names the accessor to use instead. These are the compile-time assertions;
 * the runtime test below pins down that nothing was added to the object.
 */
function expectTypeErrorsOnly() {
  const views = kv("views", number());
  const texts = kv("text", string());
  const redis = benni(fakeClient([], []), { schema: { views, texts } });

  // @ts-expect-error INCR is a counter command: use redis.counter(schema).incr(id)
  void redis.query.views.incr("post-1");

  // @ts-expect-error INCRBY is a counter command: use redis.counter(schema).incrby(id)
  void redis.query.views.incrby("post-1", 5);

  // @ts-expect-error DECR is a counter command: use redis.counter(schema).decr(id)
  void redis.query.views.decr("post-1");

  // @ts-expect-error APPEND is a string command: use redis.string(schema).append(id)
  void redis.query.texts.append("greeting", "hi");

  // @ts-expect-error STRLEN is a string command: use redis.string(schema).strlen(id)
  void redis.query.texts.strlen("greeting");

  // the accessors themselves stay callable
  void redis.counter(views).incr("post-1");
  void redis.string(texts).append("greeting", "hi");
}

void expectTypeErrorsOnly;

describe("kv view hints", () => {
  it("adds nothing at runtime", () => {
    const views = kv("views", number());
    const redis = benni(fakeClient([], []), { schema: { views } });
    const store = redis.query.views as unknown as Record<string, unknown>;

    for (const command of [
      "incr",
      "incrby",
      "incrbyfloat",
      "decr",
      "decrby",
      "append",
      "getrange",
      "setrange",
      "strlen"
    ]) {
      expect(store[command]).toBeUndefined();
      expect(Object.keys(store)).not.toContain(command);
    }
  });

  it("still serves the commands a kv keyspace does have", async () => {
    const commands: Parameters<typeof fakeClient>[0] = [];
    const views = kv("views", number());
    const redis = benni(fakeClient(commands, ["OK", "7"]), {
      schema: { views }
    });

    await redis.query.views.set("post-1", 7);
    expect(await redis.query.views.get("post-1")).toBe(7);
    expect(commands).toEqual([
      ["SET", "views:post-1", "7"],
      ["GET", "views:post-1"]
    ]);
  });
});

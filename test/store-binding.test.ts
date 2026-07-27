import { describe, expect, it } from "vitest";
import { STORE } from "../src/core/store.js";
import { beni } from "../src/database.js";
import * as s from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// Schemas carry their store factory on a non-enumerable symbol, which is what
// lets `beni()` dispatch without naming every store (and so lets a bundler
// drop the kinds an app never declares). These tests pin the two consequences:
// the binding must be invisible to ordinary object inspection, and a schema
// that has lost it must fail loudly rather than silently resolve to nothing.

const users = s.hash("user", { name: s.string(), score: s.number() });

describe("schema store bindings", () => {
  it("does not show up in keys, spread, or JSON", () => {
    expect(Object.keys(users)).toEqual(["kind", "prefix", "fields", "key"]);
    expect(JSON.parse(JSON.stringify(users))).toEqual({
      kind: "hash",
      prefix: "user",
      fields: { name: {}, score: {} }
    });
    expect(Object.getOwnPropertyNames(users)).not.toContain(
      "Symbol(beni.store)"
    );
  });

  it("is attached for every kind a schema builder produces", () => {
    const built: unknown[] = [
      s.kv("profile", s.string()),
      s.hash("user", { name: s.string() }),
      s.set("roles", s.string()),
      s.list("feed", s.string()),
      s.zset("board", s.string()),
      s.stream("events", { body: s.string() }),
      s.bitmap("flags"),
      s.geo("places", s.string()),
      s.hll("uniques", s.string()),
      s.channel("news", s.string()),
      s.pattern("news:*", s.string()),
      s.script("noop", {
        keys: ["k"],
        args: {},
        returns: s.number(),
        lua: "return 1"
      })
    ];
    for (const schema of built) {
      expect((schema as Record<symbol, unknown>)[STORE]).toBeDefined();
    }
  });

  it("rejects a copied schema at bind time, naming the export", () => {
    const client = fakeClient([], []);
    // Object spread drops the symbol — the one thing that used to work and
    // now does not, so it has to fail with an actionable message.
    expect(() => beni(client, { schema: { users: { ...users } } })).toThrow(
      /schema\.users .*no store binding/s
    );
  });

  it("rejects a copied schema passed to an accessor", () => {
    const redis = beni(fakeClient([], []));
    expect(() => redis.hash({ ...users })).toThrow(/hash schema/);
  });

  it("still ignores non-schema exports on the schema module", () => {
    const redis = beni(fakeClient([], []), {
      schema: { users, notASchema: { hello: "world" }, alsoNot: 42 }
    });
    expect(Object.keys(redis.query)).toEqual(["users"]);
  });
});

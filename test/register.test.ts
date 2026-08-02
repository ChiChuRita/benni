import { describe, expect, it } from "vitest";
import type { RedisCommand } from "../src/core/types.js";
import { type Benni, benni } from "../src/index.js";
import { hash, number, string } from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// Registering the schema module once is what makes the bare `Benni` a fully
// typed handle, so a helper signature never has to repeat `typeof schema`.
//
// This augmentation is program-wide, which is the point: it is exactly what an
// app writes in one file. Nothing else in this repo names the bare `Benni`, so
// this file owning the registration is safe.

const users = hash("user", { name: string(), score: number() });
const board = hash("board", { title: string() });

const schema = { users, board };

declare module "../src/index.js" {
  interface Register {
    schema: typeof schema;
  }
}

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;

describe("Register", () => {
  it("types the bare Benni handle from the registered schema", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, [1]), { schema });

    // No generic argument: the registration supplies it.
    async function bump(handle: Benni, id: string) {
      return handle.query.users.hset(id, { name: "Ada", score: 10 });
    }

    await bump(redis, "42");
    expect(commands[0]?.[0]).toBe("HSET");
  });

  it("resolves the registry to the same type an explicit generic does", () => {
    type Registered = Benni["query"];
    type Explicit = Benni<typeof schema>["query"];
    type _Same = Expect<Equal<Registered, Explicit>>;
    type _HasBoth = Expect<Equal<keyof Registered, "users" | "board">>;

    expect(true).toBe(true);
  });
});

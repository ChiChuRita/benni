import { describe, expect, it } from "vitest";
import type { RedisCommand } from "../src/core/types.js";
import { type Benni, benni } from "../src/index.js";
import {
  budget,
  cache,
  hash,
  idempotency,
  json,
  lock,
  number,
  queue,
  ratelimit,
  semaphore,
  string
} from "../src/schema.js";
import { fakeClient } from "./fake-client.js";

// The primitives declare themselves as schema values, so they land in
// `redis.query` next to the data stores and carry their own configuration.

type Profile = { name: string; score: number };

const users = hash("user", { name: string(), score: number() });
const profiles = cache("profile", {
  ttlMs: 60_000,
  codec: json<Profile>()
});
const apiLimit = ratelimit("api", { limit: 10, windowMs: 60_000 });
const orderLocks = lock("order", { ttlMs: 10_000 });
const gpuSlots = semaphore("gpu", { limit: 4 });
const generate = queue<{ prompt: string }, string>("generate");
const charges = idempotency<{ id: string }>("charge");
const tokens = budget("tokens", { limit: 1_000_000, windowMs: 86_400_000 });

const schema = {
  users,
  profiles,
  apiLimit,
  orderLocks,
  gpuSlots,
  generate,
  charges,
  tokens
};

function bind(commands: RedisCommand[], replies: unknown[]) {
  return benni(fakeClient(commands, replies as never), { schema });
}

describe("primitives in the query registry", () => {
  it("reaches a cache by its export name, with its declared prefix", async () => {
    const commands: RedisCommand[] = [];
    const redis = bind(commands, [JSON.stringify({ name: "Ada", score: 10 })]);

    const hit = await redis.query.profiles.peek("42");

    expect(hit).toEqual({ name: "Ada", score: 10 });
    expect(commands).toEqual([["GET", "profile:{42}"]]);
  });

  it("reaches a limiter by its export name", async () => {
    const commands: RedisCommand[] = [];
    const redis = bind(commands, ["sha", [1, 9, 1000, 0]]);

    const result = await redis.query.apiLimit.check("user:1");

    expect(result).toMatchObject({ success: true, limit: 10, remaining: 9 });
    // The declared prefix reached the key the script was handed.
    expect(commands.at(-1)).toContain("api:user:1");
  });

  it("reaches a lock by its export name", async () => {
    const commands: RedisCommand[] = [];
    const redis = bind(commands, ["OK"]);

    const handle = await redis.query.orderLocks.acquire("42");

    expect(handle?.key).toBe("order:42");
    expect(commands[0]?.slice(3)).toEqual(["NX", "PX", 10_000]);
  });

  it("reaches a semaphore, a queue, a budget, and an idempotency key", () => {
    const redis = bind([], []);

    // Resolving each one is the assertion: a missing binding throws here.
    expect(typeof redis.query.gpuSlots.acquire).toBe("function");
    expect(typeof redis.query.generate.enqueue).toBe("function");
    expect(typeof redis.query.tokens.reserve).toBe("function");
    expect(typeof redis.query.charges.run).toBe("function");
  });

  it("binds every primitive to the handle's own client", async () => {
    const first: RedisCommand[] = [];
    const second: RedisCommand[] = [];
    const a = bind(first, ["OK"]);
    const b = bind(second, ["OK"]);

    await a.query.orderLocks.acquire("42");

    expect(first).toHaveLength(1);
    expect(second).toHaveLength(0);
    // Two handles over one schema module never share a connection.
    expect(a.query.orderLocks).not.toBe(b.query.orderLocks);
  });

  it("refuses a copied primitive schema at bind time, naming the export", () => {
    expect(() =>
      benni(fakeClient([], []), { schema: { apiLimit: { ...apiLimit } } })
    ).toThrow(/schema\.apiLimit/);
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;

describe("primitive schema types", () => {
  it("carries the value type from the declaration to the store", async () => {
    const commands: RedisCommand[] = [];
    const redis = bind(commands, [JSON.stringify({ name: "Ada", score: 10 })]);

    const hit = await redis.query.profiles.peek("42");
    type _Hit = Expect<Equal<typeof hit, Profile | null>>;
    // The queue's payload and result travel the same way.
    type _Payload = Expect<
      Equal<typeof generate.$inferInput, { prompt: string }>
    >;
    type _Result = Expect<Equal<typeof generate.$inferOutput, string>>;

    expect(hit?.name).toBe("Ada");
  });

  it("keeps the $infer anchors type-only", () => {
    expect(Object.keys(profiles)).not.toContain("$inferOutput");
    expect((profiles as Record<string, unknown>).$inferOutput).toBeUndefined();
  });

  it("types a handle through Benni<typeof schema>", async () => {
    const commands: RedisCommand[] = [];
    const redis: Benni<typeof schema> = bind(commands, ["OK"]);

    // The registry is reachable through the named handle type, primitives
    // included — this is the signature a helper function would carry.
    async function takeLock(handle: Benni<typeof schema>) {
      return handle.query.orderLocks.acquire("42");
    }

    await expect(takeLock(redis)).resolves.not.toBeNull();
  });
});

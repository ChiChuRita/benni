import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient, RedisCommand } from "../src/core/index.js";
import { cacheHandler } from "../src/next/index.js";
import { node } from "../src/node/index.js";
import { fakeClient } from "./fake-client.js";

describe("cacheHandler tag sets", () => {
  it("indexes a tag with one atomic EVAL carrying the entry TTL", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK", 1, 1]);

    await new (cacheHandler({ client }))().set(
      "/blog",
      { kind: "PAGE" },
      { revalidate: 60, tags: ["posts", "layout"] }
    );

    // SADD and the expiry decision have to run together: whether the set is
    // new is the only thing that distinguishes "needs its first TTL" from
    // "was deliberately made permanent", and a plain EXPIRE ... GT after the
    // SADD can never install a TTL at all.
    const tagCommands = commands.slice(1);
    expect(tagCommands.map((command) => command[0])).toEqual(["EVAL", "EVAL"]);
    expect(tagCommands[0]?.slice(2)).toEqual([
      1,
      "{next-cache}:tag:posts",
      "/blog",
      60
    ]);
    expect(tagCommands[1]?.slice(2)).toEqual([
      1,
      "{next-cache}:tag:layout",
      "/blog",
      60
    ]);
  });

  it("passes a zero TTL for an entry that never expires", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK", 1]);

    await new (cacheHandler({ client }))().set(
      "/page",
      { kind: "PAGE" },
      { revalidate: false, tags: ["static"] }
    );

    expect(commands[1]?.slice(2)).toEqual([
      1,
      "{next-cache}:tag:static",
      "/page",
      0
    ]);
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("cacheHandler tag sets (live)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  /** A fresh namespace per test, so nothing here depends on run order. */
  const uid = () => `hunt-next:${run}:${Math.random().toString(36).slice(2)}`;
  const prefixes: string[] = [];
  const handlerFor = (prefix: string) => {
    prefixes.push(prefix);
    return new (cacheHandler({ client, prefix }))();
  };
  const tagTtl = async (prefix: string, tag: string) =>
    Number(await client.send(["TTL", `{${prefix}}:tag:${tag}`]));

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    // Permanent tag sets would otherwise survive the run.
    for (const prefix of prefixes) {
      await client.send(["DEL", `{${prefix}}:tag:posts`]);
    }
    await client.close();
  });

  it("gives a brand new tag set the entry's TTL", async () => {
    const prefix = uid();

    await handlerFor(prefix).set(
      "/a",
      { kind: "PAGE" },
      { revalidate: 60, tags: ["posts"] }
    );

    // -1 here is the whole bug: the set exists forever while the entries it
    // names expire out from under it.
    expect(await tagTtl(prefix, "posts")).toBeGreaterThan(0);
    expect(await tagTtl(prefix, "posts")).toBeLessThanOrEqual(60);
  });

  it("extends a tag set's TTL but never shortens it", async () => {
    const prefix = uid();
    const handler = handlerFor(prefix);

    await handler.set(
      "/a",
      { kind: "PAGE" },
      { revalidate: 60, tags: ["posts"] }
    );
    await handler.set(
      "/b",
      { kind: "PAGE" },
      { revalidate: 5, tags: ["posts"] }
    );
    // The short-lived entry must not take the set down with it.
    expect(await tagTtl(prefix, "posts")).toBeGreaterThan(5);

    await handler.set(
      "/c",
      { kind: "PAGE" },
      { revalidate: 300, tags: ["posts"] }
    );
    expect(await tagTtl(prefix, "posts")).toBeGreaterThan(60);
  });

  it("keeps a tag set permanent once an entry that never expires joins it", async () => {
    const prefix = uid();
    const handler = handlerFor(prefix);

    await handler.set(
      "/forever",
      { kind: "PAGE" },
      { revalidate: false, tags: ["posts"] }
    );
    expect(await tagTtl(prefix, "posts")).toBe(-1);

    // Bootstrapping the first TTL with EXPIRE ... NX would re-expire the set
    // here, and /forever could then never be revalidated by tag again.
    await handler.set(
      "/b",
      { kind: "PAGE" },
      { revalidate: 60, tags: ["posts"] }
    );
    expect(await tagTtl(prefix, "posts")).toBe(-1);

    await handler.revalidateTag("posts");
    expect(await client.send(["EXISTS", `{${prefix}}:tag:posts`])).toBe(0);
  });

  it("drops a tag set once its last entry has expired", async () => {
    const prefix = uid();

    await handlerFor(prefix).set(
      "/a",
      { kind: "PAGE" },
      { revalidate: 1, tags: ["posts"] }
    );
    await new Promise((resolve) => setTimeout(resolve, 1_500));

    expect(await client.send(["EXISTS", `{${prefix}}:entry:/a`])).toBe(0);
    expect(await client.send(["EXISTS", `{${prefix}}:tag:posts`])).toBe(0);
  });
});

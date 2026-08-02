import { describe, expect, it } from "vitest";
import * as z from "zod";
import * as mini from "zod/mini";
import type { RedisCommand } from "../src/core/index.js";
import { ReplyShapeError, ValidationError } from "../src/core/index.js";
import { benni } from "../src/index.js";
import type { InferOutput } from "../src/schema.js";
import { kv } from "../src/schema.js";
import { zodCodec, zodJson } from "../src/zod/index.js";
import { fakeClient } from "./fake-client.js";

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;

const isoDate = z.codec(z.iso.datetime(), z.date(), {
  decode: (iso) => new Date(iso),
  encode: (date) => date.toISOString()
});

describe("zodCodec", () => {
  it("round-trips rich types through their encoded string form", () => {
    const codec = zodCodec(isoDate);
    type _Out = Expect<Equal<ReturnType<typeof codec.decode>, Date>>;
    type _In = Expect<Equal<Parameters<typeof codec.encode>[0], Date>>;

    const stored = codec.encode(new Date("2026-07-12T10:00:00.000Z"));
    expect(stored).toBe("2026-07-12T10:00:00.000Z");
    const revived = codec.decode(stored);
    expect(revived).toBeInstanceOf(Date);
    expect(revived.toISOString()).toBe("2026-07-12T10:00:00.000Z");

    // The encoded side must be a string schema.
    // @ts-expect-error — z.number() encodes to a number, not a string
    zodCodec(z.number());
  });

  it("validates both directions: ValidationError on write, ReplyShapeError on read", () => {
    const codec = zodCodec(z.email());

    expect(() => codec.encode("not-an-email")).toThrow(ValidationError);
    expect(() => codec.encode("not-an-email")).toThrow(/could not encode/);

    let caught: unknown;
    try {
      codec.decode("still-not-an-email");
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ReplyShapeError);
    expect((caught as ReplyShapeError).reply).toBe("still-not-an-email");
    expect((caught as Error).message).toContain("Invalid email address");
  });

  it("accepts zod/mini schemas (built on zod/v4/core)", () => {
    const codec = zodCodec(mini.string());
    expect(codec.encode("hello")).toBe("hello");
    expect(codec.decode("hello")).toBe("hello");
  });

  it("rejects async schemas with a synchronous-schema error", () => {
    const codec = zodCodec(z.string().refine(async () => true));
    expect(() => codec.encode("x")).toThrow(ValidationError);
    expect(() => codec.decode("x")).toThrow(/synchronous zod schema/);
  });
});

describe("zodJson", () => {
  const user = z.object({ name: z.string(), created: isoDate });
  const users = kv("user", zodJson(user));

  it("stores validated JSON and revives codec fields, end to end", async () => {
    type _Out = Expect<
      Equal<InferOutput<typeof users>, { name: string; created: Date }>
    >;

    const wire = '{"name":"ada","created":"2020-01-01T00:00:00.000Z"}';
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, ["OK", wire]));

    await redis.kv(users).set("u1", {
      name: "ada",
      created: new Date("2020-01-01T00:00:00.000Z")
    });
    expect(commands).toEqual([["SET", "user:u1", wire]]);

    const found = await redis.kv(users).get("u1");
    expect(found?.created).toBeInstanceOf(Date);
    expect(found?.created.toISOString()).toBe("2020-01-01T00:00:00.000Z");
  });

  it("rejects an invalid write before anything is sent", async () => {
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, []));
    await expect(
      redis.kv(users).set("u1", { name: 42, created: new Date() } as never)
    ).rejects.toThrow(ValidationError);
    expect(commands).toEqual([]);
  });

  it("surfaces decode failures as ReplyShapeError naming the issue path", () => {
    const codec = zodJson(user);

    expect(() => codec.decode("{not json")).toThrow(ReplyShapeError);
    expect(() => codec.decode('{"name":"ada","created":"garbage"}')).toThrow(
      /created: Invalid ISO datetime/
    );

    let caught: unknown;
    try {
      codec.decode('{"name":"ada","created":"garbage"}');
    } catch (error) {
      caught = error;
    }
    expect((caught as ReplyShapeError).reply).toBe(
      '{"name":"ada","created":"garbage"}'
    );
  });

  it("rejects values JSON.stringify cannot represent", () => {
    const codec = zodJson(z.undefined());
    expect(() => codec.encode(undefined)).toThrow(ValidationError);
  });
});

import { describe, expect, it } from "vitest";
import * as z from "zod";
import type { RedisCommand } from "../src/core/index.js";
import { ValidationError } from "../src/core/index.js";
import { benni } from "../src/index.js";
import { kv } from "../src/schema.js";
import { zodCodec, zodJson } from "../src/zod/index.js";
import { fakeClient } from "./fake-client.js";

describe("zodJson non-finite numbers", () => {
  it("rejects NaN / Infinity instead of storing them as JSON null", () => {
    const codec = zodJson(z.record(z.string(), z.unknown()));

    expect(() => codec.encode({ rate: Number.POSITIVE_INFINITY })).toThrow(
      ValidationError
    );
    expect(() => codec.encode({ n: Number.NaN })).toThrow(/non-finite/);
    // A top-level non-finite would otherwise store the bare string "null",
    // making a written key decode identically to a missing one.
    expect(() => zodJson(z.custom<number>()).encode(Number.NaN)).toThrow(
      ValidationError
    );
  });

  it("sends nothing when a write carries a non-finite number", async () => {
    const scores = kv("score", zodJson(z.record(z.string(), z.unknown())));
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, ["OK"]));

    await expect(
      redis.kv(scores).set("s1", { rate: Number.POSITIVE_INFINITY })
    ).rejects.toThrow(ValidationError);
    expect(commands).toEqual([]);
  });
});

describe("zod encode failures stay ValidationError", () => {
  it("wraps a zod encode error that is not an issue list", () => {
    // A one-way .transform() has no encode direction, so zod raises
    // $ZodEncodeError, which does not even extend TypeError.
    const codec = zodCodec(
      z.string().transform((s) => s.length) as unknown as z.ZodType<
        unknown,
        string
      >
    );

    let caught: unknown;
    try {
      codec.encode("abc");
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ValidationError);
    expect(caught).toBeInstanceOf(TypeError);
    expect((caught as Error).message).toMatch(/could not encode/);
  });

  it("wraps BigInt and circular values from JSON.stringify", () => {
    let caught: unknown;
    try {
      zodJson(z.object({ n: z.bigint() })).encode({ n: 1n });
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ValidationError);
    expect((caught as Error).message).toMatch(/BigInt/);

    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(() => zodJson(z.any()).encode(circular)).toThrow(ValidationError);
  });
});

describe("zodCodec encoded side", () => {
  it("rejects a schema that encodes to a non-string instead of writing [object Object]", async () => {
    const codec = zodCodec(z.any());

    expect(() => codec.encode({ a: 1 })).toThrow(ValidationError);
    expect(() => codec.encode({ a: 1 })).toThrow(/must be a string schema/);
    expect(codec.encode("still-fine")).toBe("still-fine");

    const blobs = kv("blob", zodCodec(z.any()));
    const commands: RedisCommand[] = [];
    const redis = benni(fakeClient(commands, ["OK"]));
    await expect(redis.kv(blobs).set("b1", { a: 1 })).rejects.toThrow(
      ValidationError
    );
    expect(commands).toEqual([]);
  });
});

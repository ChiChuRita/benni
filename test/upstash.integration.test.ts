import { Buffer } from "node:buffer";
import { createClient } from "redis";
import { describe, expect, it } from "vitest";
import { RedisServerError } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import { upstash } from "../src/upstash/index.js";
import { expectRedisClientContract } from "./redis-contract.js";

// Point at any Upstash-REST-compatible endpoint. In CI this is
// hiett/serverless-redis-http (SRH) in front of a plain Redis container:
//   BENNI_UPSTASH_URL=http://127.0.0.1:8079 BENNI_UPSTASH_TOKEN=example_token
const upstashUrl = process.env.BENNI_UPSTASH_URL;
const upstashToken = process.env.BENNI_UPSTASH_TOKEN ?? "example_token";
const describeUpstash = upstashUrl ? describe : describe.skip;
// The Redis behind the REST endpoint, for writing bytes REST cannot send.
const redisUrl = process.env.BENNI_REDIS_URL;
const itWithRedis = redisUrl ? it : it.skip;

describeUpstash("upstash", () => {
  it("passes the shared Redis client contract over HTTP", async () => {
    expect(upstashUrl).toBeDefined();
    // session is intentionally absent, so the contract test's blocking/WATCH
    // block is skipped; transaction (/multi-exec) and every store run.
    await expectRedisClientContract(
      () =>
        upstash({
          url: upstashUrl as string,
          token: upstashToken
        }),
      {
        // SRH answers a failed /multi-exec with an empty-bodied HTTP 500, so the
        // rejection carries no Redis error to normalize. Verified against
        // hiett/serverless-redis-http:latest in front of redis:8; the /pipeline
        // path on the same server does return `{ error: "WRONGTYPE ..." }`, which
        // is why only the transaction assertion narrows.
        transactionErrorsCarryNoReply: true
      }
    );
  });

  it("round-trips multi-byte UTF-8 text", async () => {
    const client = upstash({ url: upstashUrl as string, token: upstashToken });
    const key = `benni:test:upstash:utf8:${Date.now()}`;
    const text = "héllo 🎉 — 日本語";
    try {
      await client.send(["SET", key, text]);
      await expect(client.send(["GET", key])).resolves.toBe(text);
      await expect(
        client.pipeline([
          ["GET", key],
          ["STRLEN", key]
        ])
      ).resolves.toEqual([text, new TextEncoder().encode(text).length]);
    } finally {
      await client.send(["DEL", key]);
      await client.close();
    }
  });

  // Without Upstash-Encoding: base64 the server cannot put these bytes in
  // JSON and serverless-redis-http answers the GET with an empty body.
  itWithRedis(
    "reads a value that is not UTF-8, decoded exactly as a TCP adapter decodes it",
    async () => {
      const key = `benni:test:upstash:binary:${Date.now()}`;
      const writer = await createClient({ url: redisUrl }).connect();
      const tcp = node({ url: redisUrl });
      const client = upstash({
        url: upstashUrl as string,
        token: upstashToken
      });
      try {
        await writer.set(
          key,
          Buffer.from([0xff, 0xfe, 0x00, 0x62, 0x69, 0x6e])
        );
        const overTcp = await tcp.send(["GET", key]);
        await expect(client.send(["GET", key])).resolves.toBe(overTcp);
        expect(overTcp).toBe("\uFFFD\uFFFD\u0000bin");
      } finally {
        await writer.del(key);
        await writer.close();
        await tcp.close();
        await client.close();
      }
    }
  );

  it("reports a rejected token as a transport error, not a Redis reply", async () => {
    const client = upstash({ url: upstashUrl as string, token: "wrong-token" });
    const error = await client.send(["PING"]).then(
      () => undefined,
      (thrown: unknown) => thrown
    );
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(RedisServerError);
    expect((error as Error).message).toMatch(/^Upstash HTTP 401/);
  });

  it("stays closed after close()", async () => {
    const client = upstash({ url: upstashUrl as string, token: upstashToken });
    await expect(client.send(["PING"])).resolves.toBe("PONG");
    await client.close();
    await expect(client.send(["PING"])).rejects.toThrow(/client is closed/);
  });
});

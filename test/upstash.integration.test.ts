import { describe, expect, it } from "vitest";
import { upstash } from "../src/upstash/index.js";
import { expectRedisClientContract } from "./redis-contract.js";

// Point at any Upstash-REST-compatible endpoint. In CI this is
// hiett/serverless-redis-http (SRH) in front of a plain Redis container:
//   BENNI_UPSTASH_URL=http://127.0.0.1:8079 BENNI_UPSTASH_TOKEN=example_token
const upstashUrl = process.env.BENNI_UPSTASH_URL;
const upstashToken = process.env.BENNI_UPSTASH_TOKEN ?? "example_token";
const describeUpstash = upstashUrl ? describe : describe.skip;

describeUpstash("upstash", () => {
  it("passes the shared Redis client contract over HTTP", async () => {
    expect(upstashUrl).toBeDefined();
    // session is intentionally absent, so the contract test's blocking/WATCH
    // block is skipped; transaction (/multi-exec) and every store run.
    await expectRedisClientContract(() =>
      Promise.resolve(
        upstash({
          url: upstashUrl as string,
          token: upstashToken
        })
      )
    );
  });
});

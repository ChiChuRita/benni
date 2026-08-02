import assert from "node:assert/strict";
import { benni } from "benni";
import { node } from "benni/node";
import { json, kv } from "benni/schema";

const url = Deno.env.get("BENNI_REDIS_URL");
if (!url) throw new Error("BENNI_REDIS_URL is required");

const client = await node({ url });
const profiles = kv(
  `benni:deno:${Date.now()}:${crypto.randomUUID()}`,
  json()
);
const redis = benni(client, { schema: { profiles } });

try {
  const profile = { name: "Ada", score: 10 };
  await redis.query.profiles.set("42", profile);
  assert.deepEqual(await redis.query.profiles.get("42"), profile);
} finally {
  await redis.query.profiles.del("42");
  await client.close?.();
}

console.log("Deno + node-redis adapter smoke test passed");

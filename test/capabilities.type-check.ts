// Compile-time only, like types.type-check.ts: a handle's type offers exactly
// what its client can do. Every `@ts-expect-error` is an assertion that the
// line does NOT compile; `pnpm typecheck` fails if one of them starts to.
import type { BunClient } from "../src/bun/index.js";
import type { FullRedisClient, RedisClient } from "../src/core/index.js";
import { type AnyBenni, type Benni, benni } from "../src/index.js";
import { channel, hash, json, number, pattern, string } from "../src/schema.js";
import type { UpstashClient } from "../src/upstash/index.js";

const users = hash("user", { name: string(), score: number() });
const chat = channel("chat", json<{ text: string }>());
const allChats = pattern("chat:*", json<{ text: string }>());
const schema = { users, chat, allChats };

declare const full: FullRedisClient;
declare const bunClient: BunClient;
declare const upstashClient: UpstashClient;
declare const custom: RedisClient;

// node / ioredis: everything.
const onNode = benni({ client: full, schema });
void onNode.session;
void onNode.watch;
void onNode.query.chat.subscribe;
void onNode.query.allChats.subscribe;
void onNode.pubsub.pattern(allChats).subscribe;
void onNode.pubsub.channel(chat, 1).subscribe;

// Bun: sessions and channels, no patterns.
const onBun = benni({ client: bunClient, schema });
void onBun.session;
void onBun.query.chat.subscribe;
// @ts-expect-error Bun cannot pattern-subscribe
void onBun.pubsub.pattern;
// @ts-expect-error a pattern resource has nothing but subscribe, so it is dropped
void onBun.query.allChats;

// Upstash: publish only, no sessions.
const onUpstash = benni({ client: upstashClient, schema });
void onUpstash.query.chat.publish({ text: "hi" });
void onUpstash.query.chat.at(7).publish({ text: "hi" });
void onUpstash.pubsub.channel(chat, 7).publish({ text: "hi" });
// @ts-expect-error HTTP holds no session
void onUpstash.session;
// @ts-expect-error WATCH needs a session
void onUpstash.watch;
// @ts-expect-error HTTP holds no subscriber connection
void onUpstash.query.chat.subscribe;
// @ts-expect-error nor on a per-entity channel
void onUpstash.pubsub.channel(chat, 7).stream;
// @ts-expect-error nor pattern subscriptions
void onUpstash.pubsub.pattern;

// A hand-written client with only the required members gets the base surface.
const onCustom = benni({ client: custom });
// @ts-expect-error optional capabilities are not assumed
void onCustom.session;

// A handle over another handle keeps the first one's client type.
const shared = benni({ client: onUpstash, schema });
// @ts-expect-error still Upstash underneath
void shared.session;

// `Benni` assumes the full client; other adapters name theirs.
const typedNode: Benni<typeof schema> = onNode;
const typedUpstash: Benni<typeof schema, UpstashClient> = onUpstash;
// @ts-expect-error an Upstash handle is not a full one
const wrongly: Benni<typeof schema> = onUpstash;
void [typedNode, typedUpstash, wrongly];

// AnyBenni takes every handle.
function library(redis: AnyBenni) {
  return redis.raw.send(["PING"]);
}
void [library(onNode), library(onBun), library(onUpstash), library(onCustom)];

// The schema is required once the type names one: the 0.1 hole where
// `benni<typeof schema>({ client })` compiled into an empty `redis.query`.
// @ts-expect-error schema missing
benni<typeof schema>({ client: full });
benni<typeof schema>({ client: full, schema });

// The positional form is gone.
// @ts-expect-error one config object only
benni(full, { schema });

// Promise and factory sources are gone: adapters return clients synchronously.
// @ts-expect-error no promise sources
benni({ client: Promise.resolve(full) });
// @ts-expect-error no factory sources
benni({ client: () => full });

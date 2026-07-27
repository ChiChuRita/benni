import { describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import {
  codecs,
  createCounterStore,
  createHashStore,
  createKeyValueStore,
  createListStore,
  createPubSubPublisher,
  createSetStore,
  createSortedSetStore,
  createStringStore,
  defineHash,
  defineKeyspace,
  defineList,
  definePubSubChannel,
  defineSet,
  defineSortedSet,
  defineStream,
  type InferHashInput,
  type InferHashOutput,
  type PendingStreamEntry,
  type RedisKey,
  type StreamEntry
} from "../src/core/index.js";
import type { SameSlotScriptKeys } from "../src/core/keys.js";
import { beni } from "../src/index.js";
import {
  hash as schemaHash,
  json as schemaJson,
  kv as schemaKv,
  number as schemaNumber,
  script as schemaScript,
  string as schemaString
} from "../src/schema.js";

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const client = null as unknown as RedisClient;

const db = beni(client);

const schemaProfiles = schemaKv(
  "schema-profile",
  schemaJson<{ name: string; score: number }>()
);
const schemaProfileStore = db.kv(schemaProfiles);
type SchemaProfileSetValue = Parameters<typeof schemaProfileStore.set>[1];
type SchemaProfileGetValue = Awaited<ReturnType<typeof schemaProfileStore.get>>;
type _SchemaProfileSetValue = Expect<
  Equal<SchemaProfileSetValue, { name: string; score: number }>
>;
type _SchemaProfileGetValue = Expect<
  Equal<SchemaProfileGetValue, { name: string; score: number } | null>
>;

function expectSetOverloadReturnTypes() {
  const value = { name: "Ada", score: 1 };
  const plain: Promise<void> = schemaProfileStore.set("42", value);
  const withTtl: Promise<void> = schemaProfileStore.set("42", value, {
    ttlSeconds: 60
  });
  const nx: Promise<boolean> = schemaProfileStore.set("42", value, {
    nx: true
  });
  const xx: Promise<boolean> = schemaProfileStore.set("42", value, {
    xx: true
  });
  void plain;
  void withTtl;
  void nx;
  void xx;
}

void expectSetOverloadReturnTypes;

const schemaUsers = schemaHash("schema-user", {
  name: schemaString(),
  score: schemaNumber()
});
const schemaUserStore = db.hash(schemaUsers);
// hset/hget are overloaded (whole-record vs single-field); Parameters/ReturnType
// resolve to the last overload, so probe the whole-record forms via calls.
const schemaUserRecordSet = (value: { name: string; score: number }) =>
  schemaUserStore.hset("42", value);
const schemaUserRecordGet = () => schemaUserStore.hget("42");
type SchemaUserSetValue = Parameters<typeof schemaUserRecordSet>[0];
type SchemaUserGetValue = Awaited<ReturnType<typeof schemaUserRecordGet>>;
type SchemaUserIncrementFieldName = Parameters<
  typeof schemaUserStore.hincrby
>[1];
type _SchemaUserSetValue = Expect<
  Equal<SchemaUserSetValue, { name: string; score: number }>
>;
type _SchemaUserGetValue = Expect<
  Equal<SchemaUserGetValue, { name: string; score: number } | null>
>;
type _SchemaUserIncrementFieldName = Expect<
  Equal<SchemaUserIncrementFieldName, "score">
>;

const schemaIncrementBy = schemaScript("incrementBy", {
  keys: ["counter"],
  args: {
    amount: schemaNumber()
  },
  returns: schemaNumber(),
  lua: "return redis.call('INCRBY', KEYS[1], ARGV[1])"
});
const schemaIncrementRunner = db.script(schemaIncrementBy);
type SchemaScriptRunInput = Parameters<typeof schemaIncrementRunner.run>[0];
type SchemaScriptRunValue = Awaited<
  ReturnType<typeof schemaIncrementRunner.run>
>;
// `keys` is wrapped in SameSlotScriptKeys so cross-slot script keys are a
// compile error; for a single key, and for any set whose tags agree, it
// resolves to the caller's own record.
type _SchemaScriptRunInput = Expect<
  Equal<
    SchemaScriptRunInput,
    {
      readonly keys: SameSlotScriptKeys<
        readonly ["counter"],
        { readonly counter: string }
      >;
      readonly args: { amount: number };
    }
  >
>;
// A record whose tags agree is still assignable, so the wrapper is invisible
// to callers who get it right. (It is an intersection, so it is not `Equal` to
// the bare record; assignability is the property that matters.)
type _ScriptKeysAcceptMatchingTags =
  SameSlotScriptKeys<
    readonly ["from", "to"],
    { readonly from: "cart:{u1}"; readonly to: "orders:{u1}" }
  > extends { readonly from: string; readonly to: string }
    ? true
    : never;
const _scriptKeysAcceptMatchingTags: _ScriptKeysAcceptMatchingTags = true;
void _scriptKeysAcceptMatchingTags;
type _SchemaScriptRunValue = Expect<Equal<SchemaScriptRunValue, number>>;

const profiles = defineKeyspace(
  "profile",
  codecs.json<{ name: string; score: number }>()
);
const profileStore = createKeyValueStore(client, profiles);

type ProfileKey = ReturnType<typeof profiles.key<"42">>;
type ProfileSetValue = Parameters<typeof profileStore.set>[1];
type ProfileGetValue = Awaited<ReturnType<typeof profileStore.get>>;
type ProfileMGetValue = Awaited<ReturnType<typeof profileStore.mget>>;

type _ProfileKey = Expect<Equal<ProfileKey, "profile:42">>;
type _ProfileSetValue = Expect<
  Equal<ProfileSetValue, { name: string; score: number }>
>;
type _ProfileGetValue = Expect<
  Equal<ProfileGetValue, { name: string; score: number } | null>
>;
type _ProfileMGetValue = Expect<
  Equal<ProfileMGetValue, Array<{ name: string; score: number } | null>>
>;

const demoProfiles = defineKeyspace("demo", codecs.string(), {
  ids: ["test1", "test2"]
});
const demoProfileStore = createKeyValueStore(client, demoProfiles);
type DemoKey = ReturnType<typeof demoProfiles.key<"test1">>;
type DemoAutocompleteKey = RedisKey<"demo", "test1" | "test2">;
type DemoStoreId = Parameters<typeof demoProfileStore.set>[0];
type _DemoKey = Expect<Equal<DemoKey, "demo:test1">>;
type _DemoAutocompleteKey = Expect<
  Equal<DemoAutocompleteKey, "demo:test1" | "demo:test2">
>;
type _DemoStoreId = Expect<Equal<DemoStoreId, "test1" | "test2">>;

const counters = createCounterStore(
  client,
  defineKeyspace("counter", codecs.number())
);
type CounterValue = Awaited<ReturnType<typeof counters.incrby>>;
type _CounterValue = Expect<Equal<CounterValue, number>>;

const strings = createStringStore(
  client,
  defineKeyspace("text", codecs.string())
);
type StringAppendValue = Parameters<typeof strings.append>[1];
type StringGetRangeValue = Awaited<ReturnType<typeof strings.getrange>>;
type _StringAppendValue = Expect<Equal<StringAppendValue, string>>;
type _StringGetRangeValue = Expect<Equal<StringGetRangeValue, string>>;

const knownStrings = createStringStore(
  client,
  defineKeyspace("known", codecs.string(), { ids: ["one", "two"] })
);
type KnownStringId = Parameters<typeof knownStrings.append>[0];
type _KnownStringId = Expect<Equal<KnownStringId, "one" | "two">>;

const userEvents = definePubSubChannel(
  "events:user",
  codecs.json<{ id: string; action: "created" | "deleted" }>()
);
const pubSubPublisher = createPubSubPublisher(client);
type UserEventChannelName = typeof userEvents.name;
type UserEventMessage = Parameters<typeof userEvents.encode>[0];
type _UserEventChannelName = Expect<Equal<UserEventChannelName, "events:user">>;
type _UserEventMessage = Expect<
  Equal<UserEventMessage, { id: string; action: "created" | "deleted" }>
>;

const roles = createSetStore(client, defineSet("roles", codecs.string()));
type RoleAddMember = Parameters<typeof roles.sadd>[1][number];
type RoleMembers = Awaited<ReturnType<typeof roles.smembers>>;
type RolePop = Awaited<ReturnType<typeof roles.spop>>;
type RoleUnion = Awaited<ReturnType<typeof roles.sunion>>;
type _RoleAddMember = Expect<Equal<RoleAddMember, string>>;
type _RoleMembers = Expect<Equal<RoleMembers, string[]>>;
type _RolePop = Expect<Equal<RolePop, string | null>>;
type _RoleUnion = Expect<Equal<RoleUnion, string[]>>;

const jobs = createListStore(client, defineList("jobs", codecs.string()));
type JobPushValue = Parameters<typeof jobs.rpush>[1][number];
type JobRange = Awaited<ReturnType<typeof jobs.lrange>>;
// lpop is overloaded (scalar vs count); probe the scalar form via a call.
const jobLeftPop = () => jobs.lpop("a");
type JobPop = Awaited<ReturnType<typeof jobLeftPop>>;
type _JobPushValue = Expect<Equal<JobPushValue, string>>;
type _JobRange = Expect<Equal<JobRange, string[]>>;
type _JobPop = Expect<Equal<JobPop, string | null>>;

const leaderboard = createSortedSetStore(
  client,
  defineSortedSet("leaderboard", codecs.json<{ name: string }>())
);
type LeaderboardEntry = Extract<
  Parameters<typeof leaderboard.zadd>[1],
  readonly unknown[]
>[number];
// zrange/zpopmin are overloaded; probe the specific forms via calls.
const leaderboardRange = () => leaderboard.zrange("g", { start: 0, stop: -1 });
const leaderboardRangeWithScores = () =>
  leaderboard.zrange("g", { start: 0, stop: -1, withScores: true });
const leaderboardPop = () => leaderboard.zpopmin("g");
type LeaderboardRange = Awaited<ReturnType<typeof leaderboardRange>>;
type LeaderboardRangeWithScores = Awaited<
  ReturnType<typeof leaderboardRangeWithScores>
>;
type LeaderboardPop = Awaited<ReturnType<typeof leaderboardPop>>;
type _LeaderboardEntry = Expect<
  Equal<
    LeaderboardEntry,
    { readonly member: { name: string }; readonly score: number }
  >
>;
type _LeaderboardRange = Expect<
  Equal<LeaderboardRange, Array<{ name: string }>>
>;
type _LeaderboardRangeWithScores = Expect<
  Equal<
    LeaderboardRangeWithScores,
    Array<{ readonly member: { name: string }; readonly score: number }>
  >
>;
type _LeaderboardPop = Expect<
  Equal<
    LeaderboardPop,
    { readonly member: { name: string }; readonly score: number } | null
  >
>;

const users = defineHash("user", {
  name: codecs.string(),
  score: codecs.number()
});
const userStore = createHashStore(client, users);

type UserInput = InferHashInput<typeof users.fields>;
type UserOutput = InferHashOutput<typeof users.fields>;
// hset/hget are overloaded; probe the whole-record forms via calls and the
// single-field forms with an explicit type argument.
const userRecordSet = (value: UserInput) => userStore.hset("42", value);
const userRecordGet = () => userStore.hget("42");
type UserSetValue = Parameters<typeof userRecordSet>[0];
type UserGetValue = Awaited<ReturnType<typeof userRecordGet>>;
type UserSetNameValue = Parameters<typeof userStore.hset<"name">>[2];
type UserGetScoreValue = Awaited<ReturnType<typeof userStore.hget<"score">>>;
type UserIncrementFieldName = Parameters<typeof userStore.hincrby>[1];

type _UserInput = Expect<Equal<UserInput, { name: string; score: number }>>;
type _UserOutput = Expect<Equal<UserOutput, { name: string; score: number }>>;
type _UserSetValue = Expect<Equal<UserSetValue, UserInput>>;
type _UserGetValue = Expect<Equal<UserGetValue, UserOutput | null>>;
type _UserSetNameValue = Expect<Equal<UserSetNameValue, string>>;
type _UserGetScoreValue = Expect<Equal<UserGetScoreValue, number | null>>;
type _UserIncrementFieldName = Expect<Equal<UserIncrementFieldName, "score">>;

function expectTypeErrorsOnly() {
  // @ts-expect-error score must be a number.
  void userStore.hset("42", { name: "beni", score: "2" });

  // @ts-expect-error field value must match the declared field codec.
  void userStore.hset("42", "score", "2");

  // @ts-expect-error field name must exist on the hash schema.
  void userStore.hget("42", "missing");

  // @ts-expect-error only numeric hash fields can be incremented.
  void userStore.hincrby("42", "name", 1);

  // @ts-expect-error key-value store value must match the JSON codec type.
  void profileStore.set("42", { name: "beni" });

  // @ts-expect-error known keyspaces only accept declared ids.
  void demoProfileStore.set("test3", "beni");

  // @ts-expect-error mset values must match the JSON codec type.
  void profileStore.mset([["42", { name: "beni" }]]);

  // @ts-expect-error counter amounts must be numbers.
  void counters.incrby("hits", "1");

  const jsonKeyspace = defineKeyspace("json", codecs.json<{ name: string }>());
  // @ts-expect-error string store requires a string keyspace.
  void createStringStore(client, jsonKeyspace);

  // @ts-expect-error string store values must be strings.
  void strings.append("a", 1);

  // @ts-expect-error known string keyspaces only accept declared ids.
  void knownStrings.strlen("three");

  // @ts-expect-error pubsub message must match the channel codec.
  void pubSubPublisher.publish(userEvents, { id: "42", action: "updated" });

  // @ts-expect-error set members must match the set codec type.
  void roles.sadd("42", [1]);

  // @ts-expect-error moved set member must match the set codec type.
  void roles.smove("a", "b", 1);

  // @ts-expect-error list values must match the list codec type.
  void jobs.rpush("a", [1]);

  // @ts-expect-error sorted-set members must match the member codec type.
  void leaderboard.zadd("game", [{ member: { label: "alice" }, score: 1 }]);

  // @ts-expect-error sorted-set scores must be numbers.
  void leaderboard.zadd("game", [{ member: { name: "alice" }, score: "1" }]);
}

void expectTypeErrorsOnly;

const streamEvents = defineStream("events", {
  type: codecs.string(),
  size: codecs.number()
});
type EventStreamEntry = StreamEntry<typeof streamEvents.fields>;
type EventPendingEntry = PendingStreamEntry<typeof streamEvents.fields>;
type EventLiveValue = EventStreamEntry["value"];
type EventPendingValue = EventPendingEntry["value"];

// A live-read entry's value is never null (undeclared fields are modeled as a
// Partial, but the whole value is always present).
type _EventLiveValue = Expect<
  Equal<EventLiveValue, Partial<{ type: string; size: number }>>
>;
// A pending/claim entry's value is nullable: an XDELed entry still in the PEL
// decodes as a tombstone (value === null) and must still be acked.
type _EventPendingValue = Expect<
  Equal<EventPendingValue, Partial<{ type: string; size: number }> | null>
>;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

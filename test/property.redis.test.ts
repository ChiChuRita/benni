import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import {
  codecs,
  createHashStore,
  createKeyValueStore,
  createListStore,
  createSetStore,
  createSortedSetStore,
  defineHash,
  defineKeyspace,
  defineList,
  defineSet,
  defineSortedSet,
  scanHash,
  scanKeyspace,
  scanSet,
  scanSortedSet
} from "../src/core/index.js";
import { node } from "../src/node/index.js";

// Differential model-based tests against a live Redis: fast-check generates
// random operation sequences, each op runs through the typed store while a
// plain in-memory model applies the documented Redis semantics, and the two
// must agree — on every return value and on the final observable state.

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

// Ids exercise the awkward corners of key building: empty, colon, unicode.
const IDS = ["a", "b", "c:d", "", "☃ id"] as const;
type Id = (typeof IDS)[number];
const idArb = fc.constantFrom<Id>(...IDS);
const anyText = fc.string({ unit: "grapheme", maxLength: 20 });
const finiteDouble = fc.double({ noNaN: true, noDefaultInfinity: true });

// Unique per-process tag so reruns and crashed runs can never collide.
const session = `pt${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
let sequence = 0;
function prefix(kind: string): string {
  sequence += 1;
  return `${session}:${kind}:${sequence}`;
}

// -0 normalizes to +0 everywhere: Redis has no -0 score/number on the wire.
function canonical(value: number): number {
  return value === 0 ? 0 : value;
}

describeRedis("differential properties against live Redis", () => {
  let client: RedisClient;

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  async function cleanup(keys: readonly string[]): Promise<void> {
    if (keys.length > 0) await client.send(["DEL", ...keys]);
  }

  describe("kv store vs model", () => {
    const jsonArb = fc.jsonValue({ maxDepth: 2, stringUnit: "grapheme" });
    const opArb = fc.oneof(
      fc.record({ t: fc.constant("set" as const), id: idArb, v: jsonArb }),
      fc.record({ t: fc.constant("setnx" as const), id: idArb, v: jsonArb }),
      fc.record({ t: fc.constant("setxx" as const), id: idArb, v: jsonArb }),
      fc.record({ t: fc.constant("getset" as const), id: idArb, v: jsonArb }),
      fc.record({ t: fc.constant("getdel" as const), id: idArb }),
      fc.record({ t: fc.constant("del" as const), id: idArb })
    );

    it("agrees with the model on every op and the final state", async () => {
      await fc.assert(
        fc.asyncProperty(fc.array(opArb, { maxLength: 25 }), async (ops) => {
          const space = defineKeyspace(prefix("kv"), codecs.json<unknown>());
          const store = createKeyValueStore(client, space);
          const model = new Map<Id, string>(); // encoded form
          try {
            for (const op of ops) {
              const before = model.get(op.id);
              switch (op.t) {
                case "set":
                  await store.set(op.id, op.v);
                  model.set(op.id, JSON.stringify(op.v));
                  break;
                case "setnx": {
                  const written = await store.set(op.id, op.v, { nx: true });
                  expect(written).toBe(before === undefined);
                  if (written) model.set(op.id, JSON.stringify(op.v));
                  break;
                }
                case "setxx": {
                  const written = await store.set(op.id, op.v, { xx: true });
                  expect(written).toBe(before !== undefined);
                  if (written) model.set(op.id, JSON.stringify(op.v));
                  break;
                }
                case "getset": {
                  const old = await store.getset(op.id, op.v);
                  expect(old).toEqual(
                    before === undefined ? null : JSON.parse(before)
                  );
                  model.set(op.id, JSON.stringify(op.v));
                  break;
                }
                case "getdel": {
                  const old = await store.getdel(op.id);
                  expect(old).toEqual(
                    before === undefined ? null : JSON.parse(before)
                  );
                  model.delete(op.id);
                  break;
                }
                case "del": {
                  const removed = await store.del(op.id);
                  expect(removed).toBe(before === undefined ? 0 : 1);
                  model.delete(op.id);
                  break;
                }
              }
            }
            for (const id of IDS) {
              const encoded = model.get(id);
              await expect(store.get(id)).resolves.toEqual(
                encoded === undefined ? null : JSON.parse(encoded)
              );
              await expect(store.exists(id)).resolves.toBe(
                encoded !== undefined
              );
              // Raw cross-check: the bytes in Redis are exactly the encoding.
              await expect(client.send(["GET", space.key(id)])).resolves.toBe(
                encoded ?? null
              );
            }
            const all = await store.mget([...IDS]);
            expect(all).toEqual(
              IDS.map((id) => {
                const encoded = model.get(id);
                return encoded === undefined ? null : JSON.parse(encoded);
              })
            );
          } finally {
            await cleanup(IDS.map((id) => space.key(id)));
          }
        }),
        { numRuns: 100 }
      );
    }, 120_000);
  });

  describe("hash store vs model", () => {
    type Fields = { name: string; score: number; flag: boolean };
    const recordArb = fc.record({
      name: anyText,
      score: finiteDouble.map(canonical),
      flag: fc.boolean()
    });
    const opArb = fc.oneof(
      fc.record({
        t: fc.constant("setRecord" as const),
        id: idArb,
        v: recordArb
      }),
      fc.record({ t: fc.constant("setName" as const), id: idArb, v: anyText }),
      fc.record({
        t: fc.constant("setScore" as const),
        id: idArb,
        v: finiteDouble.map(canonical)
      }),
      fc.record({
        t: fc.constant("setFlag" as const),
        id: idArb,
        v: fc.boolean()
      }),
      fc.record({
        t: fc.constant("hdel" as const),
        id: idArb,
        field: fc.constantFrom(
          "name" as const,
          "score" as const,
          "flag" as const
        )
      }),
      fc.record({ t: fc.constant("del" as const), id: idArb })
    );

    it("agrees with the model on every op and the final state", async () => {
      await fc.assert(
        fc.asyncProperty(fc.array(opArb, { maxLength: 25 }), async (ops) => {
          const users = defineHash(prefix("hash"), {
            name: codecs.string(),
            score: codecs.number(),
            flag: codecs.boolean()
          });
          const store = createHashStore(client, users);
          const model = new Map<Id, Partial<Fields>>();
          const record = (id: Id) => {
            const existing = model.get(id);
            if (existing) return existing;
            const fresh: Partial<Fields> = {};
            model.set(id, fresh);
            return fresh;
          };
          try {
            for (const op of ops) {
              switch (op.t) {
                case "setRecord":
                  await store.hset(op.id, op.v);
                  model.set(op.id, { ...op.v });
                  break;
                case "setName": {
                  const added = await store.hset(op.id, "name", op.v);
                  expect(added).toBe(record(op.id).name === undefined ? 1 : 0);
                  record(op.id).name = op.v;
                  break;
                }
                case "setScore": {
                  const added = await store.hset(op.id, "score", op.v);
                  expect(added).toBe(record(op.id).score === undefined ? 1 : 0);
                  record(op.id).score = op.v;
                  break;
                }
                case "setFlag": {
                  const added = await store.hset(op.id, "flag", op.v);
                  expect(added).toBe(record(op.id).flag === undefined ? 1 : 0);
                  record(op.id).flag = op.v;
                  break;
                }
                case "hdel": {
                  const existing = model.get(op.id);
                  const removed = await store.hdel(op.id, op.field);
                  expect(removed).toBe(
                    existing?.[op.field] === undefined ? 0 : 1
                  );
                  if (existing) {
                    delete existing[op.field];
                    if (Object.keys(existing).length === 0) {
                      model.delete(op.id);
                    }
                  }
                  break;
                }
                case "del": {
                  const removed = await store.del(op.id);
                  expect(removed).toBe(model.has(op.id) ? 1 : 0);
                  model.delete(op.id);
                  break;
                }
              }
            }
            for (const id of IDS) {
              const expected = model.get(id);
              await expect(store.hgetall(id)).resolves.toEqual(
                expected ?? null
              );
              await expect(store.hlen(id)).resolves.toBe(
                expected === undefined ? 0 : Object.keys(expected).length
              );
              const complete =
                expected !== undefined &&
                expected.name !== undefined &&
                expected.score !== undefined &&
                expected.flag !== undefined;
              if (expected === undefined) {
                await expect(store.hget(id)).resolves.toBeNull();
              } else if (complete) {
                await expect(store.hget(id)).resolves.toEqual(expected);
              } else {
                await expect(store.hget(id)).rejects.toThrow(
                  "missing declared field"
                );
              }
              await expect(store.hget(id, "name")).resolves.toBe(
                expected?.name ?? null
              );
            }
          } finally {
            await cleanup(IDS.map((id) => users.key(id)));
          }
        }),
        { numRuns: 100 }
      );
    }, 120_000);
  });

  describe("set store vs model", () => {
    const memberArb = fc.string({ unit: "grapheme", maxLength: 12 });
    const opArb = fc.oneof(
      fc.record({
        t: fc.constant("sadd" as const),
        id: idArb,
        members: fc.array(memberArb, { minLength: 1, maxLength: 4 })
      }),
      fc.record({
        t: fc.constant("srem" as const),
        id: idArb,
        members: fc.array(memberArb, { minLength: 1, maxLength: 4 })
      }),
      fc.record({ t: fc.constant("spop" as const), id: idArb }),
      fc.record({
        t: fc.constant("sismember" as const),
        id: idArb,
        member: memberArb
      })
    );

    it("agrees with the model on every op and the final members", async () => {
      await fc.assert(
        fc.asyncProperty(fc.array(opArb, { maxLength: 25 }), async (ops) => {
          const tags = defineSet(prefix("set"), codecs.string());
          const store = createSetStore(client, tags);
          const model = new Map<Id, Set<string>>();
          const members = (id: Id) => {
            const existing = model.get(id);
            if (existing) return existing;
            const fresh = new Set<string>();
            model.set(id, fresh);
            return fresh;
          };
          try {
            for (const op of ops) {
              switch (op.t) {
                case "sadd": {
                  const target = members(op.id);
                  let expected = 0;
                  for (const member of op.members) {
                    if (!target.has(member)) expected += 1;
                    target.add(member);
                  }
                  await expect(store.sadd(op.id, op.members)).resolves.toBe(
                    expected
                  );
                  break;
                }
                case "srem": {
                  const target = members(op.id);
                  let expected = 0;
                  for (const member of op.members) {
                    if (target.delete(member)) expected += 1;
                  }
                  await expect(store.srem(op.id, op.members)).resolves.toBe(
                    expected
                  );
                  break;
                }
                case "spop": {
                  const target = members(op.id);
                  const popped = await store.spop(op.id);
                  if (target.size === 0) {
                    expect(popped).toBeNull();
                  } else {
                    expect(popped).not.toBeNull();
                    expect(target.has(popped as string)).toBe(true);
                    target.delete(popped as string);
                  }
                  break;
                }
                case "sismember":
                  await expect(store.sismember(op.id, op.member)).resolves.toBe(
                    members(op.id).has(op.member)
                  );
                  break;
              }
            }
            for (const id of IDS) {
              const expected = [...members(id)].sort();
              const actual = (await store.smembers(id)).sort();
              expect(actual).toEqual(expected);
              await expect(store.scard(id)).resolves.toBe(expected.length);
            }
          } finally {
            await cleanup(IDS.map((id) => tags.key(id)));
          }
        }),
        { numRuns: 100 }
      );
    }, 120_000);
  });

  describe("list store vs model", () => {
    // A tiny value pool makes LREM collisions and duplicates frequent.
    const valueArb = fc.integer({ min: -5, max: 5 });
    const opArb = fc.oneof(
      fc.record({
        t: fc.constant("lpush" as const),
        id: idArb,
        values: fc.array(valueArb, { minLength: 1, maxLength: 3 })
      }),
      fc.record({
        t: fc.constant("rpush" as const),
        id: idArb,
        values: fc.array(valueArb, { minLength: 1, maxLength: 3 })
      }),
      fc.record({ t: fc.constant("lpop" as const), id: idArb }),
      fc.record({ t: fc.constant("rpop" as const), id: idArb }),
      fc.record({
        t: fc.constant("lpopN" as const),
        id: idArb,
        count: fc.integer({ min: 1, max: 4 })
      }),
      fc.record({
        t: fc.constant("ltrim" as const),
        id: idArb,
        start: fc.integer({ min: -6, max: 6 }),
        stop: fc.integer({ min: -6, max: 6 })
      }),
      fc.record({
        t: fc.constant("lrem" as const),
        id: idArb,
        count: fc.integer({ min: -2, max: 2 }),
        value: valueArb
      }),
      fc.record({
        t: fc.constant("lset" as const),
        id: idArb,
        slot: fc.nat(),
        value: valueArb
      }),
      fc.record({
        t: fc.constant("lindex" as const),
        id: idArb,
        slot: fc.nat()
      })
    );

    it("agrees with the model on every op and the final range", async () => {
      await fc.assert(
        fc.asyncProperty(fc.array(opArb, { maxLength: 30 }), async (ops) => {
          const queue = defineList(prefix("list"), codecs.number());
          const store = createListStore(client, queue);
          const model = new Map<Id, number[]>();
          const items = (id: Id) => {
            const existing = model.get(id);
            if (existing) return existing;
            const fresh: number[] = [];
            model.set(id, fresh);
            return fresh;
          };
          try {
            for (const op of ops) {
              const target = items(op.id);
              switch (op.t) {
                case "lpush": {
                  for (const value of op.values) target.unshift(value);
                  await expect(store.lpush(op.id, op.values)).resolves.toBe(
                    target.length
                  );
                  break;
                }
                case "rpush": {
                  target.push(...op.values);
                  await expect(store.rpush(op.id, op.values)).resolves.toBe(
                    target.length
                  );
                  break;
                }
                case "lpop":
                  await expect(store.lpop(op.id)).resolves.toBe(
                    target.shift() ?? null
                  );
                  break;
                case "rpop":
                  await expect(store.rpop(op.id)).resolves.toBe(
                    target.pop() ?? null
                  );
                  break;
                case "lpopN":
                  await expect(
                    store.lpop(op.id, { count: op.count })
                  ).resolves.toEqual(target.splice(0, op.count));
                  break;
                case "ltrim": {
                  const length = target.length;
                  const start =
                    op.start < 0 ? Math.max(length + op.start, 0) : op.start;
                  const stop =
                    op.stop < 0
                      ? length + op.stop
                      : Math.min(op.stop, length - 1);
                  const kept =
                    start > stop ? [] : target.slice(start, stop + 1);
                  target.length = 0;
                  target.push(...kept);
                  await store.ltrim(op.id, op.start, op.stop);
                  break;
                }
                case "lrem": {
                  let removed = 0;
                  const limit =
                    op.count === 0
                      ? Number.POSITIVE_INFINITY
                      : Math.abs(op.count);
                  const indexes: number[] = [];
                  if (op.count >= 0) {
                    for (let i = 0; i < target.length && removed < limit; i++) {
                      if (target[i] === op.value) {
                        indexes.push(i);
                        removed += 1;
                      }
                    }
                  } else {
                    for (
                      let i = target.length - 1;
                      i >= 0 && removed < limit;
                      i--
                    ) {
                      if (target[i] === op.value) {
                        indexes.push(i);
                        removed += 1;
                      }
                    }
                  }
                  for (const index of indexes.sort((a, b) => b - a)) {
                    target.splice(index, 1);
                  }
                  await expect(
                    store.lrem(op.id, op.count, op.value)
                  ).resolves.toBe(removed);
                  break;
                }
                case "lset": {
                  if (target.length === 0) break;
                  const index = op.slot % target.length;
                  target[index] = op.value;
                  await store.lset(op.id, index, op.value);
                  break;
                }
                case "lindex": {
                  if (target.length === 0) {
                    await expect(store.lindex(op.id, 0)).resolves.toBeNull();
                    break;
                  }
                  const index = op.slot % target.length;
                  await expect(store.lindex(op.id, index)).resolves.toBe(
                    target[index]
                  );
                  break;
                }
              }
            }
            for (const id of IDS) {
              await expect(store.lrange(id, 0, -1)).resolves.toEqual(items(id));
              await expect(store.llen(id)).resolves.toBe(items(id).length);
            }
          } finally {
            await cleanup(IDS.map((id) => queue.key(id)));
          }
        }),
        { numRuns: 100 }
      );
    }, 120_000);
  });

  describe("sorted-set store vs model", () => {
    // ASCII members so lexicographic tie-breaks match JS string sort.
    const memberArb = fc.constantFrom("aa", "bb", "cc", "dd", "ee", "ff");
    const scoreArb = fc.oneof(
      { weight: 5, arbitrary: finiteDouble.map(canonical) },
      {
        weight: 1,
        arbitrary: fc.constantFrom(
          Number.POSITIVE_INFINITY,
          Number.NEGATIVE_INFINITY
        )
      }
    );
    const opArb = fc.oneof(
      fc.record({
        t: fc.constant("zadd" as const),
        id: idArb,
        entries: fc.array(fc.record({ score: scoreArb, member: memberArb }), {
          minLength: 1,
          maxLength: 3
        })
      }),
      fc.record({
        t: fc.constant("zrem" as const),
        id: idArb,
        members: fc.array(memberArb, { minLength: 1, maxLength: 2 })
      }),
      fc.record({
        t: fc.constant("zincrby" as const),
        id: idArb,
        amount: fc.double({
          noNaN: true,
          noDefaultInfinity: true,
          min: -1e6,
          max: 1e6
        }),
        member: memberArb
      }),
      fc.record({
        t: fc.constant("zscore" as const),
        id: idArb,
        member: memberArb
      })
    );

    it("agrees with the model on every op and the final ranking", async () => {
      await fc.assert(
        fc.asyncProperty(fc.array(opArb, { maxLength: 25 }), async (ops) => {
          const board = defineSortedSet(prefix("zset"), codecs.string());
          const store = createSortedSetStore(client, board);
          const model = new Map<Id, Map<string, number>>();
          const scores = (id: Id) => {
            const existing = model.get(id);
            if (existing) return existing;
            const fresh = new Map<string, number>();
            model.set(id, fresh);
            return fresh;
          };
          try {
            for (const op of ops) {
              const target = scores(op.id);
              switch (op.t) {
                case "zadd": {
                  let added = 0;
                  for (const entry of op.entries) {
                    if (!target.has(entry.member)) added += 1;
                    target.set(entry.member, canonical(entry.score));
                  }
                  await expect(store.zadd(op.id, op.entries)).resolves.toBe(
                    added
                  );
                  break;
                }
                case "zrem": {
                  let removed = 0;
                  for (const member of op.members) {
                    if (target.delete(member)) removed += 1;
                  }
                  await expect(store.zrem(op.id, op.members)).resolves.toBe(
                    removed
                  );
                  break;
                }
                case "zincrby": {
                  // Incrementing an infinite score by a finite amount stays
                  // infinite in IEEE754, exactly as Redis computes it.
                  const next = canonical(
                    (target.get(op.member) ?? 0) + op.amount
                  );
                  target.set(op.member, next);
                  const returned = await store.zincrby(
                    op.id,
                    op.amount,
                    op.member
                  );
                  expect(canonical(returned)).toBe(next);
                  break;
                }
                case "zscore": {
                  const expected = target.get(op.member);
                  const actual = await store.zscore(op.id, op.member);
                  expect(actual === null ? null : canonical(actual)).toBe(
                    expected ?? null
                  );
                  break;
                }
              }
            }
            for (const id of IDS) {
              const expected = [...scores(id).entries()]
                .map(([member, score]) => ({ member, score }))
                .sort(
                  (a, b) =>
                    a.score - b.score ||
                    (a.member < b.member ? -1 : a.member > b.member ? 1 : 0)
                );
              const actual = await store.zrange(id, {
                start: 0,
                stop: -1,
                withScores: true
              });
              expect(
                actual.map((entry) => ({
                  member: entry.member,
                  score: canonical(entry.score)
                }))
              ).toEqual(expected);
              await expect(store.zcard(id)).resolves.toBe(expected.length);
            }
          } finally {
            await cleanup(IDS.map((id) => board.key(id)));
          }
        }),
        { numRuns: 100 }
      );
    }, 120_000);
  });

  describe("scan coverage", () => {
    it("scanKeyspace yields exactly the keyspace's keys — even for glob-special prefixes", async () => {
      // Prefixes drawn from an alphabet heavy in Redis glob metacharacters:
      // scanKeyspace must treat the prefix as literal text, never a pattern.
      const hostileUnit = fc.constantFrom(..."ab*?[]\\^-".split(""));
      await fc.assert(
        fc.asyncProperty(
          fc.string({ unit: hostileUnit, minLength: 1, maxLength: 6 }),
          fc.uniqueArray(fc.string({ unit: "grapheme", maxLength: 8 }), {
            maxLength: 15
          }),
          fc.integer({ min: 1, max: 100 }),
          async (rawPrefix, ids, count) => {
            const space = defineKeyspace(
              `${prefix("scan")}${rawPrefix}`,
              codecs.string()
            );
            const store = createKeyValueStore(client, space);
            try {
              for (const id of ids) await store.set(id, "x");
              const found = new Set<string>();
              for await (const key of scanKeyspace(client, space, { count })) {
                found.add(key);
              }
              expect([...found].sort()).toEqual(
                ids.map((id) => space.key(id)).sort()
              );
            } finally {
              await cleanup(ids.map((id) => space.key(id)));
            }
          }
        ),
        { numRuns: 80 }
      );
    }, 120_000);

    it("scanSet and scanSortedSet yield exactly the stored members", async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.uniqueArray(fc.string({ unit: "grapheme", maxLength: 10 }), {
            maxLength: 150
          }),
          fc.integer({ min: 1, max: 50 }),
          async (members, count) => {
            const tags = defineSet(prefix("sscan"), codecs.string());
            const board = defineSortedSet(prefix("zscan"), codecs.string());
            try {
              await createSetStore(client, tags).sadd("s", members);
              await createSortedSetStore(client, board).zadd(
                "z",
                members.map((member, index) => ({ score: index, member }))
              );
              const setFound = new Set<string>();
              for await (const member of scanSet(client, tags, "s", {
                count
              })) {
                setFound.add(member);
              }
              expect([...setFound].sort()).toEqual([...members].sort());

              const zsetFound = new Map<string, number>();
              for await (const entry of scanSortedSet(client, board, "z", {
                count
              })) {
                zsetFound.set(entry.member, entry.score);
              }
              expect(zsetFound.size).toBe(members.length);
              for (const [index, member] of members.entries()) {
                expect(zsetFound.get(member)).toBe(index);
              }
            } finally {
              await cleanup([tags.key("s"), board.key("z")]);
            }
          }
        ),
        { numRuns: 40 }
      );
    }, 120_000);

    it("scanHash yields exactly the declared fields of a written record", async () => {
      const recordArb = fc.record({
        name: anyText,
        score: finiteDouble.map(canonical),
        flag: fc.boolean()
      });
      await fc.assert(
        fc.asyncProperty(
          recordArb,
          fc.integer({ min: 1, max: 20 }),
          async (record, count) => {
            const users = defineHash(prefix("hscan"), {
              name: codecs.string(),
              score: codecs.number(),
              flag: codecs.boolean()
            });
            try {
              await createHashStore(client, users).hset("h", record);
              const found = new Map<string, unknown>();
              for await (const entry of scanHash(client, users, "h", {
                count
              })) {
                found.set(entry.field, entry.value);
              }
              expect(found).toEqual(
                new Map<string, unknown>([
                  ["name", record.name],
                  ["score", record.score],
                  ["flag", record.flag]
                ])
              );
            } finally {
              await cleanup([users.key("h")]);
            }
          }
        ),
        { numRuns: 60 }
      );
    }, 120_000);
  });
});

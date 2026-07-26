import { replyShapeError } from "./errors.js";
import {
  createKeyLifecycleOps,
  decodeStringArrayReply,
  expectNumber
} from "./helpers.js";
import type { RedisClient, RedisKeyPart, SetSchema } from "./types.js";

export function createSetStore<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(client: RedisClient, schema: SetSchema<TInput, TOutput, string, TId>) {
  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    /**
     * SADD — add `members`, returning how many were newly added. No-op
     * returning 0 when `members` is empty.
     * @example await redis.set(roles).sadd("42", ["admin", "user"]);
     */
    async sadd(id: TId, members: readonly TInput[]): Promise<number> {
      if (members.length === 0) return 0;
      return expectNumber(
        await client.send([
          "SADD",
          schema.key(id),
          ...members.map((member) => schema.encode(member))
        ]),
        "SADD"
      );
    },
    /**
     * SREM — remove `members`, returning how many were removed. No-op
     * returning 0 when `members` is empty.
     */
    async srem(id: TId, members: readonly TInput[]): Promise<number> {
      if (members.length === 0) return 0;
      return expectNumber(
        await client.send([
          "SREM",
          schema.key(id),
          ...members.map((member) => schema.encode(member))
        ]),
        "SREM"
      );
    },
    /** SISMEMBER — whether `member` is in the set. */
    async sismember(id: TId, member: TInput): Promise<boolean> {
      return (
        expectNumber(
          await client.send([
            "SISMEMBER",
            schema.key(id),
            schema.encode(member)
          ]),
          "SISMEMBER"
        ) === 1
      );
    },
    /**
     * SMISMEMBER — membership of each of `members`, in order. Empty input
     * returns `[]` without a round trip.
     */
    async smismember(id: TId, members: readonly TInput[]): Promise<boolean[]> {
      if (members.length === 0) return [];
      const reply = await client.send([
        "SMISMEMBER",
        schema.key(id),
        ...members.map((member) => schema.encode(member))
      ]);
      if (!Array.isArray(reply)) {
        throw replyShapeError("SMISMEMBER", "array", reply);
      }
      return reply.map((value) => {
        if (typeof value !== "number") {
          throw replyShapeError("SMISMEMBER item", "number", value);
        }
        return value === 1;
      });
    },
    /**
     * SMEMBERS — all members of the set, decoded.
     * @example const roles = await redis.set(roles).smembers("42");
     */
    async smembers(id: TId): Promise<TOutput[]> {
      const reply = await client.send(["SMEMBERS", schema.key(id)]);
      if (!Array.isArray(reply)) {
        throw replyShapeError("SMEMBERS", "array", reply);
      }
      return reply.map((value) => {
        if (typeof value !== "string") {
          throw replyShapeError("SMEMBERS item", "string", value);
        }
        return schema.decode(value);
      });
    },
    /** SCARD — number of members in the set. */
    async scard(id: TId): Promise<number> {
      return expectNumber(
        await client.send(["SCARD", schema.key(id)]),
        "SCARD"
      );
    },
    /** SPOP — remove and return one random member, or `null` if empty. */
    async spop(id: TId): Promise<TOutput | null> {
      const reply = await client.send(["SPOP", schema.key(id)]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("SPOP", "string or null", reply);
      }
      return schema.decode(reply);
    },
    /** SRANDMEMBER — one random member without removing it, or `null`. */
    async srandmember(id: TId): Promise<TOutput | null> {
      const reply = await client.send(["SRANDMEMBER", schema.key(id)]);
      if (reply === null) return null;
      if (typeof reply !== "string") {
        throw replyShapeError("SRANDMEMBER", "string or null", reply);
      }
      return schema.decode(reply);
    },
    /** SUNION — union of `id` with `others`, decoded. */
    async sunion(id: TId, others: readonly TId[]): Promise<TOutput[]> {
      return decodeStringArrayReply(
        await client.send([
          "SUNION",
          schema.key(id),
          ...others.map((other) => schema.key(other))
        ]),
        "SUNION",
        schema
      );
    },
    /** SINTER — intersection of `id` with `others`, decoded. */
    async sinter(id: TId, others: readonly TId[]): Promise<TOutput[]> {
      return decodeStringArrayReply(
        await client.send([
          "SINTER",
          schema.key(id),
          ...others.map((other) => schema.key(other))
        ]),
        "SINTER",
        schema
      );
    },
    /** SDIFF — members of `id` not present in any of `others`, decoded. */
    async sdiff(id: TId, others: readonly TId[]): Promise<TOutput[]> {
      return decodeStringArrayReply(
        await client.send([
          "SDIFF",
          schema.key(id),
          ...others.map((other) => schema.key(other))
        ]),
        "SDIFF",
        schema
      );
    },
    /** SINTERCARD — cardinality of the intersection of `id` with `others`. */
    async sintercard(id: TId, others: readonly TId[]): Promise<number> {
      return expectNumber(
        await client.send([
          "SINTERCARD",
          others.length + 1,
          schema.key(id),
          ...others.map((other) => schema.key(other))
        ]),
        "SINTERCARD"
      );
    },
    /** SUNIONSTORE — store the union into `destination`; returns its size. */
    async sunionstore(
      destination: TId,
      id: TId,
      others: readonly TId[]
    ): Promise<number> {
      return expectNumber(
        await client.send([
          "SUNIONSTORE",
          schema.key(destination),
          schema.key(id),
          ...others.map((other) => schema.key(other))
        ]),
        "SUNIONSTORE"
      );
    },
    /** SINTERSTORE — store the intersection into `destination`; returns its size. */
    async sinterstore(
      destination: TId,
      id: TId,
      others: readonly TId[]
    ): Promise<number> {
      return expectNumber(
        await client.send([
          "SINTERSTORE",
          schema.key(destination),
          schema.key(id),
          ...others.map((other) => schema.key(other))
        ]),
        "SINTERSTORE"
      );
    },
    /** SDIFFSTORE — store the difference into `destination`; returns its size. */
    async sdiffstore(
      destination: TId,
      id: TId,
      others: readonly TId[]
    ): Promise<number> {
      return expectNumber(
        await client.send([
          "SDIFFSTORE",
          schema.key(destination),
          schema.key(id),
          ...others.map((other) => schema.key(other))
        ]),
        "SDIFFSTORE"
      );
    },
    /**
     * SMOVE — atomically move `member` from `source` to `destination`;
     * `true` if it was moved.
     */
    async smove(
      source: TId,
      destination: TId,
      member: TInput
    ): Promise<boolean> {
      return (
        expectNumber(
          await client.send([
            "SMOVE",
            schema.key(source),
            schema.key(destination),
            schema.encode(member)
          ]),
          "SMOVE"
        ) === 1
      );
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", schema.key(id)]), "DEL");
    }
  };
}

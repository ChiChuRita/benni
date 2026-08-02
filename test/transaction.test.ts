import { describe, expect, it } from "vitest";
import {
  booleanNumberReply,
  createTransaction,
  numberReply,
  okReply,
  stringOrNullReply,
  stringReply
} from "../src/core/transaction.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

function bareClient(): RedisClient {
  return {
    async send() {
      return null;
    },
    async pipeline() {
      return [];
    },
    async close() {}
  };
}

describe("createTransaction", () => {
  it("sends queued commands through client.transaction and decodes the tuple", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, ["OK", "benni", 2, 1, "gone"]);

    const results = await createTransaction(client)
      .add(["SET", "user:42", "benni"], okReply)
      .add(["GET", "user:42"], stringOrNullReply)
      .add(["INCR", "user:42:hits"], numberReply)
      .add(["EXISTS", "user:42"], booleanNumberReply)
      .add(["GETDEL", "user:42"], stringReply)
      .exec();

    expect(results).toEqual([undefined, "benni", 2, true, "gone"]);
    expect(commands).toEqual([
      ["SET", "user:42", "benni"],
      ["GET", "user:42"],
      ["INCR", "user:42:hits"],
      ["EXISTS", "user:42"],
      ["GETDEL", "user:42"]
    ]);
  });

  it("resolves an empty tuple without sending anything", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, []);

    await expect(createTransaction(client).exec()).resolves.toEqual([]);

    expect(commands).toEqual([]);
  });

  it("resolves an empty tuple even when the client lacks transaction support", async () => {
    await expect(createTransaction(bareClient()).exec()).resolves.toEqual([]);
  });

  it("is immutable so a partially built transaction can be reused", async () => {
    const commands: RedisCommand[] = [];
    const client = fakeClient(commands, [1, "a", 2, "b"]);
    const base = createTransaction(client).add(["INCR", "hits"], numberReply);

    const first = base.add(["GET", "a"], stringReply);
    const second = base.add(["GET", "b"], stringReply);

    await expect(first.exec()).resolves.toEqual([1, "a"]);
    await expect(second.exec()).resolves.toEqual([2, "b"]);
    expect(commands).toEqual([
      ["INCR", "hits"],
      ["GET", "a"],
      ["INCR", "hits"],
      ["GET", "b"]
    ]);
  });

  it("throws when the client does not support transactions", async () => {
    await expect(
      createTransaction(bareClient()).add(["PING"], stringReply).exec()
    ).rejects.toThrow("Redis client does not support transactions");
  });

  it("throws when EXEC returns the wrong number of replies", async () => {
    const client = fakeClient([], ["OK"]);

    await expect(
      createTransaction(client)
        .add(["SET", "a", "1"], okReply)
        .add(["SET", "b", "2"], okReply)
        .exec()
    ).rejects.toThrow("Expected Redis EXEC to return 2 replies");
  });

  it("throws when EXEC does not return an array", async () => {
    const client: RedisClient = {
      ...bareClient(),
      async transaction() {
        return "OK" as unknown as RedisReply[];
      }
    };

    await expect(
      createTransaction(client).add(["PING"], stringReply).exec()
    ).rejects.toThrow("Expected Redis EXEC to return array");
  });
});

describe("transaction reply decoders", () => {
  it("okReply asserts OK and returns undefined", () => {
    expect(okReply("OK")).toBeUndefined();
    expect(() => okReply("QUEUED")).toThrow(
      "Expected Redis transaction reply to return OK"
    );
    expect(() => okReply(null)).toThrow(
      "Expected Redis transaction reply to return OK"
    );
  });

  it("numberReply accepts only numbers", () => {
    expect(numberReply(2)).toBe(2);
    expect(() => numberReply("2")).toThrow(
      "Expected Redis transaction reply to return number"
    );
  });

  it("stringReply accepts only strings", () => {
    expect(stringReply("benni")).toBe("benni");
    expect(() => stringReply(null)).toThrow(
      "Expected Redis transaction reply to return string"
    );
  });

  it("stringOrNullReply accepts strings and null", () => {
    expect(stringOrNullReply("benni")).toBe("benni");
    expect(stringOrNullReply(null)).toBeNull();
    expect(() => stringOrNullReply(1)).toThrow(
      "Expected Redis transaction reply to return string or null"
    );
  });

  it("booleanNumberReply maps 1 to true and other numbers to false", () => {
    expect(booleanNumberReply(1)).toBe(true);
    expect(booleanNumberReply(0)).toBe(false);
    expect(() => booleanNumberReply("1")).toThrow(
      "Expected Redis transaction reply to return number"
    );
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const typedClient = null as unknown as RedisClient;

const typedTransaction = createTransaction(typedClient)
  .add(["SET", "user:42", "benni"], okReply)
  .add(["GET", "user:42"], stringOrNullReply)
  .add(["INCR", "hits"], numberReply)
  .add(["EXISTS", "user:42"], booleanNumberReply)
  .add(["GETDEL", "user:42"], stringReply);

type TransactionResults = Awaited<ReturnType<typeof typedTransaction.exec>>;
type _TransactionResults = Expect<
  Equal<TransactionResults, [void, string | null, number, boolean, string]>
>;

type EmptyTransactionResults = Awaited<
  ReturnType<ReturnType<typeof createTransaction>["exec"]>
>;
type _EmptyTransactionResults = Expect<Equal<EmptyTransactionResults, []>>;

const customDecoded = createTransaction(typedClient).add(
  ["STRLEN", "user:42"],
  (reply) => (typeof reply === "number" ? reply > 0 : false)
);
type CustomDecodedResults = Awaited<ReturnType<typeof customDecoded.exec>>;
type _CustomDecodedResults = Expect<Equal<CustomDecodedResults, [boolean]>>;

async function expectTypeErrorsOnly() {
  const [ok, value, hits] = await createTransaction(typedClient)
    .add(["SET", "user:42", "benni"], okReply)
    .add(["GET", "user:42"], stringOrNullReply)
    .add(["INCR", "hits"], numberReply)
    .exec();
  void ok;

  // @ts-expect-error a nullable string result is not assignable to string.
  const forcedString: string = value;
  void forcedString;

  // @ts-expect-error a number result is not assignable to boolean.
  const forcedBoolean: boolean = hits;
  void forcedBoolean;

  // @ts-expect-error decoders must accept the whole RedisReply union.
  void createTransaction(typedClient).add(["GET", "k"], (reply: Date) => reply);

  // @ts-expect-error commands must be RedisCommand tuples, not bare strings.
  void createTransaction(typedClient).add("GET", stringReply);

  // @ts-expect-error exec accepts no arguments.
  void createTransaction(typedClient).exec("now");
}

void expectTypeErrorsOnly;

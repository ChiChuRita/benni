---
title: "Transactions"
description: "Use redis.multi() to run commands atomically with MULTI/EXEC and decode replies into a typed tuple."
---

Use `redis.multi()` to run several commands atomically with `MULTI`/`EXEC` and decode the replies into a typed tuple. It is the same builder a [session](/benni/advanced/sessions/) exposes as `s.multi()`.

## Build And Execute

```ts
import { numberReply, okReply, stringOrNullReply } from "benni";
import { kv, number, string } from "benni/schema";

const names = kv("name", string());
const hits = kv("hits", number());
const drafts = kv("draft", string());

const [, visits, draft] = await redis
  .multi()
  .add(["SET", names.key("42"), names.encode("Ada")], okReply)
  .add(["INCR", hits.key("42")], numberReply)
  .add(["GETDEL", drafts.key("42")], stringOrNullReply)
  .exec();
//    ^? [void, number, string | null]
```

Each `.add(command, decoder)` queues one command and extends the result tuple type. `exec()` sends everything as one transaction and returns the decoded tuple, so results stay position-typed.

Note the argument encoding: values go through the schema's own codec (`names.encode("Ada")`), never through `String(...)` or `JSON.stringify(...)`. That is the point of the next section.

## Encode Values With The Schema's Codec

`.add()` takes a raw command tuple, so Benni cannot encode arguments for you the way `redis.kv(schema).set()` does. It does not follow that you have to hand-encode them. Every schema exposes the codec its own store uses:

- Value-carrying keyspaces (`kv`, `set`, `list`, `zset`, `geo`) expose `encode(value)` and `decode(stored)` directly on the schema. A `hll` schema exposes `encode` only, since a HyperLogLog cannot be read back.
- `hash` and `stream` schemas expose `fields`, where each entry is a `Codec` with its own `.encode()` and `.decode()`.

```ts
import { hash, kv, number, string } from "benni/schema";

const links = hash("link", { url: string(), createdAt: number() });
const clicks = kv("clicks", number());

await redis
  .multi()
  .add(
    [
      "HSET",
      links.key("typed"),
      "url",
      links.fields.url.encode("https://typed.example"),
      "createdAt",
      links.fields.createdAt.encode(Date.now())
    ],
    numberReply
  )
  .add(["SET", clicks.key("typed"), clicks.encode(0)], okReply)
  .exec();
```

Reading those keys back through the typed stores returns `createdAt` as a real `number` and `clicks` as a real `number`, because the transaction wrote exactly the bytes the stores decode.

Why bother, when `String(Date.now())` produces the same string today? Because the codec is the same one the typed store uses, so a value the store would accept is the value that lands in Redis. A hand-rolled `String(...)` agrees with the codec by coincidence, and stops agreeing the moment the codec differs from plain stringification: `boolean()` stores `"1"`, not `"true"`; `json()` refuses non-finite numbers that `JSON.stringify` turns into `null`; a [Zod codec](/benni/integrations/zod/) may normalize the value on the way in. When the schema changes, codec-encoded writes follow it and hand-encoded ones silently do not.

It also moves failures to the write site. `links.fields.createdAt.encode("2026-08-02")` does not compile, because the field's input type is `number`. A bad value that reaches the codec at runtime fails there (`ValidationError: number codec cannot encode a non-finite value`) instead of being stored and then poisoning a later read with a `ReplyShapeError` from a different part of the codebase.

## Reply Decoders

```ts
okReply;            // asserts "OK", returns void
numberReply;        // number
stringReply;        // string
stringOrNullReply;  // string | null
booleanNumberReply; // 1 -> true, 0 -> false
```

Decoders throw a `ReplyShapeError` (a `TypeError` subclass) when the reply shape does not match, so a wrong decoder fails loudly instead of leaking untyped values. Any `(reply: RedisReply) => T` function works as a decoder for other shapes.

`okReply` yields `void`, which is `undefined` at runtime, so a successful `SET` reads as `undefined` in the result tuple. A tuple of `[3, undefined]` is a transaction where **both** commands succeeded: an `OK` carries no information worth typing, so the slot is `void` rather than a `true` nobody would check. A failed command does not produce `undefined`, it throws.

## The Decoder Is Not Checked Against The Command

Here is the guarantee `.add()` cannot give you. A decoder is a plain `(reply: RedisReply) => T` function, and nothing ties it to the command string sitting beside it. Pair `numberReply` with a `SET` and it compiles:

```ts
await redis
  .multi()
  .add(["SET", names.key("42"), names.encode("Ada")], numberReply) // compiles, wrong
  .exec();
// ReplyShapeError: Expected Redis transaction reply to return number, got string "OK"
```

The error is precise, but it arrives at `exec()`, one round trip after the compiler could have caught it, and after the write committed. Encoding is type-checked; the decoder pairing is the one thing in a transaction you still have to get right by reading. The common pairings:

| Command | Decoder | Slot type |
| --- | --- | --- |
| `SET`, `MSET`, `RENAME` | `okReply` | `void` |
| `INCR`, `INCRBY`, `HSET`, `DEL`, `SADD`, `ZADD`, `LLEN` | `numberReply` | `number` |
| `GET`, `GETDEL`, `HGET`, `LPOP` | `stringOrNullReply` | `string \| null` |
| `EXPIRE`, `SETNX`, `SISMEMBER` | `booleanNumberReply` | `boolean` |

To have the compiler check the whole operation, reach for a [Lua script](/benni/advanced/scripts/) instead. `script()` declares its `keys`, `args`, and `returns` codec alongside the body, so nothing about it is paired by convention.

## Builders Are Immutable

Each `.add` returns a new builder, so partial transactions can be shared and branched without affecting each other:

```ts
const base = redis.multi().add(["INCR", "hits"], numberReply);

const withA = base.add(["GET", "a"], stringOrNullReply);
const withB = base.add(["GET", "b"], stringOrNullReply);
```

An empty transaction resolves to `[]` without contacting Redis.

## Declaring Keys

`.add()` queues raw command tuples, so Benni cannot tell which keys they touch. On Redis Cluster that matters, because every key in one transaction must hash to the same slot. Declare them with `.keys()`:

```ts
await redis
  .multi()
  .keys([carts.key("u1"), orders.key("u1")])
  .add(["INCR", carts.key("u1")], numberReply)
  .add(["SADD", orders.key("u1"), orders.encode("x")], numberReply)
  .exec();
```

Keys accumulate across calls, and the declared set is checked at compile time and, under a cluster guard such as `benni(client, { cluster: assertSameSlot })`, again before `EXEC` is sent. This is a declaration rather than a derivation: a key you queue but never declare is not checked. On a single-node Redis you can skip it entirely. See [Redis Cluster](/benni/advanced/cluster/).

## Requirements And Limits

The bound client must implement the optional `transaction` method of the `RedisClient` interface (the Node and Bun adapters do). Otherwise `exec()` throws `TypeError: Redis client does not support transactions`.

For check-and-set logic that reads before it writes, use [optimistic transactions](/benni/advanced/optimistic-transactions/): `redis.watch()` runs a `WATCH`/`MULTI`/`EXEC` loop that retries on conflict. Very hot keys are still better served by a [Lua script](/benni/advanced/scripts/), which runs atomically on the server without a retry loop.

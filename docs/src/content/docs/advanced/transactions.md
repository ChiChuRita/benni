---
title: "Transactions"
description: "Use redis.multi() to run commands atomically with MULTI/EXEC and decode replies into a typed tuple."
---

Use `redis.multi()` to run several commands atomically with `MULTI`/`EXEC` and decode the replies into a typed tuple. It is the same builder a [session](/benni/advanced/sessions/) exposes as `s.multi()`.

## Build And Execute

```ts
import { numberReply, okReply, stringOrNullReply } from "benni";

const [, hits, draft] = await redis
  .multi()
  .add(["SET", "user:42", "Ada"], okReply)
  .add(["INCR", "user:42:hits"], numberReply)
  .add(["GETDEL", "user:42:draft"], stringOrNullReply)
  .exec();
//    ^? [void, number, string | null]
```

Each `.add(command, decoder)` queues one command and extends the result tuple type. `exec()` sends everything as one transaction and returns the decoded tuple, so results stay position-typed.

## Reply Decoders

```ts
okReply;            // asserts "OK", returns void
numberReply;        // number
stringReply;        // string
stringOrNullReply;  // string | null
booleanNumberReply; // 1 -> true, 0 -> false
```

Decoders throw a `TypeError` when the reply shape does not match, so a wrong decoder fails loudly instead of leaking untyped values. Any `(reply: RedisReply) => T` function works as a decoder for other shapes.

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
  .add(["SADD", orders.key("u1"), "x"], numberReply)
  .exec();
```

Keys accumulate across calls, and the declared set is checked at compile time and, under `benni(client, { cluster: true })`, again before `EXEC` is sent. This is a declaration rather than a derivation: a key you queue but never declare is not checked. On a single-node Redis you can skip it entirely. See [Redis Cluster](/benni/advanced/cluster/).

## Requirements And Limits

The bound client must implement the optional `transaction` method of the `RedisClient` interface (the Node and Bun adapters do). Otherwise `exec()` throws `TypeError: Redis client does not support transactions`.

For check-and-set logic that reads before it writes, use [optimistic transactions](/benni/advanced/optimistic-transactions/): `redis.watch()` runs a `WATCH`/`MULTI`/`EXEC` loop that retries on conflict. Very hot keys are still better served by a [Lua script](/benni/advanced/scripts/), which runs atomically on the server without a retry loop.

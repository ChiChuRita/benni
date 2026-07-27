---
"beni": minor
---

Add cluster-safe keys: declare where a schema puts its Redis Cluster hash tag, and catch cross-slot mistakes at compile time and before they are sent.

Beni models slot co-location, not cluster topology. Routing stays your driver's job (pass `createCluster()` or an ioredis `Cluster`). What no driver can do is know, before you send, that a command's keys belong together, and Beni is the only TypeScript client positioned to: keys come from schemas rather than string concatenation.

Every keyed schema factory now takes an opt-in `hashTag` option. Omitting it leaves today's `prefix:id` layout and behaviour byte-for-byte unchanged.

- `hashTag: "prefix"` builds `{prefix}:id`, pinning a keyspace to one slot so every within-schema multi-key method (`mget`, `sunionstore`, `zmpop`, `bitop`, `pfmerge`, `lmove`, and about twenty more) becomes legal on a cluster.
- `hashTag: "id"` builds `prefix:{id}`, keeping keys spread while co-locating the same id across schemas, so `cart:{u1}` and `order:{u1}` can appear in one command.

Because the tag lives in the key's template-literal type, cross-slot combinations are a compile error on `script().run()`, `redis.watch()`, and a new `multi().keys([...])` declaration:

```ts
await redis.script(moveItem).run({
  keys: { from: carts.key("u1"), to: orders.key("u2") },
  //                                  ^ Type '"order:{u2}"' is not assignable to type
  //                                    'KeysMustShareOneHashSlot<"order:{u2}", "u1">'
  args: { amount: 1 }
});
```

A passing check means "no provable conflict", not "provably co-located": untagged keys and keys built from runtime ids pass silently. For those, install the runtime guard from the new `beni/cluster` entry:

```ts
import { assertSameSlot } from "beni/cluster";

const redis = beni(client, { cluster: assertSameSlot });
```

It verifies every multi-key command before it is sent and throws `CrossSlotError` naming both keys, both slots, and the layout that fixes it. Off by default, because cross-slot commands are legal on a single-node Redis; turn it on in development and CI.

You pass the checker rather than `true` for a reason worth stating: `beni()` has to reference the guard to install it, so a boolean would mean the root entry names it and no bundler could drop it, putting the CRC16 table and the error's fix-hint prose in every app that never turns the check on. Taking it as a value keeps all of that in `beni/cluster`, which is about 1.4 KB gzipped, roughly 15% of the default root entry. When the guard is absent each check is an optional call on an undefined function, which short-circuits argument evaluation, so the key arrays are never even built.

`beni/cluster` also exports `slotOf` and `hashTagOf`, verified against a live cluster-enabled Redis for every generated key.

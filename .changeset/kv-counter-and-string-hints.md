---
"benni": patch
---

Reaching for a counter or string command on a kv store now gets an error that names the fix.

`counter` and `string` are alternate views over a kv keyspace rather than kinds of their own, so `INCR` lives on `redis.counter(schema)` and `APPEND` on `redis.string(schema)`. Guessing `redis.query.views.incr("post-1")` first is common, and the old error answered it by printing every method the store does have, naming no fix:

```text
Property 'incr' does not exist on type '{ set: { (id: RedisKeyPart, value: number,
options: ConditionalSetOptions): Promise<boolean>; ... 10 more ...;
persist(id: RedisKeyPart): Promise<...>; } & Pick<...>'.
```

The store now carries a type-only member per absent command whose parameter type is the fix, so the fix is the error text:

```text
Argument of type '"post-1"' is not assignable to parameter of type
'"INCR is a counter command: use redis.counter(schema).incr(id)"'.
```

Covered: `incr`, `incrby`, `incrbyfloat`, `decr`, `decrby`, `append`, `getrange`, `setrange`, `strlen`. Nothing is added at runtime, so calling one from untyped JavaScript fails the way an absent method already failed, and `Object.keys` on a kv store is unchanged.

Also documented, all three found in the same DX pass:

- `ReplyShapeError` carries the value Redis returned on **`.reply`**, not `.value`. The API reference already said so; the README philosophy bullet and `llms.txt` did not, which is where someone looks mid-incident.
- `examples.md` now shows reading a field off an `xrange` entry. An entry is `{ id, value }` and `value` is a `Partial` of the declared fields, because a stream entry can legally carry any subset of them.
- [Philosophy](https://chichurita.github.io/benni/getting-started/philosophy/) and `llms.txt` now state how arguments map to commands, so the shape of a method you have not called yet is predictable: one fixed form takes positional arguments in the command's own order (`zremrangebyscore(id, min, max)`), while modifiers or several forms take a single options object (`zrange(id, { start, stop, rev })`). `zrange` keeps its bounds in the object because they are indexes, scores, or lex bounds depending on the modifier beside them.

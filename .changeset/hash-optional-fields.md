---
"benni": minor
---

Hash fields can be declared `optional()`, so adding a field to a schema no longer breaks every record written before it.

```ts
import { hash, number, optional, string } from "benni/schema";

export const users = hash("user", {
  name: string(),
  score: number(),
  bio: optional(string())
});

const user = await redis.query.users.hget("42");
//    ^? { name: string; score: number; bio?: string } | null
```

Until now, `hget(id)` threw `PartialRecordError` whenever any declared field was missing, so a new field meant backfilling every stored record before the deploy that read it.

- **Reads.** A missing optional field is absent from the record (`"bio" in user` is `false`); it is never `undefined` or `null` as a value. A missing required field still throws `PartialRecordError`, whose `missing` now lists only the required fields and whose message reads "missing required field(s)" rather than "missing declared field(s)". `hgetall` is unchanged: every field optional, never throws.
- **Writes.** `hset(id, record)` lets an optional field be left out, still requires every required field (a forgotten one stays a compile error), and replaces the record: an optional field left out or passed as `undefined` is deleted with an `HDEL` in the same `MULTI`/`EXEC` as the `HSET`, so the next `hget` returns what was written rather than a value left over from an earlier write. With every field present it is still one plain `HSET`.
- **Types.** `InferInput` / `InferOutput` type optional fields `?:`. `hget(id, field)`, `hset(id, field, value)`, and `hsetnx` take and return the field's own type with no `undefined` added.
- **Only hashes read the marker.** `optional()` on a kv value, a stream field, or a script arg is the wrapped codec unchanged, and the value stays required.

There is no default-value wrapper: `user.bio ?? ""` at the read keeps the fallback visible, where a default would have to invent an answer for `hgetall`, `hmget`, and `hgetdel` on a field that was never stored.

Also documented: `hgetall` and `redis.scan.hash` leave out fields the schema does not declare, deliberately. That is what keeps removing a field from a schema safe; `hkeys` lists the strays.

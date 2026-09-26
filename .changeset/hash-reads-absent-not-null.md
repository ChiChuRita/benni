---
"benni": minor
---

**Breaking:** `hmget`, `hgetex`, and `hgetdel` leave a field Redis has no value for off the result, instead of setting it to `null`.

```ts
const fields = await redis.query.users.hmget("42", ["name", "score"]);

// before: { name: "Ada", score: null }, typed { name?: string | null; score?: number | null }
if (fields.score === null) { /* not stored */ }

// after: { name: "Ada" }, typed { name?: string; score?: number }
if (fields.score === undefined) { /* not stored */ }
```

The old type allowed a missing field to be absent *or* `null`, two spellings of one fact. Absent is now the only one on every hash read that returns an object: an optional field on `hget(id)`, every field on `hgetall`, and these three. Code that tests `=== null` must test `=== undefined` (or use `??`, which already handled both). The bare `hget(id, field)` still returns `null`, since a lone value cannot be absent.

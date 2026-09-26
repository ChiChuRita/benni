---
"benni": minor
---

**Breaking:** `benni()` takes one config object, and `schema` is required whenever the schema type is named.

The positional `benni(client, { schema })` form is removed (it now throws, saying which shape to use), so there is one way to build a handle. And `benni<typeof schema>({ client })`, which compiled and then gave a `redis.query` that was empty at runtime, is now a compile error: `schema` is required whenever the type argument is not the default.

```ts
// Before
const redis = benni(client, { schema, cluster: assertSameSlot });
const typed = benni<typeof schema>({ client }); // compiled; redis.query.users was undefined

// After
const redis = benni({ client, schema, cluster: assertSameSlot });
const typed = benni({ client, schema }); // the type argument is inferred
```

`BenniOptions` no longer carries `schema` and is no longer generic; `BenniConfig<TSchema, TClient>` is the full config type.

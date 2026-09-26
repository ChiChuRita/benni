---
"benni": minor
---

**Breaking:** the `$inferInput` / `$inferOutput` properties are gone from schema types. Use `InferInput` / `InferOutput`, which already existed and now are the one way to name a schema's value types.

```ts
// before
type NewUser = typeof users.$inferInput;
type StoredProfile = typeof profiles.$inferOutput;

// after
import type { InferInput, InferOutput } from "benni/schema";
type NewUser = InferInput<typeof users>;
type StoredProfile = InferOutput<typeof profiles>;
```

The properties compiled as real, readable members of every schema, but they were `undefined` at runtime, so `users.$inferInput.name` type-checked and then threw. The type now hangs off a `declare`d, unexported symbol key that is optional in the type: nothing can index a schema with it, and the type no longer claims a property the object lacks. `InferInput` / `InferOutput` read it where a schema has it and fall back to the schema's `encode`/`decode` elsewhere, so they also cover `geo`, `hll`, `channel`, and `pattern` schemas, which never had the anchors the old doc comment said every schema carried.

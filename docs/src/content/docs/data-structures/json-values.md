---
title: "JSON Values"
description: "Benni JSON values are encoded into normal Redis string values with JSON.stringify and decoded with JSON.parse."
---

Benni JSON values are encoded into normal Redis string values with `JSON.stringify` and decoded with `JSON.parse`.

`json` has two forms. Reach for the validating one by default:

```ts
import { z } from "zod";
import { json, kv } from "benni/schema";

const Settings = z.object({
  theme: z.enum(["light", "dark"]),
  emailNotifications: z.boolean()
});

export const settings = kv("settings", json(Settings));
```

`json(validator)` accepts any [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType, …), infers the value type from it (no type parameter needed), and checks **every read** at runtime.

Write JSON:

```ts
await redis.kv(settings).set("user:42", {
  theme: "dark",
  emailNotifications: true
});
```

Read JSON:

```ts
const value = await redis.kv(settings).get("user:42");
//    ^? { theme: "light" | "dark"; emailNotifications: boolean } | null (validated)
```

Data that does not match throws a `ReplyShapeError` naming the failing paths, at the read that found it.

With Redis directly:

```ts
await nodeRedis.set(
  "settings:user:42",
  JSON.stringify({
    theme: "dark",
    emailNotifications: true
  })
);

const raw = await nodeRedis.get("settings:user:42");
const value = raw === null ? null : JSON.parse(raw);
```

## `json<T>()` Is The Unchecked Escape Hatch

The second form takes a TypeScript type instead of a validator:

```ts
type Settings = {
  theme: "light" | "dark";
  emailNotifications: boolean;
};

export const settings = kv("settings", json<Settings>()); // no runtime check
```

`json<T>()` is a pure cast. `T` is asserted, never verified: `JSON.parse` runs, the result is handed back as `T`, and nothing compares the two. Be clear about what that means:

- A stored value missing required fields reads back as a complete `T`. Store `{"slug":"a"}` under a `json<ClickEvent>()` where `ClickEvent` has three required fields, and the read resolves to `{ slug: "a" }` typed as a full `ClickEvent`. Nothing throws; the `undefined`s surface much later, somewhere else.
- A field of the wrong type reads back as the declared type.
- Extra fields are kept and invisible to the types.
- Only genuinely malformed JSON throws, and only because `JSON.parse` refuses it.

Use it when the value's provenance is genuinely beyond doubt and you accept that cost: a cache entry your own code wrote in the same deploy, a value you are about to revalidate anyway, or a hot path where you have measured the validator and decided against it. Anything that crosses a version boundary, a service boundary, or a schema change (which is most stored data, since Redis outlives your process) wants `json(validator)`.

Note which direction each form covers. Standard Schema defines reads only, so `json(validator)` validates reads and trusts writes. To validate both, and to store rich types like `Date` that genuinely round-trip, use a [Zod codec](/benni/integrations/zod/) via `zodJson(schema)`.

Both forms work in every value-carrying schema, not just `kv`: `list("events", json(ClickEvent))` validates each element it decodes, while `list("events", json<ClickEvent>())` does not.

See [schema builders](/benni/api/schema-builders/) for the full codec list.

Use JSON key-value schemas when your app treats the value as a document. Use hashes when individual fields need independent Redis operations.

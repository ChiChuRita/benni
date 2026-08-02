---
title: "JSON Values"
description: "Benni JSON values are encoded into normal Redis string values with JSON.stringify and decoded with JSON.parse."
---

Benni JSON values are encoded into normal Redis string values with `JSON.stringify` and decoded with `JSON.parse`.

```ts
import { json, kv } from "benni/schema";

type Settings = {
  theme: "light" | "dark";
  emailNotifications: boolean;
};

export const settings = kv("settings", json<Settings>());
```

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
//    ^? Settings | null
```

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

With `json<T>()`, `T` is trusted, not validated at runtime. To validate reads, pass any [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType, …) instead; the value type is inferred from it:

```ts
import { z } from "zod";

const Settings = z.object({
  theme: z.enum(["light", "dark"]),
  emailNotifications: z.boolean()
});

export const settings = kv("settings", json(Settings));

const value = await redis.kv(settings).get("user:42");
//    ^? { theme: "light" | "dark"; emailNotifications: boolean } | null (validated)
```

Invalid stored data throws a `ReplyShapeError` naming the issues. See [schema builders](/benni/api/schema-builders/) for details.

Standard Schema defines only the read direction. To also validate writes, and to store rich types like `Date` that genuinely round-trip, use a [Zod codec](/benni/integrations/zod/) via `zodJson(schema)` instead.

Use JSON key-value schemas when your app treats the value as a document. Use hashes when individual fields need independent Redis operations.

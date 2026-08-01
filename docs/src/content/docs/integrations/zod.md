---
title: "Zod"
description: "Bidirectional Zod codecs as Beni field codecs: writes validated with z.encode, reads validated with z.decode, and rich types (Date, bigint, URL) that genuinely round-trip."
---

Beni's core already accepts any [Standard Schema](https://standardschema.dev)
validator via [`json(schema)`](/beni/api/schema-builders/), but Standard
Schema only defines *one* direction, so that validates **reads only**, and
writes are a blind `JSON.stringify`. [Zod codecs](https://zod.dev/codecs)
(Zod 4.1+) define both directions, and `beni/zod` runs them both:

- **Writes are validated.** A bad value throws `ValidationError` at the
  `set`, before anything is sent, not at some later read in another process.
- **Rich types round-trip.** `Date`, `bigint`, `URL`, custom classes, stored
  in their string / JSON-safe form, revived on read. With plain `json<T>()`,
  a `Date` field silently comes back as a `string`.

```ts
import * as z from "zod";
import { kv } from "beni/schema";
import { zodCodec, zodJson } from "beni/zod";

const isoDate = z.codec(z.iso.datetime(), z.date(), {
  decode: (iso) => new Date(iso),
  encode: (date) => date.toISOString()
});

const user = z.object({ name: z.string(), created: isoDate });
export const users = kv("user", zodJson(user));

await redis.kv(users).set("u1", { name: "ada", created: new Date() });
const found = await redis.kv(users).get("u1");
//    ^? { name: string; created: Date } | null (created is a real Date)
```

Zod is an **optional peer dependency** (`zod@^4.1.0`); only the
`beni/zod` subpath imports it. The adapter is built against `zod/v4/core`,
so schemas from both `zod` and `zod/mini` work.

## `zodCodec(schema)`: string-stored fields

Takes any Zod schema or codec whose *encoded* (input) side is a string and
returns a Beni `Codec`. Use it anywhere a codec is accepted: kv values,
hash fields, list items, set and sorted-set members, stream fields, pub/sub
messages.

```ts
import { hash, string } from "beni/schema";
import { zodCodec } from "beni/zod";

export const sessions = hash("session", {
  userId: string(),
  expiresAt: zodCodec(isoDate) // Date in your code, ISO string in Redis
});
```

A plain string schema works too: `zodCodec(z.email())` stores the string
as-is and validates it in both directions. Passing a schema whose encoded
side isn't a string (`z.number()`, `z.date()`, …) is a compile error.

## `zodJson(schema)`: JSON-stored values

A stronger [`json(schema)`](/beni/data-structures/json-values/): writes run
`z.encode` (validated, codec fields converted to their JSON-safe form) then
`JSON.stringify`; reads run `JSON.parse` then `z.decode` (validated, codec
fields revived).

Fields that aren't JSON-safe need a codec to a JSON-safe form, like `isoDate`
above. A bare `z.date()` field would stringify on write but fail loudly on
read, which is still better than `json<T>()`'s silent type lie, but a codec
is the actual fix.

## Useful codecs

Zod doesn't ship codec presets; its [codecs page](https://zod.dev/codecs)
maintains copy-paste implementations for the common ones: `Date` ↔ ISO
string (above), plus:

```ts
const bigintString = z.codec(z.string().regex(/^-?\d+$/), z.bigint(), {
  decode: (s) => BigInt(s),
  encode: (b) => b.toString()
});

const urlString = z.codec(z.url(), z.instanceof(URL), {
  decode: (s) => new URL(s),
  encode: (url) => url.href
});
```

## Errors

The adapter maps into Beni's unified error classes:

- Encode failures throw
  [`ValidationError`](/beni/api/schema-builders/), a caller mistake;
  nothing is sent to Redis.
- Decode failures throw `ReplyShapeError` with the stored string attached as
  `.reply`, and the message names the failing paths
  (`created: Invalid ISO datetime`).
- Async schemas (`.refine(async …)`) can't run in a synchronous codec; both
  directions throw `ValidationError` telling you so. If such a refinement
  *rejects* instead of just failing, zod discards that promise internally, so
  the rejection also surfaces as an unhandled rejection Beni cannot claim.
  Keep async work out of the schema.
- `zodJson` refuses values JSON cannot carry faithfully: `NaN`, `Infinity`,
  `BigInt`, and circular structures all throw `ValidationError` before the
  write, exactly as the plain `json()` codec does. A non-finite number would
  otherwise be stored as `null` and read back as if the key were missing.
- `zodCodec` needs a schema whose encoded side really is a string. `z.any()`
  satisfies the type constraint without checking anything, so a non-string
  encode result throws `ValidationError` rather than reaching Redis as
  `[object Object]`.

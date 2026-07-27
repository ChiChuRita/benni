---
"beni": minor
---

Make the root entry tree-shakable — a kv-only app drops from 13.9 kB to 4.2 kB gzip.

`beni()` used to dispatch with a `switch (schema.kind)` that named all twelve
store factories, and `createStoreAccessors` / the session facade named them
again. Every one of those is a static reference, so a bundler had to retain
sorted-set, stream, geo, bitmap and the rest even for an app that declares a
single hash. The cost was flat: 13.9 kB gzip no matter what you used.

Each schema now carries its own store factory on a non-enumerable symbol,
stamped by the `define*` builder in that kind's own module. `beni()` dispatches
through the schema and names no store at all, so the only store code a bundle
retains is the kinds the app actually declares. The pub/sub hub and the script
runner became lazy for the same reason — an app with no channel never pulls in
pub/sub.

Measured with rolldown, minified + gzipped:

| app | before | after |
| --- | --- | --- |
| `beni` + kv only | 13.9 kB | 4.2 kB |
| `beni/upstash` + one hash schema | 15.2 kB | 7.0 kB |
| three kinds (hash + zset + list) | 15.2 kB | 10.2 kB |

**The public API is unchanged.** `redis.query.<name>`, `redis.hash(schema)`,
the session accessors, `QueryResource`, `Beni<typeof schema>` — same
signatures, same inferred types, verified by the existing type-level tests.

**One behavior change.** Schemas are no longer plain data: a copy that drops
the symbol (object spread, `structuredClone`, a JSON round-trip) is no longer
usable. Passing one now throws a `TypeError` naming the offending export, at
`beni()` bind time rather than at first call. Pass the schema object the
builder returned.

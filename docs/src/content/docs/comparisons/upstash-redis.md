---
title: "Benni vs @upstash/redis"
description: "An honest comparison of Benni and @upstash/redis for edge and serverless TypeScript: where they overlap, what each types, and when vendor independence matters."
---

These two overlap more than [Benni and ioredis](/benni/comparisons/ioredis/) do:
both run on edge and serverless runtimes, and both can talk to Upstash over
HTTP. In fact Benni's edge adapter speaks the **same Upstash REST protocol**:
`benni/upstash` is a client for it, not an alternative to it.

The real difference is what gets typed, and whether your application code is tied
to one transport.

## Choose by what you need

**Reach for `@upstash/redis` when** you are all-in on Upstash, want the officially
supported client, or depend on its ecosystem (`@upstash/ratelimit`,
`@upstash/vector`, and the Upstash-specific conveniences).

**Reach for Benni when** you want your declared types to survive the round trip,
and you want the same code to run over TCP in development and HTTP in production
without a rewrite.

## Types you assert vs types you declare

`@upstash/redis` is typed, and it lets you pass a type parameter on a read:

```ts
const profile = await redis.get<Profile>("profile:42");
//    ^? Profile | null
```

That is a genuine convenience, but the type is an **assertion at the call site**,
not a derivation. Nothing checks that the value written to `profile:42` was ever a
`Profile`, nothing stops a different call site from asserting a different type for
the same key, and there is no single place that says what lives there. It is a
tidier cast.

Benni inverts it. You declare the key family once, and both directions are
checked against that declaration:

```ts
// schema.ts: the single source of truth
export const profiles = kv("profile", json<Profile>());

// writes are checked against the schema…
await redis.query.profiles.set("42", profile);

// …and reads derive their type from it
const loaded = await redis.query.profiles.get("42");
//    ^? Profile | null
```

Swap `json<Profile>()` for `json(profileZodSchema)` and reads are validated at
runtime too, through any [Standard Schema](https://standardschema.dev) validator
(Zod, Valibot, ArkType) with no extra dependency. An asserted generic cannot do
that, because there is nothing to validate against.

## One API, both transports

This is the practical reason to prefer Benni even on Upstash. `@upstash/redis` is
an HTTP client, so the stateless surface is all you get. Benni's typed API is
identical across adapters:

```ts
// development: real Redis over TCP
import { node } from "benni/node";
const client = await node({ url: process.env.REDIS_URL });

// production: Upstash over HTTP
import { upstash } from "benni/upstash";
const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});
```

Everything after that line, every schema, every query, every primitive, is
unchanged. You can develop against a local Redis in Docker, deploy to the edge,
and move a workload back to a long-running Node process later without touching
application code.

It also means you are not locked to one vendor. `benni/upstash` works against any
Upstash-REST-compatible server, including self-hosted
[`serverless-redis-http`](https://github.com/hiett/serverless-redis-http) in front
of your own Redis.

## What Benni does not do

- **No Pub/Sub subscribing over HTTP.** This is a protocol limit, not a Benni
  choice: subscribing needs a persistent connection, so it requires `benni/node`
  or `benni/bun`. Publishing is a single stateless `PUBLISH` and works fine on the
  edge.
- **No blocking commands, sessions, or `WATCH` transactions over HTTP**, for the
  same reason. Pipelines and atomic `MULTI`/`EXEC` do work: they map onto the
  REST `/pipeline` and `/multi-exec` endpoints.
- **No binary command arguments over REST.** Use the `bytes()` codec, which
  stores base64 strings, or a TCP adapter.
- **Not officially supported by Upstash.** If you need a vendor support channel
  for client bugs, the official client is the safer pick.
- **No Upstash-specific extras.** Benni models Redis, not the Upstash platform.

## Side by side

| | `@upstash/redis` | Benni |
| --- | --- | --- |
| Transport | HTTP/REST | TCP *and* HTTP, same API |
| Runs on edge/serverless | Yes | Yes, via `benni/upstash` |
| Command-level types | Yes | Yes |
| Read types | Asserted per call (`get<T>`) | Derived from one schema |
| Write types checked | No | Yes |
| Runtime validation | No | Yes, via any Standard Schema validator |
| Schema-derived keys | Manual | Yes |
| Vendor independence | Upstash | Any Upstash-REST-compatible server |
| Rate limit / lock / cache / queue | `@upstash/ratelimit` and friends | Built into `benni/primitives` |
| Officially supported by Upstash | Yes | No |
| Dependencies | Zero | Zero on the edge adapter |

## Using both

Nothing stops you. Schemas describe keys, they do not own them, so
`@upstash/ratelimit` and Benni can share the same database without interfering.
`redis.raw.send([...])` is also always available if you want to issue a command
Benni has not modelled rather than reaching for a second client.

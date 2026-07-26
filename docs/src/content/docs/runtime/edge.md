---
title: "Edge (Upstash / HTTP)"
description: "Run the same typed Beni API on serverless and edge runtimes over Upstash's REST protocol, with nothing but fetch."
---

The `beni/upstash` adapter speaks the [Upstash REST protocol](https://upstash.com/docs/redis/features/restapi) over HTTP, so the **same typed Beni API** runs on serverless and edge runtimes — Cloudflare Workers, Vercel Edge, Fastly, Deno Deploy — with nothing but `fetch`. It has **zero dependencies**.

```ts
import { beni } from "beni";
import { upstash } from "beni/upstash";
import * as schema from "./schema";

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});

export const redis = beni(client, { schema });
```

There is no connection to open, so `upstash` is synchronous (no `await`). Command arrays are `POST`ed directly to the REST endpoint; `pipeline` uses `/pipeline` and `redis.multi()` uses `/multi-exec` (atomic `MULTI`/`EXEC`).

## What works and what doesn't

HTTP is stateless — one request, one response, no persistent exclusive connection. So the adapter serves the whole **command surface** but not the features that need a held connection:

| Works over HTTP | Not available over HTTP |
| --- | --- |
| All typed data-structure stores (`hash`, `kv`, `set`, `list`, `zset`, `stream`, `bitmap`, `geo`, `hll`) | [Sessions](/beni/advanced/sessions/) — `redis.session()` |
| `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` (the cursor rides in the command) | Blocking commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`/`MAX`, `XREAD BLOCK`) |
| Lua scripts, `BITFIELD`, geo, HyperLogLog | `WATCH`-based optimistic transactions — `redis.watch()` |
| `redis.multi()` (atomic `/multi-exec`) | Pub/Sub — there is no `createUpstashRedisPubSub` |

`redis.session()` and `redis.watch()` throw a clear `TypeError` on this client, because the adapter deliberately omits `session`. When you need those, use a TCP adapter ([Node](/beni/runtime/node/) or [Bun](/beni/runtime/bun-and-deno/)) on a long-lived server.

Binary (`Uint8Array`) command arguments are not supported over REST — use the `bytes()` codec, which stores base64 strings, or a TCP adapter.

## Any Upstash-REST-compatible endpoint

The adapter is not tied to Upstash's hosted service. It works against anything that speaks the same protocol, including [`serverless-redis-http`](https://github.com/hiett/serverless-redis-http) (SRH) — a self-hostable proxy you can run in front of a plain Redis for local development or CI:

```sh
docker run -p 8079:80 \
  -e SRH_MODE=env -e SRH_TOKEN=example_token \
  -e SRH_CONNECTION_STRING="redis://host.docker.internal:6379" \
  hiett/serverless-redis-http
```

```ts
const client = upstash({
  url: "http://127.0.0.1:8079",
  token: "example_token"
});
```

Beni runs the same shared client-contract suite that pins the Node and Bun adapters against SRH over HTTP, so the typed stores behave identically to a TCP connection (minus the session-only features above).

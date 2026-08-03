---
title: "Edge (Upstash / HTTP)"
description: "Run the same typed Benni API on serverless and edge runtimes over Upstash's REST protocol, with nothing but fetch."
---

The `benni/upstash` adapter speaks the [Upstash REST protocol](https://upstash.com/docs/redis/features/restapi) over HTTP, so the **same typed Benni API** runs on serverless and edge runtimes (Cloudflare Workers, Vercel Edge, Fastly, Deno Deploy) with nothing but `fetch`. It has **zero dependencies**.

```ts
import { benni } from "benni";
import { upstash } from "benni/upstash";
import * as schema from "./schema";

const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string
});

export const redis = benni(client, { schema });
```

There is no connection to open, so `upstash` is synchronous (no `await`). Command arrays are `POST`ed directly to the REST endpoint; `pipeline` uses `/pipeline` and `redis.multi()` uses `/multi-exec` (atomic `MULTI`/`EXEC`).

## What works and what doesn't

HTTP is stateless: one request, one response, no persistent exclusive connection. So the adapter serves the whole **command surface** but not the features that need a held connection:

| Works over HTTP | Not available over HTTP |
| --- | --- |
| All typed data-structure stores (`hash`, `kv`, `set`, `list`, `zset`, `stream`, `bitmap`, `geo`, `hll`) | [Sessions](/benni/advanced/sessions/) via `redis.session()` |
| `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` (the cursor rides in the command) | Blocking commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`/`MAX`, `XREAD BLOCK`) |
| Lua scripts, `BITFIELD`, geo, HyperLogLog | `WATCH`-based optimistic transactions via `redis.watch()` |
| `redis.multi()` (atomic `/multi-exec`) | [Pub/Sub](/benni/data-structures/pubsub/) **subscribing** (there is no subscriber connection to hold) |
| Pub/Sub **publishing** (`PUBLISH` is one stateless command) | |

`redis.session()` and `redis.watch()` throw a clear `TypeError` on this client, because the adapter deliberately omits `session`. `redis.pubsub.channel(...).subscribe(...)` throws the same way, because it omits `subscriber` for the same reason. When you need those, use a TCP adapter ([Node](/benni/runtime/node/) or [Bun](/benni/runtime/bun-and-deno/)) on a long-lived server.

Publishing is the useful half on the edge, and it needs nothing held open. An edge handler can fan an event out to long-lived workers that subscribe over TCP:

```ts
await redis.pubsub.channel(userEvents).publish({ id: "42", action: "created" });
```

Binary (`Uint8Array`) command arguments are not supported over REST; use the `bytes()` codec, which stores base64 strings, or a TCP adapter.

## Any Upstash-REST-compatible endpoint

The adapter is not tied to Upstash's hosted service. It works against anything that speaks the same protocol, including [`serverless-redis-http`](https://github.com/hiett/serverless-redis-http) (SRH), a self-hostable proxy you can run in front of a plain Redis for local development or CI:

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

Benni runs the same shared client-contract suite that pins the Node and Bun adapters against SRH over HTTP, so the typed stores behave identically to a TCP connection (minus the session-only features above).

### A failed transaction may not carry a Redis error

One difference the contract suite does record, because it is the endpoint's choice rather than Benni's. Over REST a service sits in front of Redis and decides what a failed `MULTI`/`EXEC` looks like on the wire, and SRH answers with a 5xx carrying nothing:

```text
POST /pipeline    [["PING"],["ZADD","str","1","member"]]
  -> 200  [{"result":"PONG"},{"error":"WRONGTYPE Operation against a key..."}]

POST /multi-exec  [["PING"],["ZADD","str","1","member"]]
  -> 500  (no body)
```

With no reply to read, `redis.multi().exec()` rejects with a transport `Error` rather than a [`RedisServerError`](/benni/api/errors/). Benni will not invent a `.code` from a gateway's status line, because that would hand you a `RedisServerError` for what might equally be an upstream outage.

What this does and does not change:

- A failed transaction **always rejects**, on every adapter. It never resolves as though it committed.
- Single commands and pipelines are unaffected: both carry `{ "error": ... }`, so both normalize to `RedisServerError` with the code parsed.
- Code that branches on `.code` should confirm the error is a `RedisServerError` first, which is the rule everywhere anyway:

```ts
try {
  await redis.multi().add(["ZADD", key, "1", "member"], numberReply).exec();
} catch (error) {
  if (error instanceof RedisServerError && error.code === "WRONGTYPE") {
    // Redis said no, and said why
  } else {
    // the transaction failed without an attributable reply: retry or surface it
  }
}
```

A hosted endpoint may well return a readable error where SRH does not. The contract suite asserts only what the transport can actually guarantee, so write the `catch` above and it is correct against both.

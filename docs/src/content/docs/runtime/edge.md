---
title: "Edge (Upstash / HTTP)"
description: "Run the same typed Benni API on serverless and edge runtimes over Upstash's REST protocol, with nothing but fetch."
---

The `benni/upstash` adapter speaks the [Upstash REST protocol](https://upstash.com/docs/redis/features/restapi) over HTTP, so the **same typed Benni API** runs on serverless and edge runtimes (Cloudflare Workers, Vercel Edge, Fastly, Deno Deploy) with nothing but `fetch`. It has **zero dependencies**.

```ts
import { benni } from "benni";
import { upstash } from "benni/upstash";
import * as schema from "./schema";

export const redis = benni({
  client: upstash({
    url: process.env.UPSTASH_REDIS_REST_URL as string,
    token: process.env.UPSTASH_REDIS_REST_TOKEN as string
  }),
  schema
});
```

There is no connection to open, so `upstash` is synchronous like every adapter, and it sends nothing until the first command. Command arrays are `POST`ed directly to the REST endpoint; `pipeline` uses `/pipeline` and `redis.multi()` uses `/multi-exec` (atomic `MULTI`/`EXEC`). A missing `url` or `token`, the usual symptom of an unset environment variable, throws `TypeError` right there instead of surfacing later as a 401.

## Timeouts and cancellation

A request that never answers hangs until the runtime gives up, unless you bound it. `timeoutMs` aborts any request, response body included, that has not finished in time, and `signal` ties every request to an `AbortSignal` of yours:

```ts
const client = upstash({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string,
  timeoutMs: 2_000,
  signal: request.signal // e.g. stop when the incoming request is cancelled
});
```

A timed-out request rejects with a `DOMException` named `"TimeoutError"`; an aborted one rejects with the signal's reason, and so does every later request on that client. Neither is a `RedisServerError`. There is no default timeout. A timeout does not undo a command the server already received: a write that times out may still have been applied.

`redis.close()` has no connection to tear down, but it is final like every adapter's: requests already in flight finish, and any command issued afterwards rejects instead of reaching the server.

Pass `onError` to see every request that failed in transit (a network error, an HTTP failure status, a timeout) in one place, for logging or metrics. The command still rejects with it; error replies from Redis and aborts through your own `signal` are not reported. There is no `onReconnect`, since there is no connection.

## Errors: what came from Redis and what did not

`RedisServerError` means Redis refused a command. Over REST that is a `400` (or, in a pipeline, a `200` element) carrying `{ "error": "WRONGTYPE ..." }`. Every other failure status is the service in front of Redis refusing or failing the *request*: `401` for a bad token, `403`, `413`, `429`, any `5xx`. Those reject as a plain `Error` whose message keeps the status and the service's text, such as `Upstash HTTP 401: Unauthorized`, so they never carry a `.code` parsed out of prose.

## Binary-safe responses

The adapter asks for base64-encoded responses (`Upstash-Encoding: base64`, the default of Upstash's own client too) and decodes them, so a value that is not valid UTF-8 no longer breaks the server's JSON encoding (`serverless-redis-http` answers such a `GET` with an empty body otherwise). Values still arrive as strings decoded as UTF-8, exactly as the TCP adapters decode them, so invalid bytes become `U+FFFD` on every adapter alike. Store binary data with the `bytes()` codec. The endpoint has to honour the header; Upstash and `serverless-redis-http` both do.

## What works and what doesn't

HTTP is stateless: one request, one response, no persistent exclusive connection. So the adapter serves the whole **command surface** but not the features that need a held connection:

| Works over HTTP | Not available over HTTP |
| --- | --- |
| All typed data-structure stores (`hash`, `kv`, `set`, `list`, `zset`, `stream`, `bitmap`, `geo`, `hll`) | [Sessions](/benni/advanced/sessions/) via `redis.session()` |
| `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` (the cursor rides in the command) | Blocking commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`/`MAX`, `XREAD BLOCK`) |
| Lua scripts, `BITFIELD`, geo, HyperLogLog | `WATCH`-based optimistic transactions via `redis.watch()` |
| `redis.multi()` (atomic `/multi-exec`) | [Pub/Sub](/benni/data-structures/pubsub/) **subscribing** (there is no subscriber connection to hold) |
| Pub/Sub **publishing** (`PUBLISH` is one stateless command) | |

A handle over this client has no `redis.session()` or `redis.watch()` in its type, because the adapter deliberately omits `session`, and its channels have `publish` but no `subscribe()`, because it omits `subscriber` for the same reason. Using one is a compile error; code that forces the call anyway gets `UnsupportedCapabilityError`. To type a handle by hand, name the client: `Benni<typeof schema, UpstashClient>`, with `import type { UpstashClient } from "benni/upstash"`. When you need those, use a TCP adapter ([Node](/benni/runtime/node/), [ioredis](/benni/runtime/ioredis/), or [Bun](/benni/runtime/bun-and-deno/)) on a long-lived server.

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

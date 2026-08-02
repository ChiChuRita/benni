---
"benni": minor
---

Server errors are now normalized across every adapter: Redis error replies arrive as one `RedisServerError`, with the error code parsed out.

Before, an error the Redis *server* returned reached the caller in whatever shape the underlying client used. On `benni/node` a `ZADD` against a key holding a hash threw node-redis's own `SimpleError`; `benni/ioredis` threw ioredis's `ReplyError`; `benni/bun` threw Bun's `RedisError`; `benni/upstash` threw a bare `Error` built from the REST payload. So `catch (error) { if (error instanceof ...) }` could not be written portably, and WRONGTYPE or NOSCRIPT handling written against one adapter silently stopped matching after a move to another one, which is the opposite of what one typed API across runtimes is supposed to buy.

- New `RedisServerError`, exported from `benni` and `benni/core`. It means the command reached Redis and Redis refused it, as opposed to `ValidationError` (benni refused the input before sending) and `ReplyShapeError` (a successful reply did not match the shape a decoder expected).
- `code` carries the reply's leading error code (`"WRONGTYPE"`, `"NOSCRIPT"`, `"NOAUTH"`, `"OOM"`, `"READONLY"`, `"BUSYGROUP"`, ...), so callers branch on a field instead of matching a substring of the message. It is `undefined` when the text carries no code, which in practice means a Lua script returned a bare `redis.error_reply(...)`.
- `command` names the command that drew the error, uppercased, wherever the throw site can attribute it: a single `send`, or a pipeline entry the adapter reports per command.
- `cause` holds the adapter-native error (or, for the HTTP adapter, the raw payload string), so nothing the underlying client attached is lost.
- `message` stays the server's text verbatim, code included, so message matching that predates this class keeps working.
- Also exported: `redisErrorCode(message)` for classifying a raw message, and `redisServerError(source, command?)`, the normalizer the adapters use, which passes an already normalized error through unchanged.

All four adapters agree, including their pipeline, `MULTI`, and `WATCH` paths. Client-side failures are deliberately left alone: a closed client, a dropped socket, an ioredis `MaxRetriesPerRequestError`, or an Upstash HTTP transport failure never came from Redis, so none of them is reported as a server error. The `MULTI` rejection unwrap on `benni/node` and `WATCH` abort detection on every adapter behave exactly as before, and cluster redirections are still followed by the cluster-aware client underneath.

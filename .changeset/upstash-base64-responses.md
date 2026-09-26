---
"benni": patch
---

`benni/upstash` can now read values that are not valid UTF-8.

The adapter asked for plain JSON responses, and a server cannot put arbitrary bytes into JSON: `hiett/serverless-redis-http` answered a `GET` of such a value with an empty body, which surfaced as `Upstash HTTP 200: non-JSON response`. It now sends `Upstash-Encoding: base64`, as Upstash's own client does by default, and decodes every string in the result. Values still arrive as UTF-8 decoded strings, exactly as `benni/node`, `benni/ioredis`, and `benni/bun` decode them, so invalid bytes become `U+FFFD` identically on every adapter; a shared test writes raw bytes over TCP and asserts the REST read equals the TCP read. Binary data still belongs in the `bytes()` codec.

The endpoint has to honour the header. Upstash does, and `serverless-redis-http` was verified to encode every string, simple replies such as `OK` included, while leaving integers, nil, and error texts alone.

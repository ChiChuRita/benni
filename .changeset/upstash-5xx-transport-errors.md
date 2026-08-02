---
"benni": patch
---

`benni/upstash` no longer reports an HTTP 5xx as a Redis error reply when the response carries an `{ "error": ... }` body.

Upstash uses that envelope for two different things: a genuine Redis error (200 for a pipeline element, 4xx for a single command), and a plain service failure from whatever sits in front of Redis. The adapter only checked whether the envelope was present, not the status, so a `502` with `{ "error": "upstream unavailable" }` came back as a `RedisServerError` attributed to the command, with a `code` parsed out of the gateway's own prose. That contradicted the boundary the error reference documents: `RedisServerError` means the command reached Redis and Redis refused it.

A 5xx is now a plain `Error` again, the way a non-JSON response and a dropped socket already were, and its message keeps the body's text so the failure stays debuggable (`Upstash HTTP 502: upstream unavailable`). A 4xx carrying the same envelope is still a real server reply and still normalizes to `RedisServerError` with its code, so `NOSCRIPT` handling and the script reload path are unaffected.

Found by an independent review of the normalization work. The misclassification predates that change; what was new was documenting a boundary the code did not hold to.

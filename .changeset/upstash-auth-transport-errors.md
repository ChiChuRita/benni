---
"benni": patch
---

`benni/upstash` no longer reports a rejected token, or any other failure status outside `400`, as a Redis error reply.

The 5xx fix drew the line at the status's first digit, so a `401 { "error": "Unauthorized" }` still became a `RedisServerError`, attributed to the command, with a `code` of `"Unauthorized"` read out of the service's prose. That contradicted the adapter's own comment and the errors reference: `RedisServerError` means the command reached Redis and Redis refused it. Over REST that is exactly a `400` (or a `200` pipeline element) carrying `{ "error": ... }`; `401`, `403`, `413`, `429`, and every `5xx` are the service in front of Redis refusing or failing the request, and now reject as a plain `Error` like a 5xx and a non-JSON body already did (`Upstash HTTP 401: Unauthorized`).

`upstash()` also throws `TypeError` at construction when `url` or `token` is missing or empty, the usual result of an unset environment variable, instead of letting every command fail with a 401 that no longer names the cause.

Verified against `hiett/serverless-redis-http`, which answers a wrong token with `401 { "error": "Invalid token" }`.

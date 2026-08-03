---
"benni": patch
---

Document that a failed `MULTI`/`EXEC` over REST may not carry a Redis error, and stop the shared client contract from asserting otherwise.

The integration job had been red since the 5xx boundary landed, and the failure was a real finding rather than a flake. Over REST a service sits in front of Redis and decides what a failed transaction looks like on the wire. Reproduced against `hiett/serverless-redis-http:latest` in front of `redis:8`, the endpoint CI runs:

```text
POST /pipeline    [["PING"],["ZADD","str","1","member"]]
  -> 200  [{"result":"PONG"},{"error":"WRONGTYPE Operation against a key..."}]

POST /multi-exec  [["PING"],["ZADD","str","1","member"]]
  -> 500  (no body)
```

There is no reply to normalize, so `redis.multi().exec()` rejects with a transport `Error`. That is `benni/upstash` behaving as intended: inventing a `.code` from a gateway's status line would hand the caller a `RedisServerError` for what might equally be an upstream outage. The shared contract was asserting a guarantee the transport cannot make.

- `expectRedisClientContract` takes `transactionErrorsCarryNoReply`, and the Upstash integration test sets it with the captured evidence. The assertion narrows rather than disappearing: a failed transaction must still reject on every adapter, and the TCP adapters keep the full `RedisServerError` plus `.code` assertion.
- [Edge runtime](https://chichurita.github.io/benni/runtime/edge/) gains "A failed transaction may not carry a Redis error", with the wire traffic, what does and does not change (single commands and pipelines are unaffected), and the `catch` that is correct against both kinds of endpoint.
- `llms.txt` no longer says error handling is uniform without qualification. A failed transaction always rejects; branch on `.code` only after confirming the error is a `RedisServerError`, which rule 8 already required.

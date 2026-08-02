---
"benni": minor
---

Add per-entity Pub/Sub channels. `redis.pubsub.channel(schema, id)` now addresses `name:<id>`, derived by the same key builder every keyspace uses, so publishing and subscribing to one channel per room, per user, or per job no longer means minting a schema per call or dropping to `redis.raw`. Without an id the resource still addresses the schema's own name, exactly as before.

- `channel(name, codec, { ids })` narrows the id type the way a keyspace's `ids` option does.
- `schema.channelName(id)` and `resource.channelName(id)` resolve the concrete channel, so the string is never hand-built.
- `resource.at(id)` scopes a resource reached through `redis.query`, and is what the second argument calls underneath.
- Id-scoped publishes are matched by a `pattern()` subscription over the prefix, because both sides derive the name the same way.

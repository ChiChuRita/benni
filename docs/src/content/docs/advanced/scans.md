---
title: "Scans"
description: "Use redis.scan to iterate keys and collection members incrementally without blocking Redis."
---

Use `redis.scan` to iterate keys and collection members incrementally without blocking Redis. Each scan returns an async iterable that pages through Redis cursors behind the scenes.

## Scan All Keys

```ts
for await (const key of redis.scan.keys({ match: "user:*", count: 500 })) {
  console.log(key);
}
```

Options:

- `match` filters keys server-side with a glob pattern.
- `count` is a page-size hint per round trip, not a result limit.
- `type` restricts results to one Redis type such as `"hash"` or `"string"`.

## Scan A Schema's Keys

```ts
for await (const key of redis.scan.kv(profiles)) {
  // key is "profile:<id>"
}
```

`redis.scan.kv` defaults `match` to the schema prefix (`profile:*`), so you only see keys that belong to that schema. Pass your own `match` to narrow it further.

## Scan Collection Members

```ts
for await (const member of redis.scan.set(teamMembers, "engineering")) {
  console.log(member);
}

for await (const entry of redis.scan.hash(users, "42")) {
  // entry is { field: "name"; value: string } | { field: "score"; value: number }
}

for await (const { member, score } of redis.scan.zset(leaderboards, "global")) {
  console.log(member, score);
}
```

Member scans wrap `SSCAN`, `HSCAN`, and `ZSCAN` and accept `{ match, count }`. Values decode through the schema's codecs, and hash scans skip fields that are not declared in the schema.

## Early Break Is Safe

```ts
for await (const key of redis.scan.keys({ match: "session:*" })) {
  if (await handle(key)) break;
}
```

Redis scan cursors are stateless on the server, so breaking out of the loop simply stops issuing `SCAN` calls. There is nothing to close or clean up.

## Guarantees

`SCAN` offers a weak snapshot: keys that exist for the whole scan are returned at least once, but keys created or deleted mid-scan may or may not appear, and a key can be returned more than once. Deduplicate when your use case needs exactly-once handling.

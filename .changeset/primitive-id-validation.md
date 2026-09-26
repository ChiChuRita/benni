---
"benni": minor
---

**Breaking:** `lock` rejects an id containing a `}` that does not close a hash tag, because its lock key could not share a Redis Cluster slot with its new fence counter.

```ts
// Before: acquired (and would have hit CROSSSLOT on a cluster once the fence
// existed). After: throws ValidationError before anything is sent.
await locks.acquire("a}b");

// Unaffected: ids with no braces, and ids with a real tag.
await locks.acquire("order:42");
await locks.acquire("{tenant-1}:order:42");
```

The lock key is unchanged, `<prefix>:<id>`. The fence counter has to be co-located with it: wrapping a key with no tag in braces makes the whole key its tag, and a key that already has a tag keeps it, but a key that hashes whole *and* contains a `}` would end the wrapping tag early. That one shape is refused rather than left to fail on a cluster only. The cache's matching id check is in its own changeset.

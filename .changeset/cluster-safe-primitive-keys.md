---
"beni": patch
---

Fix two key layouts that were broken or wasteful on a Redis Cluster.

**`beni/next`** — `revalidateTag` deleted cache entries and their tag sets in one `DEL`, but the keys carried no hash tag, so the command was `CROSSSLOT` and the handler simply did not work on a cluster. Every key is now tagged into one slot (`{next-cache}:entry:…`, `{next-cache}:tag:…`). That `DEL` was also unbounded: a popular tag naming tens of thousands of entries produced a multi-megabyte command that blocks the server, so it is now chunked at 500 keys, entries before tag sets (a crash midway then leaves a tag pointing at deleted entries, which is self-healing, rather than entries with no tag, which can never be revalidated).

**`beni/primitives`** — a cache entry and its own fill lock were `cache:<id>` and `cache:lock:<id>`, which hash to different slots: two nodes per miss, with the single-flight guarantee spread across them. The id now carries the tag (`cache:{<id>}` and `cache:lock:{<id>}`), so the pair is always co-located while the cache itself still spreads across the keyspace. Tagging the prefix instead would have pinned every entry to one node, which defeats the point of a cache.

Both are key renames, so existing entries are orphaned on upgrade. Both are TTL'd, so the impact is one cold window.

---
"beni": patch
---

Fix pub/sub subscriptions that could be silently killed by a concurrent unsubscribe or close.

A `subscribe` whose SUBSCRIBE was still on the wire was invisible to the teardown path, so any overlapping `unsubscribe` (on that channel or any other) could close the leased connection out from under it. The caller was handed a subscription that looked healthy, received nothing, and poisoned every later subscribe to the same name. Subscribes and unsubscribes for the same channel or pattern are now serialised, and the lease is held for as long as a subscribe is in progress.

Also fixed: `pubsub.close()` racing a first subscribe no longer wedges that channel name for the life of the process, the subscribe rejects instead; `close()` now ends a `stream()` loop that was started without an abort signal, rather than leaving it parked forever; a channel and a pattern that spell the same string no longer share one in-flight subscribe, which left the pattern with no PSUBSCRIBE and no working unsubscribe; and `stream()` no longer leaks an abort listener on the supplied signal when opening the subscription fails.

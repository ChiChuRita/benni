---
"benni": patch
---

`budget` is marked experimental, and its docs state the worst case instead of "can drift slightly".

- The two-bucket estimate allows up to **about twice the limit** inside one sliding window: spend the whole limit just before a bucket boundary, and it decays out of the estimate over the next bucket while still being real spend in the last `windowMs`.
- Buckets are aligned to the Unix epoch, so a daily window's buckets turn over at 00:00 UTC for every id, and the previous day's spend decays over the next day.
- Every `charge`, `reserve`, and `check` walks the id's live holds inside the Lua script, up to `maxHolds` (10000). The docs give the measured cost at that cap, about 2ms of Redis server time per call.

`budget`, `defineBudget`, and `BudgetStore` carry `@experimental` in their JSDoc, the docs page opens with a caution, and `llms.txt` says so. The algorithm is unchanged: none of the small fixes removes the 2× bound without an exact log, which is the wrong shape for a daily budget.

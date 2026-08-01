---
"beni": minor
---

Fix four correctness bugs in `budget`, and bound the reservation set.

Settling is now deduplicated in Redis on the reservation token instead of only
on the handle. A settle whose reply was lost on the way back, a socket reset, a
command timeout, a failover, used to charge the budget twice when the caller
retried it, metering the user at double their real spend. The retry is now a
no-op on the server, which is what the primitive always documented.

`retryAfterMs` now reports when the window actually frees enough units for the
spend that was denied. It used to report the time to the next bucket boundary,
which frees nothing, so a client that obeyed it retried at a moment guaranteed
to fail, or waited far longer than it had to.

A process stalled for longer than a whole window no longer gets a bogus answer.
The internal stale-bucket sentinel used to escape as a real reply: `charge`
returned a spurious denial, `reserve` a spurious exhaustion, and `settle`
resolved successfully having charged nothing. Those calls now throw
`BudgetWindowRolledError`, and a hold whose `settle` throws it stays usable.

`extend()` works for estimates of 1e14 and above. The hold was stored under a
member Lua had formatted to 14 significant digits, so the heartbeat could never
find it and the hold lapsed mid-call.

New `maxHolds` option, default 10000, caps how many reservations one id may
hold at once. Summing live holds walks the whole set, and an estimate of `0`
consumes no headroom, so nothing else bounded it.

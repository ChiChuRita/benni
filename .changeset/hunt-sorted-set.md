---
"beni": patch
---

Fix five sorted-set defects: three overloads that described the wrong reply
shape, and two validators that disagreed with their neighbours.

`zrange`, `zdiff`, `zunion`, `zinter`, and `zrandmember` all typed a
`WITHSCORES` reply as bare members whenever `withScores` was a plain `boolean`
rather than the literal `true`. Excess-property checking does not catch that,
because the flag is a declared member of every one of those option types, so
the members-only overload won and the caller got `SortedSetEntry` objects typed
as members. String handling downstream either threw or silently produced
`undefined`. The members-only overloads now exclude a `boolean` flag, so the
ambiguous call is a compile error the caller has to branch on. `zrandmember`
also had the `zpopmin` problem: a value typed `SortedSetRandomMemberOptions`,
whose `count` is optional, always landed on the single-member overload while
the server answered with an array. That overload now excludes `count`.

Score bounds accept `Infinity` and `-Infinity`. `zadd` and `zincrby` already
took them and `zscore` handed them back, but `zrange { byScore }`,
`zrangestore { byScore }`, and `zremrangebyscore` rejected the value the
library itself produced, and `zcount` validated nothing at all. All four now
translate the infinities to Redis's `+inf`/`-inf` and reject only `NaN`.

`zrandmember` accepts `count: 0` and returns `[]` without a round trip, the way
`zpopmin` already did and the way Redis behaves. A count that comes out of
`Math.min(wanted, remaining)` no longer throws when it reaches zero.

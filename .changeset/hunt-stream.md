---
"beni": patch
---

Fix four stream bugs.

`xadd` typed the reply as a plain `string` for any `nomkstream` the compiler
could not see was `true`, so a computed flag, or an options object typed
`StreamAddOptions`, resolved `null` under a non-nullable type. A spelled-out
`nomkstream: someBoolean` now types as `Promise<string | null>`; an options
value whose `nomkstream` is merely optional no longer compiles, because the
reply shape is not knowable from its type.

A stream field named `__proto__` was written to Redis but silently dropped on
read, and it replaced the decoded entry's prototype. Decoding now defines the
property instead of assigning it, so no field name can reach a setter on
`Object.prototype`.

`xtrim` rejected `{ maxLen: { count: 0 } }`, which is the only way to empty a
stream without deleting the key and its consumer groups along with it. Zero is
now accepted; negative and fractional counts still throw.

`xreadgroup({ after: undefined })` is typed as a read of new deliveries but
performed a history read, so tombstones arrived with a value declared
non-nullable. The read now dispatches on the `after` value rather than the
presence of the key: `undefined` reads `>`, an entry id reads history.

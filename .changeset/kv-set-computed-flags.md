---
"benni": patch
---

`kv.set(id, value, { nx: someBoolean })` is typed `Promise<boolean>`, which is what it resolves.

A computed `nx` or `xx` flag fell through to the overload typed `Promise<void>`, while the call resolved a boolean whenever the flag was on, the same lying-overload defect fixed earlier for `lpos`, `zpopmin`, and `xadd`. Spelling out either flag, literal or computed, now selects the boolean overload, and the runtime answers on the flag's presence the same way: `{ nx: false }` resolves `true` (a SET without NX always writes) instead of `undefined`. An options value typed `KeyValueSetOptions`, whose flags are merely optional, no longer compiles, because the reply shape is not knowable from its type; spell the flag out at the call site or branch on it.

```ts
const flag = request.onlyIfNew;
// Before: typed Promise<void>, resolved true/false when flag was true
// After: typed Promise<boolean>
const written = await redis.query.profiles.set("42", profile, { nx: flag });
```

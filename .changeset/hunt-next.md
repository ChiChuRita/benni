---
"beni": patch
---

Fix `beni/next` cache-handler tag sets never getting an expiry. The handler
extended a tag set's TTL with `EXPIRE ... GT`, which Redis refuses to apply to
a key that has no expiry yet, so the set the preceding `SADD` had just created
stayed permanent: it accumulated every key ever written under that tag, kept
naming entries that had long since expired, and made every `revalidateTag`
walk the whole accumulation. Adding a key to a tag set is now one atomic step
that installs the TTL when the set is new and only extends it afterwards, so a
tag set is reclaimed once its last entry expires while an entry with
`revalidate: false` still keeps its tag set permanent.

---
"beni": minor
---

Nine small correctness fixes across the core. Three of them can break a build
or a call that used to succeed silently: the `lpos` and `script` overload
changes below, and the new schema-definition and reply checks.

`beni()` no longer refuses to bind a schema module that co-exports a validator.
Any object carrying a `kind` property used to be claimed as a beni schema and
crash at bind time, which hit every module declaring a Valibot schema or an
ArkType type next to its beni schemas, the layout `json(validator)` invites.
The store binding decides now, and a copied beni schema still fails loudly and
names the export.

A `hashTag: "id"` prefix containing `{` is rejected when the schema is defined.
Redis takes the tag from the first brace in the whole key, so such a prefix
silently voided the co-location the layout exists for and only showed up as
CROSSSLOT on a real cluster.

Counter reads and BITFIELD reads now throw `ReplyShapeError` instead of
returning a silently rounded value past `Number.MAX_SAFE_INTEGER`. The write
side already refused unsafe integers; the read side now matches.

A script that returns its own `NOSCRIPT`-coded error is no longer mistaken for
a server-side cache miss and re-run. Beni confirms with `SCRIPT EXISTS` before
reloading a cached SHA, so a script's side effects are not applied twice.

`script()` no longer accepts a forwarded or computed `nullable`, and `lpos()`
no longer accepts an options bag whose `count` is `number | undefined`. Both
used to select an overload whose declared result type could not hold what the
call actually resolved. Passing either through a helper is now a compile error.

`bytes()` throws `ReplyShapeError` with the offending value on `.reply`, like
every other codec, instead of a bare `TypeError`.

`getrange`, `setrange`, and `strlen` are documented as working in bytes, which
is what Redis does. Chunked reads of non-ASCII values must split on byte
boundaries.

---
"beni": patch
---

Follow-ups from the 2026-08-01 hunt that spanned more than one area:

- `hmget`, `hgetex`, and `hgetdel` now declare their result as optional keys
  (`{ name?: string | null }`). They only fill the field names present at
  runtime, so declaring every member of the requested union as a present key
  promised data the reply need not contain.
- A whole-record `hget` that finds some but not all declared fields now throws
  `PartialRecordError`, a new subclass of `ReplyShapeError` carrying the absent
  names on `.missing`. The reply in that case is well formed and the record is
  merely incomplete, which per-field TTLs make an ordinary outcome, so a caller
  watching for protocol or adapter faults no longer has to treat it as one.
- The `string`, `enumOf`, and `boolean` codecs reject input they cannot
  represent instead of coercing it. `encode: String` turned an undefined field
  into the literal `"undefined"`, and `input ? "1" : "0"` turned it into a real
  `false`. `number` already refused non-finite input; the rest now match.
- A `NOSCRIPT` reply is confirmed with `SCRIPT EXISTS` before the script is
  reloaded and re-run, on the freshly loaded path as well as the cached one. A
  script that returns its own `NOSCRIPT`-shaped error is byte-identical to the
  server's, and re-running one that had already applied its side effects
  applied them twice.
- The Pub/Sub hub releases its subscriber lease even when the adapter's
  `unsubscribe` rejects. On a connection that had already died the detach
  always rejects, which left the dead subscriber cached as the hub's lease.

---
"beni": patch
---

Harden the `beni/zod` codecs so they fail at the write instead of storing something unrecoverable.

`zodJson` now shares the same stringify guard as the plain `json()` codec. Previously a `NaN` or `Infinity` anywhere in the value was written as JSON `null`, which reads back as the exact sentinel a missing key returns, so a written key became indistinguishable from an absent one. Non-finite numbers, `BigInt` fields, and circular structures now throw `ValidationError` before anything is sent.

Encode failures inside the zod bridge are also `ValidationError` again. A schema containing a one-way `.transform()` used to surface zod's own `$ZodEncodeError`, which does not extend `TypeError`, breaking the documented promise that every pre-send failure is a `ValidationError` and both error classes extend `TypeError`.

`zodCodec` now checks that the schema actually encoded to a string. `z.any()` satisfies the "encoded side is a string" type constraint without checking anything, so writes through it landed in Redis as `[object Object]` with no error anywhere.

The async-schema docs are corrected too: an async refinement that rejects leaves an unhandled rejection zod discards internally, which Beni has no way to claim.

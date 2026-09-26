---
"benni": patch
---

`benni/hono`'s `cache()` now replays binary responses byte for byte.

It stored `res.clone().text()` for every content type, which decodes the body as UTF-8, so a cached image, protobuf, or pre-compressed payload came back with every invalid byte turned into `U+FFFD`. The stored entry is now `{ status, headers, body64 }`, the exact bytes base64-encoded, and the docs no longer have to restrict the middleware to text. An entry written by an earlier version, in the old `{ body }` text form, reads as a miss and is replaced on the next request, so upgrading needs no cache flush.

---
"benni": patch
---

Three documentation fixes from the same DX pass:

- `ReplyShapeError` carries the value Redis returned on **`.reply`**, not `.value`. The API reference already said so; the README philosophy bullet and `llms.txt` did not, which is where someone looks mid-incident.
- `examples.md` now shows reading a field off an `xrange` entry. An entry is `{ id, value }` and `value` is a `Partial` of the declared fields, because a stream entry can legally carry any subset of them.
- [Philosophy](https://chichurita.github.io/benni/getting-started/philosophy/) and `llms.txt` now state how arguments map to commands, so the shape of a method you have not called yet is predictable: one fixed form takes positional arguments in the command's own order (`zremrangebyscore(id, min, max)`), while modifiers or several forms take a single options object (`zrange(id, { start, stop, rev })`). `zrange` keeps its bounds in the object because they are indexes, scores, or lex bounds depending on the modifier beside them.

---
"benni": patch
---

Editor hovers now print the value type itself instead of the alias that computes it.

```text
before  const user: InferHashOutput<{ name: Codec<string, string>; score: Codec<number, number> }> | null
after   const user: { name: string; score: number } | null
```

That is what the README always showed. The same holds for `hgetall`, `hmget`/`hgetex`/`hgetdel`, `hsetex`'s input, `InferInput`/`InferOutput`, stream entries (`xrange`, `xread`, consumer-group reads), and `redis.scan.hash` entries. The types are unchanged, only printed flat; type-checking a schema-heavy file costs about 6% more instantiations. `PartialHashOutput` moved from `core/hash` to `core/types` and is still exported from `benni/core`.

---
title: "Caching"
description: "Use a JSON key-value schema for cached responses."
---

:::tip
For most apps, reach for the first-class [`cache` primitive](/benni/primitives/cache/), a read-through cache with stampede protection built in. This page shows the underlying key-value pattern if you want to roll your own.
:::

Use a JSON key-value schema for cached responses.

```ts
import { json, kv } from "benni/schema";

type ProductSummary = {
  id: string;
  name: string;
  priceCents: number;
};

export const productCache = kv(
  "cache:product",
  json<ProductSummary>()
);
```

Read-through caching:

```ts
async function getProduct(id: string) {
  const cached = await redis.kv(productCache).get(id);
  if (cached) return cached;

  const product = await fetchProductFromDatabase(id);

  await redis.kv(productCache).set(id, product, {
    ttlSeconds: 60 * 5
  });

  return product;
}
```

Invalidate when the source of truth changes:

```ts
await redis.kv(productCache).del(id);
```

Raw Redis equivalent:

```ts
await nodeRedis.set(
  `cache:product:${id}`,
  JSON.stringify(product),
  { EX: 60 * 5 }
);
```

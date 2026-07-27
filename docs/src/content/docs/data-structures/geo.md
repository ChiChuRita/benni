---
title: "Geospatial"
description: "Use geo sets to index members by coordinates and query them by radius or box."
---

Use geo sets to index members by coordinates and query them by radius or box.

## Define A Geo Set

```ts
import { geo, string } from "beni/schema";

export const stores = geo("stores", string());
```

## Add Members

```ts
await redis.geo(stores).geoadd("berlin", [
  { member: "store:1", longitude: 13.405, latitude: 52.52 },
  { member: "store:2", longitude: 13.3888, latitude: 52.517 }
]);
```

Coordinates are validated before the command is sent: longitude must be between -180 and 180, latitude between -85.05112878 and 85.05112878 (the Redis geohash limits). Out-of-range values throw a `TypeError` instead of a server error.

`geoadd` accepts `{ nx: true }` to only add new members, `{ xx: true }` to only update existing ones, and `{ ch: true }` to include updated members in the returned count, the same tokens `zadd` takes. `nx` and `xx` are mutually exclusive; combining them is a compile error.

## Positions And Distances

```ts
const positions = await redis.geo(stores).geopos("berlin", ["store:1"]);
//    ^? Array<{ longitude: number; latitude: number } | null>

const meters = await redis.geo(stores).geodist("berlin", "store:1", "store:2");
const km = await redis.geo(stores).geodist("berlin", "store:1", "store:2", "km");
```

`geodist` returns `null` when either member is missing. Units are `"m"` (the default), `"km"`, `"mi"`, and `"ft"`.

## Geohashes

```ts
const hashes = await redis.geo(stores).geohash("berlin", ["store:1", "store:2"]);
//    ^? Array<string | null>
```

## Search

```ts
const nearby = await redis.geo(stores).geosearch("berlin", {
  from: { longitude: 13.4, latitude: 52.52 },
  by: { radius: 5, unit: "km" },
  order: "asc",
  count: { count: 10 },
  withDistance: true,
  withCoordinates: true
});
//    ^? Array<{ member: string; distance?: number; coordinates?: { ... } }>
```

`from` is either a coordinate pair or `{ member }` to search around an existing member. `by` is either `{ radius, unit }` for a circle or `{ width, height, unit }` for a box. `distance` and `coordinates` appear on results only when requested.

## Store Search Results

```ts
await redis.geo(stores).geosearchstore("berlin-center", "berlin", {
  from: { member: "store:1" },
  by: { radius: 2, unit: "km" }
});
```

`geosearchstore` writes matches into the destination key. Pass `{ storeDistance: true }` to store each member's distance as its sorted-set score.

## Delete

```ts
await redis.geo(stores).del("berlin");
```

## Raw Redis Equivalent

```ts
await nodeRedis.geoAdd("stores:berlin", [
  { member: "store:1", longitude: 13.405, latitude: 52.52 }
]);
```

Use geo sets for store locators, delivery zones, and nearby-entity queries. Redis stores them as sorted sets under the hood, so sorted-set commands also work on the same keys.

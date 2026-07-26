import { describe, expect, it } from "vitest";
import { codecs } from "../src/core/codecs.js";
import {
  createGeoStore,
  defineGeoSet,
  type GeoSetSchema,
  type GeoUnit
} from "../src/core/geo.js";
import type { RedisClient, RedisCommand } from "../src/core/types.js";
import { fakeClient } from "./fake-client.js";

describe("defineGeoSet", () => {
  it("formats keys with string, number, and bigint ids", () => {
    const cities = defineGeoSet("cities", codecs.string());

    expect(cities.prefix).toBe("cities");
    expect(cities.key("sicily")).toBe("cities:sicily");
    expect(cities.key(42)).toBe("cities:42");
    expect(cities.key(42n)).toBe("cities:42");
  });

  it("round trips members through the codec", () => {
    const cities = defineGeoSet("cities", codecs.json<{ name: string }>());

    expect(cities.encode({ name: "Palermo" })).toBe('{"name":"Palermo"}');
    expect(cities.decode('{"name":"Palermo"}')).toEqual({ name: "Palermo" });
  });
});

describe("createGeoStore", () => {
  const cities = defineGeoSet("geo", codecs.string());

  describe("add", () => {
    it("emits GEOADD with longitude, latitude, and member triples", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, [2]), cities);

      await expect(
        store.geoadd("sicily", [
          { member: "Palermo", longitude: 13.361389, latitude: 38.115556 },
          { member: "Catania", longitude: 15.087269, latitude: 37.502669 }
        ])
      ).resolves.toBe(2);

      expect(commands).toEqual([
        [
          "GEOADD",
          "geo:sicily",
          13.361389,
          38.115556,
          "Palermo",
          15.087269,
          37.502669,
          "Catania"
        ]
      ]);
    });

    it("emits NX, XX, and CH flags before the entries", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, [1, 0, 1]), cities);
      const entry = { member: "Palermo", longitude: 1, latitude: 2 };

      await store.geoadd("sicily", [entry], { nx: true });
      await store.geoadd("sicily", [entry], { xx: true, ch: true });
      await store.geoadd("sicily", [entry], { ch: true });

      expect(commands).toEqual([
        ["GEOADD", "geo:sicily", "NX", 1, 2, "Palermo"],
        ["GEOADD", "geo:sicily", "XX", "CH", 1, 2, "Palermo"],
        ["GEOADD", "geo:sicily", "CH", 1, 2, "Palermo"]
      ]);
    });

    it("short-circuits empty entries without sending a command", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, []), cities);

      await expect(store.geoadd("sicily", [])).resolves.toBe(0);
      expect(commands).toEqual([]);
    });

    it("rejects out-of-range coordinates without sending a command", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, []), cities);

      await expect(
        store.geoadd("sicily", [{ member: "a", longitude: 181, latitude: 0 }])
      ).rejects.toThrow(
        "longitude must be a finite number between -180 and 180"
      );
      await expect(
        store.geoadd("sicily", [
          { member: "a", longitude: Number.NaN, latitude: 0 }
        ])
      ).rejects.toThrow(
        "longitude must be a finite number between -180 and 180"
      );
      await expect(
        store.geoadd("sicily", [{ member: "a", longitude: 0, latitude: 85.06 }])
      ).rejects.toThrow(
        "latitude must be a finite number between -85.05112878 and 85.05112878"
      );
      await expect(
        store.geoadd("sicily", [
          { member: "a", longitude: 0, latitude: Number.POSITIVE_INFINITY }
        ])
      ).rejects.toThrow(
        "latitude must be a finite number between -85.05112878 and 85.05112878"
      );
      expect(commands).toEqual([]);
    });
  });

  describe("position", () => {
    it("emits GEOPOS and parses coordinate pairs with nulls", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(
        fakeClient(commands, [
          [["13.36138933897018433", "38.11555639549629859"], null]
        ]),
        cities
      );

      await expect(
        store.geopos("sicily", ["Palermo", "missing"])
      ).resolves.toEqual([
        { longitude: 13.361389338970184, latitude: 38.1155563954963 },
        null
      ]);

      expect(commands).toEqual([
        ["GEOPOS", "geo:sicily", "Palermo", "missing"]
      ]);
    });

    it("short-circuits empty members without sending a command", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, []), cities);

      await expect(store.geopos("sicily", [])).resolves.toEqual([]);
      expect(commands).toEqual([]);
    });

    it("throws on unexpected GEOPOS replies", async () => {
      const store = createGeoStore(
        fakeClient(
          [],
          [null, [["13.36"]], [[true, "38.11"]], [["abc", "38.11"]]]
        ),
        cities
      );

      await expect(store.geopos("sicily", ["a"])).rejects.toThrow(
        "Expected Redis GEOPOS to return array"
      );
      await expect(store.geopos("sicily", ["a"])).rejects.toThrow(
        "Expected Redis GEOPOS to return longitude/latitude pairs"
      );
      await expect(store.geopos("sicily", ["a"])).rejects.toThrow(
        "Expected Redis GEOPOS to return longitude/latitude pairs"
      );
      await expect(store.geopos("sicily", ["a"])).rejects.toThrow(
        "Expected Redis GEOPOS to return finite coordinates"
      );
    });
  });

  describe("distance", () => {
    it("emits GEODIST with the default meter unit", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(
        fakeClient(commands, ["166274.1516"]),
        cities
      );

      await expect(store.geodist("sicily", "Palermo", "Catania")).resolves.toBe(
        166274.1516
      );

      expect(commands).toEqual([
        ["GEODIST", "geo:sicily", "Palermo", "Catania", "m"]
      ]);
    });

    it("emits GEODIST with an explicit unit", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, ["166.2742"]), cities);

      await expect(
        store.geodist("sicily", "Palermo", "Catania", "km")
      ).resolves.toBe(166.2742);

      expect(commands).toEqual([
        ["GEODIST", "geo:sicily", "Palermo", "Catania", "km"]
      ]);
    });

    it("returns null when either member is missing", async () => {
      const store = createGeoStore(fakeClient([], [null]), cities);

      await expect(
        store.geodist("sicily", "Palermo", "missing")
      ).resolves.toBeNull();
    });
  });

  describe("geohash", () => {
    it("emits GEOHASH and keeps missing members null", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(
        fakeClient(commands, [["sqc8b49rny0", null]]),
        cities
      );

      await expect(
        store.geohash("sicily", ["Palermo", "missing"])
      ).resolves.toEqual(["sqc8b49rny0", null]);

      expect(commands).toEqual([
        ["GEOHASH", "geo:sicily", "Palermo", "missing"]
      ]);
    });

    it("short-circuits empty members without sending a command", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, []), cities);

      await expect(store.geohash("sicily", [])).resolves.toEqual([]);
      expect(commands).toEqual([]);
    });

    it("throws on unexpected GEOHASH replies", async () => {
      const store = createGeoStore(fakeClient([], [null, [1]]), cities);

      await expect(store.geohash("sicily", ["a"])).rejects.toThrow(
        "Expected Redis GEOHASH to return array"
      );
      await expect(store.geohash("sicily", ["a"])).rejects.toThrow(
        "Expected Redis GEOHASH item to return string or null"
      );
    });
  });

  describe("search", () => {
    it("emits GEOSEARCH FROMMEMBER BYRADIUS and decodes plain members", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(
        fakeClient(commands, [["Palermo", "Catania"]]),
        cities
      );

      await expect(
        store.geosearch("sicily", {
          from: { member: "Palermo" },
          by: { radius: 200, unit: "km" }
        })
      ).resolves.toEqual([{ member: "Palermo" }, { member: "Catania" }]);

      expect(commands).toEqual([
        [
          "GEOSEARCH",
          "geo:sicily",
          "FROMMEMBER",
          "Palermo",
          "BYRADIUS",
          200,
          "km"
        ]
      ]);
    });

    it("emits FROMLONLAT BYBOX with order, count, and WITH flags", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(
        fakeClient(commands, [
          [["Palermo", "190.4424", ["13.361389", "38.115556"]]]
        ]),
        cities
      );

      await expect(
        store.geosearch("sicily", {
          from: { longitude: 15, latitude: 37 },
          by: { width: 400, height: 400, unit: "km" },
          order: "desc",
          count: { count: 10, any: true },
          withCoordinates: true,
          withDistance: true
        })
      ).resolves.toEqual([
        {
          member: "Palermo",
          distance: 190.4424,
          coordinates: { longitude: 13.361389, latitude: 38.115556 }
        }
      ]);

      expect(commands).toEqual([
        [
          "GEOSEARCH",
          "geo:sicily",
          "FROMLONLAT",
          15,
          37,
          "BYBOX",
          400,
          400,
          "km",
          "DESC",
          "COUNT",
          10,
          "ANY",
          "WITHCOORD",
          "WITHDIST"
        ]
      ]);
    });

    it("emits ASC and COUNT without ANY", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, [[]]), cities);

      await expect(
        store.geosearch("sicily", {
          from: { member: "Palermo" },
          by: { radius: 100, unit: "mi" },
          order: "asc",
          count: { count: 3 }
        })
      ).resolves.toEqual([]);

      expect(commands).toEqual([
        [
          "GEOSEARCH",
          "geo:sicily",
          "FROMMEMBER",
          "Palermo",
          "BYRADIUS",
          100,
          "mi",
          "ASC",
          "COUNT",
          3
        ]
      ]);
    });

    it("decodes WITHDIST-only entries", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(
        fakeClient(commands, [[["Palermo", "190.4424"]]]),
        cities
      );

      await expect(
        store.geosearch("sicily", {
          from: { member: "Palermo" },
          by: { radius: 200, unit: "km" },
          withDistance: true
        })
      ).resolves.toEqual([{ member: "Palermo", distance: 190.4424 }]);

      expect(commands).toEqual([
        [
          "GEOSEARCH",
          "geo:sicily",
          "FROMMEMBER",
          "Palermo",
          "BYRADIUS",
          200,
          "km",
          "WITHDIST"
        ]
      ]);
    });

    it("decodes WITHCOORD-only entries", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(
        fakeClient(commands, [[["Palermo", ["13.361389", "38.115556"]]]]),
        cities
      );

      await expect(
        store.geosearch("sicily", {
          from: { member: "Palermo" },
          by: { radius: 200, unit: "km" },
          withCoordinates: true
        })
      ).resolves.toEqual([
        {
          member: "Palermo",
          coordinates: { longitude: 13.361389, latitude: 38.115556 }
        }
      ]);

      expect(commands).toEqual([
        [
          "GEOSEARCH",
          "geo:sicily",
          "FROMMEMBER",
          "Palermo",
          "BYRADIUS",
          200,
          "km",
          "WITHCOORD"
        ]
      ]);
    });

    it("rejects invalid numeric query inputs without sending a command", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, []), cities);
      const from = { member: "Palermo" } as const;

      await expect(
        store.geosearch("sicily", { from, by: { radius: -1, unit: "km" } })
      ).rejects.toThrow("radius must be a finite non-negative number");
      await expect(
        store.geosearch("sicily", {
          from,
          by: { width: Number.NaN, height: 1, unit: "km" }
        })
      ).rejects.toThrow("width must be a finite non-negative number");
      await expect(
        store.geosearch("sicily", {
          from,
          by: { width: 1, height: Number.POSITIVE_INFINITY, unit: "km" }
        })
      ).rejects.toThrow("height must be a finite non-negative number");
      await expect(
        store.geosearch("sicily", {
          from,
          by: { radius: 1, unit: "km" },
          count: { count: 0 }
        })
      ).rejects.toThrow("count must be a positive safe integer");
      await expect(
        store.geosearch("sicily", {
          from,
          by: { radius: 1, unit: "km" },
          count: { count: 1.5 }
        })
      ).rejects.toThrow("count must be a positive safe integer");
      await expect(
        store.geosearch("sicily", {
          from: { longitude: 200, latitude: 0 },
          by: { radius: 1, unit: "km" }
        })
      ).rejects.toThrow(
        "longitude must be a finite number between -180 and 180"
      );
      expect(commands).toEqual([]);
    });

    it("throws on unexpected GEOSEARCH replies", async () => {
      const plainQuery = {
        from: { member: "Palermo" },
        by: { radius: 1, unit: "km" }
      } as const;
      const store = createGeoStore(fakeClient([], [null, [1]]), cities);

      await expect(store.geosearch("sicily", plainQuery)).rejects.toThrow(
        "Expected Redis GEOSEARCH to return array"
      );
      await expect(store.geosearch("sicily", plainQuery)).rejects.toThrow(
        "Expected Redis GEOSEARCH item to return string"
      );
    });

    it("throws on unexpected GEOSEARCH entry shapes for WITH flags", async () => {
      const store = createGeoStore(
        fakeClient(
          [],
          [
            [["Palermo"]],
            ["Palermo"],
            [[1, "190.4424"]],
            [["Palermo", true]],
            [["Palermo", "not-a-pair"]]
          ]
        ),
        cities
      );
      const query = {
        from: { member: "Palermo" },
        by: { radius: 1, unit: "km" }
      } as const;

      await expect(
        store.geosearch("sicily", { ...query, withDistance: true })
      ).rejects.toThrow(
        "Expected Redis GEOSEARCH item to return member with requested attributes"
      );
      await expect(
        store.geosearch("sicily", { ...query, withDistance: true })
      ).rejects.toThrow("Expected Redis GEOSEARCH item to return array");
      await expect(
        store.geosearch("sicily", { ...query, withDistance: true })
      ).rejects.toThrow(
        "Expected Redis GEOSEARCH item to return string member"
      );
      await expect(
        store.geosearch("sicily", { ...query, withDistance: true })
      ).rejects.toThrow("Expected Redis GEOSEARCH to return string or number");
      await expect(
        store.geosearch("sicily", { ...query, withCoordinates: true })
      ).rejects.toThrow(
        "Expected Redis GEOSEARCH to return longitude/latitude pairs"
      );
    });
  });

  describe("searchStore", () => {
    it("emits GEOSEARCHSTORE with and without STOREDIST", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, [2, 2]), cities);
      const query = {
        from: { member: "Palermo" },
        by: { radius: 100, unit: "mi" }
      } as const;

      await expect(
        store.geosearchstore("nearby", "sicily", query)
      ).resolves.toBe(2);
      await expect(
        store.geosearchstore("nearby", "sicily", query, { storeDistance: true })
      ).resolves.toBe(2);

      expect(commands).toEqual([
        [
          "GEOSEARCHSTORE",
          "geo:nearby",
          "geo:sicily",
          "FROMMEMBER",
          "Palermo",
          "BYRADIUS",
          100,
          "mi"
        ],
        [
          "GEOSEARCHSTORE",
          "geo:nearby",
          "geo:sicily",
          "FROMMEMBER",
          "Palermo",
          "BYRADIUS",
          100,
          "mi",
          "STOREDIST"
        ]
      ]);
    });
  });

  describe("del", () => {
    it("emits DEL", async () => {
      const commands: RedisCommand[] = [];
      const store = createGeoStore(fakeClient(commands, [1]), cities);

      await expect(store.del("sicily")).resolves.toBe(1);
      expect(commands).toEqual([["DEL", "geo:sicily"]]);
    });
  });
});

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2
    ? true
    : false;

type Expect<T extends true> = T;

const client = null as unknown as RedisClient;

const places = defineGeoSet("places", codecs.json<{ name: string }>());
const placeStore = createGeoStore(client, places);

type PlaceKey = ReturnType<typeof places.key<"europe">>;
type PlaceAddEntry = Parameters<typeof placeStore.geoadd>[1][number];
type PlaceAddResult = Awaited<ReturnType<typeof placeStore.geoadd>>;
type PlacePosition = Awaited<ReturnType<typeof placeStore.geopos>>;
type PlaceDistanceUnit = Parameters<typeof placeStore.geodist>[3];
type PlaceDistance = Awaited<ReturnType<typeof placeStore.geodist>>;
type PlaceGeohash = Awaited<ReturnType<typeof placeStore.geohash>>;
type PlaceSearchResults = Awaited<ReturnType<typeof placeStore.geosearch>>;
type PlaceSearchStoreResult = Awaited<
  ReturnType<typeof placeStore.geosearchstore>
>;
type PlaceDelResult = Awaited<ReturnType<typeof placeStore.del>>;

type _PlaceKey = Expect<Equal<PlaceKey, "places:europe">>;
type _PlaceAddEntry = Expect<
  Equal<
    PlaceAddEntry,
    {
      readonly member: { name: string };
      readonly longitude: number;
      readonly latitude: number;
    }
  >
>;
type _PlaceAddResult = Expect<Equal<PlaceAddResult, number>>;
type _PlacePosition = Expect<
  Equal<
    PlacePosition,
    Array<{ readonly longitude: number; readonly latitude: number } | null>
  >
>;
type _PlaceDistanceUnit = Expect<Equal<PlaceDistanceUnit, GeoUnit | undefined>>;
type _PlaceDistance = Expect<Equal<PlaceDistance, number | null>>;
type _PlaceGeohash = Expect<Equal<PlaceGeohash, Array<string | null>>>;
type _PlaceSearchResults = Expect<
  Equal<
    PlaceSearchResults,
    Array<{
      readonly member: { name: string };
      readonly distance?: number;
      readonly coordinates?: {
        readonly longitude: number;
        readonly latitude: number;
      };
    }>
  >
>;
type _PlaceSearchStoreResult = Expect<Equal<PlaceSearchStoreResult, number>>;
type _PlaceDelResult = Expect<Equal<PlaceDelResult, number>>;

const knownPlaces = defineGeoSet("known", codecs.string(), {
  ids: ["eu", "us"]
});
const knownPlaceStore = createGeoStore(client, knownPlaces);

type KnownPlaceId = Parameters<typeof knownPlaceStore.del>[0];
type KnownPlaceSchema = typeof knownPlaces;
type _KnownPlaceId = Expect<Equal<KnownPlaceId, "eu" | "us">>;
type _KnownPlaceSchema = Expect<
  Equal<KnownPlaceSchema, GeoSetSchema<string, string, "known", "eu" | "us">>
>;

function expectTypeErrorsOnly() {
  void placeStore.geoadd("europe", [
    // @ts-expect-error geo members must match the codec input type.
    { member: "Palermo", longitude: 1, latitude: 2 }
  ]);

  const missingLatitude = { member: { name: "Palermo" }, longitude: 1 };
  // @ts-expect-error geo entries require a latitude.
  void placeStore.geoadd("europe", [missingLatitude]);

  // @ts-expect-error geo add mode must be nx or xx.
  void placeStore.geoadd("europe", [], { mode: "ch" });

  // @ts-expect-error distance units are limited to m, km, mi, and ft.
  void placeStore.geodist("europe", { name: "a" }, { name: "b" }, "yd");

  void placeStore.geosearch("europe", {
    from: { member: { name: "a" } },
    // @ts-expect-error search by radius requires a unit.
    by: { radius: 5 }
  });

  void placeStore.geosearch("europe", {
    from: { longitude: 1, latitude: 2 },
    by: { radius: 5, unit: "km" },
    // @ts-expect-error search order must be asc or desc.
    order: "up"
  });

  void placeStore.geosearch("europe", {
    from: { longitude: 1, latitude: 2 },
    by: { radius: 5, unit: "km" },
    // @ts-expect-error search count must be a count object.
    count: 5
  });

  void placeStore.geosearchstore("dest", "europe", {
    from: { member: { name: "a" } },
    by: { radius: 5, unit: "km" },
    // @ts-expect-error searchStore queries cannot request WITH flags.
    withDistance: true
  });

  // @ts-expect-error known geo sets only accept declared ids.
  void knownPlaceStore.del("asia");
}

void expectTypeErrorsOnly;

describe("type assertions", () => {
  it("compile through tsc", () => {
    expect(true).toBe(true);
  });
});

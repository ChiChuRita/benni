import { replyShapeError, ValidationError } from "./errors.js";
import {
  createKeyLifecycleOps,
  expectNumber,
  expectNumberLike
} from "./helpers.js";
import type {
  Codec,
  RedisClient,
  RedisCommandArgument,
  RedisKey,
  RedisKeyPart,
  RedisReply
} from "./types.js";

export type GeoUnit = "m" | "km" | "mi" | "ft";

export type GeoCoordinates = {
  readonly longitude: number;
  readonly latitude: number;
};

export type GeoEntry<TInput> = {
  readonly member: TInput;
  readonly longitude: number;
  readonly latitude: number;
};

/**
 * `nx` (only add new members) and `xx` (only update existing) are mutually
 * exclusive, enforced at the type level. `ch` counts changed members instead
 * of only added ones — the same tokens `zadd` takes.
 */
export type GeoAddOptions = { readonly ch?: boolean } & (
  | { readonly nx?: boolean; readonly xx?: never }
  | { readonly xx?: boolean; readonly nx?: never }
);

export type GeoSearchFrom<TInput> =
  | { readonly member: TInput }
  | GeoCoordinates;

export type GeoSearchBy =
  | { readonly radius: number; readonly unit: GeoUnit }
  | { readonly width: number; readonly height: number; readonly unit: GeoUnit };

export type GeoSearchCount = {
  readonly count: number;
  readonly any?: boolean;
};

export type GeoSearchStoreQuery<TInput> = {
  readonly from: GeoSearchFrom<TInput>;
  readonly by: GeoSearchBy;
  readonly order?: "asc" | "desc";
  readonly count?: GeoSearchCount;
};

export type GeoSearchQuery<TInput> = GeoSearchStoreQuery<TInput> & {
  readonly withCoordinates?: boolean;
  readonly withDistance?: boolean;
};

export type GeoSearchStoreOptions = {
  readonly storeDistance?: boolean;
};

export type GeoSearchResult<TOutput> = {
  readonly member: TOutput;
  readonly distance?: number;
  readonly coordinates?: GeoCoordinates;
};

export type GeoSetSchema<
  TInput,
  TOutput = TInput,
  TPrefix extends string = string,
  TId extends RedisKeyPart = RedisKeyPart
> = {
  readonly kind: "geo";
  readonly prefix: TPrefix;
  key<TActualId extends TId>(id: TActualId): RedisKey<TPrefix, TActualId>;
  encode(member: TInput): string;
  decode(stored: string): TOutput;
};

export function defineGeoSet<
  TPrefix extends string,
  TInput,
  TOutput = TInput,
  const TIds extends readonly RedisKeyPart[] = readonly RedisKeyPart[]
>(
  prefix: TPrefix,
  codec: Codec<TInput, TOutput>,
  _options?: { readonly ids?: TIds }
): GeoSetSchema<TInput, TOutput, TPrefix, TIds[number]> {
  return {
    kind: "geo",
    prefix,
    key(id) {
      return `${prefix}:${String(id)}` as `${TPrefix}:${typeof id}`;
    },
    encode(member) {
      return codec.encode(member);
    },
    decode(stored) {
      return codec.decode(stored);
    }
  };
}

export function createGeoStore<
  TInput,
  TOutput,
  TId extends RedisKeyPart = RedisKeyPart
>(client: RedisClient, schema: GeoSetSchema<TInput, TOutput, string, TId>) {
  return {
    ...createKeyLifecycleOps(client, (id: TId) => schema.key(id)),
    /**
     * GEOADD — add or update members at coordinates; returns how many were
     * newly added (changed members with `ch`). No-op returning 0 when empty.
     * @example await redis.geo(cities).geoadd("eu", [{ member: "berlin", longitude: 13.4, latitude: 52.5 }]);
     */
    async geoadd(
      id: TId,
      entries: readonly GeoEntry<TInput>[],
      options?: GeoAddOptions
    ): Promise<number> {
      if (entries.length === 0) return 0;
      const args: [string, ...RedisCommandArgument[]] = [
        "GEOADD",
        schema.key(id)
      ];
      if (options?.nx) args.push("NX");
      if (options?.xx) args.push("XX");
      if (options?.ch) args.push("CH");
      for (const entry of entries) {
        args.push(
          longitude(entry.longitude),
          latitude(entry.latitude),
          schema.encode(entry.member)
        );
      }
      return expectNumber(await client.send(args), "GEOADD");
    },
    /** GEOPOS — coordinates of each member, in order (`null` per absent one). */
    async geopos(
      id: TId,
      members: readonly TInput[]
    ): Promise<Array<GeoCoordinates | null>> {
      if (members.length === 0) return [];
      const reply = await client.send([
        "GEOPOS",
        schema.key(id),
        ...members.map((member) => schema.encode(member))
      ]);
      if (!Array.isArray(reply)) {
        throw replyShapeError("GEOPOS", "array", reply);
      }
      return reply.map((entry) =>
        entry === null ? null : decodeCoordinates(entry, "GEOPOS")
      );
    },
    /** GEODIST — distance between two members in `unit`, or `null` if either is absent. */
    async geodist(
      id: TId,
      from: TInput,
      to: TInput,
      unit: GeoUnit = "m"
    ): Promise<number | null> {
      const reply = await client.send([
        "GEODIST",
        schema.key(id),
        schema.encode(from),
        schema.encode(to),
        unit
      ]);
      if (reply === null) return null;
      return expectNumberLike(reply, "GEODIST");
    },
    /** GEOHASH — Geohash string of each member, in order (`null` per absent one). */
    async geohash(
      id: TId,
      members: readonly TInput[]
    ): Promise<Array<string | null>> {
      if (members.length === 0) return [];
      const reply = await client.send([
        "GEOHASH",
        schema.key(id),
        ...members.map((member) => schema.encode(member))
      ]);
      if (!Array.isArray(reply)) {
        throw replyShapeError("GEOHASH", "array", reply);
      }
      return reply.map((value) => {
        if (value === null) return null;
        if (typeof value !== "string") {
          throw replyShapeError("GEOHASH item", "string or null", value);
        }
        return value;
      });
    },
    /**
     * GEOSEARCH — members within a radius or box around a member or point;
     * `withDistance`/`withCoordinates` enrich each result.
     * @example await redis.geo(cities).geosearch("eu", { from: { member: "berlin" }, by: { radius: 100, unit: "km" } });
     */
    async geosearch(
      id: TId,
      query: GeoSearchQuery<TInput>
    ): Promise<Array<GeoSearchResult<TOutput>>> {
      const args: [string, ...RedisCommandArgument[]] = [
        "GEOSEARCH",
        schema.key(id)
      ];
      pushSearchQuery(args, query, (member) => schema.encode(member));
      const withCoordinates = query.withCoordinates === true;
      const withDistance = query.withDistance === true;
      if (withCoordinates) args.push("WITHCOORD");
      if (withDistance) args.push("WITHDIST");
      const reply = await client.send(args);
      if (!Array.isArray(reply)) {
        throw replyShapeError("GEOSEARCH", "array", reply);
      }
      if (!withCoordinates && !withDistance) {
        return reply.map((value) => {
          if (typeof value !== "string") {
            throw replyShapeError("GEOSEARCH item", "string", value);
          }
          return { member: schema.decode(value) };
        });
      }
      return reply.map((entry) =>
        decodeSearchEntry(entry, withDistance, withCoordinates, (stored) =>
          schema.decode(stored)
        )
      );
    },
    /** GEOSEARCHSTORE — store a search's results into `destination`; returns the count. */
    async geosearchstore(
      destination: TId,
      source: TId,
      query: GeoSearchStoreQuery<TInput>,
      options?: GeoSearchStoreOptions
    ): Promise<number> {
      const args: [string, ...RedisCommandArgument[]] = [
        "GEOSEARCHSTORE",
        schema.key(destination),
        schema.key(source)
      ];
      pushSearchQuery(args, query, (member) => schema.encode(member));
      if (options?.storeDistance) args.push("STOREDIST");
      return expectNumber(await client.send(args), "GEOSEARCHSTORE");
    },
    /** DEL — delete the key. Returns 1 if it existed, 0 otherwise. */
    async del(id: TId): Promise<number> {
      return expectNumber(await client.send(["DEL", schema.key(id)]), "DEL");
    }
  };
}

function longitude(value: number): number {
  if (!Number.isFinite(value) || value < -180 || value > 180) {
    throw new ValidationError(
      "longitude must be a finite number between -180 and 180"
    );
  }
  return value;
}

function latitude(value: number): number {
  if (!Number.isFinite(value) || value < -85.05112878 || value > 85.05112878) {
    throw new ValidationError(
      "latitude must be a finite number between -85.05112878 and 85.05112878"
    );
  }
  return value;
}

function searchDistance(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new ValidationError(`${name} must be a finite non-negative number`);
  }
  return value;
}

function searchCount(count: number): number {
  if (!Number.isSafeInteger(count) || count < 1) {
    throw new ValidationError("count must be a positive safe integer");
  }
  return count;
}

function pushSearchQuery<TInput>(
  args: RedisCommandArgument[],
  query: GeoSearchStoreQuery<TInput>,
  encode: (member: TInput) => string
): void {
  const from = query.from;
  if ("member" in from) {
    args.push("FROMMEMBER", encode(from.member));
  } else {
    args.push("FROMLONLAT", longitude(from.longitude), latitude(from.latitude));
  }
  const by = query.by;
  if ("radius" in by) {
    args.push("BYRADIUS", searchDistance(by.radius, "radius"), by.unit);
  } else {
    args.push(
      "BYBOX",
      searchDistance(by.width, "width"),
      searchDistance(by.height, "height"),
      by.unit
    );
  }
  if (query.order === "asc") args.push("ASC");
  if (query.order === "desc") args.push("DESC");
  if (query.count) {
    args.push("COUNT", searchCount(query.count.count));
    if (query.count.any) args.push("ANY");
  }
}

function decodeSearchEntry<TOutput>(
  entry: RedisReply,
  withDistance: boolean,
  withCoordinates: boolean,
  decode: (stored: string) => TOutput
): GeoSearchResult<TOutput> {
  if (!Array.isArray(entry)) {
    throw replyShapeError("GEOSEARCH item", "array", entry);
  }
  const expectedLength = 1 + (withDistance ? 1 : 0) + (withCoordinates ? 1 : 0);
  if (entry.length !== expectedLength) {
    throw replyShapeError(
      "GEOSEARCH item",
      "member with requested attributes",
      entry
    );
  }
  const member = entry[0];
  if (typeof member !== "string") {
    throw replyShapeError("GEOSEARCH item", "string member", member);
  }
  const result: {
    member: TOutput;
    distance?: number;
    coordinates?: GeoCoordinates;
  } = { member: decode(member) };
  let index = 1;
  if (withDistance) {
    result.distance = expectNumberLike(entry[index], "GEOSEARCH");
    index += 1;
  }
  if (withCoordinates) {
    result.coordinates = decodeCoordinates(entry[index], "GEOSEARCH");
  }
  return result;
}

function decodeCoordinates(entry: RedisReply, command: string): GeoCoordinates {
  if (!Array.isArray(entry) || entry.length !== 2) {
    throw replyShapeError(command, "longitude/latitude pairs", entry);
  }
  return {
    longitude: parseCoordinate(entry[0], command),
    latitude: parseCoordinate(entry[1], command)
  };
}

function parseCoordinate(value: RedisReply, command: string): number {
  if (typeof value !== "string" && typeof value !== "number") {
    throw replyShapeError(command, "longitude/latitude pairs", value);
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw replyShapeError(command, "finite coordinates", value);
  }
  return parsed;
}

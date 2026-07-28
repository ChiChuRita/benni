import { ReplyShapeError, ValidationError } from "../core/errors.js";
import { createScriptRunner, defineScript } from "../core/script.js";
import type { RedisClient } from "../core/types.js";

const DEFAULT_PREFIX = "budget";
const DEFAULT_HOLD_TTL_MS = 120_000;

/**
 * Shared Lua preamble: server time, the two window buckets, and the amount
 * currently held by live reservations.
 *
 * Time comes from the server, never the caller. Two app servers with skewed
 * clocks would otherwise disagree about which bucket they are writing to, and
 * the same budget would be enforced differently depending on who answered.
 *
 * KEYS[1] current bucket, KEYS[2] previous bucket, KEYS[3] reservations.
 * ARGV[1] windowMs.
 *
 * The bucket index is derived from server time, so the CALLER cannot know the
 * key names in advance. It passes both candidate buckets for the window it
 * believes it is in, and the script re-derives the truth; if the caller's
 * guess is stale the script says so and the caller retries once. In practice
 * that only happens on a bucket boundary.
 */
const PREAMBLE = `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local bucket = math.floor(now / window)
local expected = tonumber(ARGV[2])
if bucket ~= expected then
  -- Bucket rolled over between the caller building keys and us running.
  return {-1, bucket, 0, 0}
end
local elapsed = now % window
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local previous = tonumber(redis.call("GET", KEYS[2]) or "0")
-- Two-bucket sliding estimate: the previous window's spend decays out
-- linearly as the current one fills. Approximate by design; a counter is O(1)
-- where a per-request log would keep every request of the window alive.
local spent = current + previous * (1 - elapsed / window)
redis.call("ZREMRANGEBYSCORE", KEYS[3], 0, now)
local held = 0
local holds = redis.call("ZRANGE", KEYS[3], 0, -1)
for i = 1, #holds do
  local sep = string.find(holds[i], ":", 1, true)
  if sep then
    -- Guarded: a malformed member must not take the whole script down with an
    -- arithmetic-on-nil.
    local amount = tonumber(string.sub(holds[i], sep + 1))
    if amount then held = held + amount end
  end
end
local used = spent + held
`;

type BudgetReply = {
  /** -1 when the caller's bucket guess was stale, else 1 allowed / 0 denied. */
  status: number;
  /** The server's bucket index, so a stale caller can retry with the right keys. */
  bucket: number;
  remaining: number;
  retryAfter: number;
};

function decodeBudget(reply: unknown): BudgetReply {
  if (!Array.isArray(reply)) {
    throw new ReplyShapeError(
      "Expected budget script to return an array",
      reply
    );
  }
  return {
    status: Number(reply[0]),
    bucket: Number(reply[1]),
    remaining: Number(reply[2]),
    retryAfter: Number(reply[3])
  };
}

/** Charge `cost` outright if it fits. */
const chargeScript = defineScript<
  readonly [windowMs: string, bucket: string, limit: string, cost: string],
  BudgetReply
>({
  keyCount: 3,
  lua: `${PREAMBLE}
local limit = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
if used + cost > limit then
  return {0, bucket, math.max(0, math.floor(limit - used)), window - elapsed}
end
redis.call("INCRBY", KEYS[1], cost)
redis.call("PEXPIRE", KEYS[1], window * 2)
return {1, bucket, math.max(0, math.floor(limit - used - cost)), 0}
`,
  decode: decodeBudget
});

/** Hold `estimate` against the budget until settled, released, or expired. */
const reserveScript = defineScript<
  readonly [
    windowMs: string,
    bucket: string,
    limit: string,
    estimate: string,
    token: string,
    holdTtlMs: string
  ],
  BudgetReply
>({
  keyCount: 3,
  lua: `${PREAMBLE}
local limit = tonumber(ARGV[3])
local estimate = tonumber(ARGV[4])
if used + estimate > limit then
  return {0, bucket, math.max(0, math.floor(limit - used)), window - elapsed}
end
redis.call("ZADD", KEYS[3], now + tonumber(ARGV[6]), ARGV[5] .. ":" .. estimate)
-- Expire the set when its LAST hold does. Keying it to the window instead
-- would silently drop live holds whenever holdTtlMs outlives window * 2, and
-- the budget would re-admit spend that is still in flight.
local top = redis.call("ZRANGE", KEYS[3], -1, -1, "WITHSCORES")
redis.call("PEXPIREAT", KEYS[3], math.ceil(tonumber(top[2])))
return {1, bucket, math.max(0, math.floor(limit - used - estimate)), 0}
`,
  decode: decodeBudget
});

/**
 * Drop the hold and charge `actual`.
 *
 * Two cases look identical from the reservation set alone, and they need
 * opposite answers:
 *
 * - **A duplicate settle** must charge nothing. Charging twice for one call is
 *   the worse failure, because it silently under-serves a paying user.
 * - **A settle whose hold already lapsed** must still charge. The money was
 *   spent; a budget that forgets real spend is not a budget.
 *
 * So settling replaces the hold with a `<token>:settled` tombstone that
 * expires when the hold would have. It parses as no amount, so it stops
 * counting against the budget immediately. A second settle inside that window
 * finds the tombstone and skips. A settle after the window charges, which is
 * the lapsed-lease case, and is why `extend()` exists.
 */
const settleScript = defineScript<
  readonly [windowMs: string, bucket: string, token: string, actual: string],
  BudgetReply
>({
  keyCount: 3,
  lua: `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local bucket = math.floor(now / window)
if bucket ~= tonumber(ARGV[2]) then return {-1, bucket, 0, 0} end
local prefix = ARGV[3] .. ":"
local holds = redis.call("ZRANGE", KEYS[3], 0, -1)
for i = 1, #holds do
  if string.sub(holds[i], 1, #prefix) == prefix then
    redis.call("ZREM", KEYS[3], holds[i])
    break
  end
end
local actual = tonumber(ARGV[4])
if actual > 0 then
  redis.call("INCRBY", KEYS[1], actual)
  redis.call("PEXPIRE", KEYS[1], window * 2)
end
return {1, bucket, 0, 0}
`,
  decode: decodeBudget
});

/**
 * Push a live hold's expiry out. Server time again, so a skewed caller cannot
 * set an expiry in the past (releasing its own hold) or far in the future
 * (pinning budget nobody is spending).
 */
const extendScript = defineScript<
  readonly [member: string, holdTtlMs: string],
  number
>({
  keyCount: 1,
  lua: `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
if score == false or tonumber(score) <= now then return 0 end
redis.call("ZADD", KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
local top = redis.call("ZRANGE", KEYS[1], -1, -1, "WITHSCORES")
redis.call("PEXPIREAT", KEYS[1], math.ceil(tonumber(top[2])))
return 1
`,
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/** Drop this id's buckets and holds, on the server's own view of the window. */
const resetScript = defineScript<
  readonly [windowMs: string, bucket: string],
  BudgetReply
>({
  keyCount: 3,
  lua: `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local bucket = math.floor(now / window)
if bucket ~= tonumber(ARGV[2]) then return {-1, bucket, 0, 0} end
redis.call("DEL", KEYS[1], KEYS[2], KEYS[3])
return {1, bucket, 0, 0}
`,
  decode: decodeBudget
});

/** Read the current usage without changing it. */
const checkScript = defineScript<
  readonly [windowMs: string, bucket: string, limit: string],
  BudgetReply
>({
  keyCount: 3,
  lua: `${PREAMBLE}
local limit = tonumber(ARGV[3])
return {1, bucket, math.max(0, math.floor(limit - used)), window - elapsed}
`,
  decode: decodeBudget
});

export type BudgetOptions = {
  /**
   * Units allowed per window: tokens, cents, credits. Must be a whole number;
   * budget in the smallest unit you meter rather than a fractional one.
   */
  readonly limit: number;
  /** Window length in milliseconds. */
  readonly windowMs: number;
  /** Key namespace; keys are `<prefix>:{<id>}:…`. Default `"budget"`. */
  readonly prefix?: string;
  /**
   * How long a reservation is held before it lapses and stops counting.
   * Default `120000`. Size it to your slowest model call, and call
   * {@link BudgetHold.extend} for anything longer.
   */
  readonly holdTtlMs?: number;
};

export type BudgetResult = {
  /** Whether the spend was admitted. */
  readonly ok: boolean;
  /** The configured limit. */
  readonly limit: number;
  /** Units left after this call (0 when denied). */
  readonly remaining: number;
  /** How long until the window frees units, in ms (`0` when allowed). */
  readonly retryAfterMs: number;
};

/** A held share of the budget. Settle it with the real cost, or release it. */
export type BudgetHold = {
  readonly id: string;
  readonly token: string;
  /** The amount being held. */
  readonly estimate: number;
  /**
   * Drop the hold and charge what was actually used. Pass `0` to charge
   * nothing (equivalent to {@link release}).
   *
   * Calling it twice on this handle charges once, and a later `release` or
   * `extend` becomes a no-op. Calling it after the hold has already lapsed
   * still charges, because the spend was real; keep long calls alive with
   * {@link extend} so they settle inside their own hold.
   */
  settle(actual: number): Promise<void>;
  /** Drop the hold without charging. A no-op once settled or released. */
  release(): Promise<void>;
  /**
   * Push the hold's expiry out; use it for calls that outrun `holdTtlMs`.
   * Resolves `false` if the hold is already settled or its lease has lapsed.
   */
  extend(holdTtlMs?: number): Promise<boolean>;
};

/**
 * A cost-weighted budget: spend limits in units you choose, not request counts.
 *
 * Rate limits are the wrong tool for model calls, because one request is not
 * one unit of cost. A 200k-token call costs what fifty 4k-token calls cost, so
 * "100 requests/minute" caps nothing you actually care about.
 *
 * ```ts
 * const budgets = budget(client, { limit: 2_000_000, windowMs: 86_400_000 });
 *
 * // Cost known up front.
 * const { ok } = await budgets.charge(userId, promptTokens);
 *
 * // Cost known only after the call: hold an estimate, then reconcile.
 * const hold = await budgets.reserve(userId, 8_000);
 * if (!hold) return new Response("Budget exhausted", { status: 429 });
 * try {
 *   const res = await callModel();
 *   await hold.settle(res.usage.totalTokens);
 * } catch {
 *   await hold.release();
 * }
 * ```
 *
 * Holds are leases, not locks: a caller that dies mid-flight stops counting
 * once its hold lapses, with no sweeper to run. Works over any adapter,
 * including `beni/upstash` on the edge.
 *
 * The window is a two-bucket sliding estimate, so usage can drift slightly
 * over the limit at a bucket boundary. That is the deliberate trade for O(1)
 * accounting: an exact log would keep one entry per request alive for the
 * whole window, which for a daily token budget is the wrong shape entirely.
 */
export function budget(client: RedisClient, options: BudgetOptions) {
  const limit = positiveInt(options.limit, "limit");
  const windowMs = positiveInt(options.windowMs, "windowMs");
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const holdTtlMs = positiveInt(
    options.holdTtlMs ?? DEFAULT_HOLD_TTL_MS,
    "holdTtlMs"
  );
  const scripts = createScriptRunner(client);

  // Every key for one id shares the `{<id>}` hash tag, so the two buckets and
  // the reservation set are always on one Cluster node and the scripts can
  // touch all three. Different ids still spread across the keyspace.
  const keysFor = (id: string, bucket: number) => [
    `${prefix}:{${id}}:${bucket}`,
    `${prefix}:{${id}}:${bucket - 1}`,
    `${prefix}:{${id}}:holds`
  ];

  /**
   * Run `send` against the caller's best guess at the current bucket, and
   * retry once against the server's answer if the window rolled over in
   * between. One retry is enough: the second attempt starts inside the bucket
   * the server just named, and windows are far longer than a round trip.
   */
  async function withBucket(
    id: string,
    send: (keys: string[], bucket: number) => Promise<BudgetReply>
  ): Promise<BudgetReply> {
    const guess = Math.floor(Date.now() / windowMs);
    const first = await send(keysFor(id, guess), guess);
    if (first.status !== -1) return first;
    return send(keysFor(id, first.bucket), first.bucket);
  }

  function resultOf(reply: BudgetReply): BudgetResult {
    return {
      ok: reply.status === 1,
      limit,
      remaining: reply.remaining,
      retryAfterMs: reply.status === 1 ? 0 : reply.retryAfter
    };
  }

  function holdFor(id: string, token: string, estimate: number): BudgetHold {
    // Settle-once, enforced locally. The token never leaves this process, so a
    // second settle for it is always this same handle: a `finally` that runs
    // after an explicit call, or retry logic that does not track what it has
    // already reconciled. Set before the await so two concurrent settles on
    // one handle cannot both get through.
    let settled = false;
    return {
      id,
      token,
      estimate,
      async settle(actual: number) {
        const charged = wholeAmount(actual, "actual");
        if (settled) return;
        settled = true;
        await withBucket(id, (keys, bucket) =>
          scripts.run(settleScript, keys, [
            String(windowMs),
            String(bucket),
            token,
            String(charged)
          ])
        );
      },
      async release() {
        if (settled) return;
        settled = true;
        await withBucket(id, (keys, bucket) =>
          scripts.run(settleScript, keys, [
            String(windowMs),
            String(bucket),
            token,
            "0"
          ])
        );
      },
      async extend(nextTtlMs = holdTtlMs) {
        const ttl = positiveInt(nextTtlMs, "holdTtlMs");
        if (settled) return false;
        // The reservation set has no bucket in its name, so any bucket's key
        // list yields the same third key.
        const holds = keysFor(id, 0)[2];
        const reply = await scripts.run(
          extendScript,
          [holds],
          [`${token}:${estimate}`, String(ttl)]
        );
        return reply === 1;
      }
    };
  }

  return {
    /**
     * Charge `cost` if it fits in the window. One atomic round trip; nothing is
     * charged when it does not fit.
     */
    async charge(id: string, cost: number): Promise<BudgetResult> {
      const amount = wholeAmount(cost, "cost");
      const reply = await withBucket(id, (keys, bucket) =>
        scripts.run(chargeScript, keys, [
          String(windowMs),
          String(bucket),
          String(limit),
          String(amount)
        ])
      );
      return resultOf(reply);
    },

    /**
     * Hold `estimate` units against the budget, or `null` if they do not fit.
     *
     * Holding is what keeps a burst of concurrent calls from each passing the
     * check and collectively blowing the budget: the estimate counts against
     * everyone else from the moment it is taken, and is replaced by the real
     * cost on {@link BudgetHold.settle}.
     */
    async reserve(id: string, estimate: number): Promise<BudgetHold | null> {
      const amount = wholeAmount(estimate, "estimate");
      const token = globalThis.crypto.randomUUID();
      const reply = await withBucket(id, (keys, bucket) =>
        scripts.run(reserveScript, keys, [
          String(windowMs),
          String(bucket),
          String(limit),
          String(amount),
          token,
          String(holdTtlMs)
        ])
      );
      if (reply.status !== 1) return null;
      return holdFor(id, token, amount);
    },

    /**
     * Current headroom, including live holds. Spends nothing (it does prune
     * holds whose lease has already lapsed, which is what makes the number
     * accurate).
     */
    async check(id: string): Promise<BudgetResult> {
      const reply = await withBucket(id, (keys, bucket) =>
        scripts.run(checkScript, keys, [
          String(windowMs),
          String(bucket),
          String(limit)
        ])
      );
      return { ...resultOf(reply), ok: reply.remaining > 0 };
    },

    /** Clear this id's spend and holds outright. */
    async reset(id: string): Promise<void> {
      // Derived server-side like everything else: a skewed clock (or a window
      // boundary crossed mid-call) would otherwise delete buckets nobody is
      // using and leave the real spend behind.
      await withBucket(id, (keys, bucket) =>
        scripts.run(resetScript, keys, [String(windowMs), String(bucket)])
      );
    }
  };
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `budget ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

/**
 * Amounts must be whole numbers, because the window buckets are Redis integer
 * counters (`INCRBY`) and Lua returns integers to the client. Charging `1.5`
 * would otherwise fail deep in the script with "value is not an integer", and
 * a fractional `remaining` would be truncated on the way back.
 *
 * This is not a real constraint in practice: budget in the smallest unit you
 * meter. Tokens are already whole; bill money in cents, or micro-cents when
 * per-token prices need the resolution.
 */
function wholeAmount(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ValidationError(
      `budget ${name} must be a non-negative integer, received ${value}. ` +
        "Budget in the smallest unit you meter (tokens, cents, micro-cents) " +
        "rather than a fractional one."
    );
  }
  return value;
}

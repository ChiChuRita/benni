import { ValidationError } from "../core/errors.js";
import { defineScript } from "../core/script.js";
import { hashTagOf } from "../core/slot.js";

// The lease machinery `lock`, `semaphore`, `idempotency`, and `cache` share:
// one token-checked renewal loop, one verdict on whether a lease survived, one
// retry policy. Internal: `index.ts` does not re-export this module.

// `run()` renews on a quarter of the lease, the same ratio the queue uses for
// its leases (leaseMs 60000 / heartbeatMs 15000): three renewals in a row may
// fail outright before the lease could lapse, which is what makes a transient
// blip survivable rather than fatal.
const HEARTBEAT_DIVISOR = 4;

/**
 * Extend a plain-string lease only if we still hold it. Returns 1 if extended,
 * else 0. `lock` compares a token, `idempotency` its running marker, and the
 * cache its fill token: the same check-then-PEXPIRE for all three.
 */
export const extendIfHeldScript = defineScript<
  readonly [token: string, ttlMs: string],
  number
>({
  keyCount: 1,
  lua: 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/**
 * Release only if we still hold it — never DEL a lease that has expired and
 * been re-acquired by someone else. Returns 1 if released, else 0.
 */
export const releaseIfHeldScript = defineScript<
  readonly [token: string],
  number
>({
  keyCount: 1,
  lua: 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end',
  decode: (reply) => (typeof reply === "number" ? reply : 0)
});

/**
 * Local time for lease deadlines, in milliseconds, from a monotonic clock.
 *
 * `Date.now()` follows the wall clock, and the wall clock is stepped: an NTP
 * correction backwards made an expired lease look live for the size of the
 * step, and one forwards declared a healthy lease lost. Server time decides who
 * owns a key; this only decides what *we* may still assume, and that must not
 * move when someone adjusts the system clock.
 */
export function monotonicNow(): number {
  return performance.now();
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * A delay spread uniformly over `[0.5, 1.5)` of `ms`. The mean stays `ms`, so
 * `retries × retryDelayMs` still reads as the expected wait, but contenders
 * that collided once stop waking in lockstep and colliding again.
 */
export function jittered(ms: number): number {
  return ms * (0.5 + Math.random());
}

export function positiveMs(value: number, name: string, owner: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ValidationError(
      `${owner} ${name} must be a positive integer, received ${value}`
    );
  }
  return value;
}

function nonNegativeMs(value: number, name: string, owner: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ValidationError(
      `${owner} ${name} must be a non-negative integer, received ${value}`
    );
  }
  return value;
}

/**
 * A key that lands in the same Redis Cluster slot as `key`, for a script that
 * has to touch both. Keeps `key` itself exactly as it was.
 *
 * A key with a hash tag already decides its slot by that tag, so appending to
 * it keeps the tag. A key without one hashes whole, so wrapping the whole key
 * in braces makes it the tag. What cannot be co-located is a key that hashes
 * whole *and* contains a `}` (an id like `"a}b"`, or one that opens an empty
 * `{}`), because wrapping it would end the tag early; that is rejected rather
 * than allowed to fail with CROSSSLOT on a cluster only.
 */
export function coLocatedKey(
  key: string,
  suffix: string,
  owner: string
): string {
  const tag = hashTagOf(key);
  const candidate = tag === key ? `{${key}}:${suffix}` : `${key}:${suffix}`;
  if (hashTagOf(candidate) !== tag) {
    throw new ValidationError(
      `${owner} key ${JSON.stringify(key)} cannot share a Redis Cluster slot ` +
        'with its companion key: it has no usable hash tag but contains "}". ' +
        "Use an id without braces, or one with a non-empty {tag}."
    );
  }
  return candidate;
}

/**
 * Throw unless every key shares one Redis Cluster slot. The scripts that touch
 * them are illegal on a cluster otherwise, and only for the ids that trip it,
 * which is a miserable thing to debug from a CROSSSLOT in production.
 */
export function assertCoLocated(
  keys: readonly string[],
  owner: string,
  id: string
): void {
  const tag = hashTagOf(keys[0] ?? "");
  if (keys.every((key) => hashTagOf(key) === tag)) return;
  throw new ValidationError(
    `${owner} id ${JSON.stringify(id)} splits its keys (${keys.join(", ")}) ` +
      "across Redis Cluster slots, so the scripts that touch them together " +
      'would fail with CROSSSLOT. An empty id, or one starting with "}", ' +
      'yields an empty hash tag; so does a prefix containing "{".'
  );
}

/** How a primitive names its lease in validation messages. */
export type LeaseTerms = {
  /** `"lock"`, `"semaphore"`. */
  readonly owner: string;
  /** The option naming the lease length: `"ttlMs"`, `"leaseMs"`. */
  readonly ttlName: string;
  /** What lapses: `"lock"`, `"slot"`. */
  readonly held: string;
};

/**
 * The default renewal interval for a lease. Floored at 1ms so an absurdly short
 * lease still renews rather than dividing down to a zero-delay spin.
 */
export function heartbeatFor(ttlMs: number): number {
  return Math.max(1, Math.floor(ttlMs / HEARTBEAT_DIVISOR));
}

/**
 * The renewal interval for one `run()`: `null` when the caller opted out, the
 * derived default when they said nothing, and their own value otherwise.
 *
 * Only a value the caller passed is checked against the lease, and it has to
 * leave room for a renewal *and* a retry, so half the lease is the ceiling. At
 * or above the lease the first tick lands on or after expiry, so the lease is
 * declared lost before a single renewal has been attempted — uncontended, and
 * only once a body is slow enough to reach that first tick, so the
 * misconfiguration passes every quick test and shows up under load.
 *
 * The derived default is deliberately exempt: {@link heartbeatFor} floors at
 * 1ms, which for a lease of 1 is the whole lease and can satisfy no ratio at
 * all, and a working configuration must not start throwing.
 */
export function renewalInterval(
  requested: number | false | undefined,
  ttlMs: number,
  terms: LeaseTerms
): number | null {
  if (requested === false) return null;
  if (requested === undefined) return heartbeatFor(ttlMs);
  const heartbeatMs = positiveMs(requested, "heartbeatMs", terms.owner);
  if (heartbeatMs * 2 > ttlMs) {
    throw new ValidationError(
      `${terms.owner} heartbeatMs must be at most half of ${terms.ttlName} (${ttlMs}) so a renewal lands before the ${terms.held} could lapse, received ${heartbeatMs}`
    );
  }
  return heartbeatMs;
}

/** The retry knobs `acquire()` and `run()` take. */
export type WaitOptions = {
  readonly retries?: number;
  readonly retryDelayMs?: number;
  readonly waitTimeoutMs?: number;
};

/** What to do between contended attempts, validated before the first one. */
export type WaitPolicy = {
  readonly retries: number;
  readonly retryDelayMs: number;
  readonly waitTimeoutMs: number | undefined;
};

/**
 * Validate the retry knobs up front, so a bad value throws before anything is
 * taken rather than stranding a lease between the acquire and a `try`.
 *
 * `waitTimeoutMs` alone means "keep trying until then": with no `retries` it
 * lifts the attempt count, because a deadline that still stopped after the one
 * default attempt would do nothing at all.
 */
export function waitPolicy(
  options: WaitOptions | undefined,
  owner: string
): WaitPolicy {
  const waitTimeoutMs =
    options?.waitTimeoutMs === undefined
      ? undefined
      : nonNegativeMs(options.waitTimeoutMs, "waitTimeoutMs", owner);
  return {
    retries:
      options?.retries ??
      (waitTimeoutMs === undefined ? 0 : Number.POSITIVE_INFINITY),
    retryDelayMs: options?.retryDelayMs ?? 100,
    waitTimeoutMs
  };
}

/**
 * Call `attempt` until it yields something, retries run out, or the deadline
 * passes. Sleeps are jittered, and the last one is clipped to the deadline so
 * a final attempt still lands at it rather than after it.
 */
export async function acquireWithRetry<T>(
  attempt: () => Promise<T | null>,
  policy: WaitPolicy
): Promise<T | null> {
  const deadline =
    policy.waitTimeoutMs === undefined
      ? Number.POSITIVE_INFINITY
      : monotonicNow() + policy.waitTimeoutMs;
  for (let tries = 0; ; tries++) {
    const got = await attempt();
    if (got !== null) return got;
    if (tries >= policy.retries) return null;
    const remaining = deadline - monotonicNow();
    if (remaining <= 0) return null;
    await sleep(Math.min(jittered(policy.retryDelayMs), remaining));
  }
}

/**
 * What we believe about our hold on one lease, shared by the renewal loop and
 * the caller's own `extend()`/`release()` calls so both see one view of it.
 */
export type Lease = {
  /** The TTL this acquisition was taken with, and that renewals re-apply. */
  readonly ttlMs: number;
  /** Aborts with the primitive's lost-lease error once the lease is gone. */
  readonly signal: AbortSignal;
  /** True once we know the lease is no longer ours. */
  readonly lost: boolean;
  /** True once the caller gave the lease up (or began to) deliberately. */
  readonly released: boolean;
  /**
   * Monotonic time ({@link monotonicNow}) at which the lease has certainly
   * lapsed unless renewed. Measured from *before* each round trip, so it
   * never overstates how long we hold it.
   */
  readonly expiresAt: number;
  /**
   * Token-checked renewal. Defaults to {@link ttlMs}, the TTL of *this*
   * acquisition, never a store-wide default. A `false` result aborts
   * {@link signal} unless the lease was being given up.
   */
  extend(ttlMs?: number): Promise<boolean>;
  /** Token-checked release. */
  release(): Promise<boolean>;
  /**
   * Give the lease up through a round trip of the caller's own (publishing a
   * fill, recording a result), flagged as deliberate *before* it is sent so an
   * overlapping renewal that finds the key gone is not reported as a loss.
   */
  close<R>(roundTrip: () => Promise<R>): Promise<R>;
  /** Record the loss, once, and abort {@link signal}. */
  lose(): void;
  /** A fresh instance of the primitive's lost-lease error. */
  lostError(): Error;
};

export function createLease(options: {
  readonly ttlMs: number;
  /** {@link monotonicNow} taken before the acquiring round trip. */
  readonly startedAt: number;
  readonly extend: (ttlMs: number) => Promise<boolean>;
  readonly release: () => Promise<boolean>;
  readonly lostError: () => Error;
  /** Validates a caller-supplied TTL for `extend()`. */
  readonly checkTtl: (ttlMs: number) => number;
}): Lease {
  const controller = new AbortController();
  let expiresAt = options.startedAt + options.ttlMs;
  let lost = false;
  let released = false;
  const lose = (): void => {
    if (lost) return;
    lost = true;
    controller.abort(options.lostError());
  };
  const lease: Lease = {
    ttlMs: options.ttlMs,
    signal: controller.signal,
    get lost() {
      return lost;
    },
    get released() {
      return released;
    },
    get expiresAt() {
      return expiresAt;
    },
    async extend(nextTtlMs = options.ttlMs) {
      const ms = options.checkTtl(nextTtlMs);
      // Time the renewal from before the call, not after: the server applies
      // the new expiry at some point during it, so `sentAt + ms` is the
      // earliest it can lapse. Anything later would let the deadline claim we
      // still hold a lease that has already expired.
      const sentAt = monotonicNow();
      const held = await options.extend(ms);
      if (held) expiresAt = sentAt + ms;
      else if (!released) lose();
      return held;
    },
    release() {
      return lease.close(options.release);
    },
    close(roundTrip) {
      released = true;
      return roundTrip();
    },
    lose,
    lostError: options.lostError
  };
  return lease;
}

export type RenewalOptions = {
  /** Renewal interval, or `null` to not renew at all. */
  readonly heartbeatMs: number | null;
  /** Called when a renewal round trip fails. Telemetry only. */
  readonly onRenewError?: (error: unknown) => void;
};

/** `fn`'s value, and how to judge the lease once the closing round trip ran. */
export type Renewed<T> = {
  readonly value: T;
  /**
   * Whether the work has to be reported as having run without the lease,
   * given what the closing round trip said: `true` held, `false` not ours,
   * `null` the round trip itself failed (which proves nothing either way).
   */
  lostWith(heldAtClose: boolean | null): boolean;
};

/**
 * Run `fn` while renewing `lease` every `heartbeatMs`, and stop renewing, for
 * good, however `fn` settles. Rejects with `fn`'s own error; resolves with its
 * value and a verdict to apply once the caller's closing round trip is in.
 */
export async function whileRenewing<T>(
  lease: Lease,
  fn: () => Promise<T> | T,
  renewal: RenewalOptions
): Promise<Renewed<T>> {
  const { heartbeatMs, onRenewError } = renewal;
  let stopped = false;
  let renewing = false;
  let timer: ReturnType<typeof setInterval> | null = null;
  /**
   * Stop renewing, for good. Called both from the tick that notices the lease
   * is gone and from the exit path below, because "the flag is set" is not the
   * same as "the interval is gone": a lease declared lost used to leave the
   * interval armed, and a body that ignores the abort signal and never settles
   * then span on early-returning ticks for the life of the process.
   */
  const stopRenewal = (): void => {
    stopped = true;
    if (timer === null) return;
    clearInterval(timer);
    timer = null;
  };
  /**
   * Hand a failed round trip to the caller's hook, if any, without letting the
   * hook out. A throw from it would reject the renewal promise the tick
   * discards, and an unobserved rejection is fatal in default Node: a
   * telemetry callback must not be able to take the process down.
   */
  const reportRenewError = (error: unknown): void => {
    // Deliberately silent once renewal has stopped: a round trip still in
    // flight when the lease was released can settle after the call the caller
    // awaited already returned, and reporting a failure to renew a lease we
    // have since given up is noise, not news.
    if (stopped || onRenewError === undefined) return;
    try {
      onRenewError(error);
    } catch {
      // Swallowed exactly as a failed release is. The hook exists to observe
      // renewals, not to decide the fate of the critical section.
    }
  };
  /**
   * One renewal round trip, with every outcome handled *inside* it. The tick
   * discards the returned promise, so anything escaping here would be an
   * unobserved rejection.
   */
  const renewOnce = async (): Promise<void> => {
    try {
      // `extend()` flags the lease lost itself when Redis reports it is not
      // ours, so a `false` result means there is nothing left to renew and the
      // interval can go now rather than on the next tick.
      if (!(await lease.extend(lease.ttlMs))) stopRenewal();
    } catch (error) {
      // A failed round trip is not proof the lease is gone, so the next tick
      // retries; the deadline below is what eventually calls it lost.
      reportRenewError(error);
    } finally {
      renewing = false;
    }
  };
  if (heartbeatMs !== null) {
    timer = setInterval(() => {
      // Nothing left to renew: the work is done, the body gave the lease up,
      // or it is gone. Tear the interval down rather than waking up to
      // early-return from here on.
      if (stopped || lease.lost || lease.released) {
        stopRenewal();
        return;
      }
      // The whole TTL window has passed with no successful renewal, so the
      // lease has lapsed whatever the cause: renewals that keep rejecting, or
      // one still hanging while the guard below skips ticks. Silence here is
      // the bug this renewal exists to fix.
      if (monotonicNow() >= lease.expiresAt) {
        lease.lose();
        stopRenewal();
        return;
      }
      // One renewal at a time. A round trip slower than the interval would
      // otherwise stack up calls that all re-apply the same TTL.
      if (renewing) return;
      renewing = true;
      void renewOnce();
    }, heartbeatMs);
    // Never keep the process alive for a renewal alone: an un-unref'd
    // interval is what makes `node script.js` hang after the work is done.
    (timer as { unref?: () => void }).unref?.();
  }

  try {
    const value = await fn();
    // Snapshotted before the closing round trip, so the time it takes cannot
    // push a body that finished comfortably inside its lease past the
    // deadline, and so a lease the body gave up is told apart from one the
    // caller closes next.
    const expiredOnCompletion = monotonicNow() >= lease.expiresAt;
    const releasedByBody = lease.released;
    return {
      value,
      /**
       * `lease.lost` alone is not enough, because it is only ever set from
       * inside the renewal tick: a body that blocks the event loop past the
       * TTL and then returns without awaiting anything never lets the tick run
       * at all, and since a timer is a macrotask while `await fn()` resumes on
       * a microtask, a check of the flag alone reports success for a lease
       * that had already expired.
       */
      lostWith(heldAtClose) {
        // Proven: Redis told a renewal the lease is no longer ours.
        if (lease.lost) return true;
        // Given up on purpose. Renewals and the closing round trip both find
        // the key gone, and neither of those is a loss. Read from the snapshot:
        // the caller's own close sets the flag too, and consulting it live
        // would excuse every lost lease there is.
        if (releasedByBody) return false;
        // Renewal was switched off, so a lease that lapses under a long body
        // is exactly what that opt-out documents.
        if (heartbeatMs === null) return false;
        // The deadline, read in the same turn the body finished rather than
        // only from a tick that may never have got to run.
        if (expiredOnCompletion) return true;
        // The closing round trip ran the same token check `extend()` does, so
        // `false` is Redis saying the lease had already moved on.
        return heldAtClose === false;
      }
    };
  } finally {
    stopRenewal();
  }
}

/**
 * `lock.run` and `semaphore.run` in one place: run `fn` under renewal, always
 * release, and reject with the lost-lease error if the lease did not survive.
 *
 * A body that threw propagates its own error, which a lease report would bury.
 * A body that resolved without the lease is rejected even so: it finished, but
 * not under the guarantee it asked for, and resolving is what let a lost lease
 * pass for a successful critical section.
 */
export async function runHeld<T>(
  lease: Lease,
  fn: () => Promise<T> | T,
  renewal: RenewalOptions
): Promise<T> {
  let outcome: Renewed<T>;
  let heldOnRelease: boolean | null = null;
  try {
    outcome = await whileRenewing(lease, fn, renewal);
  } finally {
    try {
      heldOnRelease = await lease.release();
    } catch {
      // A failed release must not mask fn's outcome (or replace its error);
      // the TTL is the backstop and frees the lease regardless.
    }
  }
  if (outcome.lostWith(heldOnRelease)) throw lease.lostError();
  return outcome.value;
}

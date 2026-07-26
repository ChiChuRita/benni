---
title: "Optimistic Transactions"
description: "Use redis.watch() to run a WATCH/MULTI/EXEC check-and-set that retries on conflict."
---

Use `redis.watch()` for check-and-set logic: read some keys, decide what to write, and commit atomically only if none of the watched keys changed underneath you. If a watched key changed, the commit aborts and the helper retries.

This is Redis optimistic locking (`WATCH`/`MULTI`/`EXEC`) wrapped as a retry loop. It runs on a [session](/beni/advanced/sessions/) connection, because `WATCH` state belongs to one connection.

## `redis.watch(keys, body, options?)`

```ts
await redis.watch("views:home", async (s) => {
  // read the watched keys through the session's typed stores, then return a
  // built, un-executed transaction to commit — or null to opt out
  return s.multi();
});
```

Per attempt the helper opens (or borrows) a session, sends `WATCH keys`, runs your `body`, and calls `exec()` on the transaction the body returns. Because the body hands back the built-but-not-executed transaction, you can neither forget to `exec()` nor double-`exec()`.

- The commit succeeds → `redis.watch` resolves the decoded result tuple.
- A watched key changed → `exec()` aborts, `onAbort` fires, the helper backs off (if configured) and retries with a fresh `WATCH`.
- The body returns `null` → the helper `UNWATCH`es and resolves `null` — you opted out.
- Attempts run out → the helper throws `WatchRetriesExceededError`.

Read the watched keys through the session (`s.kv(...)`, `s.hash(...)`, …) so the reads happen on the same connection that holds the `WATCH`. Build the write with `s.multi()`, whose `.add(command, decoder)` extends a position-typed result tuple exactly like [`redis.multi()`](/beni/advanced/transactions/), and whose `exec()` resolves the tuple or `null` on abort.

## Check-And-Set Example

Cap a counter at a ceiling, retrying if a concurrent writer moves it:

```ts
import { okReply, numberReply, WatchRetriesExceededError } from "beni";
import { number, kv } from "beni/schema";

const views = kv("views", number());

const result = await redis.watch(
  views.key("home"),
  async (s) => {
    const current = (await s.kv(views).get("home")) ?? 0; // read on the watching connection
    if (current >= 1_000_000) return null;                      // opt out -> resolves null
    return s
      .multi()
      .add(["SET", views.key("home"), String(current + 1)], okReply)
      .add(["INCR", `${views.key("home")}:writes`], numberReply);
  },
  {
    attempts: 5,
    onAbort: ({ attempt }) => metrics.increment("views.cas_conflict", { attempt })
  }
);
//    ^? [void, number] | null   (null = the body opted out)
```

## Balance Transfer

Move funds between two accounts atomically, aborting the whole operation if either balance shifts mid-flight, and opting out cleanly when the source lacks funds:

```ts
import { okReply } from "beni";
import { number, kv } from "beni/schema";

const balances = kv("balance", number());

async function transfer(from: string, to: string, amount: number) {
  return redis.watch(
    [balances.key(from), balances.key(to)], // watch both accounts
    async (s) => {
      const fromBalance = (await s.kv(balances).get(from)) ?? 0;
      if (fromBalance < amount) return null; // insufficient funds -> resolves null, no retry
      const toBalance = (await s.kv(balances).get(to)) ?? 0;
      return s
        .multi()
        .add(["SET", balances.key(from), String(fromBalance - amount)], okReply)
        .add(["SET", balances.key(to), String(toBalance + amount)], okReply);
    },
    { attempts: 10 }
  );
}

const outcome = await transfer("alice", "bob", 50);
if (outcome === null) {
  // either the body opted out (insufficient funds)…
}
```

## Options

```ts
redis.watch(keys, body, {
  attempts,   // total attempts, default 5, must be >= 1
  backoff,    // (attempt: number) => ms to wait before the next retry; default: no delay
  onAbort,    // ({ attempt }) => void — called on each conflict, before backoff
  session     // borrow a long-lived session for hot paths; the helper never closes it
});
```

`onAbort` fires on every conflict, so you can watch contention with metrics before it becomes an incident. `backoff` is opt-in — there is no hidden default sleep. `session` lets a hot path reuse one connection across many `redis.watch` calls; the helper closes only sessions it opened itself.

## Abort, Opt-Out, And Exhaustion

Three distinct outcomes, three distinct signals:

- **Abort (conflict).** A watched key changed before `exec()`, so `exec()` resolves `null` internally. The helper retries with a fresh `WATCH` — you never see this directly unless `attempts` runs out.
- **Opt-out.** Your body returns `null`. The helper `UNWATCH`es and `redis.watch` resolves `null`. Use it for "value already correct" or "insufficient funds" — a deliberate no-op, not a failure.
- **Exhaustion.** All `attempts` aborted. `redis.watch` throws `WatchRetriesExceededError`, which carries `.attempts`. Exhaustion is exceptional, so it throws rather than returning `null`, keeping the happy path ceremony-free.

```ts
try {
  const result = await redis.watch(/* … */);
  if (result === null) {
    // the body opted out
  }
} catch (error) {
  if (error instanceof WatchRetriesExceededError) {
    console.error(`gave up after ${error.attempts} conflicts`);
  }
}
```

A per-command runtime error inside a committed `EXEC` (for example a `WRONGTYPE`) rejects the promise. Note that Redis `MULTI` has no rollback: the other commands in that transaction still committed.

## Manual Form

For a custom loop, drive the primitives on a session directly:

```ts
await using s = await redis.session();
await s.watch([views.key("home")]);
const current = (await s.kv(views).get("home")) ?? 0;
const outcome = await s
  .multi()
  .add(["SET", views.key("home"), String(current + 1)], okReply)
  .exec();
//    ^? [void] | null
if (outcome === null) {
  // a watched key changed — re-WATCH and retry
}
```

An empty watched `exec()` throws a `TypeError` — unlike `redis.multi()`, a watched transaction that commits nothing would leave `WATCH` armed on the connection, so it is banned. `EXEC` clears watch state server-side on both success and abort, so no `UNWATCH` is needed between retries.

## When To Reach For Lua Instead

`WATCH` livelocks under heavy contention by design — many writers keep invalidating each other's reads, and retries pile up. For very hot check-and-set keys, prefer a [Lua script](/beni/advanced/scripts/): scripts run atomically on the server, reading before they write without any retry loop. The `onAbort` metrics exist precisely because this failure mode is load-dependent and invisible in low-traffic testing.

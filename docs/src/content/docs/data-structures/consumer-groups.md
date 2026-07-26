---
title: "Consumer Groups"
description: "Use stream consumer groups for at-least-once delivery across many workers, with pending tracking and crash recovery."
---

Consumer groups let several workers share a [stream](/beni/data-structures/streams/) with at-least-once delivery. Redis tracks which entries each consumer has received but not yet acknowledged (the pending entries list, or PEL), so a crashed worker's in-flight entries can be recovered by another.

Groups hang off the stream **store**, not the schema — group topology changes at deploy time, while the schema stays about data shape. You bind a group by name, then a consumer by name, and the stream id (the schema key id) stays the first argument of every call:

```ts
const group = redis.stream(auditEvents).group("processors");
const me = group.consumer(`c-${process.pid}`);
```

Everything except the blocking group read is non-blocking and runs on the shared client. The blocking read requires a [session](/beni/advanced/sessions/).

## Create A Group

```ts
const created = await group.create("login", { from: "start" });
//    ^? boolean — true = created, false = the group already existed
```

`create` is idempotent: it returns `false` when the group already exists rather than throwing. `from` is **required** and says where a new group starts reading, because defaulting would silently choose between replaying all history and skipping it:

- `"start"` — deliver the stream's full history to the group.
- `"end"` — deliver only entries added after the group is created.
- `{ entryId: "1720094400000-0" }` — deliver everything after a specific id.

`create` also creates the stream if it is missing (`MKSTREAM`); pass `{ from, mkstream: false }` to require the stream to exist first.

## Read And Ack

A consumer reads new deliveries with `xreadgroup` and acknowledges them with `xack`. New deliveries (`>`) never include tombstones:

```ts
const batch = await me.xreadgroup("login", { count: 20 }); // XREADGROUP > -> StreamEntry[]
for (const entry of batch) {
  await handleEntry(entry.value); // ^? Partial<{ type: string; userId: string }>
}
if (batch.length > 0) {
  await group.xack("login", batch.map((e) => e.id)); // XACK -> number acknowledged
}
```

`xack` also exists on the consumer (`me.xack("login", ids)`) as a convenience mirror, so worker code never has to reach back up to the group.

## Tombstones

An entry that is `XDEL`ed from the stream while it is still in a consumer's PEL decodes as a **tombstone**: its `value` is `null`. Tombstones only ever appear on history and claim paths (`xreadgroup` with `after`, `xclaim`, `xautoclaim`), never on a live `xreadgroup`. You still have to ack a tombstone to clear it from the PEL — there is nothing left to process, so ack and move on:

```ts
for (const entry of await me.xreadgroup("login", { after: "0", count: 100 })) {
  if (entry.value === null) {
    await group.xack("login", [entry.id]); // tombstone: deleted upstream, just clear it
    continue;
  }
  await handleEntry(entry.value);
  await group.xack("login", [entry.id]);
}
```

## Crash Recovery With autoClaim

When a worker dies, its unacked entries sit idle in its PEL. `xautoclaim` (`XAUTOCLAIM`) steals entries idle longer than `minIdleMs` and reassigns them to the calling consumer. It scans with a cursor: `"0-0"` starts a scan, and a returned cursor of `"0-0"` means the scan is complete.

```ts
let cursor = "0-0";
do {
  const res = await me.xautoclaim("login", {
    minIdleMs: 60_000, // only steal entries idle > 60s (assume the owner is dead)
    start: cursor,
    count: 50
  });
  //   ^? { cursor: string; entries: PendingStreamEntry[]; deletedIds: string[] }
  for (const entry of res.entries) {
    if (entry.value === null) {
      await group.xack("login", [entry.id]); // tombstone
      continue;
    }
    await handleEntry(entry.value);
    await group.xack("login", [entry.id]);
  }
  cursor = res.cursor;
} while (cursor !== "0-0");
```

On Redis 7+, entry ids that `xautoclaim` finds already deleted from the stream are dropped from the PEL by Redis itself and reported in `deletedIds` — nothing to ack for those. To recover a **specific** set of entries by id, use `xclaim` (`XCLAIM`) with the same `minIdleMs` guard.

## Inspect Pending Work

For dashboards and janitors, `xpending` gives a summary and `xpending` with options lists individual pending entries — both non-blocking on the shared client:

```ts
const summary = await group.xpending("login");
//    ^? { count; minEntryId; maxEntryId; consumers: [{ consumer; count }] }

const stuck = await group.xpending("login", { count: 10, minIdleMs: 300_000 });
for (const row of stuck) {
  //  ^? { entryId; consumer; idleMs; deliveries }
  if (row.deliveries > 5) {
    await deadLetter(row.entryId); // the delivery counter is a poison-pill detector
  }
}
```

`count` is required on the `xpending` extended form because Redis requires it. Idle thresholds are milliseconds and carry a `...Ms` suffix (`minIdleMs`, `idleMs`) — the PEL speaks milliseconds, while [blocking timeouts](/beni/advanced/blocking-operations/) speak seconds (`timeoutSeconds`), and the suffixes keep the units unambiguous at call sites.

`deleteConsumer` removes a consumer and destroys its PEL entries; `destroy` removes the whole group.

## Blocking Group Read (Session Only)

A live worker that wants to wait for new deliveries uses `xreadgroup` with a `timeoutSeconds`, which is only reachable through a session — it parks the connection like any other [blocking operation](/beni/advanced/blocking-operations/):

```ts
await redis.session(async (s) => {
  const live = s.stream(auditEvents).group("processors").consumer(`c-${process.pid}`);
  while (!stop.signal.aborted) {
    const batch = await live.xreadgroup("login", { timeoutSeconds: 5, count: 20 });
    for (const entry of batch) await handleEntry(entry.value);
    if (batch.length > 0) {
      await group.xack("login", batch.map((e) => e.id)); // ack via the shared client
    }
  }
});
```

Blocking `xreadgroup` always reads `>` (new deliveries), because Redis only honors `BLOCK` for new entries — so there is no id parameter, and no tombstones. It returns an empty array on timeout, which the `{ timeoutSeconds: 5 }` loop above treats as a heartbeat.

## Full Worker Lifecycle

The pieces above compose into a worker: recover this consumer's own history, steal from dead peers, then loop on the blocking read.

```ts
const group = redis.stream(auditEvents).group("processors");
const me = group.consumer(`c-${process.pid}`);

await group.create("login", { from: "start" }); // idempotent bootstrap

// (a) recover this consumer's own unacked work from a previous crash
for (const entry of await me.xreadgroup("login", { after: "0", count: 100 })) {
  if (entry.value === null) { await group.xack("login", [entry.id]); continue; }
  await handleEntry(entry.value);
  await group.xack("login", [entry.id]);
}

// (b) steal work abandoned by dead consumers (idle > 60s)
let cursor = "0-0";
do {
  const res = await me.xautoclaim("login", { minIdleMs: 60_000, start: cursor, count: 50 });
  for (const entry of res.entries) {
    if (entry.value === null) { await group.xack("login", [entry.id]); continue; }
    await handleEntry(entry.value);
    await group.xack("login", [entry.id]);
  }
  cursor = res.cursor;
} while (cursor !== "0-0");

// (c) live loop — the blocking group read is only reachable through a session
await redis.session(async (s) => {
  const live = s.stream(auditEvents).group("processors").consumer(`c-${process.pid}`);
  while (!stop.signal.aborted) {
    const batch = await live.xreadgroup("login", { timeoutSeconds: 5, count: 20 });
    for (const entry of batch) await handleEntry(entry.value);
    if (batch.length > 0) await group.xack("login", batch.map((e) => e.id));
  }
});
```

`XINFO`, `XGROUP SETID`/`CREATECONSUMER`, `NOACK`, and multi-stream group reads are not wrapped yet; use [`redis.raw`](/beni/core-concepts/raw-redis-access/) for those.

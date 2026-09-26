---
"benni": minor
---

`lock` hands every holder a fencing token, and says plainly what it cannot promise across a failover.

A lease cannot stop a *paused* holder (a long GC, a descheduled container) from waking after its lock lapsed and was re-granted, and writing on top of the new holder. The fix lives where the write lands, so every acquisition now carries a number to check there:

```ts
await locks.run("order:42", async ({ fence }) => {
  // 0 rows updated means a newer holder already wrote.
  await db.query(
    "UPDATE orders SET status = $1, fence = $2 WHERE id = $3 AND fence < $2",
    ["shipped", fence, "42"]
  );
});
```

- **`LockHandle.fence`** is a number that strictly increases with every grant of a lock id, across all processes. It is drawn by an `INCR` in the same Lua script as the `SET NX PX`, so two holders in quick succession can never draw fences in the opposite order to their grants. The same handle is what `run()` passes to `fn`.
- The counter lives at `{<prefix>:<id>}:fence`, in the lock's Redis Cluster slot, and never expires: one small key per lock id, because a counter that expired would restart below fences a store has already accepted.
- Acquiring is now one `EVALSHA` (after a one-time `SCRIPT LOAD`) instead of a bare `SET`, so it still needs nothing a REST adapter lacks.
- The docs and JSDoc now state that the lock assumes **one Redis primary**: replication is asynchronous, so a failover can grant the lock twice, and there is no Redlock. The lock page shows how the fence closes that gap for writes. The README and `llms.txt` stop calling the benchmarks' raw lock "token-fenced"; it was token-checked, which is not the same thing.

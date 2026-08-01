import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ValidationError } from "../src/core/errors.js";
import type { RedisClient } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import {
  queue,
  RetryJobError,
  TerminalJobError
} from "../src/primitives/index.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** A promise the test resolves by hand, to pin a handler at a known point. */
function gate() {
  let open!: () => void;
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { opened, open };
}

async function waitFor(
  predicate: () => Promise<boolean>,
  what: string
): Promise<void> {
  const deadline = Date.now() + 3_000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(5);
  }
  throw new Error(`timed out waiting for ${what}`);
}

describe("queue: RetryJobError input", () => {
  it("rejects a non-finite delay instead of stranding the job", () => {
    // Math.max(0, NaN) is NaN and Math.max(0, Infinity) is Infinity. Both reach
    // Redis as a sorted-set score it refuses, by which point the retry script
    // has already dropped the lease and rewritten the record.
    expect(() => new RetryJobError("429", Number.NaN)).toThrow(ValidationError);
    expect(() => new RetryJobError("429", Number.NaN)).toThrow(
      /finite number of milliseconds/
    );
    expect(() => new RetryJobError("429", Number.POSITIVE_INFINITY)).toThrow(
      ValidationError
    );
    expect(new RetryJobError("429", 1_234).retryAfterMs).toBe(1_234);
    expect(new RetryJobError("429", -5).retryAfterMs).toBe(0);
  });
});

describeRedis("queue: hunt regressions (live)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  let namespace = 0;
  const nextPrefix = () => `${run}:h${namespace++}`;

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("keeps a job in the scheduled index when a handler asks for a bad delay", async () => {
    // A malformed Retry-After parsed straight through used to error the retry
    // script after it had removed the lease and written status=scheduled: the
    // job was in no lifecycle index at all, so nothing could ever reserve it.
    const prefix = nextPrefix();
    const jobs = queue<null, string>(client, {
      prefix,
      backoffMs: 60_000,
      maxBackoffMs: 60_000
    });
    const header = "come back later";
    const worker = jobs.worker(
      async () => {
        throw new RetryJobError("429", Number.parseFloat(header) * 1_000);
      },
      { pollMs: 5, onError: () => {} }
    );

    const { id } = await jobs.enqueue(null);
    await waitFor(
      async () => (await jobs.get(id))?.status === "scheduled",
      "the failed attempt to be rescheduled"
    );
    await worker.stop();

    expect((await jobs.stats()).scheduled).toBe(1);
    expect(
      await client.send(["ZSCORE", `{${prefix}}:scheduled`, id])
    ).not.toBeNull();
  });

  it("refuses to re-enqueue an id that is still live", async () => {
    // Rewriting a live record left the id in two lifecycle indexes at once, and
    // a worker pair ran the supposedly single job twice.
    const jobs = queue<string, string>(client, { prefix: nextPrefix() });
    await jobs.enqueue("one", { id: "dup" });

    await expect(jobs.enqueue("two", { id: "dup" })).rejects.toBeInstanceOf(
      ValidationError
    );
    await expect(jobs.enqueue("two", { id: "dup" })).rejects.toThrow(
      /still live \(status "waiting"\)/
    );

    expect(await jobs.stats()).toEqual({
      waiting: 1,
      scheduled: 0,
      active: 0,
      dead: 0
    });
    expect((await jobs.get("dup"))?.payload).toBe("one");
  });

  it("clears the previous generation when a finished id is re-enqueued", async () => {
    // The clean-slate block only touched the hash, so the new job kept the old
    // one's dead-letter entry and its terminal event: watch() replayed the old
    // failure and ended before the new generation had produced anything.
    const jobs = queue<string, string>(client, { prefix: nextPrefix() });
    const worker = jobs.worker(
      async () => {
        throw new TerminalJobError("model refused");
      },
      { pollMs: 5, onError: () => {} }
    );

    await jobs.enqueue("one", { id: "reused" });
    await expect(jobs.wait("reused")).rejects.toThrow(/model refused/);
    await worker.stop();
    expect(await jobs.dead()).toEqual(["reused"]);

    await jobs.enqueue("two", { id: "reused", delayMs: 60_000 });

    expect(await jobs.dead()).toEqual([]);
    expect(await jobs.stats()).toEqual({
      waiting: 0,
      scheduled: 1,
      active: 0,
      dead: 0
    });
    expect(
      await client.send(["XRANGE", jobs.eventsKey("reused"), "-", "+"])
    ).toEqual([]);
  });

  it("writes the restart marker above every id the previous attempt used", async () => {
    // The retry path deleted the output stream, which resets Redis's
    // last-generated id. A marker recreated in the same millisecond as earlier
    // output lands at or below the cursor a watcher holds, so the restart
    // boundary and the chunks after it are never delivered. The seeded entry
    // stands in for that clock collision deterministically.
    const seeded = "9999999999999-0";
    const jobs = queue<null, string>(client, { prefix: nextPrefix() });
    let attempts = 0;
    const worker = jobs.worker(
      async (job) => {
        attempts += 1;
        if (attempts === 1) {
          await job.emit("partial");
          await client.send([
            "XADD",
            jobs.eventsKey(job.id),
            seeded,
            "t",
            "chunk",
            "d",
            "late"
          ]);
          throw new RetryJobError("provider hiccup", 0);
        }
        return "done";
      },
      { pollMs: 5, onError: () => {} }
    );

    const { id } = await jobs.enqueue(null);
    // Polled rather than watched: a watcher holding the seeded cursor is
    // exactly what the bug starves, and it would hang instead of reporting.
    await waitFor(
      async () => (await jobs.get(id))?.status === "completed",
      "the second attempt to complete"
    );
    await worker.stop();

    const restarted = await client.send([
      "XRANGE",
      jobs.eventsKey(id),
      `(${seeded}`,
      "+"
    ]);
    expect(Array.isArray(restarted)).toBe(true);
    const types = (restarted as unknown[]).map((entry) => {
      const fields = (entry as [string, string[]])[1];
      return fields[1];
    });
    // Everything from the second generation, restart boundary included, is
    // strictly after the cursor the first generation left the watcher holding.
    expect(types).toEqual(["restarted", "completed"]);
  });

  it("settles cancelled when a cancel lands before the handler returns", async () => {
    // cancel() promises the caller no result is coming, but settle only checked
    // the lease token, so a handler finishing before the next heartbeat
    // recorded its result anyway.
    const jobs = queue<null, string>(client, { prefix: nextPrefix() });
    const running = gate();
    const finish = gate();
    const worker = jobs.worker(
      async () => {
        running.open();
        await finish.opened;
        return "expensive result";
      },
      { pollMs: 5, heartbeatMs: 60_000, onError: () => {} }
    );

    const { id } = await jobs.enqueue(null);
    await running.opened;
    expect(await jobs.cancel(id)).toBe(true);
    finish.open();

    await waitFor(
      async () => (await jobs.get(id))?.status !== "active",
      "the handler's outcome to be settled"
    );
    const record = await jobs.get(id);
    expect(record?.status).toBe("cancelled");
    expect(record?.result).toBeNull();
    await worker.stop();
  });

  it("settles cancelled instead of scheduling another paid attempt", async () => {
    // Same race on the retry path: a retryable throw after cancel() scheduled a
    // second generation of work the caller had already stopped.
    const jobs = queue<null, string>(client, {
      prefix: nextPrefix(),
      backoffMs: 10,
      maxBackoffMs: 10
    });
    const running = gate();
    const finish = gate();
    let runs = 0;
    const worker = jobs.worker(
      async () => {
        runs += 1;
        running.open();
        await finish.opened;
        throw new Error("provider is overloaded");
      },
      { pollMs: 5, heartbeatMs: 60_000, onError: () => {} }
    );

    const { id } = await jobs.enqueue(null);
    await running.opened;
    expect(await jobs.cancel(id)).toBe(true);
    finish.open();

    await waitFor(
      async () => (await jobs.get(id))?.status !== "active",
      "the failed attempt to be settled"
    );
    expect((await jobs.get(id))?.status).toBe("cancelled");

    await sleep(60);
    expect(runs).toBe(1);
    expect(await jobs.stats()).toEqual({
      waiting: 0,
      scheduled: 0,
      active: 0,
      dead: 0
    });
    await worker.stop();
  });

  it("holds an idempotency key for the whole run and expires it after the result", async () => {
    // The mapping carried a one-shot TTL from enqueue, so a run longer than
    // that TTL lost its key mid-flight and a duplicate request paid for a
    // second generation.
    const prefix = nextPrefix();
    const jobs = queue<null, string>(client, { prefix });
    const running = gate();
    const finish = gate();
    let runs = 0;
    const worker = jobs.worker(
      async () => {
        runs += 1;
        running.open();
        await finish.opened;
        return "ok";
      },
      { pollMs: 5, onError: () => {} }
    );

    const first = await jobs.enqueue(null, {
      idempotencyKey: "req-1",
      idempotencyTtlMs: 400
    });
    await running.opened;
    await sleep(600);

    const again = await jobs.enqueue(null, {
      idempotencyKey: "req-1",
      idempotencyTtlMs: 400
    });
    expect(again).toEqual({ id: first.id, deduplicated: true });

    finish.open();
    await expect(jobs.wait(first.id)).resolves.toBe("ok");
    expect(runs).toBe(1);

    // Retention starts at completion, so a late duplicate still gets the answer.
    const ttl = await client.send(["PTTL", `{${prefix}}:idem:req-1`]);
    expect(Number(ttl)).toBeGreaterThan(0);
    expect(Number(ttl)).toBeLessThanOrEqual(400);
    await worker.stop();
  });
});

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RedisClient } from "../src/core/index.js";
import { node } from "../src/node/index.js";
import {
  JobNotFoundError,
  queue,
  RetryJobError,
  TerminalJobError
} from "../src/primitives/index.js";

const redisUrl = process.env.BENI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describeRedis("queue (live)", () => {
  let client: RedisClient;
  const run = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  let namespace = 0;
  /** A fresh prefix per test, so nothing leaks between them. */
  const nextPrefix = () => `${run}:q${namespace++}`;

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("runs an enqueued job and resolves its result", async () => {
    const jobs = queue<{ prompt: string }, string>(client, {
      prefix: nextPrefix()
    });
    const worker = jobs.worker(async (job) => `echo:${job.payload.prompt}`);

    const { id, deduplicated } = await jobs.enqueue({ prompt: "hi" });
    expect(deduplicated).toBe(false);

    await expect(jobs.wait(id)).resolves.toBe("echo:hi");

    const record = await jobs.get(id);
    expect(record?.status).toBe("completed");
    expect(record?.attempt).toBe(1);
    expect(record?.result).toBe("echo:hi");
    expect(record?.finishedAt).toBeGreaterThan(0);

    await worker.stop();
  });

  it("streams chunks and ends the watch on the terminal event", async () => {
    const jobs = queue<null, string>(client, { prefix: nextPrefix() });
    const worker = jobs.worker(async (job) => {
      for (const token of ["Hel", "lo ", "world"]) {
        await job.emit(token);
      }
      return "Hello world";
    });

    const { id } = await jobs.enqueue(null);
    const seen: string[] = [];
    let completed: string | null = null;
    for await (const event of jobs.watch(id)) {
      if (event.type === "chunk") seen.push(event.data);
      if (event.type === "completed") completed = event.result;
    }

    expect(seen.join("")).toBe("Hello world");
    expect(completed).toBe("Hello world");
    await worker.stop();
  });

  it("resumes a watch from a cursor without replaying or dropping events", async () => {
    const jobs = queue<null, string>(client, { prefix: nextPrefix() });
    const worker = jobs.worker(async (job) => {
      for (const token of ["a", "b", "c"]) await job.emit(token);
      return "abc";
    });

    const { id } = await jobs.enqueue(null);

    // Full read first, remembering the entry id of the first chunk.
    const all: Array<{ id: string; data: string }> = [];
    for await (const event of jobs.watch(id)) {
      if (event.type === "chunk") all.push({ id: event.id, data: event.data });
    }
    expect(all.map((entry) => entry.data)).toEqual(["a", "b", "c"]);

    // Reconnecting client resumes after the chunk it already rendered.
    const resumed: string[] = [];
    for await (const event of jobs.watch(id, { after: all[0]?.id })) {
      if (event.type === "chunk") resumed.push(event.data);
    }
    expect(resumed).toEqual(["b", "c"]);

    await worker.stop();
  });

  it("collapses duplicate enqueues behind an idempotency key", async () => {
    const jobs = queue<{ n: number }, number>(client, { prefix: nextPrefix() });
    let runs = 0;
    const worker = jobs.worker(async (job) => {
      runs += 1;
      await sleep(50);
      return job.payload.n * 2;
    });

    const first = await jobs.enqueue({ n: 21 }, { idempotencyKey: "req-1" });
    const second = await jobs.enqueue({ n: 99 }, { idempotencyKey: "req-1" });

    expect(second.id).toBe(first.id);
    expect(second.deduplicated).toBe(true);
    await expect(jobs.wait(first.id)).resolves.toBe(42);
    expect(runs).toBe(1);

    // The key still points at the finished job, so a late retry gets the
    // answer instead of paying for a second generation.
    const third = await jobs.enqueue({ n: 7 }, { idempotencyKey: "req-1" });
    expect(third).toEqual({ id: first.id, deduplicated: true });
    expect(runs).toBe(1);

    await worker.stop();
  });

  it("runs higher priority jobs first", async () => {
    const jobs = queue<string, string>(client, { prefix: nextPrefix() });
    const order: string[] = [];

    // Enqueue before starting the worker so all four are waiting at once.
    await jobs.enqueue("low-1", { priority: 0 });
    await jobs.enqueue("low-2", { priority: 0 });
    await jobs.enqueue("high", { priority: 9 });
    await jobs.enqueue("mid", { priority: 5 });

    const worker = jobs.worker(async (job) => {
      order.push(job.payload);
      return job.payload;
    });
    while (order.length < 4) await sleep(20);
    await worker.stop();

    expect(order).toEqual(["high", "mid", "low-1", "low-2"]);
  });

  it("holds a delayed job until it comes due", async () => {
    const jobs = queue<string, string>(client, { prefix: nextPrefix() });
    const { id } = await jobs.enqueue("later", { delayMs: 400 });

    expect((await jobs.get(id))?.status).toBe("scheduled");
    expect(await jobs.stats()).toMatchObject({ waiting: 0, scheduled: 1 });

    const startedAt = Date.now();
    const worker = jobs.worker(async (job) => job.payload);
    await expect(jobs.wait(id)).resolves.toBe("later");
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(300);

    await worker.stop();
  });

  it("retries with backoff and then dead-letters", async () => {
    const jobs = queue<string, string>(client, {
      prefix: nextPrefix(),
      maxAttempts: 3,
      backoffMs: 10,
      maxBackoffMs: 20
    });
    const attempts: number[] = [];
    const worker = jobs.worker(
      async (job) => {
        attempts.push(job.attempt);
        throw new Error("provider is overloaded");
      },
      { onError: () => {} }
    );

    const { id } = await jobs.enqueue("flaky");
    await expect(jobs.wait(id)).rejects.toThrow("provider is overloaded");

    expect(attempts).toEqual([1, 2, 3]);
    const record = await jobs.get(id);
    expect(record?.status).toBe("failed");
    expect(record?.error).toContain("provider is overloaded");
    await expect(jobs.dead()).resolves.toContain(id);

    await worker.stop();
  });

  it("dead-letters a TerminalJobError without retrying", async () => {
    const jobs = queue<string, string>(client, {
      prefix: nextPrefix(),
      maxAttempts: 5,
      backoffMs: 10
    });
    let runs = 0;
    const worker = jobs.worker(
      async () => {
        runs += 1;
        throw new TerminalJobError("model refused the request");
      },
      { onError: () => {} }
    );

    const { id } = await jobs.enqueue("bad");
    await expect(jobs.wait(id)).rejects.toThrow("model refused the request");
    expect(runs).toBe(1);
    expect((await jobs.get(id))?.status).toBe("failed");

    await worker.stop();
  });

  it("honours the delay on a RetryJobError, then succeeds", async () => {
    const jobs = queue<string, string>(client, {
      prefix: nextPrefix(),
      maxAttempts: 3,
      backoffMs: 30_000 // would blow the test if the explicit delay is ignored
    });
    const worker = jobs.worker(
      async (job) => {
        if (job.attempt === 1) throw new RetryJobError("429 from provider", 50);
        return "second time lucky";
      },
      { onError: () => {} }
    );

    const { id } = await jobs.enqueue("rate-limited");
    await expect(jobs.wait(id)).resolves.toBe("second time lucky");
    expect((await jobs.get(id))?.attempt).toBe(2);

    await worker.stop();
  });

  it("restarts the output stream when an attempt is retried", async () => {
    const jobs = queue<null, string>(client, {
      prefix: nextPrefix(),
      maxAttempts: 2,
      backoffMs: 10,
      maxBackoffMs: 20
    });
    const worker = jobs.worker(
      async (job) => {
        if (job.attempt === 1) {
          await job.emit("par");
          await job.emit("tial");
          throw new Error("connection reset mid-generation");
        }
        await job.emit("com");
        await job.emit("plete");
        return "complete";
      },
      { onError: () => {} }
    );

    const { id } = await jobs.enqueue(null);

    // A client that watched from the start must be told to discard the failed
    // attempt's tokens rather than concatenating both generations.
    const rendered: string[] = [];
    let restarts = 0;
    for await (const event of jobs.watch(id)) {
      if (event.type === "restarted") {
        restarts += 1;
        rendered.length = 0;
      }
      if (event.type === "chunk") rendered.push(event.data);
    }

    expect(restarts).toBe(1);
    expect(rendered.join("")).toBe("complete");
    await worker.stop();
  }, 10_000);

  it("requeues a dead-lettered job on retryDead", async () => {
    const jobs = queue<string, string>(client, {
      prefix: nextPrefix(),
      maxAttempts: 1,
      backoffMs: 10
    });
    let shouldFail = true;
    const worker = jobs.worker(
      async (job) => {
        if (shouldFail) throw new Error("nope");
        return `ok:${job.payload}`;
      },
      { onError: () => {} }
    );

    const { id } = await jobs.enqueue("revive");
    await expect(jobs.wait(id)).rejects.toThrow("nope");

    shouldFail = false;
    await expect(jobs.retryDead(id)).resolves.toBe(true);
    await expect(jobs.wait(id)).resolves.toBe("ok:revive");
    await expect(jobs.dead()).resolves.not.toContain(id);
    // A job that is not dead-lettered cannot be revived.
    await expect(jobs.retryDead(id)).resolves.toBe(false);

    await worker.stop();
  });

  it("cancels a waiting job before any worker sees it", async () => {
    const jobs = queue<string, string>(client, { prefix: nextPrefix() });
    const { id } = await jobs.enqueue("never-runs");

    await expect(jobs.cancel(id)).resolves.toBe(true);
    expect((await jobs.get(id))?.status).toBe("cancelled");
    // Already terminal — a second cancel reports that it changed nothing.
    await expect(jobs.cancel(id)).resolves.toBe(false);

    let ran = false;
    const worker = jobs.worker(async () => {
      ran = true;
      return "should not happen";
    });
    await sleep(200);
    await worker.stop();
    expect(ran).toBe(false);
  });

  it("aborts a running job's signal on cancel and settles it cancelled", async () => {
    const jobs = queue<string, string>(client, { prefix: nextPrefix() });
    let aborted = false;
    let started = false;

    const worker = jobs.worker(
      async (job) => {
        started = true;
        // A model call would be `fetch(url, { signal: job.signal })`; wait on
        // the signal directly so the test does not depend on a provider.
        await new Promise<void>((resolve) => {
          job.signal.addEventListener("abort", () => {
            aborted = true;
            resolve();
          });
        });
        throw new Error("aborted");
      },
      { heartbeatMs: 50, onError: () => {} }
    );

    const { id } = await jobs.enqueue("long-generation");
    while (!started) await sleep(10);

    await expect(jobs.cancel(id)).resolves.toBe(true);
    await expect(jobs.wait(id)).rejects.toThrow("was cancelled");

    expect(aborted).toBe(true);
    expect((await jobs.get(id))?.status).toBe("cancelled");
    await worker.stop();
  });

  it("settles a cancelled job on reclaim instead of running it again", async () => {
    // cancel() on an *active* job only flags the record and leaves the owning
    // worker to abort its own signal. If that worker then dies, the reclaim
    // path saw an ordinary stalled job and pushed it back to ready, starting a
    // fresh, paid-for generation of work the caller had already stopped.
    const prefix = nextPrefix();
    const stalling = queue<string, string>(client, { prefix, leaseMs: 300 });
    const rescuing = queue<string, string>(client, { prefix, leaseMs: 30_000 });

    let firstRuns = 0;
    let secondRuns = 0;

    const dying = stalling.worker(
      async () => {
        firstRuns += 1;
        await sleep(3_000); // never finishes within the test
        return "never";
      },
      { heartbeatMs: 60_000, onError: () => {} }
    );

    const { id } = await stalling.enqueue("stop-me");
    while (firstRuns === 0) await sleep(10);

    // Cancel while it is active: returns true, flags the record, leaves the
    // job in leases for the (now doomed) worker to notice.
    await expect(stalling.cancel(id)).resolves.toBe(true);

    // Let the lease lapse, then bring up a second worker to do the reclaiming.
    await sleep(400);
    const rescuer = rescuing.worker(async () => {
      secondRuns += 1;
      return "should never run";
    });
    await sleep(600);

    expect(secondRuns).toBe(0);
    const record = await rescuing.get(id);
    expect(record?.status).toBe("cancelled");
    expect(record?.finishedAt).toBeGreaterThan(0);
    await expect(rescuing.stats()).resolves.toMatchObject({ dead: 0 });

    await rescuer.stop();
    await dying.stop();
  });

  it("reclaims a job whose worker stopped heartbeating", async () => {
    const prefix = nextPrefix();
    // A worker that cannot heartbeat in time: the lease is far shorter than the
    // heartbeat interval, so its lease lapses mid-run the way a crash would.
    const stalling = queue<string, string>(client, { prefix, leaseMs: 300 });
    const healthy = queue<string, string>(client, { prefix, leaseMs: 30_000 });

    let stalledRuns = 0;
    let healthyRuns = 0;

    const stalled = stalling.worker(
      async () => {
        stalledRuns += 1;
        await sleep(1_200);
        return "stalled result";
      },
      { heartbeatMs: 60_000, onError: () => {} }
    );

    const { id } = await stalling.enqueue("recover-me");
    while (stalledRuns === 0) await sleep(10);

    // Second worker joins after the first lease has lapsed and takes over.
    await sleep(400);
    const rescuer = healthy.worker(async () => {
      healthyRuns += 1;
      return "rescued";
    });

    await expect(healthy.wait(id)).resolves.toBe("rescued");
    expect(healthyRuns).toBe(1);
    expect((await healthy.get(id))?.attempt).toBe(2);

    await rescuer.stop();
    // The original worker eventually finishes, but its token is stale: the
    // rescuer's result stands and the late settle is discarded.
    await stalled.stop();
    const final = await healthy.get(id);
    expect(final?.status).toBe("completed");
    expect(final?.result).toBe("rescued");
  }, 15_000);

  it("processes a backlog concurrently and reports stats", async () => {
    const jobs = queue<number, number>(client, { prefix: nextPrefix() });
    const ids: string[] = [];
    for (let index = 0; index < 12; index += 1) {
      ids.push((await jobs.enqueue(index)).id);
    }
    expect(await jobs.stats()).toMatchObject({ waiting: 12, active: 0 });

    let peak = 0;
    let live = 0;
    const worker = jobs.worker(
      async (job) => {
        live += 1;
        peak = Math.max(peak, live);
        await sleep(60);
        live -= 1;
        return job.payload * 2;
      },
      { concurrency: 4 }
    );

    const results = await Promise.all(ids.map((id) => jobs.wait(id)));
    await worker.stop();

    expect(results).toEqual([0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]);
    expect(peak).toBeGreaterThan(1);
    expect(peak).toBeLessThanOrEqual(4);
    expect(await jobs.stats()).toMatchObject({
      waiting: 0,
      active: 0,
      dead: 0
    });
  });

  it("reports progress on the record and the stream", async () => {
    const jobs = queue<null, string>(client, { prefix: nextPrefix() });
    const worker = jobs.worker(async (job) => {
      await job.progress(0.5);
      await sleep(30);
      return "done";
    });

    const { id } = await jobs.enqueue(null);
    const fractions: number[] = [];
    for await (const event of jobs.watch(id)) {
      if (event.type === "progress") fractions.push(event.progress);
    }

    expect(fractions).toEqual([0.5]);
    await worker.stop();
  });

  it("throws JobNotFoundError for an unknown id", async () => {
    const jobs = queue<string, string>(client, { prefix: nextPrefix() });
    await expect(jobs.get("missing")).resolves.toBeNull();
    await expect(jobs.wait("missing")).rejects.toBeInstanceOf(JobNotFoundError);
    await expect(jobs.cancel("missing")).resolves.toBe(false);
  });

  it("re-enqueuing a finished id starts from a clean record", async () => {
    // enqueue's HSET only overwrote the fields it names, so a re-used id
    // inherited the dead job's cancelRequested flag and its result TTL: a
    // worker aborted brand-new work on its first heartbeat and threw the
    // result away, and the record could expire while the id was still queued.
    const prefix = nextPrefix();
    const jobs = queue<string, string>(client, { prefix, resultTtlMs: 1_000 });

    const first = await jobs.enqueue("one", { id: "job-1" });
    expect(await jobs.cancel(first.id)).toBe(true);

    await jobs.enqueue("two", { id: "job-1" });
    const fresh = await jobs.get("job-1");
    expect(fresh?.status).toBe("waiting");
    expect(fresh?.cancelRequested).toBe(false);
    expect(fresh?.result).toBeNull();
    expect(fresh?.finishedAt).toBeNull();

    // No inherited expiry: the record outlives the queue entry.
    const ttl = await client.send(["PTTL", `{${prefix}}:job:job-1`]);
    expect(ttl).toBe(-1);

    // And it actually runs to completion rather than being cancelled.
    const worker = jobs.worker(async (job) => `ran:${job.payload}`);
    await expect(jobs.wait("job-1")).resolves.toBe("ran:two");
    await worker.stop();
  });

  it("completes a void handler once instead of retrying it to the dead letter", async () => {
    // A fire-and-forget handler returns undefined, which the default JSON
    // codec refuses to encode. The encode used to sit on the success path, so
    // the throw was classified as a job failure: the side effect ran
    // maxAttempts times and the job still dead-lettered.
    const prefix = nextPrefix();
    const jobs = queue<{ to: string }, void>(client, { prefix });
    let sends = 0;
    const worker = jobs.worker(async () => {
      sends += 1;
    });

    const { id } = await jobs.enqueue({ to: "ada@example.com" });
    await jobs.wait(id);

    const record = await jobs.get(id);
    expect(record?.status).toBe("completed");
    expect(record?.attempt).toBe(1);
    expect(record?.result).toBeNull();
    expect(sends).toBe(1);
    await expect(jobs.stats()).resolves.toMatchObject({ dead: 0 });

    await worker.stop();
  });

  it("settles an undecodable payload instead of leaking a lease-renewing timer", async () => {
    // The payload decode used to run while building the job context, outside
    // the try/finally that clears the heartbeat. A throw escaped with the
    // interval already started, so a zombie timer renewed the lease forever:
    // the job stayed active and was never reclaimed, retried, or dead-lettered.
    const prefix = nextPrefix();
    const jobs = queue<{ n: number }, string>(client, { prefix });
    const { id } = await jobs.enqueue({ n: 1 });

    // Corrupt the stored payload the way an older producer or a codec change
    // would, then let a worker pick it up.
    await client.send(["HSET", `{${prefix}}:job:${id}`, "payload", "not-json"]);

    const errors: unknown[] = [];
    let handlerRuns = 0;
    const worker = jobs.worker(
      async () => {
        handlerRuns += 1;
        return "never";
      },
      { heartbeatMs: 100, leaseMs: 1000, onError: (e) => errors.push(e) }
    );

    await sleep(400);
    await worker.stop();

    // Read the record raw: get() decodes the payload too, so it cannot report
    // on a job whose payload is the thing that is broken.
    const status = await client.send([
      "HGET",
      `{${prefix}}:job:${id}`,
      "status"
    ]);
    expect(status).toBe("failed");
    expect(handlerRuns).toBe(0);
    expect(errors).not.toHaveLength(0);

    // The lease is gone, so nothing is still renewing it.
    const leaseScore = await client.send(["ZSCORE", `{${prefix}}:leases`, id]);
    expect(leaseScore).toBeNull();
  });
  it("trims dead-letter entries once their records have expired", async () => {
    // The job record expires after resultTtlMs but its entry in the dead ZSET
    // did not, so the set grew for the life of the deployment and dead()
    // returned ids whose records were long gone.
    const prefix = nextPrefix();
    const jobs = queue<string, string>(client, {
      prefix,
      maxAttempts: 1,
      resultTtlMs: 400
    });
    const worker = jobs.worker(
      async () => {
        throw new Error("always fails");
      },
      { onError: () => {} }
    );

    const first = await jobs.enqueue("one");
    await expect(jobs.wait(first.id)).rejects.toThrow("always fails");
    await expect(jobs.dead()).resolves.toContain(first.id);

    // Past the first job's resultTtlMs, so its record is gone. The next
    // dead-letter settle is what trims the stale entry.
    await sleep(600);
    const second = await jobs.enqueue("two");
    await expect(jobs.wait(second.id)).rejects.toThrow("always fails");

    const dead = await jobs.dead();
    expect(dead).toContain(second.id);
    expect(dead).not.toContain(first.id);

    await worker.stop();
  });
});

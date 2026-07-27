import { describe, expect, it } from "vitest";
import { ReplyShapeError, ValidationError } from "../src/core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import {
  JobLeaseLostError,
  JobNotFoundError,
  queue,
  RetryJobError,
  TerminalJobError
} from "../src/primitives/index.js";
import { fakeClient } from "./fake-client.js";

/** The reply `SCRIPT LOAD` gets, followed by whatever `EVALSHA` should return. */
function scripted(...replies: RedisReply[]): RedisReply[] {
  return ["sha", ...replies];
}

function evalsha(commands: RedisCommand[]): RedisCommand {
  const found = commands.find((command) => command[0] === "EVALSHA");
  if (!found) throw new Error("no EVALSHA was sent");
  return found;
}

describe("queue: enqueue", () => {
  it("sends one hash-tagged EVALSHA and returns the new job id", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue<{ prompt: string }>(
      fakeClient(commands, scripted(["job-1", 0])),
      { prefix: "gen" }
    );

    await expect(jobs.enqueue({ prompt: "hi" })).resolves.toEqual({
      id: "job-1",
      deduplicated: false
    });

    const [load] = commands;
    expect(load?.[0]).toBe("SCRIPT");
    const call = evalsha(commands);
    // 4 keys, all sharing one Cluster hash tag.
    expect(call[2]).toBe(4);
    expect(call.slice(3, 7)).toEqual([
      "{gen}:ready",
      "{gen}:scheduled",
      "{gen}:seq",
      "{gen}:signal"
    ]);
    expect(call[7]).toBe("{gen}"); // ARGV[1], the base the script derives from
    expect(call[9]).toBe('{"prompt":"hi"}');
  });

  it("reports a deduplicated enqueue", async () => {
    const jobs = queue(fakeClient([], scripted(["existing", 1])));
    await expect(
      jobs.enqueue({ n: 1 }, { idempotencyKey: "req-1" })
    ).resolves.toEqual({ id: "existing", deduplicated: true });
  });

  it("passes an explicit id, delay, and priority through", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue(fakeClient(commands, scripted(["mine", 0])), {
      prefix: "q"
    });

    await jobs.enqueue(
      { n: 1 },
      { id: "mine", delayMs: 5_000, priority: 7, maxAttempts: 9 }
    );
    const call = evalsha(commands);
    expect(call[8]).toBe("mine");
    expect(call[11]).toBe("5000"); // delayMs
    expect(call[12]).toBe("7"); // priority
    expect(call[13]).toBe("9"); // maxAttempts
  });

  it("rejects bad input before sending anything", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue(fakeClient(commands, []));

    await expect(jobs.enqueue({}, { priority: 10 })).rejects.toBeInstanceOf(
      ValidationError
    );
    await expect(jobs.enqueue({}, { priority: -1 })).rejects.toThrow(
      /between 0 and 9/
    );
    await expect(jobs.enqueue({}, { delayMs: -1 })).rejects.toThrow(
      /non-negative/
    );
    await expect(jobs.enqueue({}, { id: "" })).rejects.toThrow(
      /must not be empty/
    );
    expect(commands).toHaveLength(0);
  });

  it("rejects bad queue options at construction", () => {
    const client = fakeClient([], []);
    expect(() => queue(client, { leaseMs: 0 })).toThrow(ValidationError);
    expect(() => queue(client, { maxAttempts: -1 })).toThrow(/maxAttempts/);
    expect(() => queue(client, { resultTtlMs: 1.5 })).toThrow(/resultTtlMs/);
    expect(() => queue(client, { eventsMaxLen: 0 })).toThrow(/eventsMaxLen/);
  });
});

describe("queue: get", () => {
  const record = [
    "id",
    "job-1",
    "status",
    "completed",
    "payload",
    '{"prompt":"hi"}',
    "attempt",
    "2",
    "maxAttempts",
    "3",
    "priority",
    "5",
    "createdAt",
    "1700000000000",
    "updatedAt",
    "1700000001000",
    "startedAt",
    "1700000000500",
    "finishedAt",
    "1700000001000",
    "result",
    '"done"',
    "progress",
    "1",
    "idempotencyKey",
    "req-1",
    "cancelRequested",
    "1"
  ];

  it("decodes a full record", async () => {
    const jobs = queue<{ prompt: string }, string>(fakeClient([], [record]), {
      prefix: "q"
    });
    const job = await jobs.get("job-1");

    expect(job).toEqual({
      id: "job-1",
      status: "completed",
      payload: { prompt: "hi" },
      attempt: 2,
      maxAttempts: 3,
      priority: 5,
      createdAt: 1_700_000_000_000,
      updatedAt: 1_700_000_001_000,
      startedAt: 1_700_000_000_500,
      finishedAt: 1_700_000_001_000,
      result: "done",
      error: null,
      progress: 1,
      idempotencyKey: "req-1",
      cancelRequested: true
    });
  });

  it("decodes a RESP3 map reply the same way", async () => {
    const map = new Map<RedisReply, RedisReply>();
    for (let index = 0; index < record.length; index += 2) {
      map.set(record[index] as string, record[index + 1] as string);
    }
    const jobs = queue<{ prompt: string }, string>(fakeClient([], [map]));
    expect((await jobs.get("job-1"))?.status).toBe("completed");
  });

  it("returns null for a missing job", async () => {
    const jobs = queue(fakeClient([], [[]]));
    await expect(jobs.get("nope")).resolves.toBeNull();
  });

  it("leaves unset optional fields null", async () => {
    const jobs = queue(
      fakeClient([], [["id", "job-2", "status", "waiting", "payload", "{}"]])
    );
    const job = await jobs.get("job-2");
    expect(job).toMatchObject({
      startedAt: null,
      finishedAt: null,
      result: null,
      error: null,
      idempotencyKey: null,
      cancelRequested: false
    });
  });

  it("throws a ReplyShapeError on a reply that is not a hash", async () => {
    const jobs = queue(fakeClient([], [42]));
    await expect(jobs.get("job-1")).rejects.toBeInstanceOf(ReplyShapeError);
  });
});

describe("queue: cancel", () => {
  it.each([
    [1, true, "cancelled outright"],
    [3, true, "flagged for the running worker"],
    [0, false, "unknown id"],
    [2, false, "already terminal"]
  ])("maps script outcome %i to %s (%s)", async (outcome, expected) => {
    const jobs = queue(fakeClient([], scripted(outcome)));
    await expect(jobs.cancel("job-1")).resolves.toBe(expected);
  });
});

describe("queue: introspection", () => {
  it("reads all four depths in one pipeline", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue(fakeClient(commands, [3, 1, 2, 0]), { prefix: "q" });

    await expect(jobs.stats()).resolves.toEqual({
      waiting: 3,
      scheduled: 1,
      active: 2,
      dead: 0
    });
    expect(commands.map((command) => command[1])).toEqual([
      "{q}:ready",
      "{q}:scheduled",
      "{q}:leases",
      "{q}:dead"
    ]);
  });

  it("lists dead-lettered ids oldest first", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue(fakeClient(commands, [["a", "b"]]), { prefix: "q" });

    await expect(jobs.dead({ count: 2 })).resolves.toEqual(["a", "b"]);
    expect(commands[0]).toEqual(["ZRANGE", "{q}:dead", 0, 1]);
  });

  it("rejects a non-positive dead() count", async () => {
    const jobs = queue(fakeClient([], []));
    await expect(jobs.dead({ count: 0 })).rejects.toBeInstanceOf(
      ValidationError
    );
  });

  it("maps retryDead outcomes", async () => {
    await expect(
      queue(fakeClient([], scripted(1))).retryDead("job-1")
    ).resolves.toBe(true);
    await expect(
      queue(fakeClient([], scripted(0))).retryDead("job-1")
    ).resolves.toBe(false);
  });

  it("exposes the key layout it uses", () => {
    const jobs = queue(fakeClient([], []), { prefix: "gen" });
    expect(jobs.jobKey("abc")).toBe("{gen}:job:abc");
    expect(jobs.eventsKey("abc")).toBe("{gen}:events:abc");
  });
});

describe("queue: watch", () => {
  /** fakeClient exposes no session(), so watch takes the polling path. */
  function entry(id: string, type: string, data: string) {
    return [id, ["t", type, "d", data]];
  }

  it("yields the backlog and stops at the terminal event", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue<null, string>(
      fakeClient(commands, [
        [
          entry("1-0", "chunk", "Hel"),
          entry("2-0", "chunk", "lo"),
          entry("3-0", "completed", '"Hello"')
        ]
      ]),
      { prefix: "q" }
    );

    const seen = [];
    for await (const event of jobs.watch("job-1")) seen.push(event);

    expect(seen).toEqual([
      { id: "1-0", type: "chunk", data: "Hel" },
      { id: "2-0", type: "chunk", data: "lo" },
      { id: "3-0", type: "completed", result: "Hello" }
    ]);
    // A full backlog read never needs to tail.
    expect(commands).toHaveLength(1);
    expect(commands[0]).toEqual(["XRANGE", "{q}:events:job-1", "-", "+"]);
  });

  it("resumes strictly after the given cursor", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue<null, string>(
      fakeClient(commands, [[entry("9-0", "cancelled", "")]])
    );

    const seen = [];
    for await (const event of jobs.watch("job-1", { after: "4-0" })) {
      seen.push(event);
    }
    expect(seen).toEqual([{ id: "9-0", type: "cancelled" }]);
    expect(commands[0]?.[2]).toBe("(4-0"); // exclusive range
  });

  it("decodes progress and restart events, ignoring unknown kinds", async () => {
    const jobs = queue<null, string>(
      fakeClient(
        [],
        [
          [
            entry("1-0", "progress", "0.25"),
            entry("2-0", "restarted", "2"),
            entry("3-0", "somethingNewer", "x"),
            entry("4-0", "failed", "boom")
          ]
        ]
      )
    );

    const seen = [];
    for await (const event of jobs.watch("job-1")) seen.push(event);
    expect(seen).toEqual([
      { id: "1-0", type: "progress", progress: 0.25 },
      { id: "2-0", type: "restarted", attempt: 2 },
      { id: "4-0", type: "failed", error: "boom" }
    ]);
  });

  it("polls when no events have arrived yet, then yields them", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue<null, string>(
      fakeClient(commands, [
        [], // XRANGE backlog: empty
        [], // XREAD poll: nothing
        1, // EXISTS: the job is real, keep waiting
        [["{q}:events:job-1", [entry("5-0", "completed", '"ok"')]]]
      ]),
      { prefix: "q" }
    );

    const seen = [];
    for await (const event of jobs.watch("job-1", { pollMs: 1 })) {
      seen.push(event);
    }
    expect(seen).toEqual([{ id: "5-0", type: "completed", result: "ok" }]);
    expect(commands.map((command) => command[0])).toEqual([
      "XRANGE",
      "XREAD",
      "EXISTS",
      "XREAD"
    ]);
  });

  it("throws JobNotFoundError when the record is gone and nothing streamed", async () => {
    const jobs = queue<null, string>(fakeClient([], [[], [], 0]));
    await expect(async () => {
      for await (const _event of jobs.watch("ghost", { pollMs: 1 })) {
        // the first poll should throw instead of yielding
      }
    }).rejects.toBeInstanceOf(JobNotFoundError);
  });

  it("stops when the caller's signal is already aborted", async () => {
    const jobs = queue<null, string>(fakeClient([], [[]]));
    const seen = [];
    for await (const event of jobs.watch("job-1", {
      signal: AbortSignal.abort()
    })) {
      seen.push(event);
    }
    expect(seen).toEqual([]);
  });
});

describe("queue: wait", () => {
  const settled = (status: string, extra: string[]) => [
    "id",
    "job-1",
    "status",
    status,
    "payload",
    "{}",
    ...extra
  ];

  it("returns a result already on the record without tailing", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue<null, string>(
      fakeClient(commands, [[], settled("completed", ["result", '"done"'])])
    );

    await expect(jobs.wait("job-1")).resolves.toBe("done");
    expect(commands.map((command) => command[0])).toEqual([
      "XREVRANGE",
      "HGETALL"
    ]);
  });

  it("throws the recorded error for a failed job", async () => {
    const jobs = queue<null, string>(
      fakeClient([], [[], settled("failed", ["error", "provider exploded"])])
    );
    await expect(jobs.wait("job-1")).rejects.toThrow("provider exploded");
  });

  it("throws for a cancelled job", async () => {
    const jobs = queue<null, string>(
      fakeClient([], [[], settled("cancelled", [])])
    );
    await expect(jobs.wait("job-1")).rejects.toThrow("was cancelled");
  });

  it("throws JobNotFoundError for an unknown id", async () => {
    const jobs = queue<null, string>(fakeClient([], [[], []]));
    await expect(jobs.wait("ghost")).rejects.toBeInstanceOf(JobNotFoundError);
  });

  it("tails from the newest existing entry when still running", async () => {
    const commands: RedisCommand[] = [];
    const jobs = queue<null, string>(
      fakeClient(commands, [
        [["7-0", ["t", "chunk", "d", "partial"]]], // XREVRANGE: newest so far
        settled("active", []),
        [["8-0", ["t", "completed", "d", '"finished"']]] // XRANGE backlog
      ])
    );

    await expect(jobs.wait("job-1")).resolves.toBe("finished");
    // The tail starts strictly after the entry that already existed, so the
    // chunk it saw is not replayed and a terminal event cannot be missed.
    expect(commands[2]?.[2]).toBe("(7-0");
  });
});

describe("queue: error classes", () => {
  it("TerminalJobError marks a job unretryable", () => {
    const error = new TerminalJobError("refused");
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("TerminalJobError");
  });

  it("RetryJobError carries a non-negative delay", () => {
    expect(new RetryJobError("429", 2_500).retryAfterMs).toBe(2_500);
    expect(new RetryJobError("429", -1).retryAfterMs).toBe(0);
  });

  it("JobNotFoundError and JobLeaseLostError name the job", () => {
    expect(new JobNotFoundError("abc").jobId).toBe("abc");
    expect(new JobLeaseLostError("abc").jobId).toBe("abc");
    expect(new JobLeaseLostError("abc").message).toContain("leaseMs");
  });
});

/**
 * A client that dispatches EVALSHA by the `-- @script <name>` marker each Lua
 * body carries, so a worker loop can be driven without a live Redis. Anything
 * the script would have written is recorded as a call instead.
 */
type ScriptCall = { script: string; args: string[] };

function routedClient(
  handlers: Partial<Record<string, (args: string[]) => RedisReply>>,
  calls: ScriptCall[] = []
): RedisClient & { calls: ScriptCall[] } {
  const shas = new Map<string, string>();
  let next = 0;

  function respond(command: RedisCommand): RedisReply {
    const [name] = command;
    if (name === "SCRIPT") {
      const lua = String(command[2]);
      const marker = /-- @script (\w+)/.exec(lua)?.[1];
      if (!marker) throw new Error("script body carries no @script marker");
      const sha = `sha-${next++}`;
      shas.set(sha, marker);
      return sha;
    }
    if (name === "EVALSHA") {
      const script = shas.get(String(command[1]));
      if (!script) throw new Error(`unknown sha ${String(command[1])}`);
      const keyCount = Number(command[2]);
      const args = command.slice(3 + keyCount).map(String);
      calls.push({ script, args });
      const handler = handlers[script];
      if (!handler) throw new Error(`no fake handler for script "${script}"`);
      return handler(args);
    }
    const handler = handlers[String(name)];
    if (handler) return handler(command.slice(1).map(String));
    return null;
  }

  return {
    calls,
    async send(command) {
      return respond(command);
    },
    async pipeline(commands) {
      return commands.map(respond);
    },
    async close() {}
  };
}

/** The reserve script's "here is a job" reply. */
function reserved(id: string, payload: string, attempt = 1, maxAttempts = 3) {
  return [
    1,
    id,
    payload,
    String(attempt),
    String(maxAttempts),
    "0",
    "1700000000000",
    "token-1"
  ];
}
/** The reserve script's "nothing to do" reply. */
const idle = [0, "-1"];

describe("queue: worker", () => {
  it("validates its options before starting a loop", () => {
    const jobs = queue(fakeClient([], []));
    const handler = async () => undefined;
    expect(() => jobs.worker(handler, { concurrency: 0 })).toThrow(
      ValidationError
    );
    expect(() => jobs.worker(handler, { heartbeatMs: -1 })).toThrow(
      /heartbeatMs/
    );
    expect(() => jobs.worker(handler, { pollMs: 0 })).toThrow(/pollMs/);
  });

  it("stops cleanly when the queue is empty", async () => {
    const client = routedClient({ reserve: () => idle });
    const worker = queue(client).worker(async () => undefined, {
      pollMs: 1,
      onError: () => {}
    });
    expect(worker.active).toBe(0);
    await worker.stop();
    expect(client.calls.every((call) => call.script === "reserve")).toBe(true);
  });

  it("runs a job and settles it completed with the encoded result", async () => {
    let served = false;
    const client = routedClient({
      reserve: () => {
        if (served) return idle;
        served = true;
        return reserved("job-1", '{"prompt":"hi"}');
      },
      settle: () => 1
    });

    const seen: Array<{ id: string; attempt: number; prompt: string }> = [];
    const worker = queue<{ prompt: string }, string>(client).worker(
      async (job) => {
        seen.push({
          id: job.id,
          attempt: job.attempt,
          prompt: job.payload.prompt
        });
        return `echo:${job.payload.prompt}`;
      },
      { pollMs: 1, onError: () => {} }
    );

    await waitFor(() => client.calls.some((call) => call.script === "settle"));
    await worker.stop();

    expect(seen).toEqual([{ id: "job-1", attempt: 1, prompt: "hi" }]);
    const settle = client.calls.find((call) => call.script === "settle");
    expect(settle?.args[4]).toBe("completed");
    expect(settle?.args[5]).toBe('"echo:hi"');
  });

  it("emits chunks through the touch script and returns the entry id", async () => {
    let served = false;
    const client = routedClient({
      reserve: () => {
        if (served) return idle;
        served = true;
        return reserved("job-1", "null");
      },
      touch: () => [1, 0, "5-0"],
      settle: () => 1
    });

    let emittedId = "";
    const worker = queue<null, string>(client).worker(
      async (job) => {
        emittedId = await job.emit("tok");
        await job.progress(0.5);
        expect(await job.heartbeat()).toBe(true);
        return "done";
      },
      { pollMs: 1, onError: () => {} }
    );

    await waitFor(() => client.calls.some((call) => call.script === "settle"));
    await worker.stop();

    expect(emittedId).toBe("5-0");
    const touches = client.calls.filter((call) => call.script === "touch");
    // chunk, progress, bare heartbeat — the emit carries the lease renewal.
    expect(touches.map((call) => [call.args[5], call.args[6]])).toEqual([
      ["chunk", "tok"],
      ["progress", "0.5"],
      ["", ""]
    ]);
  });

  it("clamps a progress fraction into 0..1", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null") : idle),
      touch: () => [1, 0, "1-0"],
      settle: () => 1
    });

    const worker = queue<null, string>(client).worker(
      async (job) => {
        await job.progress(4);
        await job.progress(-2);
        return "done";
      },
      { pollMs: 1, onError: () => {} }
    );
    await waitFor(() => client.calls.some((call) => call.script === "settle"));
    await worker.stop();

    const fractions = client.calls
      .filter((call) => call.script === "touch")
      .map((call) => call.args[6]);
    expect(fractions).toEqual(["1", "0"]);
  });

  it("retries a thrown error with backoff instead of settling", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null") : idle),
      retry: () => 1
    });

    const worker = queue<null, string>(client, {
      backoffMs: 40,
      maxBackoffMs: 40
    }).worker(
      async () => {
        throw new Error("provider is overloaded");
      },
      { pollMs: 1, onError: () => {} }
    );

    await waitFor(() => client.calls.some((call) => call.script === "retry"));
    await worker.stop();

    const retry = client.calls.find((call) => call.script === "retry");
    expect(retry?.args[5]).toContain("provider is overloaded");
    // Full jitter over a 40ms ceiling: somewhere in [20, 40].
    const delay = Number(retry?.args[4]);
    expect(delay).toBeGreaterThanOrEqual(20);
    expect(delay).toBeLessThanOrEqual(40);
  });

  it("uses the explicit delay from a RetryJobError", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null") : idle),
      retry: () => 1
    });

    const worker = queue<null, string>(client, { backoffMs: 30_000 }).worker(
      async () => {
        throw new RetryJobError("429", 1_234);
      },
      { pollMs: 1, onError: () => {} }
    );
    await waitFor(() => client.calls.some((call) => call.script === "retry"));
    await worker.stop();

    expect(client.calls.find((call) => call.script === "retry")?.args[4]).toBe(
      "1234"
    );
  });

  it("dead-letters a TerminalJobError without consuming an attempt", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null") : idle),
      settle: () => 1
    });

    const worker = queue<null, string>(client).worker(
      async () => {
        throw new TerminalJobError("model refused");
      },
      { pollMs: 1, onError: () => {} }
    );
    await waitFor(() => client.calls.some((call) => call.script === "settle"));
    await worker.stop();

    const settle = client.calls.find((call) => call.script === "settle");
    expect(settle?.args[4]).toBe("failed");
    expect(settle?.args[5]).toContain("model refused");
    expect(client.calls.some((call) => call.script === "retry")).toBe(false);
  });

  it("settles failed rather than retrying on the last attempt", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null", 3, 3) : idle),
      settle: () => 1
    });

    const worker = queue<null, string>(client).worker(
      async () => {
        throw new Error("still broken");
      },
      { pollMs: 1, onError: () => {} }
    );
    await waitFor(() => client.calls.some((call) => call.script === "settle"));
    await worker.stop();

    expect(client.calls.find((call) => call.script === "settle")?.args[4]).toBe(
      "failed"
    );
  });

  it("honours a custom isRetryable predicate", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null") : idle),
      settle: () => 1
    });

    const worker = queue<null, string>(client).worker(
      async () => {
        throw new Error("nope");
      },
      { pollMs: 1, isRetryable: () => false, onError: () => {} }
    );
    await waitFor(() => client.calls.some((call) => call.script === "settle"));
    await worker.stop();

    expect(client.calls.some((call) => call.script === "retry")).toBe(false);
  });

  it("aborts the handler signal and settles cancelled when a touch reports cancellation", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null") : idle),
      touch: () => [1, 1, "1-0"], // held, but cancellation was requested
      settle: () => 1
    });

    let aborted = false;
    const worker = queue<null, string>(client).worker(
      async (job) => {
        await job.emit("tok");
        aborted = job.signal.aborted;
        return "result the user will never see";
      },
      { pollMs: 1, onError: () => {} }
    );
    await waitFor(() => client.calls.some((call) => call.script === "settle"));
    await worker.stop();

    expect(aborted).toBe(true);
    const settle = client.calls.find((call) => call.script === "settle");
    expect(settle?.args[4]).toBe("cancelled");
    // A cancelled job stores no result.
    expect(settle?.args[5]).toBe("");
  });

  it("stops working a job whose lease was lost, and does not settle it", async () => {
    let served = 0;
    const client = routedClient({
      reserve: () => (served++ === 0 ? reserved("job-1", "null") : idle),
      touch: () => [0, 0, ""], // the lease is gone
      settle: () => 1
    });

    let thrown: unknown;
    const worker = queue<null, string>(client).worker(
      async (job) => {
        try {
          await job.emit("tok");
        } catch (error) {
          thrown = error;
          throw error;
        }
        return "unreachable";
      },
      { pollMs: 1, onError: () => {} }
    );
    await waitFor(
      () => client.calls.filter((c) => c.script === "touch").length > 0
    );
    await worker.stop();

    expect(thrown).toBeInstanceOf(JobLeaseLostError);
    // Settling would stomp whichever worker now owns the job.
    expect(client.calls.some((call) => call.script === "settle")).toBe(false);
  });

  it("reports a reserve failure through onError and keeps running", async () => {
    let attempts = 0;
    const client = routedClient({
      reserve: () => {
        attempts += 1;
        if (attempts === 1) throw new Error("connection reset");
        return idle;
      }
    });

    const errors: unknown[] = [];
    const worker = queue(client).worker(async () => undefined, {
      pollMs: 1,
      onError: (error) => errors.push(error)
    });
    await waitFor(() => attempts >= 2);
    await worker.stop();

    expect(errors[0]).toBeInstanceOf(Error);
    expect((errors[0] as Error).message).toBe("connection reset");
  });

  it("never exceeds its concurrency", async () => {
    let served = 0;
    const total = 8;
    const client = routedClient({
      reserve: () =>
        served < total ? reserved(`job-${served++}`, "null") : idle,
      settle: () => 1
    });

    let live = 0;
    let peak = 0;
    const worker = queue<null, string>(client).worker(
      async () => {
        live += 1;
        peak = Math.max(peak, live);
        await new Promise((resolve) => setTimeout(resolve, 20));
        live -= 1;
        return "ok";
      },
      { concurrency: 3, pollMs: 1, onError: () => {} }
    );

    await waitFor(
      () => client.calls.filter((c) => c.script === "settle").length === total,
      2_000
    );
    await worker.stop();

    expect(peak).toBe(3);
    expect(worker.active).toBe(0);
  });
});

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 1_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("waitFor timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

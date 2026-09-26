import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi
} from "vitest";
import { ValidationError } from "../src/core/errors.js";
import type {
  RedisClient,
  RedisCommand,
  RedisReply
} from "../src/core/types.js";
import { node } from "../src/node/index.js";
import {
  IdempotencyFingerprintMismatchError,
  IdempotencyLeaseLostError,
  IdempotencyNotRecordedError,
  idempotency
} from "../src/primitives/index.js";

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const TOKEN = "00000000-0000-4000-8000-000000000000"; // 36 characters

/** Answers by command, so renewal counts need not be scripted in advance. */
function idemFake(behavior: {
  commands: RedisCommand[];
  claim?: () => RedisReply;
  get?: () => RedisReply;
  extend?: () => RedisReply;
  complete?: () => RedisReply;
}): RedisClient & { calls(kind: string): number } {
  const counts = new Map<string, number>();
  return {
    async send(command) {
      behavior.commands.push(command);
      // `?? "OK"` would turn a deliberate `null` (key taken) back into a win.
      if (command[0] === "SET") {
        return behavior.claim === undefined ? "OK" : behavior.claim();
      }
      if (command[0] === "GET") return behavior.get?.() ?? null;
      if (command[0] === "SCRIPT") {
        const lua = String(command[2]);
        return lua.includes("PEXPIRE")
          ? "sha-extend"
          : lua.includes("ARGV[2]")
            ? "sha-complete"
            : "sha-release";
      }
      if (command[0] === "EVALSHA") {
        const kind = String(command[1]).slice(4);
        counts.set(kind, (counts.get(kind) ?? 0) + 1);
        if (kind === "extend") return behavior.extend?.() ?? 1;
        if (kind === "complete") return behavior.complete?.() ?? 1;
        return 1;
      }
      throw new Error(`Unexpected command ${String(command[0])}`);
    },
    async pipeline() {
      return [];
    },
    async close() {},
    calls: (kind) => counts.get(kind) ?? 0
  };
}

describe("idempotency claim renewal", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("renews the running marker while a slow handler runs", async () => {
    // The marker was set once with PX runningTtlMs and never renewed, so a 35s
    // charge under the 30s default lost its claim at 30s: a retry then found
    // the key free and charged the card a second time.
    const commands: RedisCommand[] = [];
    const client = idemFake({ commands });
    const once = idempotency<string>(client);

    const charge = once.run("k", () => pause(35_000).then(() => "rcpt"));
    await vi.advanceTimersByTimeAsync(35_000);

    await expect(charge).resolves.toEqual({ value: "rcpt", replayed: false });
    // Every 7.5s, token-checked against our own marker.
    expect(client.calls("extend")).toBe(4);
    const marker = commands[0]?.[2];
    const renewal = commands.find((c) => c[1] === "sha-extend");
    expect(renewal?.slice(3)).toEqual(["idem:k", marker, "30000"]);
    expect(client.calls("complete")).toBe(1);
  });

  it("aborts the handler's signal and reports it when the claim is lost", async () => {
    const commands: RedisCommand[] = [];
    // Renewal finds another caller's marker in place of ours; the complete
    // script, checking the same thing, declines to write.
    const client = idemFake({ commands, extend: () => 0, complete: () => 0 });
    const once = idempotency<string>(client, { runningTtlMs: 1_000 });

    let reason: unknown;
    const charge = once
      .run("k", async ({ signal }) => {
        signal.addEventListener("abort", () => {
          reason = signal.reason;
        });
        await pause(2_000);
        return "rcpt";
      })
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(2_000);

    expect(reason).toBeInstanceOf(IdempotencyLeaseLostError);
    const error = (await charge) as IdempotencyNotRecordedError<string>;
    expect(error).toBeInstanceOf(IdempotencyNotRecordedError);
    expect(error.value).toBe("rcpt");
    expect(error.cause).toBeInstanceOf(IdempotencyLeaseLostError);
  });

  it("releases the marker when a handler stopped by the signal throws", async () => {
    const commands: RedisCommand[] = [];
    const client = idemFake({ commands, extend: () => 0 });
    const once = idempotency<string>(client, { runningTtlMs: 1_000 });

    const charge = once
      .run(
        "k",
        ({ signal }) =>
          new Promise<string>((_resolve, reject) => {
            signal.addEventListener("abort", () => reject(signal.reason));
          })
      )
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(250);

    // The handler's own error, the abort reason, propagates unchanged.
    await expect(charge).resolves.toBeInstanceOf(IdempotencyLeaseLostError);
    expect(client.calls("release")).toBe(1);
  });
});

describe("idempotency fingerprints", () => {
  it("records the fingerprint with the marker and with the result", async () => {
    const commands: RedisCommand[] = [];
    const once = idempotency<string>(idemFake({ commands }));

    await once.run("k", () => "rcpt", { fingerprint: "sha256:abc" });

    const marker = String(commands[0]?.[2]);
    expect(marker).toMatch(/^R.{36}sha256:abc$/);
    const complete = commands.find((c) => c[1] === "sha-complete");
    expect(complete?.slice(3)).toEqual([
      "idem:k",
      marker,
      'F10:sha256:abc"rcpt"',
      "86400000"
    ]);
  });

  it("refuses to replay a result recorded for a different request", async () => {
    const handler = vi.fn(() => "second");
    const once = idempotency<string>(
      idemFake({
        commands: [],
        claim: () => null,
        get: () => 'F5:body1"first"'
      })
    );

    await expect(
      once.run("k", handler, { fingerprint: "body2" })
    ).rejects.toBeInstanceOf(IdempotencyFingerprintMismatchError);
    expect(handler).not.toHaveBeenCalled();
  });

  it("refuses at once while a different request holds the key", async () => {
    // No point waiting for a result the fingerprint would reject anyway.
    const once = idempotency<string>(
      idemFake({
        commands: [],
        claim: () => null,
        get: () => `R${TOKEN}body1`
      })
    );

    await expect(
      once.run("k", () => "x", { fingerprint: "body2" })
    ).rejects.toBeInstanceOf(IdempotencyFingerprintMismatchError);
  });

  it("replays when the fingerprints match", async () => {
    const once = idempotency<string>(
      idemFake({
        commands: [],
        claim: () => null,
        get: () => 'F5:body1"first"'
      })
    );

    await expect(
      once.run("k", () => "second", { fingerprint: "body1" })
    ).resolves.toEqual({ value: "first", replayed: true });
    // peek reads the fingerprinted record too.
    await expect(once.peek("k")).resolves.toBe("first");
  });

  it("compares only when both calls supplied one", async () => {
    // A record written without a fingerprint, by this call or by code that
    // predates the option, is still replayed; so is one read without it.
    const plain = idempotency<string>(
      idemFake({ commands: [], claim: () => null, get: () => 'D"first"' })
    );
    await expect(
      plain.run("k", () => "x", { fingerprint: "body2" })
    ).resolves.toEqual({ value: "first", replayed: true });

    const fingerprinted = idempotency<string>(
      idemFake({
        commands: [],
        claim: () => null,
        get: () => 'F5:body1"first"'
      })
    );
    await expect(fingerprinted.run("k", () => "x")).resolves.toEqual({
      value: "first",
      replayed: true
    });
  });

  it("rejects an empty fingerprint before touching Redis", async () => {
    const commands: RedisCommand[] = [];
    const once = idempotency<string>(idemFake({ commands }));
    await expect(
      once.run("k", () => "x", { fingerprint: "" })
    ).rejects.toBeInstanceOf(ValidationError);
    expect(commands).toEqual([]);
  });
});

const redisUrl = process.env.BENNI_REDIS_URL ?? process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("idempotency (live)", () => {
  let client: RedisClient;
  const run = `idem:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  beforeAll(async () => {
    client = await node({ url: redisUrl });
  });
  afterAll(async () => {
    await client.close();
  });

  it("does not run a handler slower than runningTtlMs a second time", async () => {
    // The 35s charge under a 30s marker, scaled down. A retry arriving after
    // the marker's first TTL used to find the key free and run the handler
    // again; it now waits for the first result and replays it.
    const once = idempotency<string>(client, {
      prefix: run,
      runningTtlMs: 200,
      waitTimeoutMs: 5_000,
      pollMs: 20
    });
    let calls = 0;
    const charge = async () => {
      calls++;
      await pause(700);
      return `rcpt-${calls}`;
    };

    const first = once.run("slow", charge);
    await pause(350);
    const retry = once.run("slow", charge);

    await expect(first).resolves.toEqual({ value: "rcpt-1", replayed: false });
    await expect(retry).resolves.toEqual({ value: "rcpt-1", replayed: true });
    expect(calls).toBe(1);
  });

  it("reports a lost claim instead of recording over another caller's", async () => {
    // Renewals that cannot reach Redis are what a partition looks like from
    // the holder's side: its marker lapses, a second caller claims the key,
    // and the first must neither record over it nor report plain success.
    let extendSha: string | undefined;
    const partitioned: RedisClient = {
      ...client,
      async send(command) {
        if (command[0] === "EVALSHA" && command[1] === extendSha) {
          throw new Error("connection reset");
        }
        const reply = await client.send(command);
        if (command[0] === "SCRIPT" && String(command[2]).includes("PEXPIRE")) {
          extendSha = String(reply);
        }
        return reply;
      }
    };
    const options = { prefix: `${run}:lost`, runningTtlMs: 200, pollMs: 20 };
    const holder = idempotency<string>(partitioned, options);
    const other = idempotency<string>(client, options);

    let aborted = false;
    const first = holder
      .run("k", async ({ signal }) => {
        signal.addEventListener("abort", () => {
          aborted = true;
        });
        await pause(600);
        return "first";
      })
      .catch((error: unknown) => error);
    await pause(350);
    await expect(other.run("k", () => "second")).resolves.toEqual({
      value: "second",
      replayed: false
    });

    const error = await first;
    expect(aborted).toBe(true);
    expect(error).toBeInstanceOf(IdempotencyNotRecordedError);
    expect((error as Error).cause).toBeInstanceOf(IdempotencyLeaseLostError);
    // The second caller's record stands.
    await expect(other.peek("k")).resolves.toBe("second");
  });

  it("refuses a reused key with a different fingerprint", async () => {
    const once = idempotency<string>(client, { prefix: `${run}:fp` });
    await once.run("k", () => "first", { fingerprint: "body1" });

    await expect(
      once.run("k", () => "second", { fingerprint: "body2" })
    ).rejects.toBeInstanceOf(IdempotencyFingerprintMismatchError);
    await expect(
      once.run("k", () => "second", { fingerprint: "body1" })
    ).resolves.toEqual({ value: "first", replayed: true });
  });
});

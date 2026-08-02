#!/usr/bin/env node
import assert from "node:assert/strict";
import net from "node:net";
import { performance } from "node:perf_hooks";

if (process.argv.includes("--self-test")) {
  selfTest();
  process.exit(0);
}

const redisUrl = new URL(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const iterations = Number(process.env.BENCH_ITERATIONS ?? 10_000);
const pipelineSize = Number(process.env.BENCH_PIPELINE ?? 64);

if (!Number.isInteger(iterations) || iterations < 1) {
  throw new Error("BENCH_ITERATIONS must be a positive integer");
}

if (!Number.isInteger(pipelineSize) || pipelineSize < 1) {
  throw new Error("BENCH_PIPELINE must be a positive integer");
}

class RespClient {
  #buffer = Buffer.alloc(0);
  #pending = [];
  #socket;

  constructor(socket) {
    this.#socket = socket;
    socket.on("data", (chunk) => this.#onData(chunk));
    socket.on("error", (error) => this.#rejectAll(error));
    socket.on("close", () => this.#rejectAll(new Error("Redis socket closed")));
  }

  static connect(url) {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(
        {
          host: url.hostname,
          port: Number(url.port || 6379)
        },
        () => resolve(new RespClient(socket))
      );
      socket.once("error", reject);
    });
  }

  send(command) {
    return new Promise((resolve, reject) => {
      this.#pending.push({ resolve, reject });
      this.#socket.write(encode(command), (error) => {
        if (error) reject(error);
      });
    });
  }

  async pipeline(commands) {
    const replies = commands.map(
      () =>
        new Promise((resolve, reject) => {
          this.#pending.push({ resolve, reject });
        })
    );
    this.#socket.write(commands.map(encode).join(""));
    return Promise.all(replies);
  }

  close() {
    this.#socket.end();
  }

  #onData(chunk) {
    this.#buffer = Buffer.concat([this.#buffer, chunk]);

    while (this.#pending.length > 0) {
      const parsed = parse(this.#buffer);
      if (!parsed) return;

      this.#buffer = this.#buffer.subarray(parsed.offset);
      const { resolve, reject } = this.#pending.shift();
      if (parsed.value instanceof Error) reject(parsed.value);
      else resolve(parsed.value);
    }
  }

  #rejectAll(error) {
    for (const pending of this.#pending.splice(0)) pending.reject(error);
  }
}

function encode(command) {
  let out = `*${command.length}\r\n`;
  for (const arg of command) {
    const value =
      arg instanceof Uint8Array ? Buffer.from(arg) : Buffer.from(String(arg));
    out += `$${value.length}\r\n${value.toString()}\r\n`;
  }
  return out;
}

function parse(buffer, offset = 0) {
  if (offset >= buffer.length) return null;

  const type = buffer[offset];
  const lineEnd = buffer.indexOf("\r\n", offset);
  if (lineEnd === -1) return null;

  const line = buffer.toString("utf8", offset + 1, lineEnd);
  const next = lineEnd + 2;

  if (type === 43) return { value: line, offset: next };
  if (type === 45) return { value: new Error(line), offset: next };
  if (type === 58) return { value: Number(line), offset: next };

  if (type === 36) {
    const length = Number(line);
    if (length === -1) return { value: null, offset: next };
    const end = next + length;
    if (buffer.length < end + 2) return null;
    return { value: buffer.toString("utf8", next, end), offset: end + 2 };
  }

  if (type === 42) {
    const count = Number(line);
    if (count === -1) return { value: null, offset: next };

    const values = [];
    let cursor = next;
    for (let i = 0; i < count; i += 1) {
      const parsed = parse(buffer, cursor);
      if (!parsed) return null;
      values.push(parsed.value);
      cursor = parsed.offset;
    }
    return { value: values, offset: cursor };
  }

  throw new Error(`Unsupported RESP type: ${String.fromCharCode(type)}`);
}

async function measure(name, ops, fn) {
  const started = performance.now();
  await fn();
  const ms = performance.now() - started;
  const opsPerSecond = (ops / ms) * 1000;
  console.log(
    `${name.padEnd(18)} ${opsPerSecond.toFixed(0).padStart(10)} ops/s  ${(
      ms / ops
    ).toFixed(4)} ms/op`
  );
}

function selfTest() {
  assert.equal(encode(["PING"]), "*1\r\n$4\r\nPING\r\n");
  assert.deepEqual(parse(Buffer.from("+PONG\r\n")), {
    value: "PONG",
    offset: 7
  });
  assert.deepEqual(parse(Buffer.from("$3\r\nbar\r\n")), {
    value: "bar",
    offset: 9
  });
  assert.deepEqual(parse(Buffer.from("*2\r\n+OK\r\n:1\r\n")), {
    value: ["OK", 1],
    offset: 13
  });
  assert.equal(parse(Buffer.from("$3\r\nba")), null);
  console.log("bench self-test passed");
}

async function runSuite(name, client) {
  await client.send(["PING"]);

  console.log(`\n${name}`);
  await measure("PING", iterations, async () => {
    for (let i = 0; i < iterations; i += 1) await client.send(["PING"]);
  });

  await measure("SET", iterations, async () => {
    for (let i = 0; i < iterations; i += 1) {
      await client.send(["SET", "benni:bench", i]);
    }
  });

  await measure("GET", iterations, async () => {
    for (let i = 0; i < iterations; i += 1)
      await client.send(["GET", "benni:bench"]);
  });

  const batches = Math.ceil(iterations / pipelineSize);
  await measure("PING pipeline", batches * pipelineSize, async () => {
    for (let i = 0; i < batches; i += 1) {
      await client.pipeline(
        Array.from({ length: pipelineSize }, () => ["PING"])
      );
    }
  });
}

console.log(
  `redis=${redisUrl.host} iterations=${iterations} pipeline=${pipelineSize}`
);

const respClient = await RespClient.connect(redisUrl);
try {
  await runSuite("RESP baseline", respClient);
} finally {
  respClient.close();
}

const { node } = await import("../dist/node/index.mjs");
const benniClient = await node({ url: redisUrl.href });
try {
  await runSuite("Benni node adapter", benniClient);
} finally {
  await benniClient.close();
}

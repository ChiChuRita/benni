import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

// What an editor hover prints is part of the API: the README promises
// `{ name: string; score: number } | null`, and `Expect<Equal<…>>` cannot see
// the difference between that and `InferHashOutput<{ name: Codec<…> }>`, which
// are the same type. So ask the TypeScript language service, the thing an
// editor asks, for the quickinfo text.

const testDir = dirname(fileURLToPath(import.meta.url));
const sampleFile = join(testDir, "__hover-sample__.ts");
const sample = `
import { benni } from "../src/index.js";
import type { RedisClient } from "../src/core/types.js";
import type { InferInput } from "../src/schema.js";
import { hash, number, optional, stream, string } from "../src/schema.js";

declare const client: RedisClient;
const users = hash("user", { name: string(), score: number(), bio: optional(string()) });
const events = stream("event", { kind: string(), at: number() });
const redis = benni({ client, schema: { users, events } });

export async function probe() {
  const whole = await redis.query.users.hget("42");
  const all = await redis.query.users.hgetall("42");
  const picked = await redis.query.users.hmget("42", ["name", "bio"]);
  const entries = await redis.query.events.xrange("1");
  for await (const scanned of redis.scan.hash(users, "42")) void scanned;
  const input: InferInput<typeof users> = { name: "Ada", score: 1 };
  return { whole, all, picked, entries, input };
}
`;

function quickInfo(): Map<string, string> {
  const options: ts.CompilerOptions = {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    lib: ["lib.es2022.d.ts", "lib.esnext.disposable.d.ts"],
    strict: true,
    skipLibCheck: true,
    types: []
  };
  const host: ts.LanguageServiceHost = {
    getScriptFileNames: () => [sampleFile],
    getScriptVersion: () => "1",
    getScriptSnapshot: (file) => {
      if (file === sampleFile) return ts.ScriptSnapshot.fromString(sample);
      if (!ts.sys.fileExists(file)) return undefined;
      return ts.ScriptSnapshot.fromString(readFileSync(file, "utf8"));
    },
    getCurrentDirectory: () => testDir,
    getCompilationSettings: () => options,
    getDefaultLibFileName: (settings) => ts.getDefaultLibFilePath(settings),
    fileExists: (file) => file === sampleFile || ts.sys.fileExists(file),
    readFile: (file) => (file === sampleFile ? sample : ts.sys.readFile(file)),
    readDirectory: ts.sys.readDirectory,
    directoryExists: ts.sys.directoryExists,
    getDirectories: ts.sys.getDirectories
  };
  const service = ts.createLanguageService(host, ts.createDocumentRegistry());
  const errors = service
    .getSemanticDiagnostics(sampleFile)
    .map((diagnostic) =>
      ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")
    );
  expect(errors).toEqual([]);
  const hovers = new Map<string, string>();
  for (const match of sample.matchAll(/\bconst (\w+)\b/g)) {
    const position = (match.index ?? 0) + "const ".length;
    const info = service.getQuickInfoAtPosition(sampleFile, position);
    hovers.set(
      match[1],
      ts.displayPartsToString(info?.displayParts).replace(/\s+/g, " ")
    );
  }
  return hovers;
}

describe("editor hovers print flat value types", () => {
  it("shows hash and stream values as object literals, not the aliases that build them", () => {
    const hovers = quickInfo();
    expect(hovers.get("whole")).toBe(
      "const whole: { name: string; score: number; bio?: string | undefined; } | null"
    );
    expect(hovers.get("all")).toBe(
      "const all: { name?: string | undefined; score?: number | undefined; bio?: string | undefined; } | null"
    );
    expect(hovers.get("picked")).toBe(
      "const picked: { name?: string | undefined; bio?: string | undefined; }"
    );
    expect(hovers.get("entries")).toBe(
      "const entries: { id: string; value: { kind?: string | undefined; at?: number | undefined; }; }[]"
    );
    expect(hovers.get("scanned")).toBe(
      'const scanned: { readonly field: "name"; readonly value: string; } | { readonly field: "score"; readonly value: number; } | { readonly field: "bio"; readonly value: string; }'
    );
    expect(hovers.get("input")).toBe(
      "const input: { name: string; score: number; bio?: string | undefined; }"
    );
  }, 30_000);
});

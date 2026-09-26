import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    schema: "src/schema.ts",
    "core/index": "src/core/index.ts",
    cluster: "src/cluster.ts",
    "node/index": "src/node/index.ts",
    "ioredis/index": "src/ioredis/index.ts",
    "bun/index": "src/bun/index.ts",
    "upstash/index": "src/upstash/index.ts",
    "next/index": "src/next/index.ts",
    "hono/index": "src/hono/index.ts",
    "zod/index": "src/zod/index.ts"
  },
  format: "esm",
  dts: true,
  // Deno resolves every relative specifier in .d.mts files strictly. Mirroring
  // the source tree keeps those specifiers stable, and retaining empty runtime
  // modules ensures type-only sources such as core/types.ts still have the
  // matching .mjs target Deno expects during `deno check`.
  unbundle: true,
  // No source maps in the package, JS or declaration. The output is unbundled
  // and unminified, one .mjs per source file with the same names, so a stack
  // trace into dist/ already reads like the source; the JS maps only re-embedded
  // that source (830 kB of a 1.63 MB unpacked tarball). Declaration maps point
  // at ../src/*.ts, which the package does not ship, so go-to-definition could
  // never follow them. tsdown turns maps back on whenever tsconfig.json sets
  // declarationMap, so that stays unset too. If src/ ever ships, revisit both.
  sourcemap: false,
  clean: true,
  treeshake: false
});

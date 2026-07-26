import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    schema: "src/schema.ts",
    "core/index": "src/core/index.ts",
    "node/index": "src/node/index.ts",
    "bun/index": "src/bun/index.ts",
    "upstash/index": "src/upstash/index.ts",
    "primitives/index": "src/primitives/index.ts",
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
  sourcemap: true,
  clean: true,
  treeshake: false
});

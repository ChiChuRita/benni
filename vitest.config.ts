import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      // Measure the code the unit suite is responsible for. The runtime
      // adapters (node/bun/deno) are exercised by the integration suites,
      // which require a live Redis and are skipped in the default run, so
      // they are excluded here rather than dragging the gate to zero.
      include: ["src/**/*.ts"],
      exclude: ["src/node/**", "src/bun/**", "src/deno/**", "src/index.ts"],
      reporter: ["text", "html"],
      // Floors sit a few points below current coverage (~95% statements /
      // ~95% branches) so real regressions fail the gate without it being
      // brittle to small, legitimate changes.
      thresholds: {
        statements: 92,
        branches: 88,
        functions: 92,
        lines: 92
      }
    }
  }
});

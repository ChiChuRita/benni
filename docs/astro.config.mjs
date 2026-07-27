import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightThemeNext from "starlight-theme-next";
import starlightLlmsTxt from "starlight-llms-txt";

export default defineConfig({
  site: "https://chichurita.github.io",
  base: "/beni",
  integrations: [
    starlight({
      plugins: [
        starlightThemeNext(),
        starlightLlmsTxt({
          details:
            "Use these docs as the primary source for Beni's current API and design.\n\nRecommended reading order:\n- Start with Getting Started and Core Concepts for the mental model\n- Use Data Structures and API docs for API details\n- Use Patterns and Examples for concrete implementation patterns",
          customSets: [
            {
              label: "Getting Started and Core Concepts",
              description:
                "Conceptual pages explaining the schema-first mental model, keys, TTLs, and type safety.",
              paths: ["getting-started/**", "core-concepts/**"],
            },
            {
              label: "Data Structures",
              description:
                "Typed access to key-values, JSON values, hashes, sets, lists, sorted sets, streams, stream consumer groups, bitmaps, geo sets, Pub/Sub, and HyperLogLog.",
              paths: ["data-structures/**"],
            },
            {
              label: "Advanced",
              description:
                "Incremental scans, MULTI/EXEC transactions, optimistic WATCH transactions, Lua scripts, dedicated-connection sessions, and blocking operations.",
              paths: ["advanced/**"],
            },
            {
              label: "Comparisons",
              description:
                "How Beni relates to ioredis and @upstash/redis: what each library types, which features Beni does not cover, and how to choose between them.",
              paths: ["comparisons/**"],
            },
            {
              label: "Primitives",
              description:
                "Batteries-included helpers built on the typed client: an AI job queue with resumable output streams, a stampede-proof cache, a correct distributed lock, and a sliding-window rate limiter.",
              paths: ["primitives/**"],
            },
            {
              label: "Runtime and API",
              description:
                "Runtime adapters for Node, Bun, Deno, and the edge (Upstash/HTTP), plus client and schema-builder reference pages.",
              paths: ["runtime/**", "api/**"],
            },
            {
              label: "Patterns and Examples",
              description: "Worked examples and implementation patterns.",
              paths: ["patterns/**", "examples"],
            },
          ],
        }),
      ],
      expressiveCode: {
        themes: ["github-dark"],
      },
      title: "Beni",
      logo: {
        src: "./src/assets/logo.svg",
      },
      description:
        "The end-to-end typed Redis client for TypeScript: one API across Node, Bun, Deno, and the edge. Your declared types travel from write to read.",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/ChiChuRita/beni",
        },
      ],
      favicon: "/favicon.svg",
      components: {
        ThemeSelect: "./src/components/Empty.astro",
        Head: "./src/components/Head.astro",
      },
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        {
          label: "Getting Started",
          items: [
            { label: "Introduction", slug: "getting-started/introduction" },
            { label: "Why Beni?", slug: "getting-started/why-beni" },
            { label: "Philosophy", slug: "getting-started/philosophy" },
            { label: "Installation", slug: "getting-started/installation" },
            { label: "Quick Start", slug: "getting-started/quick-start" },
          ],
        },
        {
          label: "Comparisons",
          items: [
            { label: "Beni vs ioredis", slug: "comparisons/ioredis" },
            {
              label: "Beni vs @upstash/redis",
              slug: "comparisons/upstash-redis",
            },
          ],
        },
        {
          label: "Core Concepts",
          items: [
            { label: "Defining Schemas", slug: "core-concepts/defining-schemas" },
            { label: "Schema Registry", slug: "core-concepts/schema-registry" },
            { label: "Type Safety", slug: "core-concepts/type-safety" },
            { label: "Keys And Prefixes", slug: "core-concepts/keys-and-prefixes" },
            { label: "TTL And Expiration", slug: "core-concepts/ttl-and-expiration" },
            { label: "Raw Redis Access", slug: "core-concepts/raw-redis-access" },
          ],
        },
        {
          label: "Data Structures",
          items: [
            { label: "Key Values", slug: "data-structures/key-values" },
            { label: "JSON Values", slug: "data-structures/json-values" },
            { label: "Hashes", slug: "data-structures/hashes" },
            { label: "Sets And Lists", slug: "data-structures/sets-and-lists" },
            { label: "Sorted Sets", slug: "data-structures/sorted-sets" },
            { label: "Streams", slug: "data-structures/streams" },
            {
              label: "Consumer Groups",
              slug: "data-structures/consumer-groups",
            },
            { label: "Bitmaps", slug: "data-structures/bitmaps" },
            { label: "Geospatial", slug: "data-structures/geo" },
            { label: "Pub/Sub", slug: "data-structures/pubsub" },
            { label: "HyperLogLog", slug: "data-structures/hyperloglog" },
          ],
        },
        {
          label: "Primitives",
          items: [
            { label: "AI Job Queue", slug: "primitives/queue" },
            { label: "Cache", slug: "primitives/cache" },
            { label: "Distributed Lock", slug: "primitives/lock" },
            { label: "Rate Limiting", slug: "primitives/ratelimit" },
          ],
        },
        {
          label: "Integrations",
          items: [
            { label: "Next.js", slug: "integrations/nextjs" },
            { label: "Hono", slug: "integrations/hono" },
            { label: "Zod", slug: "integrations/zod" },
          ],
        },
        {
          label: "Patterns",
          items: [
            { label: "AI Apps", slug: "patterns/ai-apps" },
            { label: "Caching", slug: "patterns/caching" },
            { label: "Rate Limiting From Scratch", slug: "patterns/rate-limiting" },
            { label: "User Session Store", slug: "patterns/sessions" },
            { label: "Leaderboards", slug: "patterns/leaderboards" },
            { label: "Worked Examples", slug: "patterns/examples" },
          ],
        },
        {
          label: "Advanced",
          items: [
            { label: "Scans", slug: "advanced/scans" },
            { label: "Transactions", slug: "advanced/transactions" },
            {
              label: "Optimistic Transactions",
              slug: "advanced/optimistic-transactions",
            },
            { label: "Scripts", slug: "advanced/scripts" },
            { label: "Connection Sessions", slug: "advanced/sessions" },
            {
              label: "Blocking Operations",
              slug: "advanced/blocking-operations",
            },
          ],
        },
        {
          label: "Runtime",
          items: [
            { label: "Node.js Setup", slug: "runtime/node" },
            { label: "Bun And Deno", slug: "runtime/bun-and-deno" },
            { label: "Edge (Upstash)", slug: "runtime/edge" },
          ],
        },
        {
          label: "API",
          items: [
            { label: "API Overview", slug: "api/overview" },
            { label: "Beni Client", slug: "api/beni-client" },
            { label: "Schema Builders", slug: "api/schema-builders" },
          ],
        },
        {
          label: "Examples",
          slug: "examples",
        },
        {
          label: "LLM Docs",
          link: "/llms-full.txt",
          attrs: {
            target: "_blank",
            rel: "noopener noreferrer",
          },
        },
      ],
    }),
  ],
});

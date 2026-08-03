import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// The docs palette lives in exactly one place, tokens.css, because it has two
// consumers that cannot see each other: Starlight loads it through customCss,
// and the standalone landing page imports it directly. When the landing page
// kept its own literal copies the two drifted, neutral greys against warm ones,
// so these tests hold the single-source-of-truth arrangement in place.
//
// The logo and favicon are the one legitimate exception: both render as
// isolated documents (<img> / rel="icon"), where custom properties and
// currentColor never reach, so each repeats the brand hex literally.

const read = (path: string) =>
  readFileSync(fileURLToPath(new URL(`../${path}`, import.meta.url)), "utf8");

const TOKENS_PATH = "docs/src/styles/tokens.css";
const OVERRIDES_PATH = "docs/src/styles/custom.css";
const LANDING_PATH = "docs/src/pages/index.astro";
const SVG_PATHS = ["docs/src/assets/logo.svg", "docs/public/favicon.svg"];

const brandFromTokens = () => {
  const match = read(TOKENS_PATH).match(/--benni-brand:\s*(#[0-9a-fA-F]{3,8})\s*;/);
  if (!match) {
    throw new Error(`--benni-brand not found in ${TOKENS_PATH}`);
  }
  return match[1].toLowerCase();
};

// Colour literals only, so a token value never gets inlined at a call site.
// Excludes HTML entities such as &#123;, whose digits would otherwise read as a
// three-digit hex.
const hexLiterals = (source: string) => [
  ...source.matchAll(/(?<!&)#[0-9a-fA-F]{3,8}\b/g)
].map((m) => m[0].toLowerCase());

const landingStyleBlock = () => {
  const match = read(LANDING_PATH).match(/<style>([\s\S]*)<\/style>/);
  if (!match) {
    throw new Error(`no <style> block found in ${LANDING_PATH}`);
  }
  return match[1];
};

describe("docs brand color", () => {
  it("declares --benni-brand exactly once", () => {
    const declarations = read(TOKENS_PATH).match(/--benni-brand:/g) ?? [];
    expect(declarations).toHaveLength(1);
  });

  it.each(SVG_PATHS)("%s uses only the brand color", (path) => {
    const brand = brandFromTokens();
    const hexes = [
      ...read(path).matchAll(/(?:fill|stroke)="(#[0-9a-fA-F]{3,8})"/g)
    ].map((m) => m[1].toLowerCase());

    // Guards both directions: a recolored SVG that no longer matches the token,
    // and an SVG that quietly stopped painting the brand at all.
    expect(hexes.length).toBeGreaterThan(0);
    expect([...new Set(hexes)]).toEqual([brand]);
  });

  // The sync comments in those SVGs are a trap: an XML comment may not contain
  // a double hyphen, so spelling the token as "benni-brand" with its dashes
  // inside one makes the file malformed and the browser renders a broken image
  // instead of the logo. Nothing else in the pipeline rejects it — Vite serves
  // the file 200 OK and sharp re-encodes it happily — so this is the only guard.
  it.each(SVG_PATHS)("%s is well-formed XML", (path) => {
    const comments = [...read(path).matchAll(/<!--([\s\S]*?)-->/g)].map(
      (m) => m[1]
    );

    for (const body of comments) {
      expect(body).not.toContain("--");
    }
  });

  it("derives the accent shades from the token rather than hardcoding them", () => {
    const accents =
      read(OVERRIDES_PATH).match(/--sl-color-accent[a-z-]*:\s*([^;]+);/g) ?? [];

    // Either the brand token itself or its lifted text variant, which tokens.css
    // mixes from that same token. Anything else means a shade was inlined.
    expect(accents.length).toBeGreaterThan(0);
    for (const declaration of accents) {
      expect(declaration).toMatch(/var\(--benni-brand(-text)?\)/);
    }
  });
});

describe("docs palette has one source", () => {
  it("keeps every colour literal in tokens.css", () => {
    expect(hexLiterals(read(TOKENS_PATH)).length).toBeGreaterThan(0);
  });

  // Both consumers must reference the tokens rather than restate them. A literal
  // in either file is a value the other one cannot see.
  it("declares no colour literals in the Starlight overrides", () => {
    expect(hexLiterals(read(OVERRIDES_PATH))).toEqual([]);
  });

  it("declares no colour literals on the landing page", () => {
    expect(hexLiterals(landingStyleBlock())).toEqual([]);
  });

  it("has the landing page import the shared tokens", () => {
    expect(read(LANDING_PATH)).toContain('import "../styles/tokens.css"');
  });
});

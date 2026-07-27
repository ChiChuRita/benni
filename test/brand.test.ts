import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// The docs accent is derived from a single --beni-brand custom property, but
// the logo and favicon cannot read it: both render as isolated documents (<img>
// / rel="icon"), where page-level custom properties and currentColor do not
// apply, so each repeats the hex literally. These tests are the guard that
// keeps the three copies from drifting apart.

const read = (path: string) =>
  readFileSync(fileURLToPath(new URL(`../${path}`, import.meta.url)), "utf8");

const CSS_PATH = "docs/src/styles/custom.css";
const SVG_PATHS = ["docs/src/assets/logo.svg", "docs/public/favicon.svg"];

const brandFromCss = () => {
  const css = read(CSS_PATH);
  const match = css.match(/--beni-brand:\s*(#[0-9a-fA-F]{3,8})\s*;/);
  if (!match) {
    throw new Error(`--beni-brand not found in ${CSS_PATH}`);
  }
  return match[1].toLowerCase();
};

describe("docs brand color", () => {
  it("declares --beni-brand exactly once", () => {
    const declarations = read(CSS_PATH).match(/--beni-brand:/g) ?? [];
    expect(declarations).toHaveLength(1);
  });

  it.each(SVG_PATHS)("%s uses only the brand color", (path) => {
    const brand = brandFromCss();
    const hexes = [
      ...read(path).matchAll(/(?:fill|stroke)="(#[0-9a-fA-F]{3,8})"/g)
    ].map((m) => m[1].toLowerCase());

    // Guards both directions: a recolored SVG that no longer matches the token,
    // and an SVG that quietly stopped painting the brand at all.
    expect(hexes.length).toBeGreaterThan(0);
    expect([...new Set(hexes)]).toEqual([brand]);
  });

  // The sync comments in those SVGs are a trap: an XML comment may not contain
  // a double hyphen, so spelling the token as "--beni-brand" inside one makes
  // the file malformed and the browser renders a broken image instead of the
  // logo. Nothing else in the pipeline rejects it — Vite serves the file 200 OK
  // and sharp re-encodes it happily — so this is the only guard.
  it.each(SVG_PATHS)("%s is well-formed XML", (path) => {
    const comments = [...read(path).matchAll(/<!--([\s\S]*?)-->/g)].map(
      (m) => m[1]
    );

    for (const body of comments) {
      expect(body).not.toContain("--");
    }
  });

  it("derives the accent shades from the token rather than hardcoding them", () => {
    const css = read(CSS_PATH);
    const accents = css.match(/--sl-color-accent[a-z-]*:\s*([^;]+);/g) ?? [];

    expect(accents.length).toBeGreaterThan(0);
    for (const declaration of accents) {
      expect(declaration).toContain("var(--beni-brand)");
    }
  });
});

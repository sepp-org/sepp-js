import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { USER_AGENT, VERSION } from "../src/version";

describe("version", () => {
  it("matches package.json (src/version.ts must be bumped with each release)", () => {
    const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
    expect(VERSION).toBe(pkg.version);
  });

  it("feeds the user agent", () => {
    expect(USER_AGENT).toBe(`sepp-js/${VERSION}`);
  });
});

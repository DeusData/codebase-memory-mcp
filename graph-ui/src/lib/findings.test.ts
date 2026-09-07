import { describe, expect, it } from "vitest";
import { kneeCount, costSentence, findingKey } from "./findings";
import { messages } from "./i18n";
import { isTestRegion, lensRegionsPayload, isBuiltinQn } from "./regions";
import type { RegionsPayload } from "./types";

describe("kneeCount", () => {
  it("separates a clear head from a flat tail", () => {
    /* 100, 90, 85 | 20, 18 — the knee is after the third item. */
    expect(kneeCount([100, 90, 85, 20, 18])).toBe(3);
  });
  it("does not collapse to rank 1 on power-law data", () => {
    /* ~1000/rank — the old largest-relative-drop rule returned 1 here. */
    const powerLaw = [1000, 500, 333, 250, 200, 167, 143, 125, 111, 100];
    expect(kneeCount(powerLaw)).toBeGreaterThanOrEqual(3);
    expect(kneeCount(powerLaw)).toBeLessThanOrEqual(5);
  });
  it("finds an obvious elbow", () => {
    /* Four high values, then a cliff — the head is those four. */
    expect(kneeCount([100, 98, 96, 94, 15, 14, 13, 12])).toBe(4);
  });
  it("returns the clamp floor on a monotone gentle series", () => {
    expect(kneeCount([100, 97, 94, 91, 88, 85, 82, 79, 76, 73])).toBe(3);
  });
  it("never exceeds half the list nor 15", () => {
    const flat = Array.from({ length: 100 }, (_, i) => 1000 - i);
    expect(kneeCount(flat)).toBeLessThanOrEqual(15);
    expect(kneeCount([9, 8, 7, 2, 1, 1])).toBeLessThanOrEqual(3);
  });
  it("degenerates safely", () => {
    expect(kneeCount([])).toBe(0);
    expect(kneeCount([5])).toBe(1);
    expect(kneeCount([5, 5])).toBe(2);
    expect(kneeCount([5, 5, 5])).toBe(3);
    expect(kneeCount([0, 0, 0, 0, 0])).toBe(3);
  });
});

describe("costSentence", () => {
  it("renders shares of codebase and commits", () => {
    expect(costSentence(9, 340, 800, 1000)).toBe(
      "9 files — 1.1% of the codebase, 34% of all commits this year",
    );
  });
  it("composes the zh sentence from the same numbers", () => {
    expect(costSentence(9, 340, 800, 1000, messages.zh.dashboard)).toBe(
      "9 个文件——占代码库的 1.1%，占今年全部提交的 34%",
    );
  });
  it("returns null without denominators", () => {
    expect(costSentence(9, 340, 0, 1000)).toBeNull();
    expect(costSentence(0, 0, 800, 1000)).toBeNull();
    expect(costSentence(9, 340, 0, 1000, messages.zh.dashboard)).toBeNull();
  });
});

describe("findingKey", () => {
  it("buckets magnitude by order of magnitude so regressions resurface", () => {
    expect(findingKey("coupling", "a⇄b", 400)).toBe(findingKey("coupling", "a⇄b", 900));
    expect(findingKey("coupling", "a⇄b", 400)).not.toBe(findingKey("coupling", "a⇄b", 4000));
  });
});

describe("product-code lens", () => {
  const payload: RegionsPayload = {
    level: "regions",
    method: "leiden+folders",
    total_nodes: 100,
    unmapped_nodes: 0,
    regions: [
      { id: 0, name: "src/core", files: 2, members: 10, cohesion: 0.5, top_nodes: [], x: 0, y: 0, z: 0, size: 1, color: "#111111" },
      { id: 1, name: "tests/model_fields", files: 2, members: 10, cohesion: 0.5, top_nodes: [], x: 0, y: 0, z: 0, size: 1, color: "#222222" },
      { id: 2, name: "tests/e2e · runner", files: 2, members: 10, cohesion: 0.5, top_nodes: [], x: 0, y: 0, z: 0, size: 1, color: "#333333" },
    ],
    edges: [
      { source: 0, target: 1, weight: 5 },
      { source: 0, target: 0, weight: 1 },
    ],
  };
  it("classifies test regions including disambiguated names", () => {
    expect(isTestRegion("src/core")).toBe(false);
    expect(isTestRegion("tests/model_fields")).toBe(true);
    expect(isTestRegion("tests/e2e · runner")).toBe(true);
    expect(isTestRegion("src/contest")).toBe(false);
  });
  it("filters regions and their edges", () => {
    const lensed = lensRegionsPayload(payload, false);
    expect(lensed.regions.map((region) => region.id)).toEqual([0]);
    expect(lensed.edges).toEqual([{ source: 0, target: 0, weight: 1 }]);
    expect(lensRegionsPayload(payload, true)).toBe(payload);
  });
});

describe("isBuiltinQn", () => {
  it("catches builtin namespaces without false positives", () => {
    expect(isBuiltinQn("proj.builtins.len")).toBe(true);
    expect(isBuiltinQn("builtins.print")).toBe(true);
    expect(isBuiltinQn("proj.src.mybuiltins_helper.run")).toBe(false);
  });
});

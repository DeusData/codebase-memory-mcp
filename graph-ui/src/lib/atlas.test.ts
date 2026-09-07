import { describe, expect, it, vi } from "vitest";
import { archRows, fetchTrace } from "./atlas";
import { surprisingCouplings, suggestedQuestions } from "./firstread";
import { squarify } from "./treemap";
import { composePrompt } from "./composer";
import type { Region, RegionsPayload } from "./types";

function region(partial: Partial<Region> & { id: number; name: string }): Region {
  return {
    files: 4,
    members: 100,
    cohesion: 0.5,
    top_nodes: [],
    x: 0,
    y: 0,
    z: 0,
    size: 20,
    color: "#123456",
    ...partial,
  };
}

describe("archRows", () => {
  it("decodes {cols, rows} sections into row objects", () => {
    const arch = {
      hotspots: { cols: ["qn", "fan_in"], rows: [["a.b.c", 42], ["d.e.f", 7]] },
      project: "demo",
    };
    expect(archRows(arch, "hotspots")).toEqual([
      { qn: "a.b.c", fan_in: 42 },
      { qn: "d.e.f", fan_in: 7 },
    ]);
    expect(archRows(arch, "absent")).toEqual([]);
    expect(archRows(null, "hotspots")).toEqual([]);
  });
});

describe("first read", () => {
  const payload: RegionsPayload = {
    level: "regions",
    method: "leiden+folders",
    total_nodes: 20000,
    unmapped_nodes: 0,
    regions: [
      region({ id: 0, name: "src/pipeline", cohesion: 0.8, members: 900 }),
      region({ id: 1, name: "tests/e2e", cohesion: 0.7, members: 400 }),
      region({ id: 2, name: "src/store", cohesion: 0.15, members: 600 }),
      region({ id: 3, name: "misc", members: 50 }),
    ],
    edges: [
      { source: 0, target: 1, weight: 120 },
      { source: 0, target: 2, weight: 40 },
      { source: 3, target: 0, weight: 999 },
    ],
  };

  it("ranks cross-area couplings with reasons and ignores misc", () => {
    const out = surprisingCouplings(payload, 5);
    expect(out).toHaveLength(2);
    /* src→tests crosses top-level areas and is heavier. */
    expect(out[0].source.name).toBe("src/pipeline");
    expect(out[0].target.name).toBe("tests/e2e");
    expect(out[0].reasons.some((reason) => reason.includes("different top-level"))).toBe(true);
    /* src/store's looseness is called out on the second coupling. */
    expect(out[1].reasons.some((reason) => reason.includes("cohesion 0.15"))).toBe(true);
  });

  it("asks split/dead/certainty questions with a why", () => {
    const questions = suggestedQuestions(payload, { deadCount: 87, unresolvedShare: 0.4 });
    const texts = questions.map((question) => question.question);
    expect(texts.some((text) => text.includes("Should src/store be split"))).toBe(true);
    expect(texts.some((text) => text.includes("87 functions with no callers"))).toBe(true);
    for (const question of questions) expect(question.why.length).toBeGreaterThan(0);
  });
});

describe("squarify", () => {
  it("tiles the full rectangle with area-proportional cells", () => {
    const rects = squarify(
      [
        { id: "a", value: 6 },
        { id: "b", value: 3 },
        { id: "c", value: 1 },
      ],
      0,
      0,
      100,
      50,
    );
    expect(rects).toHaveLength(3);
    const area = (id: string) => {
      const rect = rects.find((r) => r.id === id)!;
      return rect.w * rect.h;
    };
    expect(area("a")).toBeCloseTo(3000, 0);
    expect(area("b")).toBeCloseTo(1500, 0);
    expect(area("c")).toBeCloseTo(500, 0);
    const total = rects.reduce((a, rect) => a + rect.w * rect.h, 0);
    expect(total).toBeCloseTo(5000, 0);
    for (const rect of rects) {
      expect(rect.x).toBeGreaterThanOrEqual(0);
      expect(rect.y).toBeGreaterThanOrEqual(0);
      expect(rect.x + rect.w).toBeLessThanOrEqual(100.001);
      expect(rect.y + rect.h).toBeLessThanOrEqual(50.001);
    }
  });

  it("handles empty and zero-value input", () => {
    expect(squarify([], 0, 0, 10, 10)).toEqual([]);
    expect(squarify([{ id: "a", value: 0 }], 0, 0, 10, 10)).toEqual([]);
  });
});

describe("composePrompt", () => {
  it("cites exact identifiers and the MCP calls that reproduce context", () => {
    const prompt = composePrompt("demo", "Refactor the store locking.", [
      {
        kind: "symbol",
        id: 7,
        name: "cbm_store_close",
        qualified_name: "demo.src.store.cbm_store_close",
        file_path: "src/store/store.c",
        start_line: 100,
        end_line: 140,
      },
      { kind: "region", id: 0, name: "src/store", members: 600, why: "call community" },
      { kind: "question", question: "Should src/store be split?", why: "cohesion 0.15" },
    ]);
    expect(prompt).toContain("Refactor the store locking.");
    expect(prompt).toContain("demo.src.store.cbm_store_close — src/store/store.c:100-140");
    expect(prompt).toContain('get_code_snippet(qualified_name: "demo.src.store.cbm_store_close"');
    expect(prompt).toContain('trace_path(qualified_name: "demo.src.store.cbm_store_close"');
    expect(prompt).toContain("Should src/store be split?");
    expect(prompt).toContain("USAGE edges mean the graph could not prove");
  });
});

describe("fetchTrace", () => {
  it("encodes endpoints + mode and returns the trace payload", async () => {
    const payload = { mode: "data", max_depth: 12, reachable: true, hops: 1 };
    const mock = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(payload),
    });
    vi.stubGlobal("fetch", mock);
    try {
      const result = await fetchTrace("demo", "pkg.alpha", "#42", "data");
      expect(result).toEqual(payload);
      const url = String(mock.mock.calls[0][0]);
      expect(url).toContain("/api/trace?");
      expect(url).toContain("project=demo");
      expect(url).toContain("from=pkg.alpha");
      expect(url).toContain("to=%2342");
      expect(url).toContain("mode=data");
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

import { describe, expect, it } from "vitest";
import { messages } from "./i18n";
import { impactSentence, type ImpactPayload } from "./impact";

const payload = (overrides: Partial<ImpactPayload> = {}): ImpactPayload => ({
  basis: "CALLS",
  max_depth: 10,
  visit_cap: 50000,
  callable_total: 4200,
  node: { id: 1, name: "f" },
  reachable: 210,
  max_distance: 4,
  truncated: false,
  depth_capped: false,
  by_distance: [12, 36, 100, 62],
  regions: [{ name: "core", count: 120 }],
  regions_more: 2,
  unregioned: 3,
  nearest: [],
  tests: { count: 0, nearest: [] },
  ...overrides,
});

describe("impactSentence", () => {
  it("states reach, total and region count with locale formatting", () => {
    expect(impactSentence(payload())).toBe(
      "210 of 4,200 callables can reach this — 3 regions could notice.",
    );
    expect(impactSentence(payload({ regions_more: 0 }))).toBe(
      "210 of 4,200 callables can reach this — 1 region could notice.",
    );
  });

  it("marks a truncated or depth-capped walk as a floor", () => {
    const suffix = " (walk capped — the true count is higher)";
    expect(impactSentence(payload({ truncated: true }))).toBe(
      `210 of 4,200 callables can reach this — 3 regions could notice.${suffix}`,
    );
    expect(impactSentence(payload({ depth_capped: true }))).toContain(suffix);
  });

  it("says so honestly when nothing recorded calls the symbol", () => {
    expect(
      impactSentence(
        payload({ reachable: 0, max_distance: 0, by_distance: [], regions: [], regions_more: 0, unregioned: 0 }),
      ),
    ).toBe(
      "Nothing recorded calls this — changes here surface only where it is referenced dynamically.",
    );
  });

  it("speaks the zh catalog with the same numbers when its messages are passed", () => {
    expect(impactSentence(payload(), messages.zh.impact)).toBe(
      "全仓库 4,200 个可调用符号中，有 210 个能到达这里——3 个区域可能察觉。",
    );
    expect(impactSentence(payload({ truncated: true }), messages.zh.impact)).toContain(
      "（遍历已达上限——真实数量更高）",
    );
  });
});

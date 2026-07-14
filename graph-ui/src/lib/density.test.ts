import { describe, expect, it } from "vitest";
import {
  edgeIntensityScale,
  bloomIntensityScale,
  nodeBoostScale,
  nodeGlowBoost,
  clampDisplaySettings,
  DEFAULT_DISPLAY_SETTINGS,
  EDGE_REFERENCE_COUNT,
  NODE_REFERENCE_COUNT,
} from "./density";

/* Parse "#rrggbb" to 0..1 channels, matching how the renderer feeds colors. */
function rgb(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16);
  return [((n >> 16) & 0xff) / 255, ((n >> 8) & 0xff) / 255, (n & 0xff) / 255];
}
const glow = (hex: string) => nodeGlowBoost(...rgb(hex));

describe("edgeIntensityScale", () => {
  it("is 1 at or below the reference edge count", () => {
    expect(edgeIntensityScale(0)).toBe(1);
    expect(edgeIntensityScale(EDGE_REFERENCE_COUNT)).toBe(1);
  });

  it("shrinks monotonically as edges grow, bounded below", () => {
    const a = edgeIntensityScale(10_000);
    const b = edgeIntensityScale(80_000);
    expect(a).toBeLessThan(1);
    expect(b).toBeLessThan(a);
    expect(b).toBeGreaterThanOrEqual(0.05);
    /* Never collapses to zero even at absurd counts */
    expect(edgeIntensityScale(50_000_000)).toBeGreaterThanOrEqual(0.05);
  });

  it("keeps accumulated brightness roughly flat (scale ~ 1/sqrt(n))", () => {
    /* 4x the edges → each ~half as bright → similar total glow */
    const ref = edgeIntensityScale(EDGE_REFERENCE_COUNT * 4);
    expect(ref).toBeCloseTo(0.5, 5);
  });
});

describe("bloomIntensityScale", () => {
  it("stays full through the reference count (preserves the glow), then eases", () => {
    /* A whole mid-size repo (≤25k nodes) keeps full bloom — the wow effect. */
    expect(bloomIntensityScale(NODE_REFERENCE_COUNT)).toBe(1);
    expect(bloomIntensityScale(20_000)).toBe(1);
    /* Only very large clouds ease back, and never below the floor. */
    expect(bloomIntensityScale(100_000)).toBeLessThan(1);
    expect(bloomIntensityScale(10_000_000)).toBeGreaterThanOrEqual(0.7);
  });
});

describe("nodeBoostScale", () => {
  it("keeps node halos bright through the reference count, then eases gently", () => {
    expect(nodeBoostScale(NODE_REFERENCE_COUNT)).toBe(1);
    expect(nodeBoostScale(20_000)).toBe(1);
    const big = nodeBoostScale(100_000);
    expect(big).toBeGreaterThan(0.8);
    expect(big).toBeLessThan(1);
    /* Floored — never fully flat, even at millions of nodes. */
    expect(nodeBoostScale(10_000_000)).toBeGreaterThanOrEqual(0.8);
  });
});

describe("nodeGlowBoost", () => {
  /* Star-class palette from lib/colors.ts STAR_LEGEND */
  const BLUE_GIANT = "#80a0ff"; // 50+ connections (hubs)
  const WHITE = "#e8e8ff"; // mid degree
  const YELLOW = "#ffe080"; // mid degree
  const RED_DWARF = "#ff6050"; // leaves

  it("makes blue hubs glow the most", () => {
    expect(glow(BLUE_GIANT)).toBeGreaterThan(glow(RED_DWARF));
    expect(glow(BLUE_GIANT)).toBeGreaterThan(glow(WHITE));
    expect(glow(BLUE_GIANT)).toBeGreaterThan(glow(YELLOW));
  });

  it("gives red leaves a modest boost — above white/yellow, below blue", () => {
    expect(glow(RED_DWARF)).toBeGreaterThan(glow(WHITE));
    expect(glow(RED_DWARF)).toBeGreaterThan(glow(YELLOW));
    expect(glow(RED_DWARF)).toBeLessThan(glow(BLUE_GIANT));
  });

  it("keeps white/yellow low so they don't blow out the bloom", () => {
    /* Old formula gave white ~2.0×; it must now be well under the blue hub. */
    expect(glow(WHITE)).toBeLessThan(1.7);
    expect(glow(YELLOW)).toBeLessThan(1.7);
  });

  it("never dims a node (multiplier ≥ 1)", () => {
    for (const hex of [BLUE_GIANT, WHITE, YELLOW, RED_DWARF, "#06b6d4", "#a855f7"]) {
      expect(glow(hex)).toBeGreaterThanOrEqual(1);
    }
  });
});

describe("clampDisplaySettings", () => {
  it("clamps each setting to its range and fills defaults", () => {
    expect(clampDisplaySettings({})).toEqual(DEFAULT_DISPLAY_SETTINGS);
    expect(
      clampDisplaySettings({ edgeBrightness: 99, nodeGlow: -5, bloom: 1.5 }),
    ).toEqual({ edgeBrightness: 3, nodeGlow: 0, bloom: 1.5 });
    expect(clampDisplaySettings({ bloom: Number.NaN }).bloom).toBe(
      DEFAULT_DISPLAY_SETTINGS.bloom,
    );
  });
});

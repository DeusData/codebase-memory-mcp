import { describe, expect, it } from "vitest";
import {
  CPLX_BINS,
  CPLX_SIMPLE_MAX,
  CPLX_TAIL_MIN,
  CPLX_TAIL_START,
  complexityTakeaway,
} from "./complexity";
import { messages } from "./i18n";

/* Parse a bin label back into its bounds so the text the user reads under
 * the bars cannot drift from the numeric metadata the sentence derives
 * from. "1" → [1,1], "2–5" → [2,5], ">50" → [51,∞]. */
function parseLabel(label: string): { lo: number; hi: number } {
  if (label.startsWith(">")) return { lo: Number(label.slice(1)) + 1, hi: Infinity };
  const parts = label.split("–").map(Number);
  const lo = parts[0];
  const hi = parts.length > 1 ? parts[1] : parts[0];
  expect(Number.isFinite(lo)).toBe(true);
  expect(Number.isFinite(hi)).toBe(true);
  return { lo, hi };
}

describe("complexity bins", () => {
  it("labels agree with the numeric bounds and are contiguous", () => {
    let expectedLo = 1;
    for (const bin of CPLX_BINS) {
      const parsed = parseLabel(bin.label);
      expect(parsed.lo).toBe(expectedLo);
      expect(parsed.hi).toBe(bin.hi);
      expectedLo = bin.hi + 1;
    }
  });
  it("derived thresholds match the labels the user sees", () => {
    expect(CPLX_SIMPLE_MAX).toBe(parseLabel(CPLX_BINS[1].label).hi);
    expect(CPLX_TAIL_MIN).toBe(parseLabel(CPLX_BINS[CPLX_TAIL_START].label).lo - 1);
  });
});

describe("complexityTakeaway", () => {
  /* Expectations recounted from the label-parsed bounds, independent of the
   * metadata. Both languages' sentences derive from the same recount, so a
   * bin edit fails both or neither. */
  const hist = [4, 6, 3, 2, 1, 1];
  const total = hist.reduce((a, b) => a + b, 0);
  const simple = hist
    .filter((_, index) => parseLabel(CPLX_BINS[index].label).hi <= CPLX_SIMPLE_MAX)
    .reduce((a, b) => a + b, 0);
  const tail = hist
    .filter((_, index) => parseLabel(CPLX_BINS[index].label).lo > CPLX_TAIL_MIN)
    .reduce((a, b) => a + b, 0);
  const simpleMax = parseLabel(CPLX_BINS[1].label).hi;
  const tailMin = parseLabel(CPLX_BINS[CPLX_TAIL_START].label).lo - 1;
  const pct = ((simple / total) * 100).toFixed(0);

  it("claims exactly what the labels claim", () => {
    expect(complexityTakeaway(hist, [])).toBe(
      `${pct}% of functions are simple (≤${simpleMax}); ${tail} exceed ${tailMin}.`,
    );
  });
  it("composes the zh sentence from the same label-parsed bounds", () => {
    expect(complexityTakeaway(hist, [], messages.zh.dashboard)).toBe(
      `${pct}% 的函数是简单的（≤${simpleMax}）；${tail} 个超过 ${tailMin}。`,
    );
  });
  it("parses each language's threshold claims back to the bin metadata", () => {
    for (const m of [messages.en.dashboard, messages.zh.dashboard]) {
      const sentence = complexityTakeaway(hist, [], m)!;
      expect(Number(/≤(\d+)/.exec(sentence)![1])).toBe(CPLX_SIMPLE_MAX);
      const over = /exceed (\d+)|超过 (\d+)/.exec(sentence)!;
      expect(Number(over[1] ?? over[2])).toBe(CPLX_TAIL_MIN);
    }
  });
  it("appends the leading names when given", () => {
    expect(complexityTakeaway([10, 0, 0, 0, 0, 2], ["a", "b"])).toContain("— led by a, b");
    expect(
      complexityTakeaway([10, 0, 0, 0, 0, 2], ["a", "b"], messages.zh.dashboard),
    ).toContain("——以 a、b 为首");
  });
  it("returns null on an empty population", () => {
    expect(complexityTakeaway([0, 0, 0, 0, 0, 0], [])).toBeNull();
    expect(complexityTakeaway([0, 0, 0, 0, 0, 0], [], messages.zh.dashboard)).toBeNull();
  });
});

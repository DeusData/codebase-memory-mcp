import { describe, expect, it } from "vitest";
import { topKBySize } from "./topK";

function items(sizes: number[]): { size: number; id: number }[] {
  return sizes.map((size, id) => ({ size, id }));
}

describe("topKBySize", () => {
  it("returns all items sorted descending when k >= length", () => {
    const result = topKBySize(items([3, 1, 4, 1, 5]), 10);
    expect(result.map((i) => i.size)).toEqual([5, 4, 3, 1, 1]);
  });

  it("returns exactly the k largest items, sorted descending", () => {
    const result = topKBySize(items([3, 1, 4, 1, 5, 9, 2, 6]), 3);
    expect(result.map((i) => i.size)).toEqual([9, 6, 5]);
  });

  it("matches a full sort+slice for a larger random-ish input", () => {
    const sizes = Array.from({ length: 500 }, (_, i) => (i * 37) % 211);
    const expected = [...items(sizes)].sort((a, b) => b.size - a.size).slice(0, 20);
    const result = topKBySize(items(sizes), 20);
    expect(result.map((i) => i.size)).toEqual(expected.map((i) => i.size));
  });

  it("returns an empty array for k <= 0", () => {
    expect(topKBySize(items([1, 2, 3]), 0)).toEqual([]);
    expect(topKBySize(items([1, 2, 3]), -1)).toEqual([]);
  });

  it("handles ties without dropping items arbitrarily below k", () => {
    const result = topKBySize(items([5, 5, 5, 1, 1]), 3);
    expect(result.length).toBe(3);
    expect(result.every((i) => i.size === 5)).toBe(true);
  });
});

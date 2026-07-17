import { describe, expect, it } from "vitest";
import {
  assertChunkBudget,
  chunkBudgetViolations,
  MAX_JS_CHUNK_BYTES,
} from "./chunkBudget";

describe("production JavaScript chunk budget", () => {
  it("accepts chunks at or below the hard limit", () => {
    const chunks = [
      { fileName: "at-limit.js", bytes: MAX_JS_CHUNK_BYTES },
      { fileName: "small.js", bytes: 10_000 },
    ];
    expect(chunkBudgetViolations(chunks)).toEqual([]);
    expect(() => assertChunkBudget(chunks)).not.toThrow();
  });

  it("rejects every chunk above the hard limit", () => {
    const chunks = [
      { fileName: "too-large.js", bytes: MAX_JS_CHUNK_BYTES + 1 },
      { fileName: "also-large.js", bytes: MAX_JS_CHUNK_BYTES + 2 },
    ];
    expect(chunkBudgetViolations(chunks)).toEqual(chunks);
    expect(() => assertChunkBudget(chunks)).toThrow(/too-large\.js.*also-large\.js/);
  });
});

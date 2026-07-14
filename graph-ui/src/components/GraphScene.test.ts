import { describe, expect, it } from "vitest";
import { GRAPH_CANVAS_DPR } from "./GraphScene";

describe("GraphScene render limits", () => {
  it("caps the high-DPI WebGL backing store below the MSAA failure range", () => {
    expect(GRAPH_CANVAS_DPR[0]).toBe(1);
    expect(GRAPH_CANVAS_DPR[1]).toBeLessThanOrEqual(1.5);
  });
});

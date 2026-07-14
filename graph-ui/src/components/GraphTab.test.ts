import { describe, expect, it } from "vitest";
import { formatGraphLimitNotice } from "./GraphTab";
import type { GraphData } from "../lib/types";

describe("formatGraphLimitNotice", () => {
  it("reports when the graph response is truncated for render safety", () => {
    const data = {
      nodes: Array.from({ length: 2000 }, (_, id) => ({
        id,
        x: 0,
        y: 0,
        z: 0,
        label: "Function",
        name: `fn${id}`,
        size: 1,
        color: "#ffffff",
      })),
      edges: [],
      total_nodes: 43729,
    } satisfies GraphData;

    expect(formatGraphLimitNotice(data)).toBe(
      "Showing 2,000 of 43,729 nodes (0 edges). Raise the node budget or use filters.",
    );
  });

  it("stays quiet when the full graph is rendered", () => {
    const data = {
      nodes: [],
      edges: [],
      total_nodes: 0,
    } satisfies GraphData;

    expect(formatGraphLimitNotice(data)).toBeNull();
  });
});

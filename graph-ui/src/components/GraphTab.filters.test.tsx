/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { GraphTab } from "./GraphTab";
import type { GraphData } from "../lib/types";

/* GraphScene renders a WebGL <Canvas> which jsdom can't run — stub it out. */
vi.mock("./GraphScene", () => ({
  GraphScene: () => null,
  computeCameraTarget: () => null,
}));

const SAMPLE: GraphData = {
  nodes: [
    { id: 1, x: 0, y: 0, z: 0, label: "Function", name: "foo", size: 1, color: "#fff" },
    { id: 2, x: 1, y: 0, z: 0, label: "Class", name: "Bar", size: 1, color: "#fff" },
  ],
  edges: [{ source: 1, target: 2, type: "CALLS" }],
  total_nodes: 2,
};

function mockLayoutFetch(data: GraphData) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.startsWith("/api/layout")) {
      /* The region probe answers first: a small project — no region scene. */
      if (url.includes("level=regions")) {
        return new Response(
          JSON.stringify({
            level: "regions",
            method: "folders",
            total_nodes: data.total_nodes,
            unmapped_nodes: 0,
            regions: [],
            edges: [],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return new Response(JSON.stringify(data), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("{}", { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("GraphTab filters", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("keeps the filter sidebar visible when all nodes are filtered out", async () => {
    mockLayoutFetch(SAMPLE);

    render(<GraphTab project="demo" />);

    /* Wait for the layout to load — the filter panel header appears. */
    expect(await screen.findByText("Filters")).toBeInTheDocument();

    /* Everything starts enabled — the tri-state master reads checked.
       One click cycles all-on → all-off. */
    const master = screen.getByRole("checkbox", { name: "All filters" });
    expect(master).toHaveAttribute("aria-checked", "true");
    fireEvent.click(master);

    /* The graph area reports that everything is filtered out… */
    expect(screen.getByText("All nodes filtered out")).toBeInTheDocument();

    /* …but the filter sidebar must stay so the user can re-enable filters
       instead of being forced to reset everything. */
    expect(screen.getByText("Filters")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "All filters" })).toHaveAttribute(
      "aria-checked",
      "false",
    );
  });
});

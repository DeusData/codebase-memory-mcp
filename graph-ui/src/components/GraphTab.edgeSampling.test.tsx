/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { GraphTab } from "./GraphTab";
import type { GraphData } from "../lib/types";

/* GraphScene renders a WebGL <Canvas> which jsdom can't run — stub it out.
 * This also means the test below never actually exercises EdgeLines; it
 * targets GraphTab's own wiring (graphIndex / highlight / detail panel),
 * which is exactly what the regression was in. */
vi.mock("./GraphScene", () => ({
  GraphScene: () => null,
  computeCameraTarget: () => null,
}));

/* Force the edge render budget down to 1 so a tiny two-edge test graph
 * already exercises the "most edges got sampled away for rendering" path,
 * without needing a million-edge fixture. sampleEdges' real (deterministic,
 * stride-based) implementation is kept. */
vi.mock("../lib/edgeBudget", async () => {
  const actual =
    await vi.importActual<typeof import("../lib/edgeBudget")>(
      "../lib/edgeBudget",
    );
  return { ...actual, EDGE_RENDER_LIMIT: 1 };
});

/* bar (id 2) has one inbound edge (foo -> bar) and one outbound edge
 * (bar -> baz). With EDGE_RENDER_LIMIT mocked to 1, only one of the two
 * edges survives sampling for EdgeLines — but graphIndex, the highlight set,
 * and NodeDetailPanel's connections must still see both, since they read
 * the unsampled filtered edge list. */
const DATA: GraphData = {
  nodes: [
    { id: 1, x: 0, y: 0, z: 0, label: "Function", name: "foo", file_path: "src/foo.ts", size: 1, color: "#fff" },
    { id: 2, x: 1, y: 0, z: 0, label: "Function", name: "bar", file_path: "src/bar.ts", size: 1, color: "#fff" },
    { id: 3, x: 2, y: 0, z: 0, label: "Function", name: "baz", file_path: "src/baz.ts", size: 1, color: "#fff" },
  ],
  edges: [
    { source: 1, target: 2, type: "CALLS" },
    { source: 2, target: 3, type: "CALLS" },
  ],
  total_nodes: 3,
};

function mockLayoutFetch(data: GraphData) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.startsWith("/api/layout")) {
      return new Response(JSON.stringify(data), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("{}", { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
}

describe("GraphTab edge render sampling does not affect connectivity", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("still highlights and lists all direct connections for a node whose edges were sampled away for rendering", async () => {
    mockLayoutFetch(DATA);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();

    /* Select bar (id 2), which connects to both foo and baz. */
    fireEvent.click(screen.getByRole("button", { name: /src/ }));
    fireEvent.click(screen.getByRole("button", { name: /^bar/ }));

    /* Highlight must include bar + foo + baz, even though the render
     * budget only kept one of the two edges for EdgeLines. */
    expect(screen.getByText("3 selected")).toBeInTheDocument();

    /* The detail panel must show both connections, not "No connections". */
    expect(screen.getByRole("heading", { name: "bar" })).toBeInTheDocument();
    expect(screen.queryByText("No connections")).not.toBeInTheDocument();
    expect(screen.getByText("References")).toBeInTheDocument();
    expect(screen.getByText("Referenced by")).toBeInTheDocument();

    /* Total connection count (Out + In) is 2. */
    const total = screen.getByText("Total").parentElement;
    expect(total).toHaveTextContent("2");
  });
});

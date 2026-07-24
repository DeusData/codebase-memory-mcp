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

const SELECTION_DATA: GraphData = {
  nodes: [
    /* Multi-node directory: src/ has foo.ts + bar.ts */
    {
      id: 1,
      x: 0, y: 0, z: 0,
      label: "Function",
      name: "foo",
      file_path: "src/foo.ts",
      size: 1,
      color: "#ff0000",
    },
    {
      id: 2,
      x: 1, y: 0, z: 0,
      label: "Class",
      name: "bar",
      file_path: "src/bar.ts",
      size: 1,
      color: "#00ff00",
    },
    /* One-node directory: utils/ has only helper.ts */
    {
      id: 3,
      x: 2, y: 0, z: 0,
      label: "Function",
      name: "helper",
      file_path: "utils/helper.ts",
      size: 1,
      color: "#0000ff",
    },
    /* Node without file_path — reachable only via search */
    {
      id: 4,
      x: 3, y: 0, z: 0,
      label: "Class",
      name: "orphanFn",
      size: 1,
      color: "#ffff00",
    },
  ],
  edges: [
    { source: 1, target: 2, type: "CALLS" },
    { source: 1, target: 3, type: "CALLS" },
  ],
  total_nodes: 4,
};

function mockFetch(data: GraphData) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.startsWith("/api/layout")) {
      return new Response(JSON.stringify(data), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    /* /api/ui-config, /api/repo-info → default empty */
    return new Response("{}", { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("GraphTab selection", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("opens the detail panel when a leaf node is clicked in the file tree", async () => {
    mockFetch(SELECTION_DATA);
    render(<GraphTab project="demo" />);

    /* Wait for the layout to load. */
    expect(await screen.findByText("Filters")).toBeInTheDocument();

    /* Expand the "src" directory. */
    fireEvent.click(screen.getByRole("button", { name: /src/ }));

    /* Click the "foo" file node inside. */
    fireEvent.click(screen.getByRole("button", { name: /^foo/ }));

    /* The detail panel must render the node name as a heading. */
    expect(screen.getByRole("heading", { name: "foo" })).toBeInTheDocument();
  });

  it("does NOT open the detail panel for a directory containing exactly one node", async () => {
    mockFetch(SELECTION_DATA);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();

    /* Click the "utils" directory — it contains exactly 1 node (helper),
     * but Sidebar passes no GraphNode arg for directories. */
    fireEvent.click(screen.getByRole("button", { name: /utils/ }));

    /* The detail panel must NOT open just because nodeIds.size === 1. */
    expect(screen.queryByRole("heading", { name: "helper" })).not.toBeInTheDocument();

    /* The directory IS selected though — HUD shows count. */
    expect(screen.getByText("1 selected")).toBeInTheDocument();
  });

  it("shows the detail panel for a search result that has no file_path", async () => {
    mockFetch(SELECTION_DATA);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();

    /* Search for the orphan node. */
    const searchInput = screen.getByPlaceholderText("Search...");
    fireEvent.change(searchInput, { target: { value: "orphan" } });

    /* Click the search result — Sidebar passes "" for path (no file_path)
     * but still passes the GraphNode as the third argument. */
    fireEvent.click(screen.getByRole("button", { name: /orphanFn/ }));

    /* The detail panel must still appear. */
    expect(screen.getByRole("heading", { name: "orphanFn" })).toBeInTheDocument();
  });

  it("clears selection via the Sidebar 'Clear selection' button", async () => {
    mockFetch(SELECTION_DATA);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();

    /* Select a leaf node first. */
    fireEvent.click(screen.getByRole("button", { name: /src/ }));
    fireEvent.click(screen.getByRole("button", { name: /^foo/ }));
    expect(screen.getByRole("heading", { name: "foo" })).toBeInTheDocument();

    /* Click "Clear selection" — there are two buttons with this label
     * (Sidebar footer + HUD). Both clear, so clicking either is fine. */
    const clearButtons = screen.getAllByRole("button", { name: "Clear selection" });
    fireEvent.click(clearButtons[0]);

    /* Detail panel must disappear. */
    expect(screen.queryByRole("heading", { name: "foo" })).not.toBeInTheDocument();
  });

  it("highlights the selected node and its direct neighbors", async () => {
    mockFetch(SELECTION_DATA);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();

    /* Select foo (id:1), which has edges to bar (id:2) and helper (id:3). */
    fireEvent.click(screen.getByRole("button", { name: /src/ }));
    fireEvent.click(screen.getByRole("button", { name: /^foo/ }));

    /* foo + bar + helper = 3 nodes highlighted. */
    expect(screen.getByText("3 selected")).toBeInTheDocument();
  });
});

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
    {
      id: 1, x: 0, y: 0, z: 0, label: "Function", name: "orphan",
      file_path: "src/orphan.ts", size: 1, color: "#fff", status: "dead", in_calls: 0,
    },
    {
      id: 2, x: 1, y: 0, z: 0, label: "Function", name: "used",
      file_path: "src/used.ts", size: 1, color: "#fff", status: "normal", in_calls: 3,
    },
  ],
  edges: [{ source: 2, target: 1, type: "CALLS" }],
  total_nodes: 2,
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
  return fetchMock;
}

describe("GraphTab dead-code filters", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("shows the dead count and filters to only dead code on toggle", async () => {
    mockLayoutFetch(SAMPLE);
    render(<GraphTab project="demo" />);

    /* Panel loaded; the dead-code section reports one dead node. */
    expect(await screen.findByText("Filters")).toBeInTheDocument();
    expect(screen.getByText("1 dead")).toBeInTheDocument();

    /* Both nodes visible initially — no "filtered from" notice. */
    expect(screen.queryByText(/filtered from/)).not.toBeInTheDocument();

    /* Toggling "Show only dead code" hides the non-dead node. */
    fireEvent.click(screen.getByRole("button", { name: /Show only dead code/ }));
    expect(await screen.findByText(/filtered from 2/)).toBeInTheDocument();
  });
});

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

/* Server-classified layout: three unreferenced-coverage candidates (constructor,
 * framework method, callback) plus nodes that must not enter that set just
 * because in_calls is zero or absent. Edges match the reported degrees. */
const SAMPLE: GraphData = {
  nodes: [
    {
      id: 1, x: 0, y: 0, z: 0, label: "Method", name: "constructor",
      file_path: "src/widget.ts", size: 1, color: "#fff", status: "dead", in_calls: 0,
    },
    {
      id: 2, x: 1, y: 0, z: 0, label: "Method", name: "onCreate",
      file_path: "src/activity.ts", size: 1, color: "#fff", status: "dead", in_calls: 0,
    },
    {
      id: 3, x: 2, y: 0, z: 0, label: "Function", name: "handleClick",
      file_path: "src/ui.ts", size: 1, color: "#fff", status: "dead", in_calls: 0,
    },
    {
      id: 4, x: 3, y: 0, z: 0, label: "Function", name: "processRequest",
      file_path: "src/used.ts", size: 1, color: "#fff", status: "normal", in_calls: 2,
    },
    {
      id: 5, x: 4, y: 0, z: 0, label: "Function", name: "formatName",
      file_path: "src/format.ts", size: 1, color: "#fff", status: "normal", in_calls: 0,
    },
    {
      id: 6, x: 5, y: 0, z: 0, label: "Function", name: "main",
      file_path: "src/main.ts", size: 1, color: "#fff", status: "entry", in_calls: 0,
    },
    {
      id: 7, x: 6, y: 0, z: 0, label: "Function", name: "testProcess",
      file_path: "src/used.test.ts", size: 1, color: "#fff", status: "test", in_calls: 0,
    },
    {
      id: 8, x: 7, y: 0, z: 0, label: "Function", name: "publicApi",
      file_path: "src/api.ts", size: 1, color: "#fff", status: "exported", in_calls: 0,
    },
    {
      id: 9, x: 8, y: 0, z: 0, label: "Class", name: "App",
      file_path: "src/app.ts", size: 1, color: "#fff", status: "structural",
    },
    {
      id: 10, x: 9, y: 0, z: 0, label: "Function", name: "mystery",
      file_path: "src/mystery.ts", size: 1, color: "#fff", in_calls: 0,
    },
  ],
  edges: [
    { source: 6, target: 4, type: "CALLS" },
    { source: 8, target: 4, type: "CALLS" },
    { source: 4, target: 5, type: "USAGE" },
  ],
  total_nodes: 10,
};

const NO_CANDIDATES: GraphData = {
  nodes: [
    {
      id: 1, x: 0, y: 0, z: 0, label: "Function", name: "used",
      file_path: "src/used.ts", size: 1, color: "#fff", status: "single", in_calls: 1,
    },
    {
      id: 2, x: 1, y: 0, z: 0, label: "Function", name: "main",
      file_path: "src/main.ts", size: 1, color: "#fff", status: "entry", in_calls: 0,
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

  it("reports no known inbound references without calling the code dead or live", async () => {
    mockLayoutFetch(SAMPLE);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();
    expect(screen.getByText("Reference coverage")).toBeInTheDocument();
    expect(screen.getByText("3 with no known inbound references")).toBeInTheDocument();
    expect(
      screen.getByText(
        "No known inbound references does not prove code is unused. Static analysis may miss constructors, implicit or framework calls, and callbacks.",
      ),
    ).toBeInTheDocument();
    expect(screen.queryByText("Dead code")).not.toBeInTheDocument();
    expect(screen.queryByText(/^\d+ dead$/)).not.toBeInTheDocument();
    expect(screen.queryByText("Show only dead code")).not.toBeInTheDocument();
    expect(screen.queryByText("Dead (0 callers)")).not.toBeInTheDocument();
    /* Caveat is visible before status coloring; legend is not. */
    expect(screen.queryByText("No known inbound references")).not.toBeInTheDocument();
    expect(screen.queryByText(/filtered from/)).not.toBeInTheDocument();
  });

  it("colors the legend as coverage, not as proven dead code", async () => {
    mockLayoutFetch(SAMPLE);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Color by status" }));

    expect(screen.getByText("No known inbound references")).toBeInTheDocument();
    expect(screen.getByText("One caller")).toBeInTheDocument();
    expect(screen.queryByText("Dead code")).not.toBeInTheDocument();
    expect(screen.queryByText(/^\d+ dead$/)).not.toBeInTheDocument();
    expect(screen.queryByText("Show only dead code")).not.toBeInTheDocument();
    expect(screen.queryByText("Dead (0 callers)")).not.toBeInTheDocument();
  });

  it("filters to the backend dead category and restores the full layout", async () => {
    mockLayoutFetch(SAMPLE);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();
    expect(screen.getByText(/10 nodes/)).toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", {
        name: /Show only nodes with no known inbound references/,
      }),
    );
    expect(await screen.findByText(/filtered from 10/)).toBeInTheDocument();
    expect(screen.getByText(/3 nodes/)).toBeInTheDocument();

    const search = screen.getByPlaceholderText("Search...");
    for (const name of ["constructor", "onCreate", "handleClick"]) {
      fireEvent.change(search, { target: { value: name } });
      expect(screen.getByRole("button", { name: new RegExp(name) })).toBeInTheDocument();
    }
    /* Server-provided non-dead statuses stay out, even with in_calls of 0. */
    for (const name of ["processRequest", "formatName", "main", "testProcess", "publicApi", "App", "mystery"]) {
      fireEvent.change(search, { target: { value: name } });
      expect(screen.queryByRole("button", { name: new RegExp(name) })).not.toBeInTheDocument();
    }

    fireEvent.click(
      screen.getByRole("button", {
        name: /Show only nodes with no known inbound references/,
      }),
    );
    expect(screen.queryByText(/filtered from/)).not.toBeInTheDocument();
    expect(screen.getByText(/10 nodes/)).toBeInTheDocument();
  });

  it("shows a zero count and empty candidate selection when none are classified dead", async () => {
    mockLayoutFetch(NO_CANDIDATES);
    render(<GraphTab project="demo" />);

    expect(await screen.findByText("Filters")).toBeInTheDocument();
    expect(screen.getByText("0 with no known inbound references")).toBeInTheDocument();
    expect(screen.getByText(/2 nodes/)).toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", {
        name: /Show only nodes with no known inbound references/,
      }),
    );
    expect(screen.getByText("All nodes filtered out")).toBeInTheDocument();
    expect(screen.getByText("Filters")).toBeInTheDocument();
    expect(screen.getByText("0 with no known inbound references")).toBeInTheDocument();
  });
});

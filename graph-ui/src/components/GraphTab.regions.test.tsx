/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { GraphTab } from "./GraphTab";
import type { GraphData, RegionsPayload } from "../lib/types";

vi.mock("./GraphScene", () => ({
  GraphScene: () => null,
  computeCameraTarget: () => null,
}));

const REGIONS: RegionsPayload = {
  level: "regions",
  method: "leiden+folders",
  total_nodes: 48000,
  unmapped_nodes: 12,
  regions: [
    {
      id: 0,
      name: "src/alpha",
      hub: "alpha_one",
      why: "call community: 2 files, 100% under src/alpha",
      files: 2,
      members: 24000,
      cohesion: 0.8,
      top_nodes: ["alpha_one", "alpha_two"],
      x: 0,
      y: 0,
      z: 0,
      size: 60,
      color: "#22aa88",
    },
    {
      id: 1,
      name: "src/beta",
      hub: "beta_one",
      why: "call community: 2 files, 100% under src/beta",
      files: 2,
      members: 24000,
      cohesion: 0.7,
      top_nodes: ["beta_one"],
      x: 100,
      y: 0,
      z: 0,
      size: 60,
      color: "#8822aa",
    },
  ],
  edges: [{ source: 0, target: 1, weight: 4 }],
};

const SCOPED: GraphData = {
  nodes: [
    {
      id: 7, x: 0, y: 0, z: 0, label: "Function", name: "alpha_one",
      file_path: "src/alpha/a1.c", size: 1, color: "#fff",
    },
  ],
  edges: [],
  total_nodes: 1,
};

function mockAtlasFetch() {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.startsWith("/api/layout")) {
      if (url.includes("level=regions")) {
        return new Response(JSON.stringify(REGIONS), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response(JSON.stringify(SCOPED), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("{}", { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("GraphTab region level", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("opens large projects on the region scene, not the full galaxy", async () => {
    const fetchMock = mockAtlasFetch();
    render(<GraphTab project="demo" />);

    /* The region list is the left panel; both regions are listed. */
    expect(
      (await screen.findAllByText("src/alpha")).some((el) => el.tagName === "SPAN"),
    ).toBe(true);
    expect(
      screen.getAllByText("src/beta").some((el) => el.tagName === "SPAN"),
    ).toBe(true);
    expect(screen.getByRole("button", { name: "Load full galaxy" })).toBeInTheDocument();

    /* No full layout was fetched — only the region probe. */
    const urls = fetchMock.mock.calls.map((call) => String(call[0]));
    expect(urls.some((u) => u.includes("level=regions"))).toBe(true);
    expect(urls.some((u) => u.includes("max_nodes"))).toBe(false);
  });

  it("selects a region into its panel and opens it as a scoped layout", async () => {
    const fetchMock = mockAtlasFetch();
    render(<GraphTab project="demo" />);

    fireEvent.click(
      (await screen.findAllByText("src/alpha")).find((el) => el.tagName === "SPAN")!,
    );
    /* The region panel shows provenance and the door in. */
    expect(
      screen.getByText("call community: 2 files, 100% under src/alpha"),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Open region →" }));

    /* The scoped node arrives and the breadcrumb back to regions exists. */
    expect(await screen.findByText("region: src/alpha")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "‹ Regions" })).toBeInTheDocument();
    const urls = fetchMock.mock.calls.map((call) => String(call[0]));
    expect(urls.some((u) => u.includes("scope=region%3A0"))).toBe(true);
  });

  it("restores an opened region from the URL (?region=)", async () => {
    const fetchMock = mockAtlasFetch();
    render(<GraphTab project="demo" routeRegion="1" />);

    expect(await screen.findByText("region: src/beta")).toBeInTheDocument();
    const urls = fetchMock.mock.calls.map((call) => String(call[0]));
    expect(urls.some((u) => u.includes("scope=region%3A1"))).toBe(true);
  });

  it("reports selected nodes to the route for deep links", async () => {
    mockAtlasFetch();
    const onRouteChange = vi.fn();
    render(
      <GraphTab project="demo" routeRegion="0" onRouteChange={onRouteChange} />,
    );
    expect(await screen.findByText("region: src/alpha")).toBeInTheDocument();

    /* Select the only node via the sidebar symbol row (#1197). */
    fireEvent.click(
      screen.getAllByText("src/alpha").find((el) => el.tagName === "SPAN")!,
    ); /* expand the tree */
    fireEvent.click(await screen.findByText("alpha_one"));
    expect(onRouteChange).toHaveBeenCalledWith("7", "0");
    /* The detail panel now shows the node, not a stale or empty panel. */
    expect(screen.getByRole("heading", { name: "alpha_one" })).toBeInTheDocument();
  });
});

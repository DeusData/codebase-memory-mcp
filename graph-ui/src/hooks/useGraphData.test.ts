import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchLayout, GRAPH_RENDER_NODE_LIMIT } from "./useGraphData";

describe("fetchLayout", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("uses the safe graph render cap by default", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({ nodes: [], edges: [], total_nodes: 0 }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchLayout("large-project");

    expect(GRAPH_RENDER_NODE_LIMIT).toBe(60000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const calls = fetchMock.mock.calls as unknown as Array<[string]>;
    const [url] = calls[0];
    expect(url).toBe(
      "/api/layout?project=large-project&max_nodes=2000",
    );
  });
});

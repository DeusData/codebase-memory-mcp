import { describe, expect, it } from "vitest";
import {
  disambiguateRegionNames,
  regionsToGraphData,
  regionsViewWorthwhile,
  REGIONS_MIN_TOTAL_NODES,
  cohesionWord,
  localizeRegionWhy,
} from "./regions";
import { messages } from "./i18n";
import { minimapLayout, minimapProject } from "../components/Minimap";
import type { Region, RegionsPayload } from "./types";

function region(partial: Partial<Region> & { id: number; name: string }): Region {
  return {
    files: 2,
    members: 10,
    cohesion: 0.5,
    top_nodes: ["a"],
    x: 0,
    y: 0,
    z: 0,
    size: 20,
    color: "#123456",
    ...partial,
  };
}

function payload(regions: Region[], total = 10000): RegionsPayload {
  return {
    level: "regions",
    method: "leiden+folders",
    total_nodes: total,
    unmapped_nodes: 0,
    regions,
    edges: [{ source: 0, target: 1, weight: 3 }],
  };
}

describe("region adapters", () => {
  it("appends the hub to duplicate display names", () => {
    const out = disambiguateRegionNames([
      region({ id: 0, name: "django/db", hub: "join" }),
      region({ id: 1, name: "django/db", hub: "get_field" }),
      region({ id: 2, name: "django/core", hub: "call_command" }),
    ]);
    expect(out.map((r) => r.name)).toEqual([
      "django/db · join",
      "django/db · get_field",
      "django/core",
    ]);
  });

  it("adapts regions to scene nodes and REGION edges", () => {
    const data = regionsToGraphData(
      payload([region({ id: 0, name: "a" }), region({ id: 1, name: "b" })]),
    );
    expect(data.nodes).toHaveLength(2);
    expect(data.nodes[0].label).toBe("Region");
    expect(data.edges).toEqual([{ source: 0, target: 1, type: "REGION" }]);
    expect(data.total_nodes).toBe(10000);
  });

  it("skips the region scene for small or single-region projects", () => {
    const two = [region({ id: 0, name: "a" }), region({ id: 1, name: "b" })];
    expect(regionsViewWorthwhile(payload(two, REGIONS_MIN_TOTAL_NODES))).toBe(false);
    expect(regionsViewWorthwhile(payload(two, REGIONS_MIN_TOTAL_NODES + 1))).toBe(true);
    expect(
      regionsViewWorthwhile(payload([region({ id: 0, name: "a" })], 99999)),
    ).toBe(false);
  });
});

describe("localizeRegionWhy", () => {
  const templates = [
    "call community: 23 files, 87% under src/parser",
    "folder group (not explained by a kept call community)",
    "files outside every kept community and folder group",
    "some future server template",
  ];
  it("passes the server's English through byte-identically", () => {
    for (const why of templates) expect(localizeRegionWhy(why)).toBe(why);
  });
  it("recomposes the known templates in zh, unknown ones verbatim", () => {
    const zh = messages.zh.regions;
    expect(localizeRegionWhy(templates[0], zh)).toBe(
      "调用社区：23 个文件，87% 位于 src/parser 之下",
    );
    expect(localizeRegionWhy(templates[1], zh)).toBe(
      "目录分组（未被任何保留的调用社区解释）",
    );
    expect(localizeRegionWhy(templates[2], zh)).toBe(
      "游离于所有保留社区与目录分组之外的文件",
    );
    expect(localizeRegionWhy(templates[3], zh)).toBe(templates[3]);
  });
});

describe("cohesionWord", () => {
  it("maps scores to the 0.7/0.4 word ladder", () => {
    expect(cohesionWord(0.84)).toBe("tightly connected");
    expect(cohesionWord(0.62)).toBe("moderately connected");
    expect(cohesionWord(0.11)).toBe("loosely connected");
  });
});

describe("minimapLayout", () => {
  const region = (id: number, x: number, y: number, members: number): Region => ({
    id,
    name: `r${id}`,
    files: 1,
    members,
    cohesion: 0.5,
    top_nodes: [],
    x,
    y,
    z: 0,
    size: 10,
    color: "#123456",
  });
  it("projects region positions into the padded viewport", () => {
    const dots = minimapLayout([region(0, -100, 0, 100), region(1, 100, 50, 25)], 200, 150);
    expect(dots).toHaveLength(2);
    expect(dots[0].cx).toBe(12);
    expect(dots[1].cx).toBe(188);
    expect(dots[0].cy).toBe(12);
    expect(dots[1].cy).toBe(138);
    /* Biggest region gets the biggest dot; both stay in bounds. */
    expect(dots[0].r).toBeGreaterThan(dots[1].r);
    for (const dot of dots) {
      expect(dot.cx).toBeGreaterThanOrEqual(0);
      expect(dot.cx).toBeLessThanOrEqual(200);
    }
  });
  it("handles a single region without dividing by zero", () => {
    const dots = minimapLayout([region(7, 42, 42, 10)], 200, 150);
    expect(dots).toHaveLength(1);
    expect(Number.isFinite(dots[0].cx)).toBe(true);
  });
});

describe("minimapProject", () => {
  const region = (id: number, x: number, y: number): Region => ({
    id,
    name: `r${id}`,
    files: 1,
    members: 10,
    cohesion: 0.5,
    top_nodes: [],
    x,
    y,
    z: 0,
    size: 10,
    color: "#123456",
  });
  const regions = [region(0, -100, 0), region(1, 100, 50)];
  it("maps a world point with the same bounds as the dots", () => {
    const point = minimapProject(regions, 200, 150, { x: -100, y: 0 });
    expect(point).toEqual({ cx: 12, cy: 12 });
  });
  it("returns null for points far outside the map", () => {
    expect(minimapProject(regions, 200, 150, { x: 100000, y: 0 })).toBeNull();
  });
});

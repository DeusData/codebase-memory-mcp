import { describe, expect, it } from "vitest";
import { commitUrl, linkifyRefs, refUrl } from "./rationale";

describe("linkifyRefs", () => {
  it("splits a multi-ref subject into plain and ref segments", () => {
    expect(linkifyRefs("fix(ui): honest axes (#123, closes #45)")).toEqual([
      { text: "fix(ui): honest axes (" },
      { text: "#123", ref: 123 },
      { text: ", closes " },
      { text: "#45", ref: 45 },
      { text: ")" },
    ]);
  });

  it("keeps a ref-free subject as one plain segment", () => {
    expect(linkifyRefs("chore: bump version")).toEqual([
      { text: "chore: bump version" },
    ]);
  });

  it("handles refs at the start and the end of the subject", () => {
    expect(linkifyRefs("#7 then #8")).toEqual([
      { text: "#7", ref: 7 },
      { text: " then " },
      { text: "#8", ref: 8 },
    ]);
  });

  it("leaves '#' without digits as plain text", () => {
    expect(linkifyRefs("port to c# — see issue #")).toEqual([
      { text: "port to c# — see issue #" },
    ]);
  });
});

describe("forge URLs", () => {
  it("builds commit and issue/PR URLs from the remote base", () => {
    /* A loopback base: the composition is host-agnostic, and the UI
     * security audit forbids literal external URLs in this tree. */
    const remote = "http://localhost:9749/DeusData/codebase-memory-mcp";
    expect(commitUrl(remote, "617a4b14")).toBe(
      "http://localhost:9749/DeusData/codebase-memory-mcp/commit/617a4b14",
    );
    expect(refUrl(remote, 123)).toBe(
      "http://localhost:9749/DeusData/codebase-memory-mcp/issues/123",
    );
  });
});

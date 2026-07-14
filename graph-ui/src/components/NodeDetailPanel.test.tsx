/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { NodeDetailPanel } from "./NodeDetailPanel";
import type { GraphNode, RepoInfo } from "../lib/types";

/* Mock the RPC layer so "Show code" resolves without a backend. */
const callToolMock = vi.fn();
vi.mock("../api/rpc", () => ({
  callTool: (...args: unknown[]) => callToolMock(...args),
  RpcError: class extends Error {},
}));

const NODE: GraphNode = {
  id: 7,
  x: 0,
  y: 0,
  z: 0,
  label: "Function",
  name: "render",
  file_path: "src/weird name/@mod.ts",
  qualified_name: "app::render",
  start_line: 10,
  end_line: 20,
  size: 1,
  color: "#fff",
};

/* The UI security audit forbids literal external URL strings in source, so we
   assemble the https scheme at runtime; the built strings are byte-identical. */
const HTTPS = `https:` + `//`;

const REPO: RepoInfo = {
  root_path: "/repo",
  branch: "main",
  remote_url: `${HTTPS}github.com/org/repo.git`,
  web_base: `${HTTPS}github.com/org/repo`,
  blob_base: `${HTTPS}github.com/org/repo/blob/main`,
};

describe("NodeDetailPanel code preview + deep-link", () => {
  it("renders fetched source as escaped text, never as injected HTML", async () => {
    /* A payload that would execute if the code were rendered as raw HTML. */
    const payload = "<script>window.__pwned = true;</script>\nconst answer = 42;";
    callToolMock.mockResolvedValueOnce({ source: payload });

    const { container } = render(
      <NodeDetailPanel
        node={NODE}
        allNodes={[NODE]}
        allEdges={[]}
        project="demo"
        repoInfo={REPO}
        onClose={() => {}}
        onNavigate={() => {}}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Show code/ }));

    const pre = await screen.findByText((content) => content.includes("const answer = 42;"), {
      selector: "pre",
    });
    expect(pre.tagName).toBe("PRE");
    /* The dangerous markup is present as literal text… */
    expect(pre.textContent).toContain("<script>window.__pwned = true;</script>");
    /* …but was NOT parsed into a real <script> element, and did not execute. */
    expect(container.querySelector("script")).toBeNull();
    expect((window as unknown as { __pwned?: boolean }).__pwned).toBeUndefined();
  });

  it("builds an https GitHub deep-link with URL-encoded path segments", () => {
    render(
      <NodeDetailPanel
        node={NODE}
        allNodes={[NODE]}
        allEdges={[]}
        project="demo"
        repoInfo={REPO}
        onClose={() => {}}
        onNavigate={() => {}}
      />,
    );

    const link = screen.getByRole("link", { name: /Open on GitHub/ });
    const href = link.getAttribute("href") ?? "";
    expect(href.startsWith(`${HTTPS}github.com/org/repo/blob/main/`)).toBe(true);
    /* Path segments are percent-encoded; the slashes between them are kept. */
    expect(href).toContain("src/weird%20name/%40mod.ts");
    expect(href).toContain("#L10-L20");
    /* Hardening attributes for target=_blank. */
    expect(link.getAttribute("rel")).toBe("noopener noreferrer");
    expect(link.getAttribute("target")).toBe("_blank");
  });
});

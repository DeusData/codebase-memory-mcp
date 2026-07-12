/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DesignTab } from "./DesignTab";

const callToolMock = vi.fn();
vi.mock("../api/rpc", () => ({
  callTool: (...args: unknown[]) => callToolMock(...args),
}));

const RESPONSE = {
  project: "demo",
  status: "ready",
  boundary: "project-local",
  total: { systems: 1, tokens: 2, components: 1, modes: 0 },
  systems: [
    {
      id: 1,
      name: "Aurora",
      qualified_name: "demo.design.system.root",
      file_path: "DESIGN.md",
      line: 1,
      properties: { scope: "root", provenance: "authoritative" },
    },
  ],
  tokens: [
    {
      id: 2,
      name: "action",
      qualified_name: "demo.design.token.root.color.action",
      file_path: "design/tokens/core.tokens.json",
      line: 1,
      properties: {
        scope: "root",
        token_path: "color.action",
        token_type: "color",
        value: "#123456",
        provenance: "authoritative",
      },
    },
    {
      id: 3,
      name: "sm",
      qualified_name: "demo.design.token.root.spacing.sm",
      file_path: "DESIGN.md",
      line: 8,
      properties: {
        scope: "root",
        token_path: "spacing.sm",
        token_type: "dimension",
        value: "8px",
        provenance: "authoritative",
      },
    },
  ],
  components: [
    {
      id: 4,
      name: "button-primary",
      qualified_name: "demo.design.component.root.button-primary",
      file_path: "DESIGN.md",
      line: 12,
      properties: { scope: "root", provenance: "authoritative" },
    },
  ],
  modes: [],
  relations: [
    {
      type: "USES_TOKEN",
      source: "demo.src.styles.app.__file__",
      source_label: "File",
      target: "demo.design.token.root.color.action",
      target_label: "DesignToken",
      properties: { line: 4 },
    },
  ],
  returned_relations: 1,
  has_more: false,
};

describe("DesignTab", () => {
  beforeEach(() => {
    callToolMock.mockResolvedValue(RESPONSE);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(JSON.stringify({ lang: "en" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
  });

  afterEach(() => {
    cleanup();
    callToolMock.mockReset();
    vi.unstubAllGlobals();
  });

  it("loads curated context and filters token cards", async () => {
    render(<DesignTab project="demo" />);
    expect((await screen.findAllByText("color.action")).length).toBeGreaterThan(0);
    expect(screen.getByText("button-primary")).toBeInTheDocument();
    expect(screen.getByText(/USES TOKEN/)).toBeInTheDocument();
    expect(callToolMock).toHaveBeenCalledWith("get_design_context", {
      project: "demo",
      limit: 1000,
      offset: 0,
      relation_offset: 0,
    });

    fireEvent.change(screen.getByPlaceholderText("Search token names and paths..."), {
      target: { value: "spacing" },
    });
    expect(screen.getAllByText("spacing.sm").length).toBeGreaterThan(0);
    expect(screen.queryAllByText("color.action")).toHaveLength(0);
  });

  it("does not call the backend without a selected project", () => {
    render(<DesignTab project={null} />);
    expect(screen.getByText("Select a project to inspect its design context.")).toBeInTheDocument();
    expect(callToolMock).not.toHaveBeenCalled();
  });

  it("loads every page and applies exact scope filtering to all artifact types", async () => {
    const pageOne = {
      ...RESPONSE,
      total: { systems: 2, tokens: 2, components: 2, modes: 2 },
      systems: [
        {
          id: 10,
          name: "App",
          qualified_name: "demo.design.system.packages.app",
          file_path: "packages/app/DESIGN.md",
          line: 1,
          properties: { scope: "packages.app", provenance: "authoritative" },
        },
        {
          id: 11,
          name: "Application",
          qualified_name: "demo.design.system.packages.application",
          file_path: "packages/application/DESIGN.md",
          line: 1,
          properties: { scope: "packages.application", provenance: "authoritative" },
        },
      ],
      tokens: [
        { ...RESPONSE.tokens[0], properties: { ...RESPONSE.tokens[0].properties, scope: "packages.app" } },
      ],
      components: [
        { ...RESPONSE.components[0], name: "app-button", properties: { scope: "packages.app" } },
        {
          ...RESPONSE.components[0],
          id: 12,
          name: "application-button",
          qualified_name: "demo.design.component.packages.application.button",
          properties: { scope: "packages.application" },
        },
      ],
      modes: [
        {
          id: 13,
          name: "theme: app",
          qualified_name: "demo.design.mode.packages.app.theme.app",
          file_path: "packages/app/theme.resolver.json",
          line: 1,
          properties: { scope: "packages.app", modifier: "theme", context: "app" },
        },
        {
          id: 14,
          name: "theme: application",
          qualified_name: "demo.design.mode.packages.application.theme.application",
          file_path: "packages/application/theme.resolver.json",
          line: 1,
          properties: { scope: "packages.application", modifier: "theme", context: "application" },
        },
      ],
      has_more: true,
    };
    const pageTwo = {
      ...RESPONSE,
      total: pageOne.total,
      systems: [],
      tokens: [
        { ...RESPONSE.tokens[1], properties: { ...RESPONSE.tokens[1].properties, scope: "packages.application" } },
      ],
      components: [],
      modes: [],
      relations: [],
      returned_relations: 0,
      has_more: false,
    };
    callToolMock.mockImplementation((_tool: string, args: { offset: number }) =>
      Promise.resolve(args.offset === 0 ? pageOne : pageTwo),
    );

    render(<DesignTab project="demo" />);
    expect(await screen.findByText("app-button")).toBeInTheDocument();
    expect(screen.getByText("application-button")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /App packages\.app/ }));
    expect(screen.getByText("app-button")).toBeInTheDocument();
    expect(screen.queryByText("application-button")).not.toBeInTheDocument();
    expect(screen.getByText("theme: app")).toBeInTheDocument();
    expect(screen.queryByText("theme: application")).not.toBeInTheDocument();
    expect(callToolMock).toHaveBeenCalledTimes(2);
  });
});

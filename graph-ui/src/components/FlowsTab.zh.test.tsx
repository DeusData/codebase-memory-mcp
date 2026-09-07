/* @vitest-environment jsdom */
/* The zh locale reaches the tab exactly as it does in production: the
 * /api/ui-config override that detectLanguage honours (see i18n.test.ts).
 * The i18n module caches the resolved language per module registry, so the
 * zh render lives in its own file — FlowsTab.observed.test.tsx stays English. */
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FlowsTab } from "./FlowsTab";

describe("FlowsTab (zh locale)", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders the chrome — headings, pickers, counts, empty states — in Chinese", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation((url: string) => {
        if (String(url).includes("/api/ui-config")) {
          return Promise.resolve({ json: () => Promise.resolve({ lang: "zh" }) });
        }
        /* /api/flows — no journeys, so the empty states render too. */
        return Promise.resolve({
          ok: true,
          json: () =>
            Promise.resolve({ flows: [], callable_total: 1234, candidates_dropped: 5 }),
        });
      }),
    );
    render(
      <FlowsTab
        project="demo"
        flowId={null}
        onOpenFlow={() => {}}
        onOpenSymbol={() => {}}
        onOpenWiki={() => {}}
      />,
    );

    expect(await screen.findByText("追踪 A → B")).toBeInTheDocument();
    expect(screen.getByText("流程——入口 → 终点")).toBeInTheDocument();
    /* The counts line keeps en-US number formatting; the dropped tail rides along. */
    expect(
      screen.getByText("1,234 个可调用符号中走出 0 条调用链路 · 5 个候选未遍历"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("未检测到流程——项目可能没有明确的入口点。"),
    ).toBeInTheDocument();
    expect(screen.getByText("在左侧选择一条调用链路")).toBeInTheDocument();
    /* Placeholders and the mode toggle come from the zh catalog too. */
    expect(screen.getByPlaceholderText("起点符号…")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("终点符号…")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "控制流" })).toBeInTheDocument();
  });
});

/* @vitest-environment jsdom */
/* The zh locale reaches the panel exactly as it does in production: the
 * /api/ui-config override that detectLanguage honours (see i18n.test.ts).
 * The i18n module caches the resolved language per module registry, so the
 * zh render lives in its own file — WikiPanel.test.tsx stays English. */
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { wikiEntry } from "../wiki/entries";
import { WikiPanel } from "./WikiPanel";

describe("WikiPanel (zh locale)", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("shows the zh sentence and gloss under the English-canonical term", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ json: () => Promise.resolve({ lang: "zh" }) }),
    );
    render(<WikiPanel slug="hotspot" onClose={() => {}} onNavigate={() => {}} />);

    const entry = wikiEntry("hotspot")!;
    expect(await screen.findByText(entry.zh!.sentence!)).toBeInTheDocument();
    /* The term stays English-canonical; the zh gloss rides beside it. */
    expect(screen.getByRole("heading", { name: "hotspot" })).toBeInTheDocument();
    expect(screen.getByText(entry.zh!.gloss!)).toBeInTheDocument();
    /* Section chrome comes from the zh catalog; caps render translated. */
    expect(screen.getByText("为什么重要")).toBeInTheDocument();
    expect(screen.getByText(entry.zh!.why!)).toBeInTheDocument();
    expect(screen.getByText(`⚠ ${entry.zh!.caps![0]}`)).toBeInTheDocument();
  });
});

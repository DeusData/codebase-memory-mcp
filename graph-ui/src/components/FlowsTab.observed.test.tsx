/* @vitest-environment jsdom */
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ObservedChip } from "./FlowsTab";

/* The chip is exercised in isolation — FlowsTab itself fetches on mount.
 * The all-observed line logic lives in lib/observed.ts and is tested there. */
describe("ObservedChip", () => {
  const observed = {
    count: 3,
    label: "pytest 2026-08-27",
    last_seen: "2026-08-27T14:12:00Z",
  };

  it("renders glyph, word and count, with the run in the title", () => {
    render(<ObservedChip observed={observed} />);
    const chip = screen.getByTitle("ran in pytest 2026-08-27 · last 2026-08-27");
    expect(chip).toHaveTextContent("▶ observed ×3");
    /* No onOpenWiki → plain text, no nested button (flow-step rows are buttons). */
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("opens the wiki entry from the marker word when onOpenWiki is given", () => {
    const onOpenWiki = vi.fn();
    render(<ObservedChip observed={observed} onOpenWiki={onOpenWiki} />);
    fireEvent.click(screen.getByRole("button", { name: "observed" }));
    expect(onOpenWiki).toHaveBeenCalledWith("observed");
  });
});

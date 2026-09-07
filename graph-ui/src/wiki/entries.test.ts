import { describe, expect, it } from "vitest";
import { WIKI_ENTRIES, localizeEntry, wikiEntry, wikiEntryById } from "./entries";

describe("wiki entries", () => {
  it("every sentence is present and tooltip-sized (≤30 words)", () => {
    for (const entry of WIKI_ENTRIES) {
      expect(entry.sentence.trim().length, entry.slug).toBeGreaterThan(0);
      expect(
        entry.sentence.trim().split(/\s+/).length,
        `${entry.slug} sentence exceeds 30 words`,
      ).toBeLessThanOrEqual(30);
    }
  });

  it("ids and slugs are unique", () => {
    const ids = WIKI_ENTRIES.map((entry) => entry.id);
    const slugs = WIKI_ENTRIES.map((entry) => entry.slug);
    expect(new Set(ids).size).toBe(ids.length);
    expect(new Set(slugs).size).toBe(slugs.length);
  });

  it("every non-refused entry names what it does not cover", () => {
    for (const entry of WIKI_ENTRIES) {
      if (entry.tier === "refused") continue;
      expect(entry.notCovered?.trim(), entry.slug).toBeTruthy();
    }
  });

  it("every pairedWith and seeAlso slug resolves to an existing entry", () => {
    for (const entry of WIKI_ENTRIES) {
      if (entry.pairedWith !== undefined) {
        expect(wikiEntry(entry.pairedWith), `${entry.slug} → ${entry.pairedWith}`).toBeDefined();
      }
      for (const other of entry.seeAlso ?? []) {
        expect(wikiEntry(other), `${entry.slug} → ${other}`).toBeDefined();
      }
    }
  });

  it("ids follow the M-… register format", () => {
    for (const entry of WIKI_ENTRIES) {
      expect(entry.id, entry.slug).toMatch(/^M-[A-Z-]+$/);
    }
  });

  it("lookup helpers resolve by slug and by id", () => {
    expect(wikiEntry("hotspot")?.id).toBe("M-HOTSPOT");
    expect(wikiEntryById("M-HOTSPOT")?.slug).toBe("hotspot");
    expect(wikiEntry("no-such-metric")).toBeUndefined();
    expect(wikiEntryById("M-NO-SUCH")).toBeUndefined();
  });

  it("every entry carries zh content — sentence + notCovered, or + why when refused", () => {
    for (const entry of WIKI_ENTRIES) {
      expect(entry.zh?.sentence?.trim(), entry.slug).toBeTruthy();
      if (entry.tier === "refused") {
        expect(entry.zh?.why?.trim(), entry.slug).toBeTruthy();
      } else {
        expect(entry.zh?.notCovered?.trim(), entry.slug).toBeTruthy();
      }
    }
  });

  it("every zh sentence stays tooltip-sized (≤45 characters — zh is denser)", () => {
    for (const entry of WIKI_ENTRIES) {
      if (!entry.zh?.sentence) continue;
      expect(
        [...entry.zh.sentence.trim()].length,
        `${entry.slug} zh sentence exceeds 45 characters`,
      ).toBeLessThanOrEqual(45);
    }
  });

  it("localizeEntry swaps content per field and keeps the term English", () => {
    const entry = wikiEntry("hotspot")!;
    const zh = localizeEntry(entry, "zh");
    expect(zh.term).toBe("hotspot");
    expect(zh.sentence).toBe(entry.zh!.sentence);
    expect(zh.gloss).toBe(entry.zh!.gloss);
    /* computedParts has no zh variant — the English falls through. */
    expect(zh.computedParts).toEqual(entry.computedParts);
    const en = localizeEntry(entry, "en");
    expect(en.sentence).toBe(entry.sentence);
    expect(en.gloss).toBeUndefined();
  });
});

import { describe, expect, it } from "vitest";
import { QUESTION_FAMILIES, localizeQuestion, questionStatusCounts } from "./questions";

describe("QUESTION_FAMILIES", () => {
  it("carries exactly 18 families with unique ids F1..F18", () => {
    expect(QUESTION_FAMILIES).toHaveLength(18);
    expect(QUESTION_FAMILIES.map((family) => family.id)).toEqual(
      Array.from({ length: 18 }, (_, index) => `F${index + 1}`),
    );
  });

  it("phrases every family as a non-empty question", () => {
    for (const family of QUESTION_FAMILIES) {
      expect(family.question.trim(), family.id).not.toBe("");
      expect(family.question.endsWith("?"), family.id).toBe(true);
    }
  });

  it("names the gap wherever the answer is partial or missing", () => {
    for (const family of QUESTION_FAMILIES) {
      if (family.status === "partial" || family.status === "lacks") {
        expect(family.gap?.trim(), family.id).toBeTruthy();
      }
    }
  });

  it("points every answered family somewhere — a tab or a hint", () => {
    for (const family of QUESTION_FAMILIES) {
      if (family.status === "answers") {
        expect(family.tab !== undefined || family.hint !== undefined, family.id).toBe(
          true,
        );
      }
    }
  });

  it("derives the scorecard counts from the data", () => {
    const counts = questionStatusCounts(QUESTION_FAMILIES);
    expect(counts.answers + counts.partial + counts.lacks).toBe(
      QUESTION_FAMILIES.length,
    );
    expect(questionStatusCounts([])).toEqual({ answers: 0, partial: 0, lacks: 0 });
  });

  it("phrases every family as a question in Chinese too", () => {
    for (const family of QUESTION_FAMILIES) {
      expect(family.zh?.question?.trim(), family.id).toBeTruthy();
      expect(/[?？]$/.test(family.zh?.question ?? ""), family.id).toBe(true);
    }
  });

  it("mirrors every English hint and gap in Chinese", () => {
    for (const family of QUESTION_FAMILIES) {
      if (family.hint) expect(family.zh?.hint?.trim(), family.id).toBeTruthy();
      if (family.gap) expect(family.zh?.gap?.trim(), family.id).toBeTruthy();
    }
  });

  it("localizeQuestion swaps content for zh and falls back to English", () => {
    const family = QUESTION_FAMILIES[0];
    expect(localizeQuestion(family, "zh").question).toBe(family.zh!.question);
    expect(localizeQuestion(family, "en").question).toBe(family.question);
    const bare = { id: "X", question: "Plain?", status: "answers" as const };
    expect(localizeQuestion(bare, "zh").question).toBe("Plain?");
  });
});

import { describe, expect, it } from "vitest";
import { messages } from "./i18n";
import { personEvidence, type WhoPerson } from "./who";

const person = (overrides: Partial<WhoPerson> = {}): WhoPerson => ({
  name: "Ada",
  commits_here: 23,
  files_repo_wide: 47,
  last_seen: Date.UTC(2026, 7, 12) / 1000,
  ...overrides,
});

describe("personEvidence", () => {
  it("states commits, breadth and last-touched date as evidence", () => {
    expect(personEvidence(person())).toBe(
      "23 commits to this file this year · active in 47 files repo-wide · last touched 2026-08-12",
    );
  });

  it("drops the recency part honestly when last_seen is unproven", () => {
    expect(personEvidence(person({ last_seen: undefined }))).toBe(
      "23 commits to this file this year · active in 47 files repo-wide",
    );
  });

  it("keeps singular grammar for one commit in one file", () => {
    expect(
      personEvidence(person({ commits_here: 1, files_repo_wide: 1, last_seen: undefined })),
    ).toBe("1 commit to this file this year · active in 1 file repo-wide");
  });

  it("speaks the zh catalog with the same evidence when its messages are passed", () => {
    expect(personEvidence(person(), messages.zh.who)).toBe(
      "今年对此文件提交 23 次 · 活跃于全仓 47 个文件 · 最近改动 2026-08-12",
    );
    expect(personEvidence(person({ last_seen: undefined }), messages.zh.who)).toBe(
      "今年对此文件提交 23 次 · 活跃于全仓 47 个文件",
    );
  });
});

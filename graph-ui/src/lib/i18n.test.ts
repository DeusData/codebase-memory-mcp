import { describe, expect, it } from "vitest";
import { detectLanguage, messages } from "./i18n";

describe("i18n", () => {
  it("detects Chinese from Accept-Language and falls back to English", () => {
    expect(detectLanguage("zh-CN,zh;q=0.9,en;q=0.8")).toBe("zh");
    expect(detectLanguage("de-DE,de;q=0.9")).toBe("en");
  });

  it("ranks Accept-Language by q rather than by presence", () => {
    // A bilingual visitor who prefers English but lists Chinese as a fallback.
    expect(detectLanguage("en-US,en;q=0.9,zh;q=0.5")).toBe("en");
    expect(detectLanguage("en,zh;q=0.1")).toBe("en");
    // Chinese still wins when it is actually preferred.
    expect(detectLanguage("zh;q=0.9,en;q=0.5")).toBe("zh");
  });

  it("treats q=0 as unacceptable", () => {
    expect(detectLanguage("zh;q=0, en")).toBe("en");
    expect(detectLanguage("zh;q=0")).toBe("en");
  });

  it("matches the language subtag, not a substring", () => {
    expect(detectLanguage("en-GB")).toBe("en");
    expect(detectLanguage("zh-Hant-TW")).toBe("zh");
  });

  it("honours the override and handles empty input", () => {
    expect(detectLanguage("zh-CN", "en")).toBe("en");
    expect(detectLanguage("en-US", "zh")).toBe("zh");
    expect(detectLanguage(null)).toBe("en");
    expect(detectLanguage("")).toBe("en");
  });

  it("keeps UI chrome messages in the catalog", () => {
    expect(messages.zh.tabs.projects).toBe("项目");
    expect(messages.zh.index.newIndex).toBe("新建索引");
    expect(messages.en.index.repositoryPath).toBe("Repository path");
  });
});

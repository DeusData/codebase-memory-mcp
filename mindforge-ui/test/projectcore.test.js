"use strict";

const { afterEach, beforeEach, describe, test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFileSync } = require("child_process");
const {
  okProjectName,
  createProjectAt,
  checkStoriesInProject,
} = require("../projectcore");

describe("okProjectName", () => {
  test("accepts human folder names", () => {
    for (const name of ["my-app", "Проект 2026", "api_v2.1", "a", "x".repeat(80)])
      assert.equal(okProjectName(name), true, name);
  });

  test("rejects paths and ambiguous names", () => {
    for (const name of [null, "", " ", ".", "..", "../app", "a/b", "a\\b",
      " leading", "trailing ", "💥", "x".repeat(81)])
      assert.equal(okProjectName(name), false, String(name));
  });
});

describe("checkStoriesInProject", () => {
  let repo;
  beforeEach(() => { repo = fs.mkdtempSync(path.join(os.tmpdir(), "mf-boundary-")); });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); });

  test("allows relative and absolute paths inside the project", () => {
    assert.equal(checkStoriesInProject([
      "Edit src/app.js and test/app.test.js",
      `Write ${path.join(repo, "nested", "file.js")}.`,
    ], repo), null);
  });

  test("rejects home, absolute, traversal and Windows paths", () => {
    const cases = [
      ["Write ~/Desktop/out.txt", "~/Desktop/out.txt"],
      ["Write /tmp/out.txt).", "/tmp/out.txt"],
      ["Write ../outside.txt", "../outside.txt"],
      ["Write ../../outside.txt.", "../../outside.txt"],
      ["Write C:\\Users\\name\\out.txt", "C:\\Users\\name\\out.txt"],
    ];
    for (const [story, expected] of cases)
      assert.equal(checkStoriesInProject([story], repo), expected, story);
  });

  test("rejects an absolute path that only shares the project prefix", () => {
    const outside = repo + "-other/file.js";
    assert.equal(checkStoriesInProject([`Write ${outside}`], repo), outside);
  });
});

describe("createProjectAt", () => {
  let root;
  let projects;
  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "mf-project-"));
    projects = path.join(root, "projects");
    fs.mkdirSync(projects);
  });
  afterEach(() => { fs.rmSync(root, { recursive: true, force: true }); });

  test("rejects invalid input without creating a target", async () => {
    const badName = await createProjectAt(root, projects, "../escape");
    assert.equal(badName.ok, false);
    assert.equal(fs.existsSync(path.join(root, "escape")), false);

    const missingParent = await createProjectAt(root, path.join(root, "missing"), "app");
    assert.equal(missingParent.ok, false);
  });

  test("creates a usable git project even without the fablize installer", async () => {
    const result = await createProjectAt(root, projects, "plain-app");
    assert.equal(result.ok, true);
    assert.match(result.warning, /installer not found/);
    assert.equal(fs.existsSync(path.join(result.target, ".git")), true);
    assert.doesNotThrow(() => execFileSync("git", ["-C", result.target, "rev-parse", "--verify", "HEAD"]));
  });

  test("copies the seed, applies disciplines and commits both", async () => {
    const install = path.join(root, "install");
    const fablize = path.join(install, "fablize");
    const seed = path.join(root, "seed");
    fs.mkdirSync(fablize, { recursive: true });
    fs.mkdirSync(seed);
    fs.writeFileSync(path.join(seed, "seed.txt"), "seed\n");
    fs.writeFileSync(path.join(fablize, "install.sh"),
      '#!/usr/bin/env bash\nset -e\nmkdir -p "$1/.fablize"\nprintf installed > "$1/.fablize/installed"\n');

    const result = await createProjectAt(install, projects, "seeded", process.env, seed);
    assert.equal(result.ok, true);
    assert.equal(result.warning, undefined);
    assert.equal(fs.readFileSync(path.join(result.target, "seed.txt"), "utf8"), "seed\n");
    assert.equal(fs.readFileSync(path.join(result.target, ".fablize", "installed"), "utf8"), "installed");
    const committed = execFileSync("git", ["-C", result.target, "ls-tree", "-r", "--name-only", "HEAD"],
      { encoding: "utf8" });
    assert.match(committed, /seed\.txt/);
    assert.match(committed, /\.fablize\/installed/);
  });
});

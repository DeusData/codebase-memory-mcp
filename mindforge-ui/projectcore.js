"use strict";
// Project creation core — deliberately free of any Electron dependency so it can
// be unit-tested with plain node. main.js supplies the dialogs; this module does
// the work: mkdir → git init → apply the fablize disciplines via this repo's
// installer (bash fablize/install.sh <target> — non-interactive, self-contained).
const fs = require("fs");
const path = require("path");
const { execFile } = require("child_process");
const t = require("./i18n").t;

// Folder name: letters (incl. Cyrillic), digits, _ . -, inner spaces. Never a
// path: separators are rejected outright, no leading/trailing space or dot-dot.
const NAME_RE = /^[\p{L}\p{N}_.\-](?:[\p{L}\p{N}_.\- ]*[\p{L}\p{N}_.\-])?$/u;
const okProjectName = (s) =>
  typeof s === "string" && s.length > 0 && s.length <= 80 &&
  !/[/\\]/.test(s) && s !== "." && s !== ".." && NAME_RE.test(s);

function runIn(cwd, cmd, args, env) {
  return new Promise((resolve) => {
    execFile(cmd, args, { cwd, timeout: 60000, maxBuffer: 8 << 20, env: env || process.env },
      (err, out, errOut) => resolve({ ok: !err, out: out || "", err: errOut || (err && err.message) || "" }));
  });
}

// Creates <parentDir>/<name>: mkdir, [optional seed copy], git init, fablize disciplines.
// Returns { ok, target, steps[], warning? } — ok means "project usable" (a disciplines
// failure degrades gracefully: the project still opens, with a clear warning). seedDir, if
// given, is copied in BEFORE git init so its files are captured by the initial commit — used
// by the bundled demo project (see main.js runDemo()).
async function createProjectAt(installDir, parentDir, name, env, seedDir) {
  const steps = [];
  const step = (n, ok, out) => { steps.push({ name: n, ok, out: String(out || "").trim().slice(-800) }); return ok; };
  if (!okProjectName(name))
    return { ok: false, steps: [{ name: t("proj.stepName"), ok: false, out: t("proj.badName") }] };
  if (!parentDir || !fs.existsSync(parentDir))
    return { ok: false, steps: [{ name: t("proj.stepParent"), ok: false, out: t("proj.parentMissing") }] };
  const target = path.join(parentDir, name);
  if (fs.existsSync(target))
    return { ok: false, target, steps: [{ name: "mkdir", ok: false, out: t("proj.alreadyExists") }] };
  try { fs.mkdirSync(target, { recursive: true }); step("mkdir", true, target); }
  catch (e) { return { ok: false, target, steps: [{ name: "mkdir", ok: false, out: e.message }] }; }

  if (seedDir && fs.existsSync(seedDir)) {
    try { fs.cpSync(seedDir, target, { recursive: true }); step("seed", true, seedDir); }
    catch (e) { return { ok: false, target, steps: [...steps, { name: "seed", ok: false, out: e.message }] }; }
  }

  const g = await runIn(target, "git", ["init"], env);
  if (!step("git init", g.ok, g.out + g.err)) return { ok: false, target, steps };

  let warning;
  const installer = path.join(installDir, "fablize", "install.sh");
  if (fs.existsSync(installer)) {
    const r = await runIn(installDir, "bash", [installer, target], env);
    step(t("proj.stepDisciplines"), r.ok, r.out + r.err);
    if (!r.ok) warning = t("proj.disciplinesFailed", { installer, target });
  } else {
    step(t("proj.stepDisciplines"), false, t("proj.installerMissing", { installer }));
    warning = t("proj.noDisciplines", { dir: installDir });
  }

  // Первый коммит обязателен: запуск историй делает `git worktree add … HEAD`,
  // а HEAD в репозитории без коммитов не резолвится — история падала бы сразу.
  await runIn(target, "git", ["add", "-A"], env);
  let c = await runIn(target, "git",
    ["commit", "--allow-empty", "-m", t("proj.initCommit")], env);
  if (!c.ok) // машина без настроенной git-идентичности
    c = await runIn(target, "git",
      ["-c", "user.name=MindForge", "-c", "user.email=mindforge@local",
       "commit", "--allow-empty", "-m", t("proj.initCommit")], env);
  if (!step(t("proj.stepFirstCommit"), c.ok, c.out + c.err) && !warning)
    warning = t("proj.firstCommitFailed", { target });
  return { ok: true, target, steps, warning };
}

// Стори работают только с файлами СВОЕГО проекта: абсолютный путь или ~/ вне repoDir в
// тексте стори означает, что worktree-агент упрётся в песочницу и провалится поздно и
// непонятно (наблюдалось вживую: «создай папку на Desktop»). Ловим на этапе ПЛАНА.
// Возвращает первый «чужой» путь или null, если всё в границах проекта.
function checkStoriesInProject(stories, repoDir) {
  const root = path.resolve(String(repoDir || ""));
  const re = /~\/[^\s'"`)\],;]*|\/(?:Users|home|tmp|var|etc|opt)\/[^\s'"`)\],;]*/g;
  for (const s of stories || []) {
    const text = String(s);
    for (let m of text.match(re) || []) {
      m = m.replace(/[.:]+$/, "");                           // точка в конце предложения — не часть пути
      if (m.startsWith("~")) return m;                       // домашние пути — всегда вне
      const p = path.resolve(m);
      if (p !== root && !p.startsWith(root + path.sep)) return m;
    }
    // Относительный traversal обходит абсолютный regex, но из корня worktree
    // первый же `..` уже означает запись вне проекта. Windows-путь также всегда
    // чужой для поддерживаемых сейчас macOS/Linux worktree.
    const rel = text.match(/(?:^|[\s'"`(\[,;])((?:\.\.[/\\])+[^\s'"`)\],;]*)/);
    if (rel) return rel[1].replace(/[.:]+$/, "");
    const win = text.match(/\b[A-Za-z]:\\[^\s'"`)\],;]*/);
    if (win) return win[0].replace(/[.:]+$/, "");
  }
  return null;
}

module.exports = { okProjectName, createProjectAt, checkStoriesInProject };

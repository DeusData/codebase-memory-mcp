#!/usr/bin/env node
/*
 * W1-Smoke: erzeugt die Beweisartefakte, die tests/scaffold/w1.test.mjs liest.
 *
 *   1. dist/ loeschen und `npm run build` fahren (tsc -b und vite build)
 *   2. `npm run test:unit` unter dem Netz-Deny-Gate fahren
 *   3. verification/w1/netdeny.json und verification/w1/scaffold.json schreiben
 *
 * Die Zahl der gelaufenen Unit-Tests wird aus der Zusammenfassung von vitest
 * gelesen und nicht selbst gezaehlt: geraten waere sie wertlos, und eine
 * zweite Zaehlung waere eine zweite Wahrheit.
 */

import { spawn } from 'node:child_process';
import { mkdir, readdir, rm, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const OUT_DIR = join(ROOT, 'verification', 'w1');
const SCAFFOLD_JSON = join(OUT_DIR, 'scaffold.json');
const NETDENY_JSON = join(OUT_DIR, 'netdeny.json');
const DIST = join(ROOT, 'dist');

const log = (...parts) => console.log('[smoke-w1]', ...parts);

const NO_NETWORK_ENV = {
  NO_UPDATE_NOTIFIER: '1',
  npm_config_update_notifier: 'false',
  npm_config_audit: 'false',
  npm_config_fund: 'false',
};

/** Faehrt ein Kommando, spiegelt seine Ausgabe und liefert sie zurueck. */
function run(command, args, { cwd = ROOT } = {}) {
  return new Promise((resolveRun) => {
    const child = spawn(command, args, {
      cwd,
      env: { ...process.env, ...NO_NETWORK_ENV },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    let err = '';
    child.stdout.on('data', (d) => {
      const text = d.toString();
      out += text;
      process.stdout.write(text);
    });
    child.stderr.on('data', (d) => {
      const text = d.toString();
      err += text;
      process.stderr.write(text);
    });
    child.on('error', (e) => resolveRun({ code: 127, out, err: err + e.message }));
    child.on('close', (code) => resolveRun({ code: code ?? 1, out, err }));
  });
}

/** Zaehlt alle Dateien unterhalb eines Verzeichnisses, rekursiv. */
async function countFiles(dir) {
  let count = 0;
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    return 0;
  }
  for (const entry of entries) {
    if (entry.isDirectory()) {
      count += await countFiles(join(dir, entry.name));
    } else if (entry.isFile()) {
      count += 1;
    }
  }
  return count;
}

const stripAnsi = (text) => text.replace(/\u001B\[[0-9;]*[A-Za-z]/g, '');

/**
 * Liest die Zahl der gelaufenen Tests aus der vitest-Zusammenfassung.
 *
 * Die Zeile sieht so aus: "      Tests  27 passed (27)". Gezaehlt wird die
 * Klammer, also alle Tests inklusive uebersprungener oder gefallener, denn
 * "wie viele liefen" ist die Frage, nicht "wie viele waren gruen".
 */
function parseUnitTests(output) {
  const clean = stripAnsi(output);
  const lines = clean.split('\n');
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const match = /^\s*Tests\s+(.+?)\s*$/.exec(lines[i]);
    if (match === null) { continue; }
    const total = /\((\d+)\)\s*$/.exec(match[1]);
    if (total !== null) { return Number.parseInt(total[1], 10); }
    const passed = /(\d+)\s+passed/.exec(match[1]);
    if (passed !== null) { return Number.parseInt(passed[1], 10); }
  }
  return 0;
}

/** Zieht das JSON-Ergebnis des Gates aus dessen Ausgabe (letzte JSON-Zeile). */
function parseGateReport(output) {
  const lines = stripAnsi(output).split('\n');
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i].trim();
    if (!line.startsWith('{') || !line.endsWith('}')) { continue; }
    try {
      return JSON.parse(line);
    } catch {
      // war doch kein Report
    }
  }
  return null;
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });

  log('raeume dist/ ab');
  await rm(DIST, { recursive: true, force: true });

  log('npm run build');
  const build = await run('npm', ['run', 'build']);
  const buildExit = build.code;
  const distFiles = await countFiles(DIST);
  log(`build exit=${buildExit}, dist-Dateien=${distFiles}`);

  log('npm run test:unit unter dem Netz-Deny-Gate');
  const gate = await run('node', [
    join(ROOT, 'tools', 'net-deny-gate.mjs'),
    '--out',
    NETDENY_JSON,
    '--',
    'npm',
    'run',
    'test:unit',
  ]);
  const gateOutput = gate.out + '\n' + gate.err;
  const report = parseGateReport(gate.out);
  if (report === null) {
    throw new Error('das Netz-Deny-Gate lieferte keinen lesbaren Bericht');
  }

  const unitExit = report.exitCode ?? 1;
  const unitTests = parseUnitTests(gateOutput);
  log(
    `unit exit=${unitExit}, Tests=${unitTests}, Stichproben=${report.samples}, ` +
      `Verstoesse=${report.outboundViolations}`,
  );

  const scaffold = {
    buildExit,
    unitExit,
    unitTests,
    distFiles,
    // Zusatz, den AC1 nicht prueft, der den Beweis aber lesbar macht.
    netDenySamples: report.samples,
    netDenyOutboundViolations: report.outboundViolations,
    gateExit: gate.code,
    generatedAt: new Date().toISOString(),
  };
  await writeFile(SCAFFOLD_JSON, JSON.stringify(scaffold, null, 2) + '\n', 'utf8');
  log('geschrieben:', SCAFFOLD_JSON);
  log('geschrieben:', NETDENY_JSON);

  const ok =
    buildExit === 0 &&
    unitExit === 0 &&
    unitTests >= 10 &&
    distFiles >= 2 &&
    report.outboundViolations === 0 &&
    report.samples >= 3;

  if (!ok) {
    console.error('[smoke-w1] W1-Smoke NICHT gruen.');
    process.exitCode = 1;
    return;
  }
  log('W1-Smoke gruen.');
}

main().catch((err) => {
  console.error('[smoke-w1] unerwarteter Fehler:', err);
  process.exitCode = 1;
});

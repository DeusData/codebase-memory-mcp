#!/usr/bin/env node
/*
 * Netz-Deny-Gate: faehrt ein Kommando und beweist, dass dabei nichts die
 * Maschine verlassen hat.
 *
 *   node tools/net-deny-gate.mjs [--out <datei>] [--interval <ms>] -- <cmd...>
 *
 * Das Gate blockiert nichts, es beobachtet. Der Unterschied ist wichtig: eine
 * Firewall-Regel wuerde einen Verbindungsversuch verstecken, dieses Gate
 * schreibt ihn auf. Alle ~400 ms wird der Prozessbaum des Kommandos ermittelt
 * (rekursiv ueber pgrep -P) und mit lsof nach offenen TCP/UDP-Gegenstellen
 * gefragt. Jede Gegenstelle ausserhalb von Loopback ist ein Verstoss.
 *
 * Grenze des Beweises, ausdruecklich benannt: eine Verbindung, die zwischen
 * zwei Abtastungen aufgebaut, benutzt und geschlossen wird, kann durchrutschen.
 * Das Gate sagt "in N Stichproben war nichts nach draussen offen", nicht "es
 * gab keine einzige Verbindung". Deshalb steht die Zahl der Stichproben im
 * Ergebnis: ohne sie waere outboundViolations: 0 nicht lesbar.
 */

import { spawn, execFile } from 'node:child_process';
import { writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const DEFAULT_INTERVAL_MS = 400;

/**
 * So viele Stichproben sollen fallen, bevor der Abstand auf das volle
 * Intervall geht.
 *
 * Ein Grundtakt von 400 ms ist fuer einen Lauf gedacht, der Sekunden dauert.
 * Die Unit-Suite ist unter einer Sekunde durch, und zwei Stichproben waeren
 * eine zu duenne Aussage. Die ersten Stichproben kommen deshalb dichter.
 * Alle Stichproben fallen waehrend das Kommando laeuft; nach dem Ende wird
 * nicht weiter abgetastet, denn ein toter Prozess hat keine Sockets und die
 * Stichprobe waere eine Null, die nichts beobachtet hat.
 */
const DEFAULT_MIN_SAMPLES = 4;

/** 127.0.0.0/8, ::1 und ihre Schreibweisen. Nur diese gelten als lokal. */
export function isLoopbackHost(host) {
  if (host === undefined || host === null) { return false; }
  let h = host.trim();
  if (h.startsWith('[') && h.endsWith(']')) { h = h.slice(1, -1); }
  if (h === 'localhost' || h === '::1' || h === '0:0:0:0:0:0:0:1') { return true; }
  const v4 = h.startsWith('::ffff:') ? h.slice(7) : h;
  return /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(v4);
}

/** Trennt "127.0.0.1:52000" oder "[::1]:52000" in Host und Port. */
export function splitHostPort(endpoint) {
  const value = endpoint.trim();
  if (value.startsWith('[')) {
    const close = value.indexOf(']');
    if (close === -1) { return { host: value, port: '' }; }
    const host = value.slice(0, close + 1);
    const rest = value.slice(close + 1);
    return { host, port: rest.startsWith(':') ? rest.slice(1) : '' };
  }
  const colon = value.lastIndexOf(':');
  if (colon === -1) { return { host: value, port: '' }; }
  return { host: value.slice(0, colon), port: value.slice(colon + 1) };
}

// ------------------------------------------------------------ Prozessbaum ---

async function childPids(pid) {
  try {
    const { stdout } = await execFileAsync('pgrep', ['-P', String(pid)]);
    return stdout.split('\n').map((l) => l.trim()).filter(Boolean).map(Number);
  } catch (err) {
    // pgrep beendet sich mit 1, wenn es keine Kinder gibt. Das ist normal.
    if (err && err.code === 1) { return []; }
    return [];
  }
}

/** Sammelt den Prozessbaum unter (und einschliesslich) pid. */
async function processTree(pid) {
  const seen = new Set();
  const queue = [pid];
  while (queue.length > 0) {
    const current = queue.shift();
    if (seen.has(current)) { continue; }
    seen.add(current);
    for (const child of await childPids(current)) {
      if (!seen.has(child)) { queue.push(child); }
    }
  }
  return [...seen];
}

// ------------------------------------------------------------------ lsof ---

/**
 * Fragt lsof nach den Sockets der genannten Pids.
 *
 * Eine leere Antwort ist ein gueltiges Ergebnis und wird als Stichprobe
 * gezaehlt: "dieser Prozess hatte gerade keinen Socket offen" ist genau die
 * Aussage, die das Gate sammelt. Nur ein Fehler von lsof selbst wird vermerkt.
 */
async function lsofSockets(pids) {
  if (pids.length === 0) { return { lines: [], error: null }; }
  try {
    const { stdout } = await execFileAsync(
      'lsof',
      ['-a', '-i', '-n', '-P', '-p', pids.join(',')],
      { maxBuffer: 8 * 1024 * 1024 },
    );
    return { lines: stdout.split('\n').filter((l) => l.trim().length > 0), error: null };
  } catch (err) {
    // Exit 1 heisst bei lsof "nichts gefunden", nicht "kaputt".
    if (err && err.code === 1) { return { lines: [], error: null }; }
    return { lines: [], error: err && err.message ? err.message : String(err) };
  }
}

/**
 * Liest eine lsof-Zeile und klassifiziert die Gegenstelle.
 *
 * Nur Zeilen mit "->" beschreiben eine Verbindung zu einem Gegenueber. Ein
 * lauschender Socket (*:4200 (LISTEN)) hat keine und wird ignoriert: er
 * empfaengt, er sendet nichts nach draussen.
 */
export function classifyLine(line) {
  if (!/\s(TCP|UDP)\s/.test(line)) { return null; }
  const nameStart = line.search(/\s(TCP|UDP)\s/);
  const rest = line.slice(nameStart).trim();
  const protocol = rest.startsWith('TCP') ? 'TCP' : 'UDP';
  const address = rest.slice(3).trim().replace(/\s*\(.*\)\s*$/, '').trim();
  const arrow = address.indexOf('->');
  if (arrow === -1) { return null; }

  const localPart = address.slice(0, arrow);
  const remotePart = address.slice(arrow + 2);
  const remote = splitHostPort(remotePart);
  const command = line.trim().split(/\s+/)[0];
  const pid = Number(line.trim().split(/\s+/)[1]);

  return {
    protocol,
    command,
    pid: Number.isFinite(pid) ? pid : null,
    local: localPart.trim(),
    remote: remotePart.trim(),
    remoteHost: remote.host,
    remotePort: remote.port,
    loopback: isLoopbackHost(remote.host),
  };
}

// ------------------------------------------------------------------ Main ----

function parseArgs(argv) {
  const args = argv.slice(2);
  const sep = args.indexOf('--');
  if (sep === -1 || sep === args.length - 1) {
    throw new Error(
      'Aufruf: node tools/net-deny-gate.mjs [--out <datei>] [--interval <ms>] ' +
        '[--min-samples <n>] -- <cmd...>',
    );
  }
  const options = {
    out: null,
    intervalMs: DEFAULT_INTERVAL_MS,
    minSamples: DEFAULT_MIN_SAMPLES,
  };
  for (let i = 0; i < sep; i += 1) {
    if (args[i] === '--out') { options.out = args[i + 1]; i += 1; continue; }
    if (args[i] === '--interval') { options.intervalMs = Number(args[i + 1]); i += 1; continue; }
    if (args[i] === '--min-samples') { options.minSamples = Number(args[i + 1]); i += 1; continue; }
    throw new Error(`unbekannte Option: ${args[i]}`);
  }
  return { options, command: args.slice(sep + 1) };
}

async function main() {
  const { options, command } = parseArgs(process.argv);
  const started = Date.now();

  const child = spawn(command[0], command.slice(1), {
    stdio: ['ignore', 'inherit', 'inherit'],
    env: {
      ...process.env,
      // npm meldet sich sonst beim Registry-Update-Notifier und das waere ein
      // Verstoss, den das Gate selbst ausgeloest haette.
      NO_UPDATE_NOTIFIER: '1',
      npm_config_update_notifier: 'false',
      npm_config_audit: 'false',
      npm_config_fund: 'false',
    },
  });

  let exitCode = null;
  let exitSignal = null;
  const done = new Promise((resolveDone) => {
    child.on('exit', (code, signal) => {
      exitCode = code;
      exitSignal = signal;
      resolveDone();
    });
    child.on('error', (err) => {
      exitCode = 127;
      exitSignal = null;
      process.stderr.write(`[net-deny] Kommando nicht startbar: ${err.message}\n`);
      resolveDone();
    });
  });

  let samples = 0;
  let lsofErrors = 0;
  let maxTreeSize = 0;
  const violations = [];
  const loopback = new Map();
  let running = true;

  const sampleOnce = async () => {
    const pids = child.pid ? await processTree(child.pid) : [];
    maxTreeSize = Math.max(maxTreeSize, pids.length);
    const { lines, error } = await lsofSockets(pids);
    if (error !== null) { lsofErrors += 1; }
    samples += 1;
    for (const line of lines) {
      const socket = classifyLine(line);
      if (socket === null) { continue; }
      if (socket.loopback) {
        const key = `${socket.protocol} ${socket.remote}`;
        loopback.set(key, (loopback.get(key) ?? 0) + 1);
        continue;
      }
      violations.push({
        atMs: Date.now() - started,
        protocol: socket.protocol,
        command: socket.command,
        pid: socket.pid,
        local: socket.local,
        remote: socket.remote,
        remoteHost: socket.remoteHost,
        remotePort: socket.remotePort,
      });
    }
  };

  const warmupMs = Math.max(50, Math.floor(options.intervalMs / 4));
  const sampler = (async () => {
    await sampleOnce();
    while (running) {
      const wait = samples < options.minSamples ? warmupMs : options.intervalMs;
      await new Promise((r) => setTimeout(r, wait));
      if (!running) { break; }
      await sampleOnce();
    }
  })();

  await done;
  running = false;
  await sampler;

  const report = {
    command: command.join(' '),
    exitCode,
    exitSignal,
    samples,
    intervalMs: options.intervalMs,
    warmupIntervalMs: warmupMs,
    minSamples: options.minSamples,
    durationMs: Date.now() - started,
    maxProcessTreeSize: maxTreeSize,
    lsofErrors,
    outboundViolations: violations.length,
    violations,
    loopbackConnections: [...loopback.entries()]
      .map(([endpoint, seen]) => ({ endpoint, seen }))
      .sort((a, b) => a.endpoint.localeCompare(b.endpoint)),
    generatedAt: new Date().toISOString(),
  };

  if (options.out !== null) {
    const target = resolve(options.out);
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, JSON.stringify(report, null, 2) + '\n', 'utf8');
  }

  process.stdout.write(JSON.stringify(report) + '\n');
  process.exitCode = report.outboundViolations > 0 ? 1 : 0;
}

/*
 * Nur beim direkten Aufruf laufen. Die Klassifikationsfunktionen oben werden
 * einzeln getestet (tools/net-deny-gate.test.mjs), und ein Import darf dabei
 * kein Kommando starten.
 */
const invokedDirectly =
  typeof process.argv[1] === 'string' &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (invokedDirectly) {
  main().catch((err) => {
    process.stderr.write(`[net-deny] Fehler: ${err.message}\n`);
    process.exitCode = 1;
  });
}

/**
 * Den gebauten C-Server aus PR 1860 starten, benutzen und wieder loswerden.
 *
 * Herausgeloest aus tools/smoke-spike.mjs, damit der zweite Beweislauf nicht
 * eine zweite Fassung derselben Lebenszyklus-Regeln bekommt. Die Regeln sind
 * kurz und jede einzelne ist teuer erkauft:
 *
 *  1. **Isoliertes HOME.** Der Server legt persistierte Einstellungen und den
 *     Index unter HOME ab. In das echte HOME zu schreiben waere ein Beweislauf,
 *     der die Maschine des Nutzers veraendert.
 *  2. **MCP-Handshake auf stdin, und stdin bleibt offen.** Das HTTP-UI oeffnet
 *     erst nach initialize, und ein geschlossenes stdin beendet den
 *     stdio-Server sofort.
 *  3. **Auf GET / warten, nicht auf eine Wartezeit.** Ein Schlaf ist eine
 *     Vermutung ueber die Maschine, auf der das laeuft.
 *  4. **Am Ende zaehlen, was noch lauscht.** Ein Beweislauf, der einen Prozess
 *     zuruecklaesst, hat den naechsten schon verdorben.
 *
 * smoke-spike.mjs benutzt diese Datei bewusst nicht: es ist ausdruecklich
 * Wegwerf-Beweiscode fuer W0 und soll unberuehrt gruen bleiben, statt wegen
 * einer Aufraeumarbeit noch einmal durch Playwright zu muessen.
 */

import { execFile, spawn } from 'node:child_process';
import net from 'node:net';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

/** Wartezeit, bis der Server auf GET / antworten muss. */
export const SERVER_READY_TIMEOUT_MS = 15000;

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isPortFree(port) {
  return new Promise((resolve) => {
    const probe = net.createServer();
    probe.once('error', () => resolve(false));
    probe.once('listening', () => probe.close(() => resolve(true)));
    probe.listen(port, '127.0.0.1');
  });
}

/** Erster freier Port ab `start`. */
export async function findFreePort(start, taken = []) {
  for (let port = start; port < start + 200; port += 1) {
    if (taken.includes(port)) { continue; }
    if (await isPortFree(port)) { return port; }
  }
  throw new Error(`kein freier Port ab ${start} gefunden`);
}

/** Wie viele Prozesse auf dem Port lauschen. Null ist das Ziel nach dem Lauf. */
export async function countListeners(port) {
  try {
    const { stdout } = await execFileAsync('lsof', ['-ti', `tcp:${port}`]);
    return stdout.split('\n').map((line) => line.trim()).filter(Boolean).length;
  } catch (err) {
    // lsof endet mit 1, wenn es nichts gefunden hat. Das ist der gute Fall.
    if (err && err.code === 1) { return 0; }
    throw err;
  }
}

/**
 * Ein Repository indizieren, ueber die CLI und nicht ueber /rpc.
 *
 * Der Weg ist keine Bequemlichkeit: die Read-only-Allowlist des Servers bietet
 * index_repository auf /rpc gar nicht an. Wer indizieren will, ruft die CLI,
 * und genau das haelt der Beweislauf hier fest.
 */
export async function indexRepository(binary, { home, repoPath, project, mode = 'full' }) {
  const started = Date.now();
  const payload = JSON.stringify({ repo_path: repoPath, name: project, mode });
  const result = await new Promise((resolve, reject) => {
    const child = spawn(binary, ['cli', 'index_repository'], {
      env: { ...process.env, HOME: home },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let out = '';
    let err = '';
    child.stdout.on('data', (d) => { out += d.toString(); });
    child.stderr.on('data', (d) => { err += d.toString(); });
    child.on('error', reject);
    child.on('close', (code) => resolve({ code, out, err }));
    child.stdin.end(payload + '\n');
  });

  const line = result.out.split('\n').map((l) => l.trim()).filter(Boolean).pop();
  if (!line) {
    throw new Error(
      `index_repository lieferte keine Ausgabe (exit ${result.code}): ${result.err.trim()}`,
    );
  }
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new Error(`index_repository lieferte kein JSON: ${line.slice(0, 400)}`);
  }
  if (parsed.status !== 'indexed' && parsed.nodes === undefined) {
    throw new Error(`index_repository status=${parsed.status}: ${line.slice(0, 400)}`);
  }
  return { ...parsed, durationMs: Date.now() - started, stderr: result.err.trim() };
}

const INITIALIZE = JSON.stringify({
  jsonrpc: '2.0',
  id: 1,
  method: 'initialize',
  params: {
    protocolVersion: '2025-06-18',
    capabilities: {},
    clientInfo: { name: 'codeatlasweb-smoke', version: '0.0.1' },
  },
});

/**
 * Den Server starten und warten, bis er auf 127.0.0.1:port antwortet.
 *
 * `log` sammelt die Ausgaben des Prozesses, damit ein vorzeitiges Ende nicht
 * als Zeitueberschreitung ohne Begruendung endet.
 */
export async function startServer(binary, { home, port, log = [] }) {
  const started = Date.now();
  const child = spawn(binary, ['--ui=true', `--port=${port}`], {
    env: { ...process.env, HOME: home },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  child.stdout.on('data', (d) => { log.push(`[server stdout] ${d.toString().trim()}`); });
  child.stderr.on('data', (d) => { log.push(`[server stderr] ${d.toString().trim()}`); });

  let exited = null;
  child.on('exit', (code, signal) => { exited = { code, signal }; });
  child.stdin.write(INITIALIZE + '\n');

  const deadline = Date.now() + SERVER_READY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (exited) {
      throw new Error(
        `C-Server beendete sich vorzeitig (code=${exited.code}, signal=${exited.signal})\n` +
        log.slice(-20).join('\n'),
      );
    }
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`, { redirect: 'manual' });
      await res.arrayBuffer();
      if (res.status === 200) {
        return { child, durationMs: Date.now() - started };
      }
    } catch {
      // noch nicht da
    }
    await sleep(200);
  }
  throw new Error(
    `C-Server wurde binnen ${SERVER_READY_TIMEOUT_MS} ms nicht auf 127.0.0.1:${port} erreichbar\n` +
    log.slice(-20).join('\n'),
  );
}

/** Den Prozess beenden, erst hoeflich und dann nicht mehr. */
export async function stopServer(child, label = 'C-Server') {
  if (!child || child.exitCode !== null || child.killed) { return; }
  child.kill('SIGTERM');
  for (let i = 0; i < 50; i += 1) {
    if (child.exitCode !== null || child.signalCode !== null) { return; }
    await sleep(100);
  }
  console.error(`[smoke] ${label} reagierte nicht auf SIGTERM, sende SIGKILL`);
  child.kill('SIGKILL');
  await sleep(300);
}

#!/usr/bin/env node
// W0-Spike-Orchestrator. WEGWERF-BEWEISCODE, NICHT DIE PRODUKTARCHITEKTUR.
//
// Was hier bewiesen wird: der gebaute C-Server aus PR 1860 liefert echten
// Quelltext über /rpc get_code_snippet, und Monaco rendert ihn read-only mit
// genau einer Decoration an der Call-Site.
//
// Der Mini-Static-Server mit /rpc-Proxy weiter unten existiert nur, weil der
// C-Server /rpc und /api gegen fremde Origins mit 403 sperrt (Host-Header muss
// exakt 127.0.0.1:<port> oder localhost:<port> sein). Eine Spike-Seite auf
// einem anderen Port kann /rpc daher nicht direkt fetchen. Die Produktlösung
// ist, dass der C-Server das Frontend selbst ausliefert; dieser Proxy ist
// ausdrücklich nur Gerüst für den Beweis und fliegt danach raus.
//
// Ablauf:
//   1. Binary prüfen
//   2. isoliertes HOME anlegen (persistierte Settings dürfen nie ins echte HOME)
//   3. fixtures/atlas-sample indexieren (single-shot CLI)
//   4. C-Server auf freiem Port P >= 4210 starten (MCP-initialize auf stdin,
//      stdin bleibt offen)
//   5. qualified_name von createUser über /rpc search_graph ermitteln
//   6. Static+Proxy-Server auf P2 starten
//   7. Playwright chromium headless auf die Spike-Seite, window.__spike lesen,
//      Screenshot schreiben
//   8. beide Prozesse beenden und Restprozesse auf P und P2 zählen
//   9. verification/w0/spike.json schreiben

import { spawn } from 'node:child_process';
import { execFile } from 'node:child_process';
import { createServer } from 'node:http';
import { request as httpRequest } from 'node:http';
import net from 'node:net';
import { mkdtemp, mkdir, writeFile, readFile, stat, rm } from 'node:fs/promises';
import { existsSync, createReadStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, extname, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w0-spike';
const SYMBOL = 'createUser';
const OUT_DIR = join(ROOT, 'verification', 'w0');
const OUT_JSON = join(OUT_DIR, 'spike.json');
const OUT_PNG = join(OUT_DIR, 'spike.png');
const SCREENSHOT_REL = 'verification/w0/spike.png';

const MIN_PORT = 4210;
const SERVER_READY_TIMEOUT_MS = 15000;

const log = (...parts) => console.log('[smoke]', ...parts);
const serverLog = [];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------- Ports ----

function isPortFree(port) {
  return new Promise((resolveFree) => {
    const probe = net.createServer();
    probe.once('error', () => resolveFree(false));
    probe.once('listening', () => probe.close(() => resolveFree(true)));
    probe.listen(port, '127.0.0.1');
  });
}

async function findFreePort(start, taken = []) {
  for (let port = start; port < start + 200; port += 1) {
    if (taken.includes(port)) { continue; }
    if (await isPortFree(port)) { return port; }
  }
  throw new Error(`kein freier Port ab ${start} gefunden`);
}

async function countListeners(port) {
  try {
    const { stdout } = await execFileAsync('lsof', ['-ti', `tcp:${port}`]);
    return stdout.split('\n').map((l) => l.trim()).filter(Boolean).length;
  } catch (err) {
    // lsof beendet sich mit 1, wenn nichts gefunden wurde. Das ist der gute Fall.
    if (err && err.code === 1) { return 0; }
    throw err;
  }
}

// -------------------------------------------------------------- Indexing ----

async function indexRepository(home) {
  const started = Date.now();
  const payload = JSON.stringify({ repo_path: FIXTURE, name: PROJECT, mode: 'full' });

  const result = await new Promise((resolveRun, rejectRun) => {
    const child = spawn(BINARY, ['cli', 'index_repository'], {
      env: { ...process.env, HOME: home },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let out = '';
    let err = '';
    child.stdout.on('data', (d) => { out += d.toString(); });
    child.stderr.on('data', (d) => { err += d.toString(); });
    child.on('error', rejectRun);
    child.on('close', (code) => resolveRun({ code, out, err }));
    child.stdin.end(payload + '\n');
  });

  if (result.err.trim()) {
    serverLog.push(`[index stderr] ${result.err.trim()}`);
  }

  const line = result.out.split('\n').map((l) => l.trim()).filter(Boolean).pop();
  if (!line) {
    throw new Error(`index_repository lieferte keine Ausgabe (exit ${result.code}): ${result.err.trim()}`);
  }
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new Error(`index_repository lieferte kein JSON: ${line.slice(0, 400)}`);
  }
  if (parsed.status !== 'indexed') {
    throw new Error(`index_repository status=${parsed.status}: ${line.slice(0, 400)}`);
  }
  return { ...parsed, durationMs: Date.now() - started };
}

// ------------------------------------------------------------ C-Server -----

const INITIALIZE = JSON.stringify({
  jsonrpc: '2.0',
  id: 1,
  method: 'initialize',
  params: {
    protocolVersion: '2025-06-18',
    capabilities: {},
    clientInfo: { name: 'spike', version: '0.0.1' },
  },
});

async function startCServer(home, port) {
  const started = Date.now();
  const child = spawn(BINARY, ['--ui=true', `--port=${port}`], {
    env: { ...process.env, HOME: home },
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (d) => { serverLog.push(`[server stdout] ${d.toString().trim()}`); });
  child.stderr.on('data', (d) => { serverLog.push(`[server stderr] ${d.toString().trim()}`); });

  let exited = null;
  child.on('exit', (code, signal) => { exited = { code, signal }; });

  // MCP-Handshake: erst danach öffnet der Server das HTTP-UI. stdin bleibt
  // offen, sonst beendet sich der stdio-Server sofort.
  child.stdin.write(INITIALIZE + '\n');

  const deadline = Date.now() + SERVER_READY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (exited) {
      throw new Error(
        `C-Server beendete sich vorzeitig (code=${exited.code}, signal=${exited.signal})\n` +
        serverLog.slice(-20).join('\n'),
      );
    }
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`, { redirect: 'manual' });
      if (res.status === 200) {
        await res.arrayBuffer();
        return { child, durationMs: Date.now() - started };
      }
      await res.arrayBuffer();
    } catch {
      // noch nicht da
    }
    await sleep(200);
  }
  throw new Error(
    `C-Server wurde binnen ${SERVER_READY_TIMEOUT_MS} ms nicht auf 127.0.0.1:${port} erreichbar\n` +
    serverLog.slice(-20).join('\n'),
  );
}

async function rpc(port, name, args, id = 2) {
  const res = await fetch(`http://127.0.0.1:${port}/rpc`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } }),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`/rpc ${name} -> HTTP ${res.status}: ${text.slice(0, 300)}`);
  }
  const body = JSON.parse(text);
  if (body.error) {
    throw new Error(`/rpc ${name} -> Fehler: ${JSON.stringify(body.error).slice(0, 300)}`);
  }
  const content = body.result?.content;
  if (!Array.isArray(content) || typeof content[0]?.text !== 'string') {
    throw new Error(`/rpc ${name} -> unerwartete Antwort: ${text.slice(0, 300)}`);
  }
  return content[0].text;
}

// qualified_name-Schema: <projekt>.src.services.userService.createUser
function extractQualifiedName(text, symbol) {
  const escape = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const direct = new RegExp(escape(PROJECT) + '[\\w.$-]*\\.' + escape(symbol) + '\\b', 'g');
  const hits = text.match(direct) ?? [];
  const preferred = hits.find((h) => h.includes('services.userService'));
  if (preferred) { return preferred; }
  if (hits.length > 0) { return hits[0]; }

  // Fallback: strukturierte Antwort durchsuchen.
  try {
    const parsed = JSON.parse(text);
    const stack = [parsed];
    while (stack.length > 0) {
      const node = stack.pop();
      if (node && typeof node === 'object') {
        if (typeof node.qualified_name === 'string' && node.qualified_name.endsWith(`.${symbol}`)) {
          return node.qualified_name;
        }
        for (const value of Object.values(node)) { stack.push(value); }
      }
    }
  } catch {
    // war kein JSON
  }
  return null;
}

// ------------------------------------------- Static + /rpc-Proxy (Wegwerf) --

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ttf': 'font/ttf',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.png': 'image/png',
};

function startStaticProxy(port, cPort) {
  const allowedRoots = [join(ROOT, 'spike'), join(ROOT, 'node_modules')];

  const server = createServer((req, res) => {
    if (req.method === 'POST' && req.url.split('?')[0] === '/rpc') {
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => {
        const body = Buffer.concat(chunks);
        // Host-Header exakt setzen, Origin bewusst NICHT weiterreichen:
        // der C-Server lehnt fremde Origins mit 403 forbidden origin ab.
        const upstream = httpRequest({
          host: '127.0.0.1',
          port: cPort,
          path: '/rpc',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': body.length,
            Host: `127.0.0.1:${cPort}`,
          },
        }, (up) => {
          res.writeHead(up.statusCode ?? 502, { 'Content-Type': 'application/json; charset=utf-8' });
          up.pipe(res);
        });
        upstream.on('error', (err) => {
          res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: { message: `proxy: ${err.message}` } }));
        });
        upstream.end(body);
      });
      return;
    }

    const urlPath = decodeURIComponent(req.url.split('?')[0]);
    const candidate = resolve(join(ROOT, normalize(urlPath)));
    const allowed = allowedRoots.some((root) => candidate === root || candidate.startsWith(root + '/'));
    if (!allowed || !existsSync(candidate)) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME[extname(candidate)] ?? 'application/octet-stream' });
    createReadStream(candidate).pipe(res);
  });

  return new Promise((resolveServer, rejectServer) => {
    server.once('error', rejectServer);
    server.listen(port, '127.0.0.1', () => resolveServer(server));
  });
}

// -------------------------------------------------------------- Browser -----

async function driveBrowser(url) {
  const { chromium } = await import('playwright');
  const consoleLines = [];
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (err) {
    throw new Error(
      'Playwright konnte chromium nicht starten. Falls ein Browser-Download verlangt wird: ' +
      'ABBRUCH statt stillem Laden. Originalfehler: ' + err.message,
    );
  }
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
    const page = await context.newPage();
    page.on('console', (msg) => consoleLines.push(`[${msg.type()}] ${msg.text()}`));
    page.on('pageerror', (err) => consoleLines.push(`[pageerror] ${err.message}`));

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    try {
      await page.waitForFunction(() => window.__spike && window.__spike.ready === true, null, { timeout: 30000 });
    } catch {
      const state = await page.evaluate(() => window.__spike ?? null);
      throw new Error(
        `window.__spike wurde nicht ready: ${JSON.stringify(state)}\nKonsole:\n${consoleLines.join('\n')}`,
      );
    }

    // Der Editor braucht einen Frame, bis Decoration und Gutter gemalt sind.
    await page.waitForSelector('.atlas-callsite-glyph', { timeout: 10000 });
    await page.waitForTimeout(400);

    const spike = await page.evaluate(() => window.__spike);
    await mkdir(OUT_DIR, { recursive: true });
    await page.screenshot({ path: OUT_PNG, fullPage: true });

    return { spike, consoleLines };
  } finally {
    await browser.close();
  }
}

// ---------------------------------------------------------------- Cleanup ---

async function stopChild(child, label) {
  if (!child || child.exitCode !== null || child.killed) { return; }
  child.kill('SIGTERM');
  for (let i = 0; i < 50; i += 1) {
    if (child.exitCode !== null || child.signalCode !== null) { return; }
    await sleep(100);
  }
  log(`${label} reagierte nicht auf SIGTERM, sende SIGKILL`);
  child.kill('SIGKILL');
  await sleep(300);
}

function stopServer(server) {
  if (!server) { return Promise.resolve(); }
  return new Promise((resolveClose) => {
    server.closeAllConnections?.();
    server.close(() => resolveClose());
    setTimeout(resolveClose, 3000);
  });
}

// ------------------------------------------------------------------ Main ----

async function main() {
  const totalStarted = Date.now();
  let cServer = null;
  let staticServer = null;
  let cPort = 0;
  let staticPort = 0;
  let failure = null;
  const timings = {};
  let indexResult = null;
  let qualifiedName = null;
  let spike = null;
  let home = null;

  try {
    if (!existsSync(BINARY)) {
      throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
    }
    if (!existsSync(FIXTURE)) {
      throw new Error(`Fixture fehlt: ${FIXTURE}`);
    }

    home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w0-home-'));
    log('isoliertes HOME:', home);

    log('indexiere', FIXTURE, 'als', PROJECT, '...');
    indexResult = await indexRepository(home);
    timings.indexMs = indexResult.durationMs;
    log(`indexiert in ${timings.indexMs} ms (nodes=${indexResult.nodes ?? '?'}, edges=${indexResult.edges ?? '?'})`);

    cPort = await findFreePort(MIN_PORT);
    log('starte C-Server auf 127.0.0.1:' + cPort, '...');
    const startedServer = await startCServer(home, cPort);
    cServer = startedServer.child;
    timings.serverStartMs = startedServer.durationMs;
    log(`C-Server bereit nach ${timings.serverStartMs} ms`);

    log('suche qualified_name von', SYMBOL, 'über /rpc search_graph ...');
    const searchText = await rpc(cPort, 'search_graph', { project: PROJECT, query: SYMBOL, limit: 5 });
    qualifiedName = extractQualifiedName(searchText, SYMBOL);
    if (!qualifiedName) {
      throw new Error(`search_graph lieferte keine qualified_name für ${SYMBOL}: ${searchText.slice(0, 500)}`);
    }
    log('qualified_name:', qualifiedName);

    staticPort = await findFreePort(cPort + 1, [cPort]);
    staticServer = await startStaticProxy(staticPort, cPort);
    log(`Static+Proxy auf 127.0.0.1:${staticPort} (Wegwerf-Gerüst)`);

    const url = `http://127.0.0.1:${staticPort}/spike/index.html` +
      `?project=${encodeURIComponent(PROJECT)}&qn=${encodeURIComponent(qualifiedName)}`;
    log('Playwright chromium headless ->', url);
    const browserRun = await driveBrowser(url);
    spike = browserRun.spike;
    log('window.__spike:', JSON.stringify(spike));
    if (browserRun.consoleLines.length > 0) {
      log('Browser-Konsole:\n  ' + browserRun.consoleLines.join('\n  '));
    }
  } catch (err) {
    failure = err;
    console.error('[smoke] FEHLER:', err.message);
    if (serverLog.length > 0) {
      console.error('[smoke] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
    }
  }

  // Aufräumen: alles, was gestartet wurde, wird beendet.
  await stopChild(cServer, 'C-Server');
  await stopServer(staticServer);
  await sleep(500);

  let leftoverProcesses = 0;
  for (const port of [cPort, staticPort]) {
    if (port > 0) { leftoverProcesses += await countListeners(port); }
  }
  log('leftoverProcesses:', leftoverProcesses);

  timings.totalMs = Date.now() - totalStarted;

  let screenshotBytes = 0;
  if (existsSync(OUT_PNG)) {
    screenshotBytes = (await stat(OUT_PNG)).size;
  }

  const report = {
    serverStarted: cServer !== null && timings.serverStartMs !== undefined,
    rpcTool: 'get_code_snippet',
    monacoReadOnly: spike?.monacoReadOnly === true,
    decorationCount: spike?.decorationCount ?? 0,
    port: cPort,
    screenshot: SCREENSHOT_REL,
    leftoverProcesses,
    // Extras, die AC3 nicht prüft, aber beim Lesen des Beweises helfen.
    project: PROJECT,
    qualifiedName: qualifiedName ?? '',
    symbol: SYMBOL,
    filePath: spike?.filePath ?? '',
    startLine: spike?.startLine ?? 0,
    endLine: spike?.endLine ?? 0,
    callSiteFileLine: spike?.callSiteFileLine ?? 0,
    sourceLength: spike?.sourceLength ?? 0,
    indexNodes: indexResult?.nodes ?? 0,
    indexEdges: indexResult?.edges ?? 0,
    indexNotIndexedFiles: indexResult?.not_indexed_files_count ?? 0,
    indexSkipped: indexResult?.skipped_count ?? 0,
    indexParsePartial: indexResult?.parse_partial_count ?? 0,
    staticProxyPort: staticPort,
    screenshotBytes,
    timings,
    monacoVersion: JSON.parse(await readFile(join(ROOT, 'node_modules', 'monaco-editor', 'package.json'), 'utf8')).version,
    generatedAt: new Date().toISOString(),
    error: failure ? failure.message : null,
  };

  await mkdir(OUT_DIR, { recursive: true });
  await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n', 'utf8');
  log('geschrieben:', OUT_JSON);

  const ok = failure === null
    && report.serverStarted === true
    && report.monacoReadOnly === true
    && report.decorationCount >= 1
    && Number.isInteger(report.port) && report.port >= 4200
    && report.leftoverProcesses === 0
    && screenshotBytes > 10 * 1024;

  if (!ok) {
    console.error('[smoke] Spike NICHT grün.');
    if (home) { console.error('[smoke] isoliertes HOME bleibt zum Nachsehen liegen:', home); }
    process.exitCode = 1;
    return;
  }

  // Nur im grünen Fall aufräumen; im roten bleibt der Index zum Nachsehen da.
  if (home) { await rm(home, { recursive: true, force: true }); }
  log('Spike grün.');
}

main().catch((err) => {
  console.error('[smoke] unerwarteter Fehler:', err);
  process.exitCode = 1;
});

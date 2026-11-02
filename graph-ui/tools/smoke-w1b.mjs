#!/usr/bin/env node
/*
 * W1b-Smoke: die portierte Provider-Schicht gegen den echten Server.
 *
 * Was hier bewiesen wird, ist genau das, was Unit-Tests nicht beweisen
 * koennen: dass CbmRpcProvider, rpc-client, der Parser und der IR-Builder
 * zusammen an einem laufenden cbm/build/c/codebase-memory-mcp dieselben
 * Fakten herausholen, die das Fixture wirklich hergibt. Gemockt ist hier
 * nichts. Der Provider bekommt einen base-Ursprung und Node-18-fetch, sonst
 * ist es dieselbe Klasse, die spaeter im Browser laeuft.
 *
 * Ablauf:
 *   1. Binary und Fixture pruefen
 *   2. isoliertes HOME anlegen (persistierte Einstellungen nie ins echte HOME)
 *   3. fixtures/atlas-sample ueber die CLI indizieren; ueber /rpc geht das
 *      nicht, weil die Read-only-Allowlist index_repository nicht anbietet
 *   4. Server auf freiem Port >= 4220 starten
 *   5. src/** mit esbuild zu einem ESM-Bundle machen und importieren. Das ist
 *      kein Umweg, sondern ein zweiter Beweis: was sich buendeln laesst, hat
 *      keine Node-Abhaengigkeit mehr, und genau das ist die Bedingung dafuer,
 *      dass diese Dateien im Browser laufen.
 *   6. den Provider durch die Sequenz fahren: aufloesen, Fakten, Quelltext,
 *      IR, dazu Uebersicht, Pfadlauf und Suche als Zugabe
 *   7. abraeumen, Restprozesse zaehlen, verification/w1/provider.json schreiben
 */

import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
  countListeners,
  findFreePort,
  indexRepository,
  sleep,
  startServer,
  stopServer,
} from './lib/cbm-server.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w1b';
const SERVICE_FILE = 'src/services/userService.ts';
const OUT_DIR = join(ROOT, 'verification', 'w1');
const OUT_JSON = join(OUT_DIR, 'provider.json');
const MIN_PORT = 4220;

/** Deckel des injizierten Lesers, derselbe, den der Provider frueher selbst hielt. */
const MAX_ROUTE_SCAN_BYTES = 512 * 1024;

const log = (...parts) => console.log('[smoke-w1b]', ...parts);
const serverLog = [];

/**
 * Die erste Zeile im Rumpf einer Funktion, aus der Datei gelesen.
 *
 * Eine feste Zahl waere eine zweite Wahrheit ueber das Fixture, die beim
 * naechsten Zeilenumbruch still falsch wird. Gesucht wird die Signaturzeile,
 * genommen wird die darauf folgende, 1-basiert wie der Graph zaehlt.
 */
function bodyLineOf(source, signature) {
  const lines = source.split('\n');
  const index = lines.findIndex((line) => line.startsWith(signature));
  if (index < 0) {
    throw new Error(`Signatur ${signature} steht nicht in ${SERVICE_FILE}`);
  }
  return index + 2;
}

/** Ein Leser fuer den Route-Scan, wie ihn nur ein Node-Aufrufer stellen kann. */
function readSourceFromDisk(absolutePath) {
  try {
    if (statSync(absolutePath).size > MAX_ROUTE_SCAN_BYTES) {
      return undefined;
    }
    return readFileSync(absolutePath, 'utf8');
  } catch {
    return undefined;
  }
}

/** src/** zu einem ESM-Bundle machen, das Node laden kann. */
async function bundleProvider(outDir) {
  const { build } = await import('esbuild');
  const outfile = join(outDir, 'provider-bundle.mjs');
  await build({
    stdin: {
      contents: [
        "export { CbmRpcProvider } from '../src/provider/cbm-rpc-provider';",
        "export { RpcIntelligenceClient } from '../src/provider/rpc-client';",
        "export { buildIr } from '../src/ir/semantic-ir-builder';",
        '',
      ].join('\n'),
      resolveDir: join(ROOT, 'tools'),
      sourcefile: 'w1b-entry.ts',
      loader: 'ts',
    },
    bundle: true,
    format: 'esm',
    // `neutral` gibt dem Bundle keine Node-Globals mit. Ein Import aus node:*
    // wuerde hier auffliegen, statt spaeter im Browser.
    platform: 'neutral',
    target: 'es2022',
    outfile,
    logLevel: 'silent',
  });
  return outfile;
}

const nameOf = (entry) => entry.name ?? '';

async function main() {
  const totalStarted = Date.now();
  let serverChild = null;
  let port = 0;
  let home = null;
  let workDir = null;
  let failure = null;
  const timings = {};
  const report = {
    serverStarted: false,
    port: 0,
    leftoverProcesses: 0,
  };

  try {
    if (!existsSync(BINARY)) {
      throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
    }
    if (!existsSync(FIXTURE)) {
      throw new Error(`Fixture fehlt: ${FIXTURE}`);
    }

    home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w1b-home-'));
    workDir = await mkdtemp(join(tmpdir(), 'codeatlasweb-w1b-work-'));
    log('isoliertes HOME:', home);

    log('indexiere', FIXTURE, 'als', PROJECT, 'ueber die CLI');
    const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
    timings.indexMs = indexed.durationMs;
    log(`indexiert in ${timings.indexMs} ms (nodes=${indexed.nodes}, edges=${indexed.edges})`);

    port = await findFreePort(MIN_PORT);
    log('starte Server auf 127.0.0.1:' + port);
    const started = await startServer(BINARY, { home, port, log: serverLog });
    serverChild = started.child;
    timings.serverStartMs = started.durationMs;
    report.serverStarted = true;
    report.port = port;
    log(`Server bereit nach ${timings.serverStartMs} ms`);

    log('buendele src/** mit esbuild');
    const bundlePath = await bundleProvider(workDir);
    const { CbmRpcProvider, RpcIntelligenceClient, buildIr } =
      await import(pathToFileURL(bundlePath).href);

    const client = new RpcIntelligenceClient({ base: `http://127.0.0.1:${port}` });
    const provider = new CbmRpcProvider(client, {
      generation: 1,
      readSource: readSourceFromDisk,
    });

    const engine = await provider.engineInfo();
    if (engine.available !== true) {
      throw new Error(`engineInfo meldet den Server als nicht verfuegbar: ${engine.detail}`);
    }

    const source = await readFile(join(FIXTURE, SERVICE_FILE), 'utf8');
    const createUserLine = bodyLineOf(source, 'export function createUser(');
    const listUsersLine = bodyLineOf(source, 'export function listUsers(');
    log(`Caret-Zeilen aus der Datei: createUser@${createUserLine}, listUsers@${listUsersLine}`);

    // 1. Aufloesen an einer Zeile im Rumpf von createUser.
    const resolved = await provider.resolveSymbolAt(FIXTURE, SERVICE_FILE, createUserLine);
    if (resolved.kind !== 'ok') {
      throw new Error(`resolveSymbolAt lieferte ${resolved.kind}: ${JSON.stringify(resolved)}`);
    }
    const createUser = resolved.symbol;
    report.resolvedQualifiedName = createUser.qualifiedName ?? '';
    report.resolvedKind = createUser.kind;
    report.resolvedNodeId = createUser.nodeId ?? '';
    report.resolvedRange = createUser.range;
    report.resolvedEnclosing = resolved.enclosing.map((entry) => entry.name);
    report.caretLine = createUserLine;
    log('aufgeloest:', report.resolvedQualifiedName);

    // 2. Alle Faktenfamilien fuer createUser.
    const facts = await provider.getFacts(
      FIXTURE,
      createUser,
      ['callees', 'callers', 'throws', 'envReads', 'typeRefs', 'testedBy'],
    );
    const callees = facts.callees;
    report.calleesSymbol = createUser.qualifiedName;
    report.callees = {
      state: callees.state,
      count: callees.value.length,
      first: {
        name: callees.value[0]?.targetName ?? '',
        line: callees.value[0]?.line ?? 0,
        targetLine: callees.value[0]?.targetLine ?? 0,
        file: callees.value[0]?.targetFile ?? '',
        strategy: callees.value[0]?.strategy ?? '',
      },
      names: callees.value.map((site) => site.targetName),
      constructions: callees.value.filter((site) => site.strategy === 'construction').length,
      evidenceProviderIds: [...new Set(callees.evidence.map((entry) => entry.providerId))],
      evidenceGenerations: [...new Set(callees.evidence.map((entry) => entry.engineGeneration))],
    };
    report.throws = {
      state: facts.throws.state,
      types: facts.throws.value.map((entry) => entry.type),
      lines: facts.throws.value.map((entry) => entry.line ?? 0),
      relations: [...new Set(facts.throws.evidence.map((entry) => entry.relation))],
    };
    report.envReads = {
      state: facts.envReads.state,
      names: facts.envReads.value.map(nameOf),
    };
    report.typeRefs = {
      state: facts.typeRefs.state,
      names: facts.typeRefs.value.map(nameOf),
      qualifiedNames: facts.typeRefs.value.map((entry) => entry.qualifiedName ?? ''),
    };
    // createUser selbst: der Vollstaendigkeit halber mitgeschrieben, damit der
    // Unterschied zu listUsers im Artefakt sichtbar ist und nicht erschlossen
    // werden muss.
    report.createUserCallers = {
      state: facts.callers.state,
      count: facts.callers.value.length,
      names: facts.callers.value.map(nameOf),
      testCallers: facts.callers.value.filter((entry) => entry.isTest).length,
    };
    report.createUserTestedBy = {
      state: facts.testedBy.state,
      count: facts.testedBy.value.length,
    };

    // 3. Aufrufer und Tests fuer listUsers: das ist das Symbol, an dem das
    //    Fixture ueberhaupt eine Testkante hat.
    const resolvedList = await provider.resolveSymbolAt(FIXTURE, SERVICE_FILE, listUsersLine);
    if (resolvedList.kind !== 'ok') {
      throw new Error(`resolveSymbolAt fuer listUsers lieferte ${resolvedList.kind}`);
    }
    const listUsers = resolvedList.symbol;
    const listFacts = await provider.getFacts(FIXTURE, listUsers, ['callers', 'testedBy']);
    report.callersSymbol = listUsers.qualifiedName;
    report.callers = {
      state: listFacts.callers.state,
      count: listFacts.callers.value.length,
      names: listFacts.callers.value.map(nameOf),
      lines: listFacts.callers.value.map((entry) => entry.line ?? 0),
      testCallers: listFacts.callers.value.filter((entry) => entry.isTest).length,
    };
    report.testedBy = {
      state: listFacts.testedBy.state,
      count: listFacts.testedBy.value.length,
      first: listFacts.testedBy.value[0]?.file ?? listFacts.testedBy.value[0]?.name ?? '',
      strategies: [...new Set(listFacts.testedBy.evidence.map((entry) => entry.strategy))],
    };

    // 4. Quelltext und sein Hash.
    const snippet = await provider.getSnippet(FIXTURE, createUser.qualifiedName);
    report.snippetLength = snippet.length;
    report.snippetSha256 = createHash('sha256').update(snippet).digest('hex');
    report.snippetFirstLine = snippet.split('\n')[0];

    // 5. Die IR des aufgeloesten Symbols.
    const built = await buildIr(provider, FIXTURE, createUser, { generation: 1 });
    report.ir = {
      firstStep: built.ir.steps.value[0]?.targetName ?? '',
      stepCount: built.ir.steps.value.length,
      stepLines: built.ir.steps.value.map((step) => step.line ?? 0),
      missingTests: built.ir.missingTests.value,
      missingTestsState: built.ir.missingTests.state,
      complexityState: built.ir.complexity.state,
      writesState: built.ir.writes.state,
      externalEffectsState: built.ir.externalEffects.state,
      purposeState: built.ir.purpose.state,
      purpose: built.ir.purpose.value,
      snippetHash: built.ir.snippetHash ?? '',
      checklistItems: built.ir.checklist.length,
      warnings: built.warnings,
    };
    // Die IR desselben Symbols, an dem das Fixture eine Testkante hat. Ohne
    // sie stuende im Artefakt nur der Fall ohne Test, und der Unterschied
    // zwischen "kein Test gefunden" und "Test gefunden" ist genau das, was der
    // missingTests-Zustand traegt.
    const builtTested = await buildIr(provider, FIXTURE, listUsers, { generation: 1 });
    report.irTested = {
      symbol: listUsers.qualifiedName,
      firstStep: builtTested.ir.steps.value[0]?.targetName ?? '',
      missingTests: builtTested.ir.missingTests.value,
      missingTestsState: builtTested.ir.missingTests.state,
      warnings: builtTested.warnings,
    };

    // 6. Zugabe: was der Port sonst noch traegt. Ein Fehler hier ist ein
    //    Befund und kein Abbruch, denn AC4 haengt nicht daran.
    const extras = {};
    try {
      const overview = await provider.architectureOverview(FIXTURE);
      extras.architecture = {
        project: overview.projectName ?? '',
        totalSymbols: overview.totalSymbols,
        totalRelations: overview.totalRelations,
        languages: overview.languages.map((entry) => entry.language),
        groups: overview.groups.length,
        files: overview.files.length,
        routes: overview.routes.length,
        routeOrigins: [...new Set(overview.routes.map((route) => route.origin))],
        entryPoints: overview.entryPoints.length,
      };
    } catch (error) {
      extras.architectureError = String(error);
    }
    try {
      const deps = await provider.moduleDependencies(FIXTURE);
      extras.moduleDependencies = { edges: deps.edges.length, truncated: deps.truncated };
    } catch (error) {
      extras.moduleDependenciesError = String(error);
    }
    try {
      const walk = await provider.tracePaths(FIXTURE, createUser, 'callees', { maxDepth: 1 });
      extras.trace = walk.status === 'ok'
        ? { status: walk.status, firstLayer: walk.layers[0]?.length ?? 0 }
        : { status: walk.status, candidates: walk.candidates.length };
    } catch (error) {
      extras.traceError = String(error);
    }
    try {
      const hits = await provider.searchSymbols(FIXTURE, 'createUser', 5);
      extras.search = { count: hits.length, first: hits[0]?.qualifiedName ?? '' };
    } catch (error) {
      extras.searchError = String(error);
    }
    try {
      await provider.indexWorkspace(FIXTURE);
      extras.indexWorkspace = 'unerwartet erlaubt';
    } catch (error) {
      extras.indexWorkspaceRefusal = String(error).slice(0, 200);
    }
    report.extras = extras;
  } catch (err) {
    failure = err;
    console.error('[smoke-w1b] FEHLER:', err.message);
    if (serverLog.length > 0) {
      console.error('[smoke-w1b] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
    }
  }

  await stopServer(serverChild);
  await sleep(500);
  report.leftoverProcesses = port > 0 ? await countListeners(port) : 0;
  log('leftoverProcesses:', report.leftoverProcesses);

  timings.totalMs = Date.now() - totalStarted;
  report.project = PROJECT;
  report.fixture = 'fixtures/atlas-sample';
  report.timings = timings;
  report.generatedAt = new Date().toISOString();
  report.error = failure ? failure.message : null;

  await mkdir(OUT_DIR, { recursive: true });
  await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n', 'utf8');
  log('geschrieben:', OUT_JSON);

  if (workDir) { await rm(workDir, { recursive: true, force: true }); }

  const ok = failure === null
    && report.serverStarted === true
    && report.port >= MIN_PORT
    && report.leftoverProcesses === 0
    && report.callees?.count === 6
    && report.callers?.count === 3
    && report.testedBy?.count === 1
    && /^[0-9a-f]{64}$/.test(report.snippetSha256 ?? '');

  if (!ok) {
    console.error('[smoke-w1b] W1b-Smoke NICHT gruen.');
    if (home) { console.error('[smoke-w1b] isoliertes HOME bleibt zum Nachsehen liegen:', home); }
    process.exitCode = 1;
    return;
  }

  if (home) { await rm(home, { recursive: true, force: true }); }
  log('W1b-Smoke gruen.');
}

main().catch((err) => {
  console.error('[smoke-w1b] unerwarteter Fehler:', err);
  process.exitCode = 1;
});

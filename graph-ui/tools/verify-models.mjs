#!/usr/bin/env node
/*
 * Verifikationslauf fuer die beiden gitignorierten Verzeichnisse: models/ und
 * vendor/llama/.
 *
 *   node tools/verify-models.mjs        (oder: npm run verify:models)
 *
 * Committet sind nur models/SHA256SUMS und vendor/llama/HERKUNFT.md. Dieser
 * Lauf ist die Bruecke zwischen den beiden Textdateien und den 11,6 GB daneben:
 * er prueft jede Pruefsumme nach, und er startet den gebauten llama-server
 * wirklich, statt sein blosses Vorhandensein als "laeuft" zu lesen.
 *
 * Drei Entscheidungen, die man sonst raten muesste:
 *
 * **Der Lade-Smoke laeuft mit dem KLEINSTEN Modell und auf einem Testport.**
 * Das Ziel ist "diese Runtime laedt ein GGUF und antwortet", nicht "dieses
 * Modell ist gut". Das kleinste Modell macht daraus einen Lauf von Sekunden.
 * Der Port liegt bei 4300 aufwaerts und nicht auf 4141: der Produktport gehoert
 * dem Sidecar, den ein Leser gerade laufen haben koennte, und ein Pruefskript,
 * das ihm den Port wegnimmt, waere ein Pruefskript, das man nicht laufen lassen
 * kann, waehrend man arbeitet.
 *
 * **`--version` allein reicht nicht.** Es beweist, dass das Binary startet und
 * seine dylibs findet, aber nichts ueber Metal, den GGUF-Leser oder den
 * HTTP-Server. Deshalb wird zusaetzlich ein Modell geladen und `/health` bis
 * `{"status":"ok"}` gefragt. Erst beides zusammen ist `llamaServerRuns`.
 *
 * **Der Prozess wird gezaehlt, nicht gehofft.** Am Ende steht, ob auf dem
 * Testport noch jemand lauscht. Ein Verifikationslauf, der einen 700-MB-Prozess
 * hinterlaesst, hat den naechsten schon verdorben.
 */

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream, existsSync, statSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { countListeners, findFreePort, sleep } from './lib/cbm-server.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const MODELS_DIR = join(ROOT, 'models');
const SUMS = join(MODELS_DIR, 'SHA256SUMS');
const SERVER = join(ROOT, 'vendor', 'llama', 'llama-server');
const HERKUNFT = join(ROOT, 'vendor', 'llama', 'HERKUNFT.md');
const OUT_DIR = join(ROOT, 'verification', 'w5');
const OUT_JSON = join(OUT_DIR, 'models.json');

/** Ab hier werden Testports gesucht. 4141 gehoert dem Sidecar und bleibt frei. */
const MIN_PORT = 4300;

/** Wie lange der Lade-Smoke auf `{"status":"ok"}` warten darf. */
const READY_TIMEOUT_MS = 90000;

const log = (...parts) => console.log('[verify-models]', ...parts);

function sha256(path) {
    return new Promise((done, fail) => {
        const hash = createHash('sha256');
        const stream = createReadStream(path);
        stream.on('error', fail);
        stream.on('data', (chunk) => hash.update(chunk));
        stream.on('end', () => done(hash.digest('hex')));
    });
}

/** Eine Zeile im Format von shasum: `<hex>  <dateiname>`. */
function parseSums(text) {
    return text
        .split('\n')
        .map((line) => line.trim())
        .filter((line) => line.length > 0 && !line.startsWith('#'))
        .map((line) => {
            const match = /^([0-9a-f]{64})\s+\*?(.+)$/i.exec(line);
            if (match === null) {
                return { expected: '', file: line, malformed: true };
            }
            return { expected: match[1].toLowerCase(), file: match[2].trim(), malformed: false };
        });
}

/** Das kleinste Modell, das die Pruefsummendatei nennt und das wirklich daliegt. */
function smallestModel(entries) {
    const present = entries
        .filter((entry) => entry.exists)
        .map((entry) => ({ file: entry.file, size: entry.size }))
        .sort((a, b) => a.size - b.size);
    return present[0];
}

/** Den gebauten Server einmal mit `--version` fragen. */
function serverVersion() {
    return new Promise((done) => {
        const child = spawn(SERVER, ['--version'], { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
        let out = '';
        child.stdout.on('data', (d) => { out += d.toString(); });
        // llama-server schreibt seine Version auf stderr. Beides wird gelesen,
        // damit die Antwort nicht davon abhaengt, welchen Strom diese Fassung
        // gerade benutzt.
        child.stderr.on('data', (d) => { out += d.toString(); });
        child.on('error', (error) => done({ code: 127, out: error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

/**
 * Ein Modell wirklich laden und auf `{"status":"ok"}` warten.
 *
 * Waehrend des Ladens antwortet llama-server mit 503 und `Loading model`; das
 * ist der erwartete Zwischenstand und kein Fehler. Erst 200 mit `status: ok`
 * heisst geladen.
 */
async function loadSmoke(model, port) {
    const started = Date.now();
    const child = spawn(
        SERVER,
        ['--host', '127.0.0.1', '--port', String(port), '-m', join('models', model), '-c', '512'],
        { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] },
    );
    const output = [];
    child.stdout.on('data', (d) => output.push(d.toString()));
    child.stderr.on('data', (d) => output.push(d.toString()));

    let exited = null;
    child.on('exit', (code, signal) => { exited = { code, signal }; });

    const result = { ready: false, durationMs: 0, statuses: [], props: null, error: '' };
    const deadline = Date.now() + READY_TIMEOUT_MS;
    while (Date.now() < deadline) {
        if (exited !== null) {
            result.error = `llama-server endete vorzeitig (code=${exited.code}, signal=${exited.signal})`;
            break;
        }
        try {
            const response = await fetch(`http://127.0.0.1:${port}/health`);
            const body = await response.text();
            const seen = `${response.status} ${body.slice(0, 60)}`;
            if (result.statuses[result.statuses.length - 1] !== seen) {
                result.statuses.push(seen);
            }
            if (response.status === 200 && body.includes('"ok"')) {
                result.ready = true;
                break;
            }
        } catch {
            // Der Port ist noch nicht offen. Erwartet in der ersten Sekunde.
        }
        await sleep(500);
    }
    result.durationMs = Date.now() - started;

    if (result.ready) {
        try {
            const props = await (await fetch(`http://127.0.0.1:${port}/props`)).json();
            result.props = {
                model_path: props.model_path ?? '',
                model_ftype: props.model_ftype ?? '',
                n_ctx: props.default_generation_settings?.n_ctx ?? null,
                total_slots: props.total_slots ?? null,
            };
        } catch (error) {
            result.error = `/props war nicht lesbar: ${error.message}`;
        }
    }

    if (exited === null) {
        child.kill('SIGTERM');
        for (let i = 0; i < 50 && exited === null; i += 1) {
            await sleep(100);
        }
        if (exited === null) {
            child.kill('SIGKILL');
            await sleep(300);
        }
    }
    result.tail = output.join('').split('\n').filter((line) => line.trim().length > 0).slice(-12);
    return result;
}

async function main() {
    const started = Date.now();
    let port = 0;
    const report = {
        files: 0,
        checksumsOk: false,
        llamaServerRuns: false,
        version: '',
    };
    const extras = { models: [], missing: [], unexpected: [], errors: [] };

    if (!existsSync(SUMS)) {
        throw new Error(`models/SHA256SUMS fehlt: ${SUMS}`);
    }
    if (!existsSync(HERKUNFT)) {
        throw new Error(`vendor/llama/HERKUNFT.md fehlt: ${HERKUNFT}`);
    }

    // ------------------------------------------------------ 1. Pruefsummen
    const entries = parseSums(await readFile(SUMS, 'utf8'));
    report.files = entries.length;
    let allOk = entries.length > 0;
    for (const entry of entries) {
        const path = join(MODELS_DIR, entry.file);
        const record = { file: entry.file, expected: entry.expected, exists: existsSync(path), size: 0, actual: '', ok: false };
        if (entry.malformed) {
            record.note = 'Zeile hat nicht die Form "<sha256>  <datei>"';
            extras.errors.push(`unlesbare Zeile in SHA256SUMS: ${entry.file}`);
            allOk = false;
            extras.models.push(record);
            continue;
        }
        if (!record.exists) {
            extras.missing.push(entry.file);
            allOk = false;
            extras.models.push(record);
            continue;
        }
        record.size = statSync(path).size;
        record.actual = await sha256(path);
        record.ok = record.actual === entry.expected;
        if (!record.ok) {
            allOk = false;
        }
        log(`${record.ok ? 'ok  ' : 'FALSCH'} ${entry.file} (${record.size} Byte)`);
        extras.models.push(record);
    }
    report.checksumsOk = allOk;

    // Was in models/ liegt, aber in keiner Zeile steht. Kein Fehler, aber ein
    // Befund: eine Datei ohne Pruefsumme ist eine Datei ohne Herkunft.
    const { readdir } = await import('node:fs/promises');
    const named = new Set(entries.map((entry) => entry.file));
    for (const name of await readdir(MODELS_DIR)) {
        if (name !== 'SHA256SUMS' && !named.has(name)) {
            extras.unexpected.push(name);
        }
    }

    // ------------------------------------------------ 2. Die Runtime selbst
    if (!existsSync(SERVER)) {
        extras.errors.push(`llama-server fehlt: ${SERVER}`);
    } else {
        const version = await serverVersion();
        const line = version.out.split('\n').map((l) => l.trim()).find((l) => l.startsWith('version:')) ?? '';
        report.version = line.replace(/^version:\s*/, '');
        extras.versionExit = version.code;
        extras.versionOutput = version.out.trim().split('\n').slice(0, 4);
        log(`llama-server --version: ${report.version || '(keine Zeile "version:")'}`);

        const smallest = smallestModel(extras.models);
        if (smallest === undefined) {
            extras.errors.push('kein Modell zum Laden vorhanden');
        } else {
            port = await findFreePort(MIN_PORT);
            log(`Lade-Smoke mit ${smallest.file} auf Port ${port}`);
            const smoke = await loadSmoke(smallest.file, port);
            extras.loadSmoke = { model: smallest.file, port, ...smoke };
            report.llamaServerRuns = report.version.length > 0 && smoke.ready === true;
            log(`Lade-Smoke: ready=${smoke.ready} nach ${smoke.durationMs} ms`);
            if (smoke.error.length > 0) {
                extras.errors.push(smoke.error);
            }
        }
    }

    await sleep(300);
    extras.leftoverProcesses = port > 0 ? await countListeners(port) : 0;
    log('leftoverProcesses:', extras.leftoverProcesses);

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            durationMs: Date.now() - started,
            generatedAt: new Date().toISOString(),
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const ok =
        report.checksumsOk === true
        && report.llamaServerRuns === true
        && extras.leftoverProcesses === 0
        && extras.errors.length === 0;
    if (!ok) {
        console.error('[verify-models] NICHT gruen:', JSON.stringify({
            checksumsOk: report.checksumsOk,
            llamaServerRuns: report.llamaServerRuns,
            leftoverProcesses: extras.leftoverProcesses,
            errors: extras.errors,
        }));
        process.exitCode = 1;
        return;
    }
    log(`gruen: ${report.files} Modelle, Runtime ${report.version}`);
}

main().catch((err) => {
    console.error('[verify-models] Fehler:', err.message);
    process.exitCode = 1;
});

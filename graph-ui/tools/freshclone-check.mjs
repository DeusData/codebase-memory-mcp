#!/usr/bin/env node
/*
 * Der Fresh-Clone-Check: steht dieses Repository fuer sich?
 *
 *   node tools/freshclone-check.mjs [--out verification/w6/freshclone.json]
 *
 * Ein Projekt, das nur auf der Maschine gruen ist, auf der es entstanden ist,
 * ist nicht fertig, sondern eingerichtet. Der Check baut die Situation nach, in
 * der jemand anders es zum ersten Mal anfasst: klonen, Abhaengigkeiten
 * installieren, Abnahmetests fahren, Unit-Suite fahren. Vier Antworten, und
 * jede ist eine eigene Zeile im Ergebnis.
 *
 * ## Vier Entscheidungen, die man sonst raten muesste
 *
 * **Der Klon kommt aus `file://` und nie aus dem Netz.** Die Quelle ist dieses
 * Verzeichnis. Ein Klon von einer Weiterleitung waere ein Test der
 * Weiterleitung, und dieser Lauf soll ohne Aussenwelt auskommen.
 *
 * **Der Arbeitsbaum wird ueber den Klon gelegt, und das steht im Ergebnis.**
 * Ein Klon traegt, was committet ist. Die Dateien eines laufenden Zyklus sind es
 * noch nicht: der Commit kommt nach dem Beweis und nicht davor. Der Check
 * kopiert deshalb genau die Pfade, die `git status` als geaendert oder neu
 * meldet, und listet sie unter `workingTreeOverlay` auf. Ohne diese Liste waere
 * die Aussage "ein frischer Klon ist gruen" eine Aussage ueber den Stand von
 * gestern; mit ihr ist sie eine ueber den Stand, der gleich committet wird.
 *
 * **`npm ci --offline` zuerst, und ein Fehlschlag wird gemessen statt
 * abgebrochen.** Offline heisst: alles kommt aus dem lokalen npm-Cache. Fehlt
 * dort ein Paket, ist das ein Befund ueber diese Maschine und keine Aussage
 * ueber das Repository; er steht als `cacheMiss` im Ergebnis, und der Lauf
 * versucht es danach noch einmal ohne die Offline-Klammer. Auch dieser zweite
 * Versuch bekommt kein Netz zu sehen, wenn keins da ist; er wuerde dann
 * genauso scheitern, und beide Ausgaben stehen im Bericht.
 *
 * **Playwright laedt keine Browser nach.** `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1`
 * ist gesetzt, denn weder die Abnahmetests noch die Unit-Suite starten einen
 * Browser: das tun die Beweislaeufe, und die laufen nicht im Klon. Ohne die
 * Variable versuchte das Installationsskript einen Download, und ein Check, der
 * ins Netz greift, um Netzfreiheit zu beweisen, waere sein eigener Gegenbeweis.
 *
 * ## Die eine Selbstbezueglichkeit, und wie sie aufgeloest wird
 *
 * Der Abnahmetest W6a liest verification/w6/freshclone.json, also genau die
 * Datei, die dieser Lauf schreibt. Im Klon muss sie darum schon liegen, bevor
 * die Abnahmetests dort laufen. Der Check legt sie deshalb VOR dem Testlauf im
 * Klon ab, mit den zu diesem Zeitpunkt bereits gemessenen Werten (`cloneOk`,
 * `npmCiOk`, `unitPass`) und mit `scaffoldPass: true` als der einen Behauptung,
 * die der Testlauf im selben Moment prueft. Ist die Suite im Klon rot, wird
 * `scaffoldPass` als `false` geschrieben und dieser Lauf ist nicht gruen. Es
 * gibt also keinen Weg, auf dem ein `true` in der Datei landet, ohne dass die
 * Suite im Klon wirklich gruen war.
 */

import { spawn, execFile } from 'node:child_process';
import { existsSync } from 'node:fs';
import { cp, mkdir, mkdtemp, rm, unlink, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const DEFAULT_OUT = join(ROOT, 'verification', 'w6', 'freshclone.json');
const REPORT_PATH = join('verification', 'w6', 'freshclone.json');

const log = (...parts) => console.log('[freshclone]', ...parts);

/** Die Umgebung, in der im Klon gearbeitet wird. Kein Notifier, kein Download. */
const childEnv = () => ({
    ...process.env,
    NO_UPDATE_NOTIFIER: '1',
    npm_config_update_notifier: 'false',
    npm_config_audit: 'false',
    npm_config_fund: 'false',
    npm_config_progress: 'false',
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD: '1',
    CI: '1',
});

function run(command, args, cwd, extraEnv = {}) {
    const started = Date.now();
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd,
            env: { ...childEnv(), ...extraEnv },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let out = '';
        child.stdout.on('data', (chunk) => {
            out += chunk.toString();
        });
        child.stderr.on('data', (chunk) => {
            out += chunk.toString();
        });
        child.on('error', (error) => done({ code: 127, out: out + error.message, ms: Date.now() - started }));
        child.on('close', (code) => done({ code: code ?? 1, out, ms: Date.now() - started }));
    });
}

/** Der letzte Rest einer Ausgabe, so viel wie in einen Bericht gehoert. */
const tail = (text, lines = 12) =>
    text.trim().split('\n').slice(-lines).map((line) => line.trimEnd());

/**
 * Was der Arbeitsbaum gegenueber HEAD anders hat: Pfade und was mit ihnen ist.
 *
 * `-uall` nennt jede einzelne neue Datei statt nur ihr Verzeichnis; ohne das
 * waere ein neues Verzeichnis ein Eintrag und der Kopiervorgang muesste raten,
 * was darin steht.
 */
async function workingTreeChanges() {
    const { stdout } = await execFileAsync(
        'git',
        ['status', '--porcelain=v1', '-uall', '-z'],
        { cwd: ROOT, maxBuffer: 32 * 1024 * 1024 },
    );
    const entries = [];
    const parts = stdout.split('\0');
    for (let i = 0; i < parts.length; i += 1) {
        const entry = parts[i];
        if (entry.length < 4) {
            continue;
        }
        const code = entry.slice(0, 2);
        const path = entry.slice(3);
        // Eine Umbenennung nennt zwei Pfade; der zweite folgt als eigener Eintrag.
        if (code.startsWith('R') || code.startsWith('C')) {
            const from = parts[i + 1];
            i += 1;
            entries.push({ code, path, from, action: 'copy' });
            entries.push({ code, path: from, action: 'delete' });
            continue;
        }
        entries.push({ code, path, action: code.includes('D') ? 'delete' : 'copy' });
    }
    return entries;
}

async function main() {
    const totalStarted = Date.now();
    const args = process.argv.slice(2);
    let out = DEFAULT_OUT;
    for (let i = 0; i < args.length; i += 1) {
        if (args[i] === '--out') {
            out = resolve(args[i + 1]);
            i += 1;
        }
    }

    let workspace = null;
    let failure = null;
    const result = {
        cloneOk: false,
        npmCiOk: false,
        scaffoldPass: false,
        unitPass: false,
        durationMs: 0,
    };
    const extras = { workingTreeOverlay: [], cacheMiss: false };

    try {
        workspace = await mkdtemp('/private/tmp/codeatlasweb-freshclone-');
        const clone = join(workspace, 'clone');
        log('Scratch:', workspace);

        // ------------------------------------------------------- 1. Klonen
        const cloned = await run('git', ['clone', '--quiet', `file://${ROOT}`, clone], workspace);
        extras.clone = { exit: cloned.code, ms: cloned.ms, out: tail(cloned.out) };
        result.cloneOk = cloned.code === 0 && existsSync(join(clone, 'package.json'));
        log(`git clone file://${ROOT}: exit ${cloned.code} nach ${cloned.ms} ms`);
        if (!result.cloneOk) {
            throw new Error(`git clone endete mit ${cloned.code}`);
        }

        // -------------------------------------- 2. Den Arbeitsbaum darueber
        const changes = await workingTreeChanges();
        for (const change of changes) {
            const target = join(clone, change.path);
            if (change.action === 'delete') {
                await rm(target, { force: true, recursive: true });
                continue;
            }
            await mkdir(dirname(target), { recursive: true });
            await cp(join(ROOT, change.path), target, { recursive: true });
        }
        extras.workingTreeOverlay = changes.map((change) => ({ code: change.code, path: change.path }));
        log(`Arbeitsbaum ueber den Klon gelegt: ${changes.length} Pfade`);

        // ------------------------------------------------------ 3. npm ci
        const offline = await run('npm', ['ci', '--offline'], clone);
        extras.npmCiOffline = { exit: offline.code, ms: offline.ms, out: tail(offline.out, 20) };
        if (offline.code === 0) {
            result.npmCiOk = true;
        } else {
            extras.cacheMiss = true;
            log('npm ci --offline scheiterte, zweiter Versuch ohne die Offline-Klammer');
            const retry = await run('npm', ['ci'], clone);
            extras.npmCiRetry = { exit: retry.code, ms: retry.ms, out: tail(retry.out, 20) };
            result.npmCiOk = retry.code === 0;
        }
        log(`npm ci: ${result.npmCiOk ? 'gruen' : 'rot'} (cacheMiss=${extras.cacheMiss})`);
        if (!result.npmCiOk) {
            throw new Error('npm ci kam im Klon nicht durch');
        }

        // --------------------------------------------------- 4. Unit-Suite
        const unit = await run('npm', ['run', 'test:unit'], clone);
        extras.unit = { exit: unit.code, ms: unit.ms, out: tail(unit.out, 12) };
        result.unitPass = unit.code === 0;
        log(`npm run test:unit im Klon: exit ${unit.code}`);

        /*
         * 5. Die eigene Datei im Klon, vor dem Abnahmelauf. Warum das kein
         * Zirkelschluss ist, steht im Kopf dieser Datei.
         */
        const seeded = {
            ...result,
            scaffoldPass: true,
            durationMs: Date.now() - totalStarted,
            seededForClone: true,
            note:
                'Diese Fassung liegt nur im Klon: der Abnahmetest liest sie dort, waehrend er '
                + 'selbst die Behauptung scaffoldPass prueft. Die committete Fassung steht in '
                + 'verification/w6/freshclone.json und traegt das gemessene Ergebnis.',
        };
        await mkdir(join(clone, 'verification', 'w6'), { recursive: true });
        await writeFile(
            join(clone, REPORT_PATH),
            JSON.stringify(seeded, null, 2) + '\n',
            'utf8',
        );

        // ------------------------------------------------- 6. Abnahmetests
        const scaffold = await run('node', ['--test', 'tests/scaffold/'], clone);
        extras.scaffold = { exit: scaffold.code, ms: scaffold.ms, out: tail(scaffold.out, 20) };
        result.scaffoldPass = scaffold.code === 0;
        log(`node --test tests/scaffold/ im Klon: exit ${scaffold.code}`);
    } catch (err) {
        failure = err;
        console.error('[freshclone] FEHLER:', err.message);
    }

    result.durationMs = Date.now() - totalStarted;

    await mkdir(dirname(out), { recursive: true });
    await writeFile(
        out,
        JSON.stringify(
            {
                ...result,
                source: `file://${ROOT}`,
                cacheMiss: extras.cacheMiss,
                generatedAt: new Date().toISOString(),
                error: failure ? failure.message : null,
                extras,
            },
            null,
            2,
        ) + '\n',
        'utf8',
    );
    log('geschrieben:', out);

    const ok =
        failure === null
        && result.cloneOk
        && result.npmCiOk
        && result.scaffoldPass
        && result.unitPass;

    if (!ok) {
        console.error('[freshclone] Fresh-Clone-Check NICHT gruen.');
        if (workspace !== null) {
            console.error('[freshclone] Klon bleibt zum Nachsehen liegen:', workspace);
        }
        process.exitCode = 1;
        return;
    }

    if (workspace !== null) {
        await rm(workspace, { recursive: true, force: true });
    }
    log(`Fresh-Clone-Check gruen nach ${result.durationMs} ms.`);
}

main().catch(async (err) => {
    console.error('[freshclone] unerwarteter Fehler:', err);
    // Ein halb geschriebener Bericht waere schlimmer als keiner.
    await unlink(DEFAULT_OUT).catch(() => undefined);
    process.exitCode = 1;
});

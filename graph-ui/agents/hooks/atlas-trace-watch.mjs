#!/usr/bin/env node
/**
 * Die zweite Quelle: Schreibvorgaenge auf der Platte.
 *
 * Sie ist der Rueckfall fuer alles, was keine Werkzeug-Hooks kennt. Ein Agent,
 * der mit einem Editor oder einem Skript arbeitet, ruft keinen Hook auf; was
 * dabei trotzdem sichtbar ist, ist die Datei, die sich aendert.
 *
 * ## Was sie sieht, und was nicht
 *
 * Sie sieht **nur Schreibvorgaenge**. Kein Lesen, kein Suchen, kein Testlauf,
 * kein Befehl. Genau so ist jedes Ereignis gekennzeichnet (`source: "fs"`), und
 * das ist keine Formalie: wer spaeter eine Aufzeichnung ansieht, muss wissen,
 * dass die Ruhe zwischen zwei Schreibvorgaengen nicht Ruhe bedeutet, sondern
 * Arbeit, die hier nicht ankommt. Ein Protokoll, das seine eigene Blindheit
 * verschweigt, ist schlimmer als keines.
 *
 * Sie schreibt **keine Dateiinhalte**, so wenig wie der Hook daneben: ein
 * Ereignis nennt den Pfad und die Zeit, mehr nicht. Einen Zeilenbereich kann
 * sie nicht nennen, denn ein Dateisystem meldet keine Zeilen; das Feld fehlt
 * darum, statt eine Zahl zu erfinden.
 *
 * ## Aufruf
 *
 *   node agents/hooks/atlas-trace-watch.mjs <verzeichnis>
 *
 * Wohin geschrieben wird, sagt ATLAS_TRACE_FILE; ohne die Variable ist es
 * ~/.atlas-trace/events.jsonl. Wie der Agent heisst, sagt ATLAS_AGENT_NAME;
 * ohne sie steht dort "file-writes", weil das die Wahrheit ueber diese Quelle
 * ist: sie kennt keinen Agenten, sie kennt eine Datei, die sich geaendert hat.
 *
 * Kein Paket, nur node:fs.
 */
import { appendFileSync, mkdirSync, watch } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { homedir } from 'node:os';

const ROOT = process.argv[2] ?? process.cwd();
const OUT = process.env['ATLAS_TRACE_FILE']
    ?? join(homedir(), '.atlas-trace', 'events.jsonl');
const AGENT = process.env['ATLAS_AGENT_NAME'] ?? 'file-writes';

/** Der Lauf. Acht Zeichen, wie die Kennung des Hooks daneben. */
const RUN = String(Date.now()).slice(-8);

/** Was nie interessiert: Bauwerk, Fremdcode, Beweisbilder, das Protokoll selbst. */
const IGNORE = /(^|\/)(node_modules|\.git|dist|cbm|vendor|models|verification|coverage)(\/|$)/;

/**
 * Derselbe Pfad zweimal in dieser Zeitspanne ist ein Ereignis, nicht zwei.
 *
 * Ein Editor schreibt gern mehrfach (Datei anlegen, Inhalt schreiben, Rechte
 * setzen), und ein Bild soll die Handlung zeigen und nicht die Schreibtechnik.
 */
const COALESCE_MS = 400;

const lastSeen = new Map();
let seq = 0;

function record(path) {
    const now = Date.now();
    if (now - (lastSeen.get(path) ?? 0) < COALESCE_MS) {
        return;
    }
    lastSeen.set(path, now);
    seq += 1;
    const line = {
        ts: now,
        agent: AGENT,
        run: RUN,
        seq,
        phase: 'end',
        tool: 'Write',
        path,
        source: 'fs',
        detail: 'seen as a write on disk; reads, searches and test runs are invisible here',
    };
    try {
        appendFileSync(OUT, JSON.stringify(line) + '\n', 'utf8');
        process.stdout.write(`${new Date(now).toISOString()}  ${path}\n`);
    } catch {
        /* Ein volles Dateisystem kostet das Protokoll, nicht die Arbeit. */
    }
}

try {
    mkdirSync(dirname(OUT), { recursive: true });
} catch {
    /* Steht es schon da, ist alles gut; steht es nicht, meldet der erste Schreibversuch es. */
}

watch(ROOT, { recursive: true }, (_event, name) => {
    if (typeof name !== 'string' || IGNORE.test(name)) {
        return;
    }
    record(relative('.', name) || name);
});

process.stdout.write(`watching ${ROOT}, appending to ${OUT}\n`);

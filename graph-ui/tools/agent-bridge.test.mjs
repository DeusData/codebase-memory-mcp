/*
 * Die Bruecke, gegen einen echten Loopback-Port.
 *
 * Vier Zusicherungen, und drei davon sind Verbote:
 *
 *  1. Sie LIEST nur. Es gibt keine Route, die etwas hinzufuegt, und die
 *     Ereignisdatei ist nach dem Lauf byteidentisch.
 *  2. Sie spricht nur Loopback: ein fremder Ursprung wird abgewiesen.
 *  3. Im Wiedergabemodus schweigt sie, bis jemand den Takt setzt. Ohne diese
 *     Naht muesste ein Beweislauf auf die Wanduhr warten.
 *  4. Die Wiederaufnahme ueberspringt genau das, was der Klient schon hat, und
 *     nichts sonst.
 */

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { isAfter, parseArgs, parseSince, startBridge } from './agent-bridge.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const EVENTS = join(ROOT, 'fixtures', 'agent-events', 'w11a-replay.jsonl');

const sha256 = (path) => createHash('sha256').update(readFileSync(path)).digest('hex');

/** Einen SSE-Strom lesen, bis `wanted` Ereignisse angekommen sind. */
async function readStream(url, wanted, headers = {}) {
    const controller = new AbortController();
    const response = await fetch(url, {
        headers: { Accept: 'text/event-stream', ...headers },
        signal: controller.signal,
    });
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const events = [];
    let hello = null;
    let buffer = '';
    const deadline = Date.now() + 2500;
    while (events.length < wanted && Date.now() < deadline) {
        /*
         * Ein Wettlauf gegen die Uhr, weil ein Strom nicht endet: `read()`
         * wartet auf das naechste Stueck, und wenn keines mehr kommt (was der
         * gute Fall ist, solange der Takt steht), wartet es fuer immer.
         */
        const chunk = await Promise.race([
            reader.read(),
            new Promise((done) => setTimeout(() => done({ done: true, timedOut: true }), 600)),
        ]);
        if (chunk.done) {
            break;
        }
        buffer += decoder.decode(chunk.value, { stream: true });
        const parts = buffer.split('\n\n');
        buffer = parts.pop() ?? '';
        for (const part of parts) {
            const name = /^event: (.+)$/m.exec(part)?.[1];
            const data = /^data: (.+)$/m.exec(part)?.[1];
            if (data === undefined) {
                continue;
            }
            if (name === 'hello') {
                hello = JSON.parse(data);
            } else if (name === 'trace') {
                events.push(JSON.parse(data));
            }
        }
    }
    controller.abort();
    return { hello, events, status: response.status };
}

let bridge = null;
let port = 0;
let hashBefore = '';

beforeAll(async () => {
    hashBefore = sha256(EVENTS);
    /* Ein hoher, unwahrscheinlicher Port: 4141, 4142, 4390 und 4391 gehoeren
     * dem Arbeitsplatz, alles bis 4700 den Beweislaeufen. */
    for (let candidate = 4810; candidate < 4860; candidate += 1) {
        try {
            bridge = await startBridge({ file: EVENTS, port: candidate, mode: 'replay' });
            port = candidate;
            break;
        } catch {
            // belegt, der naechste
        }
    }
    if (bridge === null) {
        throw new Error('kein freier Port fuer die Bruecke gefunden');
    }
});

afterAll(async () => {
    await bridge?.close();
});

describe('die Bruecke', () => {
    it('liest die Aufzeichnung und sagt, was sie gelesen hat', async () => {
        const response = await fetch(`http://127.0.0.1:${port}/health`);
        const health = await response.json();
        expect(health.mode).toBe('replay');
        expect(health.events).toBe(45);
        expect(health.unreadable).toBe(0);
        expect(health.emitted).toBe(0);
    });

    it('schweigt im Wiedergabemodus, bis jemand den Takt setzt', async () => {
        const before = await readStream(`http://127.0.0.1:${port}/events`, 1);
        expect(before.hello?.mode).toBe('replay');
        expect(before.events).toHaveLength(0);

        const answer = await fetch(`http://127.0.0.1:${port}/replay/advance?count=3`, {
            method: 'POST',
        });
        expect((await answer.json()).emitted).toBe(3);

        const after = await readStream(`http://127.0.0.1:${port}/events`, 3);
        expect(after.events).toHaveLength(3);
        expect(after.events[0].agent).toBe('explorer');
    });

    it('kennzeichnet jede Wiedergabe als solche und behaelt die aufgezeichnete Zeit', async () => {
        const { events } = await readStream(`http://127.0.0.1:${port}/events`, 3);
        for (const event of events) {
            expect(event.replay).toBe(true);
            expect(event.ts_recorded).toBeLessThan(event.ts);
        }
        // Die Abstaende bleiben, was sie waren.
        const shift = events[0].ts - events[0].ts_recorded;
        for (const event of events) {
            expect(event.ts - event.ts_recorded).toBe(shift);
        }
    });

    it('nimmt bei der Wiederaufnahme genau das wieder auf, was fehlt', async () => {
        const seen = 'c3a80f52:1,a1f2c3d4:1';
        const { events } = await readStream(
            `http://127.0.0.1:${port}/events?since=${encodeURIComponent(seen)}`, 1,
        );
        expect(events.map((event) => `${event.run}:${event.seq}`)).toEqual(['b7e4d19c:1']);
    });

    it('weist einen fremden Ursprung ab', async () => {
        const response = await fetch(`http://127.0.0.1:${port}/health`, {
            headers: { Origin: 'http://example.com' },
        });
        expect(response.status).toBe(403);
        await response.text();
    });

    it('kennt keine Route, die etwas hinzufuegt', async () => {
        for (const route of ['/events', '/append', '/write', '/']) {
            const response = await fetch(`http://127.0.0.1:${port}${route}`, {
                method: 'POST',
                body: JSON.stringify({ ts: 1, agent: 'x', run: 'r', seq: 1, tool: 'Edit' }),
                headers: { 'Content-Type': 'application/json' },
            });
            await response.text();
            expect([404, 405], route).toContain(response.status);
        }
        expect(sha256(EVENTS)).toBe(hashBefore);
    });
});

describe('die reinen Teile', () => {
    it('liest die Argumente ohne Bibliothek', () => {
        const options = parseArgs(['--replay', 'a.jsonl', '--port', '4711']);
        expect(options.mode).toBe('replay');
        expect(options.port).toBe(4711);
        expect(options.file.endsWith('a.jsonl')).toBe(true);
        expect(parseArgs([]).mode).toBe('live');
        expect(parseArgs([]).port).toBe(4142);
    });

    it('uebergeht eine unlesbare Wiederaufnahme-Angabe, statt den Strom zu verweigern', () => {
        expect([...parseSince('a:1,b:2').entries()]).toEqual([['a', 1], ['b', 2]]);
        expect(parseSince('nonsense').size).toBe(0);
        expect(parseSince(undefined).size).toBe(0);
    });

    it('schickt ein Ereignis genau dann, wenn es hinter der bekannten Nummer liegt', () => {
        const seen = parseSince('r:5');
        expect(isAfter({ run: 'r', seq: 5 }, seen)).toBe(false);
        expect(isAfter({ run: 'r', seq: 6 }, seen)).toBe(true);
        expect(isAfter({ run: 'other', seq: 1 }, seen)).toBe(true);
    });
});

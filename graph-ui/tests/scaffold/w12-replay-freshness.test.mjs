// W12 correction acceptance: replay time belongs to the measured agent state.
// Run: node --test tests/scaffold/w12-replay-freshness.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const artifact = () => JSON.parse(readFileSync(
    join(ROOT, 'verification', 'w12', 'buttons.json'), 'utf8',
));

test('AC1: each measured agent state gets a fresh local replay source', () => {
    const readings = artifact().extras?.agentReplayReadings;
    assert.ok(readings && typeof readings === 'object',
        'die direkten Replay-Messungen je Agentenzustand fehlen');
    for (const state of ['agents-live', 'agents-fullscreen']) {
        const reading = readings[state];
        assert.ok(reading, `${state}: Replay-Messung fehlt`);
        assert.equal(reading.reason, `fresh for ${state}`,
            `${state}: die Quelle wurde nicht unmittelbar fuer diesen Zustand erneuert`);
        assert.equal(reading.health?.mode, 'replay');
        assert.equal(reading.health?.events, 73,
            `${state}: die lokale W11b-Fixture ist nicht vollstaendig`);
        assert.equal(reading.health?.emitted, 0,
            `${state}: die Quelle war vor der Zustandsmessung nicht frisch`);
        assert.deepEqual(reading.advance, { emitted: 73, total: 73, remaining: 0 },
            `${state}: der echte Replay-Advance lieferte nicht alle Ereignisse`);
        assert.ok(Number.isInteger(reading.actors?.total) && reading.actors.total >= 2,
            `${state}: weniger als zwei Akteure sichtbar`);
        assert.ok(Number.isInteger(reading.actors?.foreign) && reading.actors.foreign >= 1,
            `${state}: kein fremder Replay-Akteur sichtbar`);
        assert.ok(Number.isInteger(reading.actors?.you) && reading.actors.you >= 1,
            `${state}: die lokale Leser-Akteurin fehlt`);
    }
});

test('AC2: the fresh replay states complete without a cascading overlay failure', () => {
    const states = artifact().extras?.store?.states;
    for (const state of ['agents-live', 'agents-fullscreen', 'settings-flat']) {
        assert.equal(states?.[state]?.complete, true,
            `${state}: der Zustand ist nicht vollstaendig erreichbar`);
        assert.deepEqual(states?.[state]?.findings, [],
            `${state}: der Zustand hinterliess Befunde`);
    }
});

test('AC3: the last two rounds are new, complete and finding-free', () => {
    const rounds = artifact().rounds;
    assert.ok(Array.isArray(rounds) && rounds.length >= 5,
        'nach dem roten vierten Versuch fehlen zwei neue vollstaendige Runden');
    const last = rounds.slice(-2);
    assert.deepEqual(last.map((round) => round.n), [4, 5],
        'nur die neuen Durchgaenge 4 und 5 duerfen den korrigierten Harness belegen');
    for (const round of last) {
        assert.equal(round.complete, true, `Durchgang ${round.n} ist nicht vollstaendig`);
        assert.equal(round.newFindings, 0, `Durchgang ${round.n} hat neue Befunde`);
        assert.equal(round.states?.length, 22, `Durchgang ${round.n} misst nicht alle Zustaende`);
        assert.equal(round.filters, 30, `Durchgang ${round.n} misst nicht alle Filter`);
    }
});

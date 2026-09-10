// Acceptance for the release blockers found by the independent W12/W15 audit.
// Run: node --test tests/scaffold/release-blockers.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const json = (path) => JSON.parse(readFileSync(join(ROOT, path), 'utf8'));
const w15 = () => json('verification/w15/hybrid.json');
const w12 = () => json('verification/w12/buttons.json');

test('AC1: a normal chat question stays built after the model is enabled', () => {
    const reading = w15().ordinaryChatAfterEnable;
    assert.ok(reading, 'die Browsermessung einer neuen Frage nach dem Einschalten fehlt');
    assert.equal(reading.completionRequests, 0,
        'eine normale Chatfrage darf ohne AI-Klick keine Completion anfordern');
    assert.equal(reading.status, 'answered');
    assert.ok(reading.cards >= 1, 'die gebaute Antwort braucht Karten');
    assert.ok(reading.citations >= 1, 'die gebaute Antwort braucht Belege');
    assert.equal(reading.builtProvenanceVisible, true,
        'die normale Antwort muss ihre gebaute Herkunft zeigen');
    assert.equal(reading.aiProvenanceVisible, false,
        'ohne AI-Klick darf keine Modellherkunft erscheinen');
    assert.equal(reading.aiButtonVisible, true,
        'die Modellfassung muss als ausdrueckliche Wahl angeboten werden');
});

test('AC2: every explicit AI button uses the selected router model', () => {
    const artifact = w15();
    assert.ok(typeof artifact.selectedRouterModel === 'string'
        && artifact.selectedRouterModel.length > 5,
    'die aktive Router-Modell-id fehlt');
    assert.equal(artifact.requestsBeforeFirstAiClick, 0,
        'vor dem ersten AI-Klick ging eine Completion-Anfrage ab');
    for (const area of ['twin', 'flow', 'chat']) {
        const reading = artifact.areas?.[area];
        assert.ok(reading, `${area}: Messung fehlt`);
        assert.deepEqual(reading.requestModels, [artifact.selectedRouterModel],
            `${area}: AI-Anfrage benutzt nicht exakt die aktive Modell-id`);
        assert.equal(reading.requestModelPresent, true,
            `${area}: model-Feld fehlt im Request`);
        assert.equal(reading.provenanceMatchesSelection, true,
            `${area}: sichtbare Herkunft nennt nicht die aktive Modellwahl`);
    }
});

test('AC3: every enabled W12 control has a measured mouse effect', () => {
    const controls = w12().extras.controls;
    const enabled = controls.filter((control) => !control.disabled && !control.ariaDisabled);
    assert.equal(enabled.length, 244, 'die bekannte Menge bedienbarer Controls hat sich geaendert');
    const ineffective = enabled.filter((control) => control.mouse?.changed !== true);
    assert.deepEqual(ineffective.map((control) => ({
        state: control.state,
        label: control.label,
        selector: control.selector,
        via: control.mouse?.via ?? '',
    })), [], 'bedienbare Controls ohne nachgewiesene Mauswirkung');
});

test('AC4: splitters, reader range and entry search use honest mouse routes', () => {
    const controls = w12().extras.controls;
    const splitters = controls.filter((control) => control.role === 'separator');
    assert.equal(splitters.length, 4, 'vier Splitter erwartet');
    for (const control of splitters) {
        assert.match(control.mouse?.via ?? '', /drag/i, `${control.label}: kein echter Maus-Drag`);
    }
    const range = controls.find((control) => control.selector.includes('atlas-twin-depth'));
    assert.ok(range, 'Reader-Range fehlt');
    assert.match(range.mouse?.via ?? '', /range|position/i, 'Reader-Range wurde nur angeklickt');
    const search = controls.find((control) => control.selector.includes('atlas-entry-input'));
    assert.ok(search, 'Entry-Suchfeld fehlt');
    assert.match(search.mouse?.via ?? '', /focus/i, 'Mausfokus des Entry-Suchfelds ist nicht belegt');
});

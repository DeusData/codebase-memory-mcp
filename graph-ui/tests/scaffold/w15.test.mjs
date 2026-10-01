// W15 acceptance tests: one AI button in each of the three areas.
//
// Der Nutzer hat am 30.08.2026 entschieden, woertlich: "ich will nicht, dass
// es automatisch umschaltet. mach einfach ai button in alle drei llm on und
// dann noch ein ai button zusaetzlich zum switchen."
//
// Gemessene Ausgangslage: der Twin hat das Muster schon, der Flow hat keinen
// Modellanschluss, und der Chat ist umgekehrt kaputt: ohne Modell antwortet er
// gar nicht, sondern schickt den Leser weg. Sechs Anfragen, null Karten.
//
// Run: node --test tests/scaffold/w15.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const art = () => JSON.parse(read('verification/w15/hybrid.json'));

const AREAS = ['twin', 'flow', 'chat'];

test('AC1: der Chat antwortet auch ohne Modell', () => {
    const a = art();
    assert.equal(a.chatAnswersWithoutModel, true,
        'ohne Modell schickt der Chat den Leser noch weg, statt zu antworten');
    assert.ok(a.cardsWithoutModel >= 1,
        `mindestens eine Karte erwartet, waren ${a.cardsWithoutModel}`);
    assert.ok(a.citationsWithoutModel >= 1,
        `ohne Beleg ist eine Antwort keine, waren ${a.citationsWithoutModel}`);
    assert.ok(typeof a.answerWithoutModel === 'string'
        && /\.ts:\d+/.test(a.answerWithoutModel),
    `die Antwort nennt weder Datei noch Zeile: ${a.answerWithoutModel}`);
    /* Der Hinweis auf das ausgeschaltete Modell darf danebenstehen, aber die
     * Antwort nicht mehr ersetzen. */
    assert.equal(a.offNoticeReplacesAnswer, false,
        'der Hinweis steht noch anstelle der Antwort');
});

test('AC2: mehr Nachbarschaft heisst mehr Karten', () => {
    const a = art();
    assert.equal(a.hopsAddCards, true, 'die Hop-Weite aendert nichts');
    const byHops = a.cardsByHops;
    assert.ok(byHops && typeof byHops === 'object', 'cardsByHops fehlt');
    assert.ok(byHops['0'] < byHops['1'],
        `1 hop muss mehr Karten bringen als 0 (${byHops['0']} vs ${byHops['1']})`);
    assert.ok(byHops['1'] <= byHops['2'],
        `2 hops duerfen nicht weniger sein als 1 (${byHops['1']} vs ${byHops['2']})`);
    assert.ok(Array.isArray(a.addedByHop) && a.addedByHop.length >= 2,
        'was jede Weite hinzunimmt, gehoert namentlich ins Artefakt');
});

test('AC3: ein AI-Knopf in jedem der drei Bereiche', () => {
    const a = art();
    assert.equal(a.aiButtons, 3,
        `bei eingeschaltetem Modell drei AI-Knoepfe erwartet, waren ${a.aiButtons}`);
    assert.equal(a.aiButtonsWhenOff, 0,
        'bei ausgeschaltetem Modell darf kein AI-Knopf dastehen');
    for (const area of AREAS) {
        const entry = a.areas?.[area];
        assert.ok(entry, `${area} fehlt in der Messung`);
        assert.equal(entry.hasAiButton, true, `${area}: kein AI-Knopf`);
        assert.equal(entry.aiButtonEnabled, true, `${area}: der AI-Knopf ist tot`);
    }
});

test('AC4: ein zweiter Knopf schaltet zurueck, ohne neu zu fragen', () => {
    const a = art();
    assert.equal(a.restoreButtons, 3,
        `drei Zurueck-Knoepfe erwartet, waren ${a.restoreButtons}`);
    assert.equal(a.roundTripIdentical, true,
        'nach hin und zurueck ist der gebaute Text nicht mehr derselbe');
    for (const area of AREAS) {
        const entry = a.areas[area];
        assert.equal(entry.restoredIdentical, true,
            `${area}: der gebaute Text kam nicht zeichengleich zurueck`);
        assert.equal(entry.restoreAsksModelAgain, false,
            `${area}: das Zurueckschalten fragt das Modell erneut`);
    }
});

test('AC5: was vom Modell kommt, ist gezeichnet', () => {
    const a = art();
    assert.equal(a.provenanceEverywhere, true, 'ein Text steht ohne Herkunft da');
    for (const area of AREAS) {
        const entry = a.areas[area];
        assert.ok(typeof entry.aiProvenance === 'string' && entry.aiProvenance.length > 10,
            `${area}: die Modellfassung traegt keine Herkunft`);
        assert.ok(entry.aiProvenance.includes(a.modelName),
            `${area}: der Modellname fehlt im Herkunftsvermerk`);
        assert.ok(typeof entry.builtProvenance === 'string' && entry.builtProvenance.length > 10,
            `${area}: die gebaute Fassung traegt keine Herkunft`);
        assert.notEqual(entry.aiProvenance, entry.builtProvenance,
            `${area}: beide Fassungen tragen denselben Vermerk`);
    }
});

test('AC6: der Waechter gilt in allen drei Bereichen', () => {
    const a = art();
    assert.ok(a.guardRefusals >= 15,
        `je Bereich mindestens fuenf verworfene Faelschungen erwartet, waren ${a.guardRefusals}`);
    for (const area of AREAS) {
        const refusals = a.areas[area].refusals;
        assert.ok(Array.isArray(refusals) && refusals.length >= 5,
            `${area}: zu wenige gepruefte Faelschungen`);
        for (const r of refusals) {
            assert.equal(r.applied, false,
                `${area}: eine Faelschung ging durch: ${r.answer?.slice(0, 90)}`);
            assert.ok(typeof r.reason === 'string' && r.reason.length > 10,
                `${area}: eine Ablehnung ohne Grund ist fuer den Leser keine`);
        }
        assert.equal(a.areas[area].guardIsTheRealOne, true,
            `${area}: geprueft wurde ein Nachbau, nicht die echte Pruefung`);
    }
});

test('AC7: nichts schaltet von selbst', () => {
    const a = art();
    assert.equal(a.autoRequests, 0,
        `ohne Klick darf keine Anfrage ans Modell gehen, waren ${a.autoRequests}`);
    assert.ok(a.requestsAfterClick >= 1,
        'nach dem Klick muss eine Anfrage nachweisbar sein, sonst misst der Lauf nichts');
});

test('AC8: der Beweislauf und seine Verdrahtung', () => {
    const a = art();
    assert.ok(a.port >= 4680, `Port >= 4680 erwartet, war ${a.port}`);
    assert.equal(a.leftoverProcesses, 0);
    assert.equal(a.overlapViolations, 0);
    assert.equal(a.clippingViolations, 0);
    assert.equal(a.cutWithoutHint, 0);
    assert.equal(a.touchedUserSidecar, false,
        'der Modellport des Nutzers wurde angefasst');
    for (const shot of ['chat-off.png', 'chat-ai.png', 'flow-ai.png', 'twin-ai.png']) {
        const p = join(ROOT, 'verification', 'w15', shot);
        assert.ok(existsSync(p), `${shot} fehlt`);
        assert.ok(statSync(p).size > 20 * 1024, `${shot} verdaechtig klein`);
    }
    const nd = JSON.parse(read('verification/w15/netdeny.json'));
    assert.equal(nd.outboundViolations, 0);
    assert.ok(/smoke-w15/.test(nd.command));
    const pkg = JSON.parse(read('package.json'));
    assert.ok(pkg.scripts?.['smoke:w15'], 'Script smoke:w15 fehlt');
});

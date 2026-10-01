// W14 correction acceptance tests: every symbol gets its own honest sentence.
//
// Der Anlass ist gemessen, nicht vermutet. Eine Probe ueber acht Symbole und
// fuenf Leserstufen (30.08.2026) hat gezeigt: `query`, `toUser` und `insert`
// liefern auf den beiden unteren Stufen einen ZEICHENGLEICHEN Koerper, nach
// Namensmaskierung sind 36 von 40 Texten verschieden, und der Name der Funktion
// kommt auf vier von fuenf Stufen bei keinem einzigen Symbol vor. Zwei
// Facetten-Knoepfe blenden nichts aus. Bei drei Symbolen wird ein leeres
// Flussdiagramm gezeichnet, bei einem ging die Flaeche voellig leer auf.
//
// Diese Datei prueft das Ergebnis des Beweislaufs, nicht seine Behauptung:
// jede Zahl unten kommt aus einer Messung am gerenderten DOM.
//
// Run: node --test tests/scaffold/w14.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const art = () => JSON.parse(read('verification/w14/symbols.json'));

/* Die acht Symbole der Ausgangsmessung. Der Lauf misst dieselben, sonst
 * vergliche der Nachher-Wert eine andere Sache als der Vorher-Wert. */
const EIGHT = ['createUser', 'getOrder', 'listUsers', 'query',
    'toUser', 'insert', 'hotspotScan', 'validateUser'];
const LEVELS = ['vibe coder', 'junior', 'medior', 'senior', 'architect'];

test('AC0: der Lauf hat wirklich acht Symbole auf fuenf Stufen gemessen', () => {
    const a = art();
    assert.ok(a.symbols && typeof a.symbols === 'object', 'symbols fehlt');
    for (const name of EIGHT) {
        const entry = a.symbols[name];
        assert.ok(entry, `${name} fehlt in der Messung`);
        assert.ok(entry.picked === true, `${name} wurde nicht geoeffnet`);
        for (const level of LEVELS) {
            const body = entry.levels?.[level];
            assert.ok(body, `${name}/${level} fehlt`);
            assert.ok(Number.isFinite(body.chars) && body.chars > 200,
                `${name}/${level}: verdaechtig kurzer Koerper (${body.chars})`);
        }
    }
});

test('AC1: kein Symbol teilt seinen Text mit einem anderen', () => {
    const a = art();
    /*
     * Verglichen wird NACH Ersetzung des Symbolnamens durch eine Marke.
     * Ohne diese Ersetzung wuerde AC2 (der Name steht im Text) dieses
     * Kriterium von selbst erfuellen, und AC1 haette aufgehoert, etwas zu
     * messen: zwei wortgleiche Absaetze mit ausgetauschtem Namen sind genau
     * der Befund, um den es hier geht. Der Lauf muss also bestaetigen, dass
     * er so vergleicht.
     */
    assert.equal(a.comparedWithNameMasked, true,
        'der Vergleich muss den Symbolnamen maskieren, sonst misst AC1 nichts');
    assert.deepEqual(a.identicalGroups, [],
        'zwei Symbole liefern denselben Text: '
        + JSON.stringify(a.identicalGroups));
    assert.equal(a.distinctTexts, 40,
        `40 verschiedene Texte erwartet (8 Symbole mal 5 Stufen), waren ${a.distinctTexts}`);
    /* Der Ausgangswert ist aus den 40 gespeicherten Rohtexten nach derselben
     * Namensmaskierung erneut berechnet: 36, nicht die vorher frei eingetragene
     * Zahl 30. Beide Zahlen binden den Nenner und den Messwert. */
    assert.equal(a.textsBeforeMeasured, 40,
        'die Ausgangsmessung muss alle acht Symbole auf allen fuenf Stufen enthalten');
    assert.equal(a.distinctTextsBefore, 36,
        `36 verschiedene Ausgangstexte erwartet, behauptet wurden ${a.distinctTextsBefore}`);
});

test('AC2: die Funktion wird auf jeder Stufe beim Namen genannt', () => {
    const a = art();
    assert.equal(a.nameInBody, 40,
        `der Symbolname muss in allen 40 Koerpern stehen, war in ${a.nameInBody}`);
    for (const name of EIGHT) {
        for (const level of LEVELS) {
            assert.equal(art().symbols[name].levels[level].namesSymbol, true,
                `${name}/${level} nennt das Symbol nicht`);
        }
    }
});

test('AC3: ein Blatt sagt zuerst, was es ist', () => {
    const a = art();
    assert.equal(a.leafLeadsPositive, true,
        'die Auskunft an einem Blatt fuehrt weiter mit einer Verneinung');
    assert.ok(Array.isArray(a.leafLeads) && a.leafLeads.length >= 3,
        'die gemessenen Anfangssaetze der Blaetter gehoeren ins Artefakt');
    for (const lead of a.leafLeads) {
        assert.ok(/\b\d+\s*(lines|Zeilen)\b/.test(lead.text),
            `"${lead.symbol}": der Anfangssatz nennt die Zeilenzahl nicht: ${lead.text}`);
        assert.ok(!/^(It does not|It never|Nothing|There is no)/i.test(lead.text.trim()),
            `"${lead.symbol}": faengt weiter mit einer Verneinung an: ${lead.text}`);
    }
    assert.ok(Number.isFinite(a.negationsPerLeaf) && a.negationsPerLeaf <= 1,
        `hoechstens eine Verneinung je Blatt erwartet, waren ${a.negationsPerLeaf}`);
});

test('AC4: der geteilte Zustand steht auf der untersten Stufe', () => {
    const a = art();
    assert.equal(a.sharedStateNamed, true, 'der geteilte Zustand fehlt');
    const vibe = a.symbols.query.levels['vibe coder'];
    assert.ok(/\brows\b/.test(vibe.text),
        'query haengt laut Index per USAGE an `rows`; das Wort fehlt auf der vibe-coder-Stufe');
});

test('AC5: die Aufrufer stehen auf jeder Stufe', () => {
    const a = art();
    assert.equal(a.callersNamed, 3,
        `query hat laut Index drei Aufrufer, genannt wurden ${a.callersNamed}`);
    for (const level of LEVELS) {
        const text = art().symbols.query.levels[level].text;
        for (const caller of ['getOrder', 'listUsers', 'hotspotScan']) {
            assert.ok(text.includes(caller),
                `query/${level}: der Aufrufer ${caller} fehlt`);
        }
    }
});

test('AC6: ein Knopf, der nichts ausblendet, ist keiner', () => {
    const a = art();
    assert.equal(a.facetsAllRemove, true,
        'mindestens ein Facetten-Knopf entfernt nichts');
    assert.ok(Array.isArray(a.facets) && a.facets.length >= 5,
        'die Messung je Facette gehoert ins Artefakt');
    for (const facet of a.facets) {
        assert.ok(facet.shown === false || facet.removesSomething === true,
            `${facet.label}: steht da, entfernt aber nichts`);
        if (facet.shown) {
            assert.ok(facet.restores === true,
                `${facet.label}: bringt das Weggenommene nicht zurueck`);
        }
    }
});

test('AC7: kein leeres Diagramm, und nie eine leere Flaeche', () => {
    const a = art();
    assert.equal(a.noEmptyDiagram, true, 'bei null Schritten wird noch gezeichnet');
    assert.equal(a.neverBlankOverlay, true,
        'die Flussflaeche ging leer auf, ohne Kaesten und ohne Grund');
    for (const name of ['query', 'toUser', 'insert']) {
        const flow = a.flows?.[name];
        assert.ok(flow, `${name}: die Flussmessung fehlt`);
        assert.equal(flow.steps, 0, `${name} sollte null Schritte haben`);
        assert.equal(flow.svgBoxes, 0, `${name}: es wird noch ein Kasten gezeichnet`);
        assert.equal(flow.pagerButtons, 0, `${name}: die Blaetterleiste steht noch da`);
        assert.ok(typeof flow.reason === 'string' && flow.reason.length > 20,
            `${name}: der Grund fehlt, warum es nichts zu gehen gibt`);
    }
    const rich = a.flows?.createUser;
    assert.ok(rich, 'createUser: die Flussmessung fehlt');
    assert.ok(rich.svgBoxes > 0 || (rich.reason && rich.reason.length > 20),
        'createUser: die Flaeche ging leer auf, ohne Kaesten und ohne Grund');
});

test('AC8: der Import-Fund misst die Datei, nicht das Symbol', () => {
    const a = art();
    assert.equal(a.importFindingIsFileLevel, true,
        'der Fund wird noch je Symbol gerechnet');
    const readings = a.importReadings;
    assert.ok(readings && typeof readings === 'object',
        'die direkten Importmessungen fehlen');
    assert.ok(readings.getOrder?.entries > 0, 'getOrder wurde nicht gemessen');
    assert.equal(readings.getOrder?.findings, 0,
        'insert wird in orderService.ts von create benutzt und ist kein Fund');
    assert.ok(readings.toUser?.entries > 0, 'toUser wurde nicht gemessen');
    assert.equal(readings.toUser?.findings, 0,
        'die Importe von userService.ts werden dateiweit benutzt');
    const probe = readings.unusedProbe;
    assert.equal(probe?.symbol, 'registerOrderRoutes',
        'der echte Unused-Positivfall muss von den beiden Negativfaellen getrennt sein');
    assert.equal(probe?.importName, 'insert');
    assert.equal(probe?.sourceImportCount, 1,
        'der Positivfall muss insert genau einmal importieren');
    assert.equal(probe?.sourceUseCount, 0,
        'insert darf im Positivfall ausserhalb der Importdeklaration nicht benutzt werden');
    assert.ok(probe?.entries > 0, 'der echte Positivfall wurde nicht gerendert');
    assert.equal(probe?.findings, 1,
        'der echte dateiweit ungenutzte Import muss genau ein Finding erzeugen');
    assert.match(probe?.text ?? '', /insert/i,
        'das sichtbare Finding nennt den tatsaechlich ungenutzten Import nicht');
    assert.equal(a.selfRelativisingNote, false,
        'der Nebensatz, der den Fund zuruecknimmt, ist ueberfluessig geworden');
});

test('AC9: oben steht nur, was das Fenster betrifft', () => {
    const a = art();
    assert.equal(a.menusDoNotOverlap, true,
        `ein Name steht in beiden Leisten: ${JSON.stringify(a.menuOverlap)}`);
    assert.equal(a.everyTargetStillReachable, true,
        `ein frueher erreichbares Ziel ist verschwunden: ${JSON.stringify(a.unreachable)}`);
    assert.ok(Array.isArray(a.topBar) && Array.isArray(a.tabBar),
        'beide Leisten gehoeren ins Artefakt');
    assert.ok(a.shortcutsStillWork === true, 'die Tastenkuerzel gelten nicht mehr');
});

test('AC10: keine Zahl ohne Sache', () => {
    const a = art();
    assert.equal(a.noTokenLineWithoutCards, true,
        'die Token-Rechnung steht noch da, wo keine Karten gebaut wurden');
});

test('AC11: der Beweislauf und seine Verdrahtung', () => {
    const a = art();
    assert.ok(a.port >= 4660, `Port >= 4660 erwartet, war ${a.port}`);
    assert.equal(a.leftoverProcesses, 0, 'der Lauf hat Prozesse liegen lassen');
    assert.equal(a.overlapViolations, 0);
    assert.equal(a.clippingViolations, 0);
    assert.equal(a.cutWithoutHint, 0);
    const shots = a.screenshotReadings;
    assert.ok(shots && typeof shots === 'object',
        'die Zustandsmessung unmittelbar vor den Screenshots fehlt');
    for (const [name, level] of [['leaf-vibe.png', 'vibe coder'], ['leaf-junior.png', 'junior']]) {
        const reading = shots[name];
        assert.equal(reading?.symbol, 'query', `${name} zeigt nicht das gemessene Blatt query`);
        assert.equal(reading?.level, level, `${name} zeigt die falsche Leserstufe`);
        assert.equal(reading?.steps, 0, `${name} zeigt kein Blatt ohne ausgehende Schritte`);
        assert.equal(reading?.visibleTooltips, 0, `${name} wird von einem Tooltip verdeckt`);
    }
    for (const shot of ['leaf-vibe.png', 'leaf-junior.png', 'flow-empty.png', 'menus.png']) {
        const p = join(ROOT, 'verification', 'w14', shot);
        assert.ok(existsSync(p), `${shot} fehlt`);
        assert.ok(statSync(p).size > 20 * 1024, `${shot} verdaechtig klein`);
    }
    const nd = JSON.parse(read('verification/w14/netdeny.json'));
    assert.equal(nd.outboundViolations, 0);
    assert.ok(/smoke-w14/.test(nd.command));
    const pkg = JSON.parse(read('package.json'));
    assert.ok(pkg.scripts?.['smoke:w14'], 'Script smoke:w14 fehlt');
});

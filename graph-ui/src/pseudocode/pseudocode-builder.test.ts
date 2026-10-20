/*
 * Portiert am 2026-08-29 aus CodeAtlasIDE,
 * theia-extensions/codeatlas-views/test/pseudocode-builder.test.ts (345 Zeilen)
 * samt __snapshots__/pseudocode-builder.test.ts.snap (4 Snapshots).
 *
 * Ein Unterschied, und er ist derselbe wie bei render-model.test.ts dieses
 * Projekts: die vier Snapshots kommen NICHT als Snapshot-Datei mit, sondern als
 * direkte Zusicherungen ueber genau denselben Text. Ein Snapshot ist so viel
 * wert wie der Reviewer, der seinen Diff annimmt; hier steht stattdessen jede
 * Zeile, die das Produkt sagt, als Erwartung im Test, so dass beim Brechen
 * dasteht, WELCHER Satz sich geaendert hat.
 */

/**
 * What the pseudocode builder is allowed to say.
 *
 * Three kinds of assertion, and the first two are the reason the suite exists.
 *
 * The first kind is about invention. A block of numbered steps is the most
 * believable thing this product draws, so every line has to be traceable to a
 * fact in the IR it was built from: as many steps as there are recorded call
 * sites and no more, the verb the index recorded and not a nicer one, and a
 * raise line only where the index reported a raised type. The cheapest way to
 * break this feature is to add a plausible line, and the cheapest way to notice
 * is to count.
 *
 * The second kind is about the honest footer. A block assembled from eight
 * symbols of which three had facts is not a description of eight symbols, and
 * nothing else on screen can tell a reader that. So the coverage figures, the
 * named absences and the capped note are asserted directly rather than through a
 * snapshot, where a wrong number would still look plausible.
 *
 * The third kind pins the wording of a whole block. It is what catches a change
 * nobody meant to make to what the product says.
 *
 * The IRs are the committed recordings of what the 0.9.0 provider actually
 * returns for the frozen sample workspace. The closure is handcrafted from the
 * same recordings, because a walk is a backend answer and there is no recording
 * of one; every symbol, edge and line number in it is taken from the IRs beside
 * it, and its bounds are set so that the capped case is exercised.
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import type { SemanticIR } from '../core/semantic-ir';
import type { ClosureDocument, PseudocodeDocument } from './pseudocode-builder';
import {
    applyRefinedPseudocode,
    buildPseudocode,
    closureDocumentOf,
    leadingNumberOf,
    pseudocodeText,
} from './pseudocode-builder';

const HERE = dirname(fileURLToPath(import.meta.url));
const IR_FIXTURES = join(HERE, '..', 'twin', '__fixtures__');
const FIXTURES = join(HERE, '__fixtures__');

function ir(name: string): SemanticIR {
    return JSON.parse(readFileSync(join(IR_FIXTURES, `ir-${name}.json`), 'utf8')) as SemanticIR;
}

const CREATE_USER = ir('createUser');
const LIST_USERS = ir('listUsers');
const VALIDATE_USER = ir('validateUser');
const INSERT = ir('insert');
const QUERY = ir('query');
const USER_CREATE = ir('userService-create');

const CLOSURE: ClosureDocument = JSON.parse(
    readFileSync(join(FIXTURES, 'closure-userService-create.json'), 'utf8'),
) as ClosureDocument;

/**
 * Das geladene Layout, so weit die Zusicherungen zu W8c es brauchen.
 *
 * Die Form ist die von `/api/layout` (src/galaxy/types.ts), die qualifizierten
 * Namen sind die der aufgezeichneten IRs daneben. `toUser` fehlt absichtlich:
 * ein Ziel, das der Graph nicht kennt, muss ohne Notiz dastehen und nicht mit
 * einer geratenen.
 */
const GRAPH = {
    nodes: [
        {
            id: 1,
            name: 'validateUser',
            qualified_name: 'codeatlas-atlas-sample.src.util.validate.validateUser',
            label: 'Function',
        },
        {
            id: 2,
            name: 'ValidationError',
            qualified_name: 'codeatlas-atlas-sample.src.util.validate.ValidationError',
            label: 'Class',
        },
    ],
    edges: [{ source: 1, target: 2, type: 'RAISES' }],
};

const kinds = (document: PseudocodeDocument, kind: string): number =>
    document.lines.filter((line) => line.kind === kind).length;

const said = (document: PseudocodeDocument): string => `${document.title}\n${pseudocodeText(document)}`;

describe('the symbol scope', () => {
    const document = buildPseudocode({ kind: 'symbol', label: 'createUser' }, { irs: [CREATE_USER] });

    it('draws one line per recorded fact and not one more', () => {
        expect(kinds(document, 'step')).toBe(CREATE_USER.steps.value.length);
        expect(kinds(document, 'raise')).toBe(CREATE_USER.throws.value.length);
        expect(kinds(document, 'env')).toBe(1);
        // No heading and no note: one symbol needs neither.
        expect(kinds(document, 'group')).toBe(0);
        expect(kinds(document, 'note')).toBe(0);
    });

    it('gives the acceptance run more than the four lines it asks for', () => {
        expect(document.lines.length).toBeGreaterThanOrEqual(4);
    });

    it('numbers every line once, continuously', () => {
        expect(document.lines.map((line) => line.order)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    });

    it('points every line at somewhere a reader can open', () => {
        for (const line of document.lines) {
            expect(line.sourceRef?.uri.length).toBeGreaterThan(0);
            expect(line.sourceRef?.line).toBeGreaterThan(0);
        }
    });

    it('points a step at its call site, in the file that makes the call', () => {
        const step = document.lines[0];
        expect(step.sourceRef).toEqual({ uri: CREATE_USER.symbol.uri, line: 24 });
        // Never the callee's declaration, which is line 19 of another file.
        expect(step.sourceRef?.uri).not.toContain('validate.ts');
    });

    it('points a raise at the site the index recorded, wherever that is', () => {
        const raise = document.lines.find((line) => line.kind === 'raise');
        expect(raise?.sourceRef).toEqual({
            uri: 'file:///workspace/atlas-sample/src/util/validate.ts',
            line: 23,
        });
    });

    it('falls back to the declaration for an environment read, which records no line', () => {
        const env = document.lines.find((line) => line.kind === 'env');
        expect(env?.sourceRef).toEqual({
            uri: CREATE_USER.symbol.uri,
            line: CREATE_USER.symbol.range.start.line + 1,
        });
    });

    it('uses the verb the index recorded and never a nicer one', () => {
        const constructions = CREATE_USER.steps.value.filter((call) => call.strategy === 'construction');
        expect(constructions.length).toBeGreaterThan(0);
        for (const call of constructions) {
            const line = document.lines.find((entry) => entry.targetName === call.targetName);
            expect(line?.text).toContain('construct ');
        }
        const plain = document.lines.find((entry) => entry.targetName === 'validateUser');
        expect(plain?.text).toContain('call ');
    });

    it('counts one symbol, all of it covered', () => {
        expect(document.honest).toEqual({ coveredSymbols: 1, uncovered: [], capped: false });
        expect(document.scopeSymbols).toBe(1);
    });

    it('reads the same way twice', () => {
        expect(pseudocodeText(buildPseudocode({ kind: 'symbol', label: 'createUser' }, { irs: [CREATE_USER] })))
            .toBe(pseudocodeText(document));
    });

    it('says what it says', () => {
        expect(said(document)).toBe([
            'Steps in createUser',
            '1. call validateUser',
            '2. construct ValidationError',
            '3. construct UserEntity',
            '4. call listUsers',
            '5. call insert',
            '6. call toUser',
            '7. may raise ValidationError',
            '8. read DB_URL from the environment',
        ].join('\n'));
    });
});

/*
 * W8c: die Zeile sagt jetzt auch, WOHIN sie fuehrt, und was der Index ueber das
 * Ziel schon hergibt. Beides steht neben dem Text und nie darin, weil die
 * Pruefung einer Umformulierung genau den Text vergleicht, der verschickt
 * wurde.
 */
describe('where a line leads', () => {
    const document = buildPseudocode({ kind: 'symbol', label: 'createUser' }, { irs: [CREATE_USER] });

    it('carries the callee\'s declaration beside the call site, not instead of it', () => {
        const step = document.lines[0];
        expect(step.sourceRef).toEqual({ uri: CREATE_USER.symbol.uri, line: 24 });
        expect(step.targetRef).toEqual({
            uri: 'file:///workspace/atlas-sample/src/util/validate.ts',
            line: 19,
        });
        expect(step.targetLine).toBe(19);
    });

    it('leaves the target absent where the index recorded no file for it', () => {
        const blind = buildPseudocode(
            { kind: 'symbol', label: 'blind' },
            {
                irs: [{
                    ...CREATE_USER,
                    steps: { ...CREATE_USER.steps, value: [{ targetName: 'mystery', line: 24 }], evidence: [] },
                    throws: { ...CREATE_USER.throws, value: [], evidence: [] },
                    reads: { ...CREATE_USER.reads, value: [], evidence: [] },
                }],
            },
        );
        expect(blind.lines[0].targetRef).toBeUndefined();
        expect(blind.lines[0].targetFile).toBeUndefined();
        // Und die Zeile ist trotzdem da: es gibt keine Laenge, ab der ein
        // Schritt verschwindet.
        expect(blind.lines[0].text).toBe('1. call mystery');
    });

    it('never puts the target or the note into the text a rewrite is checked against', () => {
        const enriched = buildPseudocode(
            { kind: 'symbol', label: 'createUser' },
            { irs: [CREATE_USER], graph: GRAPH },
        );
        expect(pseudocodeText(enriched)).toBe(pseudocodeText(document));
        expect(applyRefinedPseudocode(enriched, pseudocodeText(enriched))).toBeDefined();
    });
});

describe('what lies behind a call', () => {
    const enriched = buildPseudocode(
        { kind: 'symbol', label: 'createUser' },
        { irs: [CREATE_USER], graph: GRAPH },
    );

    it('says what the called symbol can raise, from the loaded graph alone', () => {
        const step = enriched.lines.find((line) => line.targetName === 'validateUser');
        expect(step?.behind?.map((note) => note.text)).toEqual(['may raise ValidationError']);
    });

    it('says nothing about a callee the graph does not know', () => {
        const step = enriched.lines.find((line) => line.targetName === 'toUser');
        expect(step?.behind).toBeUndefined();
    });

    it('measures what was in reach and what was not, instead of promising it', () => {
        expect(enriched.enrichment.usable.map((entry) => entry.kind)).toEqual(['raises']);
        expect(enriched.enrichment.usable[0].symbols).toBe(1);
        expect(enriched.enrichment.missing.map((entry) => entry.kind))
            .toContain('what the callee reads or writes');
    });

    it('says so when this window holds no graph at all', () => {
        const bare = buildPseudocode({ kind: 'symbol', label: 'createUser' }, { irs: [CREATE_USER] });
        expect(bare.enrichment.usable).toEqual([]);
        expect(bare.enrichment.missing[0].kind).toBe('every kind below');
        expect(bare.lines.every((line) => line.behind === undefined)).toBe(true);
    });
});

describe('a symbol the index recorded nothing for', () => {
    const document = buildPseudocode({ kind: 'symbol', label: 'query' }, { irs: [QUERY] });

    it('says so rather than drawing an empty block', () => {
        expect(kinds(document, 'note')).toBe(1);
        expect(document.lines[0].text).toContain('query');
        expect(document.honest.coveredSymbols).toBe(0);
        expect(document.honest.uncovered).toEqual(['query']);
    });
});

describe('the class scope', () => {
    // Two members standing in for a class's methods: the shape the surface hands
    // over is a list of IRs, and the builder's job is the grouping.
    const document = buildPseudocode(
        { kind: 'class', label: 'UserService' },
        { irs: [LIST_USERS, INSERT] },
    );

    it('puts a heading over each member', () => {
        expect(document.lines.filter((line) => line.kind === 'group').map((line) => line.text))
            .toEqual(['method listUsers:', 'method insert:']);
    });

    it('keeps one numbering across the whole block, so a heading never restarts it', () => {
        const numbered = document.lines.filter((line) => line.order !== undefined).map((line) => line.order);
        expect(numbered).toEqual([1]);
    });

    it('names the member that contributed nothing', () => {
        expect(document.honest.coveredSymbols).toBe(1);
        expect(document.honest.uncovered).toEqual(['insert']);
        expect(document.scopeSymbols).toBe(2);
    });

    it('says what it says', () => {
        expect(said(document)).toBe([
            'Steps in class UserService',
            'method listUsers:',
            '1. call query',
            'method insert:',
            'the index recorded no calls, raised errors or environment reads for insert',
        ].join('\n'));
    });
});

describe('a scope the surface could not resolve in full', () => {
    it('says the range was sampled, and counts that as a bound it hit', () => {
        const document = buildPseudocode(
            { kind: 'class', label: 'UserService', partial: true },
            { irs: [LIST_USERS] },
        );
        const note = document.lines[document.lines.length - 1];
        expect(note.kind).toBe('note');
        expect(note.text).toContain('sampled this range');
        expect(document.honest.capped).toBe(true);
    });

    it('says nothing of the sort when every line was read', () => {
        const document = buildPseudocode({ kind: 'class', label: 'UserService' }, { irs: [LIST_USERS] });
        expect(document.lines.some((line) => line.text.includes('sampled this range'))).toBe(false);
        expect(document.honest.capped).toBe(false);
    });
});

describe('the selection scope', () => {
    const document = buildPseudocode(
        { kind: 'selection', label: 'createUser' },
        { irs: [CREATE_USER, USER_CREATE] },
    );

    it('groups by symbol, in the order the surface resolved them', () => {
        expect(document.lines.filter((line) => line.kind === 'group').map((line) => line.text))
            .toEqual(['createUser:', 'create:']);
    });

    it('counts the symbols the selection touched', () => {
        expect(document.scopeSymbols).toBe(2);
        expect(document.honest.coveredSymbols).toBe(2);
        expect(document.title).toContain('2 selected symbols');
    });

    it('says what it says', () => {
        expect(said(document)).toBe([
            'Steps in 2 selected symbols',
            'createUser:',
            '1. call validateUser',
            '2. construct ValidationError',
            '3. construct UserEntity',
            '4. call listUsers',
            '5. call insert',
            '6. call toUser',
            '7. may raise ValidationError',
            '8. read DB_URL from the environment',
            'create:',
            '9. call createUser',
        ].join('\n'));
    });
});

describe('the closure scope', () => {
    const irs = [CREATE_USER, LIST_USERS, VALIDATE_USER, INSERT, USER_CREATE];
    const document = buildPseudocode({ kind: 'closure', label: 'create' }, { irs, closure: CLOSURE });

    it('follows the walk order, not the order the IRs arrived in', () => {
        expect(document.lines.filter((line) => line.kind === 'group').map((line) => line.text)).toEqual([
            'create:',
            'createUser:',
            'insert:',
            'listUsers:',
            'toUser:',
            'UserEntity:',
            'ValidationError:',
            'validateUser:',
        ]);
    });

    it('gives the acceptance run more than the three symbols it asks for', () => {
        expect(CLOSURE.symbols.length).toBeGreaterThanOrEqual(3);
    });

    it('groups a symbol the walk found but no IR arrived for, and says nothing was listed', () => {
        const index = document.lines.findIndex((line) => line.text === 'toUser:');
        expect(document.lines[index + 1].kind).toBe('note');
        expect(document.honest.uncovered).toContain('toUser');
    });

    it('says out loud that the walk stopped at its bound', () => {
        expect(document.honest.capped).toBe(true);
        const note = document.lines[document.lines.length - 1];
        expect(note.kind).toBe('note');
        expect(note.text).toContain('not expanded');
        expect(note.text).toContain('1 more symbol');
    });

    it('never claims a bound that was not reached', () => {
        const complete = buildPseudocode(
            { kind: 'closure', label: 'create' },
            { irs, closure: { ...CLOSURE, truncated: false, visited: CLOSURE.symbols.length } },
        );
        expect(complete.honest.capped).toBe(false);
        expect(complete.lines.some((line) => line.text.includes('not expanded'))).toBe(false);
    });

    it('counts what contributed and what did not', () => {
        expect(document.scopeSymbols).toBe(8);
        expect(document.honest.coveredSymbols).toBe(4);
        // `insert` is here for a different reason from the other three: an IR
        // for it did arrive, and the index recorded nothing in it. Both are
        // "nothing was listed" to a reader, which is what the footer says.
        expect(document.honest.uncovered).toEqual(['insert', 'toUser', 'UserEntity', 'ValidationError']);
    });

    it('says what it says', () => {
        expect(said(document)).toBe([
            'Steps in create and the code it reaches',
            'create:',
            '1. call createUser',
            'createUser:',
            '2. call validateUser',
            '3. construct ValidationError',
            '4. construct UserEntity',
            '5. call listUsers',
            '6. call insert',
            '7. call toUser',
            '8. may raise ValidationError',
            '9. read DB_URL from the environment',
            'insert:',
            'the index recorded no calls, raised errors or environment reads for insert',
            'listUsers:',
            '10. call query',
            'toUser:',
            'the index recorded no calls, raised errors or environment reads for toUser',
            'UserEntity:',
            'the index recorded no calls, raised errors or environment reads for UserEntity',
            'ValidationError:',
            'the index recorded no calls, raised errors or environment reads for ValidationError',
            'validateUser:',
            '11. construct ValidationError',
            '12. may raise ValidationError',
            'and 1 more symbol not expanded: the walk stopped at its bound',
        ].join('\n'));
    });
});

describe('the walk of this project, folded into the shape the builder reads', () => {
    it('carries the root, the symbols in walk order and both bounds across', () => {
        const document = closureDocumentOf({
            root: CREATE_USER.symbol,
            nodes: [
                { symbol: CREATE_USER.symbol, hop: 0 },
                { symbol: VALIDATE_USER.symbol, hop: 1, via: CREATE_USER.symbol.qualifiedName },
            ],
            edges: [{ from: 'a', to: 'b', line: 24 }],
            truncated: true,
            visited: 9,
            depth: 3,
            cap: 8,
        });
        expect(document.symbols.map((symbol) => symbol.name)).toEqual(['createUser', 'validateUser']);
        expect(document.root).toBe(CREATE_USER.symbol);
        expect(document.edges).toEqual([{ from: 'a', to: 'b', line: 24 }]);
        expect(document.truncated).toBe(true);
        expect(document.visited).toBe(9);
    });
});

describe('putting a rewrite back onto the block', () => {
    const document = buildPseudocode({ kind: 'symbol', label: 'createUser' }, { irs: [CREATE_USER] });
    const restated = (): string[] =>
        document.lines.map((line) => `${line.order}. said differently`);

    it('takes the wording and keeps every source reference', () => {
        const rewritten = applyRefinedPseudocode(document, restated().join('\n'));
        expect(rewritten).toBeDefined();
        expect(rewritten!.lines.map((line) => line.sourceRef))
            .toEqual(document.lines.map((line) => line.sourceRef));
        expect(rewritten!.lines[0].text).toBe('1. said differently');
        expect(rewritten!.honest).toEqual(document.honest);
    });

    it('refuses a rewrite that dropped a step', () => {
        expect(applyRefinedPseudocode(document, restated().slice(0, -1).join('\n'))).toBeUndefined();
    });

    it('refuses a rewrite that added one', () => {
        expect(applyRefinedPseudocode(document, [...restated(), '9. an invented step'].join('\n'))).toBeUndefined();
    });

    it('refuses a rewrite that reordered them', () => {
        const swapped = restated();
        [swapped[0], swapped[1]] = [swapped[1], swapped[0]];
        expect(applyRefinedPseudocode(document, swapped.join('\n'))).toBeUndefined();
    });

    it('refuses a rewrite that dropped the numbering', () => {
        expect(applyRefinedPseudocode(document, document.lines.map(() => 'said differently').join('\n')))
            .toBeUndefined();
    });

    it('tolerates the blank lines a model puts between paragraphs', () => {
        expect(applyRefinedPseudocode(document, `${restated().join('\n\n')}\n`)).toBeDefined();
    });

    it('keeps an unnumbered heading unnumbered', () => {
        const grouped = buildPseudocode({ kind: 'class', label: 'UserService' }, { irs: [LIST_USERS, INSERT] });
        const good = grouped.lines.map((line) => line.order === undefined ? 'a heading' : `${line.order}. said differently`);
        expect(applyRefinedPseudocode(grouped, good.join('\n'))).toBeDefined();
        const bad = grouped.lines.map((_unused, index) => `${index + 1}. said differently`);
        expect(applyRefinedPseudocode(grouped, bad.join('\n'))).toBeUndefined();
    });
});

describe('reading a leading number', () => {
    it('finds one where there is one', () => {
        expect(leadingNumberOf('12. call insert')).toBe(12);
        expect(leadingNumberOf('3) call insert')).toBe(3);
    });

    it('finds none where there is none', () => {
        expect(leadingNumberOf('method label:')).toBeUndefined();
        expect(leadingNumberOf('2024 was a year')).toBeUndefined();
    });
});

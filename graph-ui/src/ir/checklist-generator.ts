/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/ir/checklist-generator.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen,
 * Ids eingeschlossen: ein Haken, den jemand in der IDE gesetzt hat, muss hier
 * derselbe Haken sein, und das entscheidet sich an der Id-Ableitung.
 * Aenderungen gegenueber dem Original: die Importpfade zeigen auf die
 * portierten Dateien, und sha1 kommt aus core/hash statt aus node:crypto, weil
 * dieser Code im Browser laeuft. Die Ableitung selbst ist dieselbe.
 */
/**
 * The review checklist: what a reader still owes before claiming they
 * understand a symbol.
 *
 * Three properties are load-bearing.
 *
 * Identity is stable across rebuilds. An item's id is a hash of its category
 * and of what it points at, never of its position in the list or of the index
 * generation. That is what lets a reader tick "understand the call to
 * validateUser", reindex the workspace, and still find the item ticked. Hashing
 * the label instead would break the moment the wording is improved, so the
 * qualified name of the target wins whenever there is one.
 *
 * Identity is also stable across a move. Every key below names something about
 * the repository and nothing about the machine holding it: a qualified name
 * where the graph resolved one, and a workspace-relative path where it did not.
 * The tests category is the one that has to fall back to a path, because the
 * 0.9.0 engine records a test caller as a file and a name rather than as a
 * resolved symbol, and that path is made relative here. Keying it on the
 * absolute URI, as this generator did until C19, meant that cloning a
 * repository into another directory silently dropped exactly one category of
 * the reader's record while keeping the rest. The direction was safe, since a
 * lost mark understates what somebody has read, but it was arbitrary, and a
 * product whose subject is honesty about what is known cannot have one category
 * quietly forgetting itself.
 *
 * Marks recorded under the old absolute keys are not migrated. They stay in the
 * workspace's `understanding.json` as entries nothing asks for any more, so
 * nothing is destroyed; the tests rows come back unticked once, and from then
 * on they survive a move like every other row.
 *
 * An empty category is omitted, never rendered empty. A checklist that shows
 * "Error handling" with nothing under it reads as "this symbol raises nothing",
 * which is a claim this generator is in no position to make: it sees the facts
 * it was given and not the reason they are empty. The knowledge state that
 * explains an absence lives on the `Fact`, and the panel renders it there.
 *
 * Items are derived, never invented. Every item points at something the graph
 * returned, so every item can be opened.
 *
 * That last property is now carried by the item itself. An item's `target` is
 * the place following it goes: the callee for a call, the caller for a caller,
 * the raise site for an error, the focused symbol itself for the obligations
 * that are about the symbol rather than about somewhere else. Before this, the
 * target was recovered by each consumer from the facts beside the checklist,
 * which meant two surfaces could disagree about where the same item led. A
 * consumer with a fallback of its own may keep it for items whose target could
 * not be resolved; it will simply stop being reached for the ones that can.
 */

import { sha1Hex } from '../core/hash';
import type { SymbolRef } from '../core/focus-protocol';
import { toEditorRange } from '../core/positions';
import type {
    CallerRef,
    CallSite,
    ChecklistCategory,
    ChecklistItem,
    DataRef,
    KnowledgeState,
    TestRef,
    ThrowRef
} from '../core/semantic-ir';

import { toFileUri, toWorkspaceRelative } from './file-uri';

/** Facts the checklist is derived from. Plain data, so the generator stays pure. */
export interface ChecklistInput {
    /** Workspace root, used to turn engine file references into openable URIs. */
    root: string;
    /**
     * The symbol the checklist is about.
     *
     * Also the target of every item whose obligation is about the symbol rather
     * than about somewhere else: an environment value the symbol reads is not
     * declared anywhere the graph can point at, and the honest place to send a
     * reader asking about it is the line that reads it.
     */
    symbol: SymbolRef;
    calls: CallSite[];
    callers: CallerRef[];
    throws: ThrowRef[];
    envReads: DataRef[];
    typeRefs: DataRef[];
    tests: TestRef[];
    /** How much the tests family is trusted, which decides the wording of the empty case. */
    testsState: KnowledgeState;
}

/**
 * How many callers are worth listing individually.
 *
 * A symbol with ninety callers does not need ninety checklist items; it needs
 * one risk and a list view. The cap is on the checklist, never on the facts.
 */
export const MAX_CALLER_ITEMS = 10;

/** Strategy string the provider stamps on a call site whose target is a class. */
export const CONSTRUCTION_STRATEGY = 'construction';

/** The sentence shown when the tests family is empty and only inferred. */
export const NO_TEST_CALLERS_LABEL =
    'No test callers found (inferred): decide if this needs a test';

/**
 * A checklist item's identity.
 *
 * Category plus target, hashed, so the id survives rewording and reordering.
 * sha1 is the right tool here: this is a lookup key, not a security boundary,
 * and a shorter digest keeps the IR readable when it is dumped for support.
 */
export function checklistItemId(category: ChecklistCategory, target: string): string {
    return sha1Hex(`${category}|${target}`);
}

/**
 * What a `tests` item is keyed on: the test module as this repository names it.
 *
 * Exported because two things outside this file have to be able to reproduce it
 * exactly. The unit suite asserts that two workspaces holding identical content
 * at different absolute paths produce the same id, and the fixture recorder
 * checks that the ids it is about to commit are the ones the product would
 * derive for the path the fixture claims to be at. Both would otherwise carry a
 * second copy of this derivation, and a second copy is one refactor away from
 * agreeing with nothing.
 */
export function testItemKey(root: string, file: string | undefined, name: string): string {
    return `test:${toWorkspaceRelative(root, file)}:${name}`;
}

/** Build one item, deriving its id from what it points at rather than from its wording. */
function item(
    category: ChecklistCategory,
    label: string,
    targetQualifiedName: string | undefined,
    target: SymbolRef | undefined
): ChecklistItem {
    return {
        id: checklistItemId(category, targetQualifiedName ?? label),
        category,
        label,
        target,
        done: false
    };
}

/**
 * A navigable reference to one line of one file.
 *
 * Deliberately carries no `nodeId`: this is a place in a file, not a resolved
 * graph node, and claiming otherwise would make the focus pipeline report an
 * index-backed focus for something the index never resolved.
 */
function refAt(
    root: string,
    file: string | undefined,
    oneBasedLine: number | undefined,
    name: string,
    qualifiedName?: string
): SymbolRef | undefined {
    const uri = toFileUri(root, file);
    if (uri === undefined) {
        return undefined;
    }
    const range = toEditorRange(oneBasedLine ?? 1, oneBasedLine ?? 1);
    return {
        name,
        qualifiedName,
        kind: 'unknown',
        uri,
        range,
        selectionRange: { start: range.start, end: range.start }
    };
}

/**
 * Where a call leads: the declaration of its target, never its call site.
 *
 * A call to `validateUser` on line 24 of `userService.ts` is declared on line
 * 19 of `validate.ts`, and both numbers are true about different files.
 * Following the item has to arrive at the declaration, which is what the reader
 * has not read yet.
 */
function calleeTarget(root: string, call: CallSite): SymbolRef | undefined {
    return refAt(root, call.targetFile, call.targetLine ?? call.line, call.targetName, call.targetQualifiedName);
}

/** Keep the first occurrence of each key, so the engine's own order is preserved. */
function distinctBy<T>(entries: T[], keyOf: (entry: T) => string): T[] {
    const seen = new Set<string>();
    const out: T[] = [];
    for (const entry of entries) {
        const key = keyOf(entry);
        if (key.length === 0 || seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(entry);
    }
    return out;
}

/**
 * Everything a reader should confirm about one symbol.
 *
 * Pure and synchronous: the same facts always produce the same checklist, which
 * is what makes the ids worth caching against.
 */
export function generateChecklist(input: ChecklistInput): ChecklistItem[] {
    const items: ChecklistItem[] = [];
    const { root, symbol } = input;
    // Where an item goes when it is an obligation about the focused symbol
    // rather than about somewhere else, and where the ones that could resolve a
    // place of their own fall back to when the engine recorded no file.
    const here = symbol;

    // core-logic: one item per distinct callee. The reader cannot claim to know
    // what a symbol does without knowing what it delegates to.
    const callees = distinctBy(input.calls, call => call.targetQualifiedName ?? call.targetName);
    for (const call of callees) {
        items.push(item(
            'core-logic',
            `Understand the call to ${call.targetName}`,
            call.targetQualifiedName,
            calleeTarget(root, call) ?? here
        ));
    }

    // inputs: the types the symbol names. A type reference is a shape the reader
    // is assumed to know and usually does not.
    for (const type of distinctBy(input.typeRefs, ref => ref.qualifiedName ?? ref.name)) {
        items.push(item(
            'inputs',
            `Know the shape of ${type.name}`,
            type.qualifiedName,
            refAt(root, type.file, type.line, type.name, type.qualifiedName) ?? here
        ));
    }

    // implementations: a construction is a class the reader has not read yet,
    // which is a different obligation from following a function call.
    const constructed = distinctBy(
        input.calls.filter(call => call.strategy === CONSTRUCTION_STRATEGY),
        call => call.targetQualifiedName ?? call.targetName
    );
    for (const call of constructed) {
        items.push(item(
            'implementations',
            `Read the class ${call.targetName} being constructed`,
            call.targetQualifiedName,
            calleeTarget(root, call) ?? here
        ));
    }

    // callers: who breaks when this changes. Capped, because past a point the
    // answer is a risk and not a list.
    const callers = distinctBy(input.callers, caller => caller.qualifiedName ?? caller.name)
        .slice(0, MAX_CALLER_ITEMS);
    for (const caller of callers) {
        items.push(item(
            'callers',
            `See who calls this: ${caller.name}`,
            caller.qualifiedName,
            refAt(root, caller.file, caller.line, caller.name, caller.qualifiedName) ?? here
        ));
    }

    // error-handling: one item per raised type, plus the question the per-type
    // items do not answer, which is what happens to the error further up.
    const throwTypes = distinctBy(input.throws, entry => entry.type);
    for (const entry of throwTypes) {
        items.push(item(
            'error-handling',
            `Understand when ${entry.type} is raised`,
            `raises:${entry.type}`,
            refAt(root, entry.file, entry.line, entry.type) ?? here
        ));
    }
    if (throwTypes.length > 0) {
        const named = throwTypes.map(entry => entry.type).join(', ');
        // Deliberately the focused symbol and not a raise site: the question is
        // what happens to the error above this symbol, and the only place the
        // reader can start answering it from is here.
        items.push(item('error-handling', `Check how ${named} is handled upstream`, `handled:${named}`, here));
    }

    // config: an environment value is a deployment obligation, not a code one,
    // and it is the one thing on this list that fails in production only. It is
    // declared nowhere the graph can point at, so the item opens the symbol
    // that reads it.
    for (const env of distinctBy(input.envReads, ref => ref.name)) {
        items.push(item(
            'config',
            `Confirm ${env.name} is set in every environment`,
            `env:${env.name}`,
            refAt(root, env.file, env.line, env.name, env.qualifiedName) ?? here
        ));
    }

    // tests: either the tests that exist, or the honest sentence about the ones
    // that do not. The empty case is only stated when a search actually ran.
    if (input.tests.length > 0) {
        // Keyed on the test module as this repository names it, never on where
        // the repository happens to sit: see the note at the top of the file.
        for (const test of distinctBy(input.tests, entry => testItemKey(root, entry.file, entry.name))) {
            items.push(item(
                'tests',
                `Re-read the test ${test.name}`,
                testItemKey(root, test.file, test.name),
                refAt(root, test.file, test.line, test.name) ?? here
            ));
        }
    } else if (input.testsState === 'inferred' || input.testsState === 'known') {
        // "Decide if this needs a test" is a decision about the symbol, so the
        // item opens the symbol.
        items.push(item('tests', NO_TEST_CALLERS_LABEL, 'tests:none', here));
    }

    return items;
}

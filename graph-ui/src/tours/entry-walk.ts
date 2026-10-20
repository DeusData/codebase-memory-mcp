/**
 * The walk that starts where the maintainer said, and goes forward.
 *
 * The project walk answers "where do I start". This answers the other question,
 * the one somebody asks on their second day: "I know the name of the thing I
 * care about, now show me what it touches." The order is the closure's order,
 * which is breadth first over the calls the index recorded, so step one is the
 * chosen symbol and every later step is something reachable from it.
 *
 * Three rules, and the first one is the reason the mode exists.
 *
 * **There is no preamble.** The first step is the symbol the reader chose. Not
 * the configuration, not the types, not a summary of the project: those are the
 * project walk's answer to a different question, and putting them in front of a
 * chosen entry point would be telling somebody who said where they wanted to
 * start that they were wrong.
 *
 * **Every sentence is a reading of an edge.** A step says who calls what, on
 * which line, and how far out it sits. It never says what the code does; nothing
 * here has read a line of source, exactly as in the project walk's generator.
 *
 * **The bound is stated at the end, not implied by stopping.** A walk that ran
 * into its cap ends with a sentence naming the cap and the depth. A walk that
 * reached the end of the graph ends with nothing, because "you have seen it all"
 * is a claim about what the index did not record.
 */

import type { ClosureResult } from '../provider/closure';
import { TOUR_SCHEMA_VERSION } from '../core/tour-protocol';
import type { TourDocument, TourStepRecord } from '../core/tour-protocol';
import { twinLocationOf } from '../twin/twin-target';
import { capNote } from './tour-player';

/**
 * A walk plus the one sentence a walk is allowed to end with.
 *
 * The sentence is beside the document rather than inside it because it is not a
 * step and never becomes one: it describes the walk, and a document whose last
 * step was a paragraph about the walk would be a step nobody can open.
 */
export interface ActiveTour {
    /**
     * Which of the two walks this is.
     *
     * Carried rather than inferred from the strategy, because a surface asks a
     * different question than a document does: "may I offer to regenerate this"
     * and "does this walk have a chosen root" are about the mode, not about how
     * the order was derived. Reading it back out of `generated.strategy` would
     * work today and would be a second meaning attached to that field.
     */
    kind: 'project' | 'entry';
    document: TourDocument;
    /** What the card says under the last step, or empty when there is nothing to say. */
    endNote: string;
}

/** Id of the walk one chosen symbol produces. Readable, and derived from the name alone. */
export function entryTourId(qualifiedName: string): string {
    const slug = qualifiedName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
    return `entry-${slug.length > 0 ? slug : 'symbol'}`;
}

/** `1 call` or `n calls`, so a generated sentence never reads as machine output. */
function countOf(count: number, singular: string, plural: string): string {
    return `${count} ${count === 1 ? singular : plural}`;
}

/** The display name of a qualified name, for a sentence that names a caller. */
function lastSegment(qualifiedName: string | undefined): string {
    if (qualifiedName === undefined || qualifiedName.length === 0) {
        return 'the previous step';
    }
    return qualifiedName.split('.').pop() ?? qualifiedName;
}

/**
 * The forward walk over one closure, as a tour document.
 *
 * A reached symbol the index gave no file for is not a step: a step that cannot
 * be opened is a dead row, and inventing a path for it would be worse. How many
 * were left out is said in the end note rather than swallowed.
 */
export function entryWalkTour(closure: ClosureResult): ActiveTour {
    const rootName = closure.root.name;
    const outgoing = closure.edges.filter((edge) => edge.from === closure.root.qualifiedName).length;

    const taken = new Set<string>();
    const steps: TourStepRecord[] = [];
    let withoutFile = 0;

    for (const node of closure.nodes) {
        const location = twinLocationOf(node.symbol);
        if (location.path.length === 0) {
            withoutFile += 1;
            continue;
        }
        const qualifiedName = node.symbol.qualifiedName;
        const base = (qualifiedName ?? node.symbol.name).toLowerCase()
            .replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'step';
        let id = base;
        let suffix = 2;
        while (taken.has(id)) {
            id = `${base}-${suffix}`;
            suffix += 1;
        }
        taken.add(id);

        const sentences: string[] = [];
        if (node.hop === 0) {
            sentences.push(`You chose this as the way in, so the walk starts here rather than at the top of the project.`);
            sentences.push(
                outgoing === 0
                    ? 'The index records no call out of it, so there is nothing further along this direction.'
                    : `The index records ${countOf(outgoing, 'call', 'calls')} out of it, and the steps after this one are where those calls lead.`,
            );
        } else {
            const via = lastSegment(node.via);
            const edge = closure.edges.find(
                (entry) => entry.to === qualifiedName && entry.from === node.via,
            );
            const where = edge?.line === undefined ? '' : ` on line ${edge.line}`;
            sentences.push(`The index records a call from ${via} to ${node.symbol.name}${where}.`);
            sentences.push(
                `It sits ${countOf(node.hop, 'hop', 'hops')} out from where you started, in ${location.path}.`,
            );
        }
        if (qualifiedName === undefined) {
            sentences.push('The index could not resolve this name to a symbol of its own, so this step opens the place the call named.');
        }

        steps.push({
            id,
            title: node.hop === 0
                ? `Start here: ${node.symbol.name}`
                : `Hop ${node.hop}: ${node.symbol.name}`,
            description: sentences.join(' '),
            order: steps.length,
            primary: qualifiedName === undefined
                ? { kind: 'file', filePath: location.path }
                : {
                    kind: 'symbol',
                    filePath: location.path,
                    line: location.line,
                    name: node.symbol.name,
                    qualifiedName,
                    symbolKind: node.symbol.kind,
                },
        });
    }

    const notes: string[] = [];
    const cap = capNote(closure.truncated, closure.cap, closure.depth);
    if (cap.length > 0) {
        notes.push(cap);
    }
    if (withoutFile > 0) {
        notes.push(
            `${countOf(withoutFile, 'reached symbol carries', 'reached symbols carry')} no file in the index, `
            + 'so there is nothing to open for it and it is not a step',
        );
    }

    return {
        kind: 'entry',
        document: {
            schemaVersion: TOUR_SCHEMA_VERSION,
            id: entryTourId(closure.root.qualifiedName ?? rootName),
            title: `Forward from ${rootName}`,
            generated: {
                strategy: 'forward-walk',
                ...(closure.truncated ? { truncated: true } : {}),
                edgeCount: closure.edges.length,
                brokenEdges: [],
            },
            steps,
        },
        endNote: notes.join('. '),
    };
}

import { useMemo, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';
import { galaxyPathText as text } from './galaxy-strings';
import { graphNodeName, graphNodeTitle, nodeFilePath } from './node-names';
import type { ScopePathStep } from './scope-path';
import type { GraphNode } from './types';
import { FitLabel } from './toolbar-fit';

const PICKER_LIMIT = 40;

function closeOnEscape(event: KeyboardEvent<HTMLDetailsElement>) {
    if (event.key !== 'Escape') return;
    event.stopPropagation(); event.currentTarget.open = false;
    event.currentTarget.querySelector('summary')?.focus();
}

/** A searchable list of the nodes already loaded in this scope; nothing is fetched. */
export function PathPicker({ nodes, onPick }: { nodes: readonly GraphNode[]; onPick: (node: GraphNode) => void }) {
    const [query, setQuery] = useState('');
    const details = useRef<HTMLDetailsElement>(null);
    const matches = useMemo(() => {
        const needle = query.trim().toLocaleLowerCase();
        // Name matches first, as in the node search: exact, prefix, anywhere, then path or qualified name.
        // A Branch node is found by its shown name and by its name in the index (round 4, N1).
        const rank = (node: GraphNode) => Math.min(...[graphNodeName(node), node.name].map(value => {
            const name = value.toLocaleLowerCase();
            return name === needle ? 0 : name.startsWith(needle) ? 1 : name.includes(needle) ? 2 : 3;
        }));
        return nodes.filter(node => !needle || [graphNodeName(node), node.name, node.qualified_name, nodeFilePath(node)]
            .some(value => value?.toLocaleLowerCase().includes(needle)))
            .sort((a, b) => rank(a) - rank(b) || graphNodeName(a).localeCompare(graphNodeName(b)) || a.id - b.id);
    }, [nodes, query]);
    return <details ref={details} className="atlas-graph-path-picker" onKeyDown={closeOnEscape}>
        <summary title={text.pathToTitle}><FitLabel wide={text.pathTo} narrow={text.pathToNarrow} /></summary>
        <div className="atlas-graph-path-menu" role="group" aria-label={text.pathToTitle}>
            <input type="search" aria-label={text.pathSearch} placeholder={text.pathSearchPlaceholder}
                value={query} onChange={event => setQuery(event.target.value)} />
            <ul>{matches.slice(0, PICKER_LIMIT).map(node => <li key={node.id}>
                <button type="button" onClick={() => {
                    onPick(node);
                    if (details.current) details.current.open = false;
                }} title={graphNodeTitle(node)}><strong>{graphNodeName(node)}</strong><span>{nodeFilePath(node) ?? node.qualified_name ?? node.label}</span></button>
            </li>)}</ul>
            {matches.length === 0 && <small>{text.pathNoMatch}</small>}
            {matches.length > PICKER_LIMIT && <small>{text.pathMore(matches.length - PICKER_LIMIT)}</small>}
        </div>
    </details>;
}

/** The compact step list: one line per hop or call site, stepping moves the highlight. */
export function PathSteps({ heading, note, steps, active, nameOf, lines, onStep, onClear }: {
    heading: string;
    note?: string;
    steps: readonly ScopePathStep[];
    active: number;
    nameOf: (id: number) => string;
    /** Call order shows the call-site line; a path shows the hop. */
    lines: boolean;
    onStep: (index: number) => void;
    onClear: () => void;
}) {
    return <section className="atlas-galaxy-path-panel" aria-label={text.panel} data-testid="atlas-galaxy-path-panel">
        <header>
            <strong>{heading}</strong>
            <button type="button" title={text.clearTitle} onClick={onClear}>{text.clear}</button>
        </header>
        {note && <p>{note}</p>}
        {steps.length > 0 && <>
            <ol>{steps.map((step, index) => <li key={`${index}:${step.edge.id ?? ''}`}>
                <button type="button" data-active={index === active} aria-current={index === active ? 'step' : undefined}
                    onClick={() => onStep(index)}>
                    <span className="atlas-galaxy-path-hop">{lines
                        ? step.edge.line === undefined ? text.lineUnknown : text.line(step.edge.line)
                        : text.hop(index + 1)}</span>
                    <code>{text.step(nameOf(step.from), step.edge.type, nameOf(step.to), step.edge.source === step.from)}</code>
                </button>
            </li>)}</ol>
            <footer>
                <button type="button" disabled={active <= 0} onClick={() => onStep(active - 1)}>{text.previous}</button>
                <span>{text.position(active + 1, steps.length)}</span>
                <button type="button" disabled={active >= steps.length - 1} onClick={() => onStep(active + 1)}>{text.next}</button>
            </footer>
        </>}
    </section>;
}

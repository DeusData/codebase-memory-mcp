import { useEffect, useState, type ReactNode } from 'react';

import type { SymbolRef } from '../core/focus-protocol';
import type { Evidence, SemanticIR } from '../core/semantic-ir';
import type { ImportsGroup } from '../pseudocode/imports-group';
import { SectionView, type TwinStatus } from './TwinPanel';
import {
    buildSections, evidenceFor, importsSection, runtimeSection,
    type TwinRow, type TwinSection,
} from './twin-view-model';
import './selected-code.css';

export interface SelectedCodeRange {
    /** Editor coordinates, 1-based. Text is the literal selection, unmodified. */
    startLine: number;
    startColumn: number;
    endLine: number;
    endColumn: number;
    text: string;
}

export interface SelectedCodePanelProps {
    filePath: string;
    symbol?: SymbolRef;
    ir?: SemanticIR;
    status: TwinStatus;
    message?: string;
    selection?: SelectedCodeRange;
    fileSymbols?: readonly SymbolRef[];
    fileSymbolsStatus?: 'loading' | 'ready' | 'error';
    fileSymbolsMessage?: string;
    coverageNote?: string;
    pinned: boolean;
    onTogglePin: () => void;
    onAsk: () => void;
    onShowGraph: () => void;
    onFollow: (symbol: SymbolRef) => void;
    onPointRow?: (row: TwinRow | undefined) => void;
    onOpenFlow?: () => void;
    callOutline?: ReactNode;
    imports?: ImportsGroup;
}

const SECTION_TITLES: Readonly<Record<string, string>> = {
    purpose: 'Indexed summary',
    steps: 'Outgoing calls',
    callers: 'Called by',
    state: 'References and environment',
    errors: 'Error types',
    effects: 'External effects',
    tests: 'Related tests',
    risks: 'Structural observations',
    runtime: 'Recorded runtime calls',
    imports: 'File imports',
};

function unavailableText(section: TwinSection): string | undefined {
    if (section.state === 'notIndexed') return 'This information is not indexed yet.';
    if (section.state === 'unsupported') return 'The current index does not provide this information.';
    if (section.state === 'unknown') return 'This information is unavailable from the current index.';
    return undefined;
}

function sectionSummary(section: TwinSection): string {
    if (section.rows.length > 0) return String(section.rows.length);
    if (unavailableText(section)) return 'Unavailable';
    if (section.text) return '';
    return section.state === 'ambiguous' ? 'Unresolved' : 'None found';
}

/** Reuse evidence-bearing rows without the legacy reader levels or model controls. */
function FactDisclosure(props: {
    section: TwinSection;
    openEvidence?: string;
    entriesFor: (path: string) => Evidence[];
    onToggleEvidence: (path: string) => void;
    onFollow: (symbol: SymbolRef) => void;
    onPointRow?: (row: TwinRow | undefined) => void;
}) {
    const title = SECTION_TITLES[props.section.name] ?? props.section.title;
    const unavailable = unavailableText(props.section);
    const section: TwinSection = {
        ...props.section,
        title,
        // Missing answers must never inherit a checked-empty sentence.
        emptyText: unavailable ?? props.section.emptyText,
        ...(props.section.name === 'tests'
            ? { note: 'Links inferred from indexed callers; these are not test results or measured coverage.' }
            : {}),
    };
    return (
        <details className="selected-code-fact" data-section={section.name}>
            <summary>
                <span>{title}</span>
                <span className="selected-code-count">{sectionSummary(section)}</span>
            </summary>
            <SectionView
                section={section}
                voiceOf={() => undefined}
                openEvidence={props.openEvidence}
                entriesFor={props.entriesFor}
                onToggleEvidence={props.onToggleEvidence}
                onActivate={(row) => { if (row.target) props.onFollow(row.target); }}
                onPoint={props.onPointRow}
            />
        </details>
    );
}

export default function SelectedCodePanel(props: SelectedCodePanelProps) {
    const { filePath, symbol, selection, ir, imports } = props;
    const [openEvidence, setOpenEvidence] = useState<string>();
    const identity = `${filePath}:${symbol?.qualifiedName ?? symbol?.name ?? ''}:${ir?.generation ?? ''}`;
    useEffect(() => setOpenEvidence(undefined), [identity]);
    const ready = props.status === 'ready' && ir !== undefined && symbol !== undefined;
    const sections = ready ? buildSections(ir).filter((section) =>
        section.name !== 'purpose' && (section.name !== 'effects' || section.populated)
        && (section.name !== 'risks' || section.populated)) : [];
    const runtime = ready && ir.runtime && ir.runtime.value.length > 0 ? runtimeSection(ir) : undefined;
    if (runtime) sections.push(runtime);
    if (imports) sections.push(importsSection(imports));
    const entriesFor = (path: string): Evidence[] =>
        imports?.entries.find((entry) => entry.factPath === path)?.evidence
        ?? (ready ? evidenceFor(ir, path) : []);
    const hasFile = filePath.length > 0;
    const range = symbol
        ? `${symbol.range.start.line + 1}-${Math.max(symbol.range.start.line + 1, symbol.range.end.line + (symbol.range.end.character > 0 ? 1 : 0))}`
        : undefined;
    const noSymbolMessage = props.status === 'loading' ? 'Reading selected code...'
        : props.status === 'failed' ? (props.message || 'Could not read symbol details.')
        : props.status === 'not-indexed' ? (props.message || 'This file is not indexed yet.')
        : undefined;
    return (
        <section className="selected-code" aria-label="Selected code" data-testid="selected-code-panel" data-pinned={props.pinned}>
            <header className="selected-code-header">
                <h2>Selected code</h2>
                <button type="button" className="selected-code-pin" disabled={!hasFile || (!props.pinned && (props.status === 'loading' || props.fileSymbolsStatus === 'loading'))}
                    aria-pressed={props.pinned} onClick={props.onTogglePin}
                    title={props.pinned ? 'Follow the current editor selection' : 'Keep this context while exploring elsewhere'}>
                    {props.pinned ? 'Pinned' : 'Pin'}
                </button>
            </header>
            {!hasFile ? (
                <p className="selected-code-message">Open a file to inspect its symbols and connections.</p>
            ) : (
                <>
                    <div className="selected-code-identity">
                        {symbol && <div className="selected-code-symbol"><strong>{symbol.name}</strong><span>{symbol.kind}</span></div>}
                        <div className="selected-code-path" title={filePath}>{filePath}{range && <span>:{range}</span>}</div>
                        {props.pinned && <p className="selected-code-note">Context pinned. Unpin to follow the editor.</p>}
                        {ready && ir.signature?.value && <code className="selected-code-signature">{ir.signature.value}</code>}
                    </div>
                    {selection && selection.text.length > 0 && (
                        <div className="selected-code-selection" data-testid="selected-code-selection">
                            <details>
                                <summary>Exact selection <span>{selection.startLine}:{selection.startColumn} to {selection.endLine}:{selection.endColumn}</span></summary>
                                <pre><code>{selection.text}</code></pre>
                            </details>
                            <p className="selected-code-note">Chat receives the marked text.{symbol ? ` Relationships below describe ${symbol.name}.` : ' File connections remain separate from the selection.'}</p>
                        </div>
                    )}
                    <div className="selected-code-actions">
                        <button type="button" className="selected-code-ask" onClick={props.onAsk}>Ask about this</button>
                        <button type="button" onClick={props.onShowGraph}>Show in graph</button>
                    </div>
                    {noSymbolMessage && <p className="selected-code-message" role="status">{noSymbolMessage}</p>}
                    {!symbol && (
                        <div className="selected-code-file" data-testid="selected-code-file">
                            <h3>Symbols in this file</h3>
                            {props.fileSymbolsStatus === 'loading' ? <p className="selected-code-note">Loading symbols...</p>
                                : props.fileSymbolsStatus === 'error' ? <p className="selected-code-note" role="status">{props.fileSymbolsMessage || 'Could not load file symbols.'}</p>
                                : props.fileSymbols && props.fileSymbols.length > 0 ? (
                                    <ul>{props.fileSymbols.map((entry) => <li key={`${entry.qualifiedName ?? entry.name}:${entry.range.start.line}`}>
                                        <button type="button" onClick={() => props.onFollow(entry)}>
                                            <span>{entry.name}</span><span>{entry.kind} · {entry.range.start.line + 1}</span>
                                        </button>
                                    </li>)}</ul>
                                ) : <p className="selected-code-note">{props.fileSymbolsStatus === 'ready' ? 'No symbols found in the current index.' : 'Select code in the reader to inspect it.'}</p>}
                            {props.fileSymbolsMessage && props.fileSymbolsStatus !== 'error' && <p className="selected-code-note">{props.fileSymbolsMessage}</p>}
                        </div>
                    )}
                    <div className="selected-code-facts" key={identity}>
                        {sections.map((section) => <FactDisclosure key={section.name} section={section}
                            openEvidence={openEvidence} entriesFor={entriesFor}
                            onToggleEvidence={(path) => setOpenEvidence((current) => current === path ? undefined : path)}
                            onFollow={props.onFollow} onPointRow={props.onPointRow} />)}
                        {ready && (props.callOutline || props.onOpenFlow) && (
                            <details className="selected-code-fact" data-section="outline">
                                <summary><span>Call outline</span></summary>
                                <p className="selected-code-note">Indexed call sequence, not complete control flow.</p>
                                {props.onOpenFlow && <button type="button" className="selected-code-flow" onClick={props.onOpenFlow}>Open flow view</button>}
                                {props.callOutline}
                            </details>
                        )}
                        {ready && (ir.writes.state === 'unsupported' || ir.externalEffects.state === 'unsupported') && (
                            <details className="selected-code-fact" data-section="limits">
                                <summary><span>Index limits</span></summary>
                                <p className="selected-code-note">{[ir.writes.state === 'unsupported' ? 'Assignments and writes are not tracked.' : '', ir.externalEffects.state === 'unsupported' ? 'External side effects are not tracked.' : ''].filter(Boolean).join(' ')} Absence of a relationship is not proof that it cannot occur.</p>
                            </details>
                        )}
                    </div>
                    {props.coverageNote && <p className="selected-code-note selected-code-coverage">{props.coverageNote}</p>}
                </>
            )}
        </section>
    );
}

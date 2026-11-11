/**
 * The Bug Wizard: the way in the index expects, the way a recording took, and
 * the difference between them.
 *
 * Four steps, in this order, and none of them optional. The anatomy is the
 * reference project's (CodeAtlasIDE,
 * codeatlas-views/src/browser/traces/bug-wizard-widget.tsx) and so is the reason
 * for each one:
 *
 * **Choose the symbol.** Adopted from whatever the twin is showing, because
 * somebody who opens this panel is already looking at the function that
 * misbehaved. Changed through the same search the rest of the surface uses, so
 * there is one answer to "which symbol is this about" rather than two that can
 * drift apart.
 *
 * **The expected path.** Upstream chains from the index, labelled as a reading
 * of the index and never as a claim that any of it ran. A walk that stopped at
 * its bound says so, because a chain that was cut looks exactly like a chain
 * that ended.
 *
 * **The observed path.** What the analysis backend hands back about those calls,
 * with the count, the run label and when it was last seen. With nothing to hand
 * back this is the honest empty state: the sentence that claims nothing, the
 * command that feeds a run in, the format of one, and the one sentence about
 * what this backend can and cannot answer afterwards.
 *
 * **Where they differ.** Two lists, never one number, with their own headings
 * and their own sentences. Both empty is its own sentence too.
 *
 * Purely presentational, like the other overlays here: what to read is App.tsx's
 * business, what the lists mean is bug-paths.ts's, and this file draws them.
 *
 * ## What a click does, and what the panel does not do
 *
 * Every name here is a name and never a stored reference; the click hands the
 * hop back and the resolution happens then, against this index. And the wizard
 * publishes nothing on its own: a reader comparing two paths has not asked the
 * reader, the twin and the galaxy to retarget under them. Only an explicit click
 * on a hop navigates.
 */

import type { JSX } from 'react';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import type { BugPathEdge, BugPathNode, BugPathsDto } from './bug-paths';
import {
    BUG_WIZARD_BUSY,
    BUG_WIZARD_CHANGE_TARGET,
    BUG_WIZARD_CHANGE_TOOLTIP,
    BUG_WIZARD_CLOSE,
    BUG_WIZARD_EDGE_IN_INDEX,
    BUG_WIZARD_EDGE_NOT_IN_INDEX,
    BUG_WIZARD_EDGE_UNASKED,
    BUG_WIZARD_EDGE_VERB,
    BUG_WIZARD_ENTRY_BADGE,
    BUG_WIZARD_ENTRY_TOOLTIP,
    BUG_WIZARD_FAILED,
    BUG_WIZARD_NO_DIVERGENCE,
    BUG_WIZARD_NO_PROJECT,
    BUG_WIZARD_NO_TARGET,
    BUG_WIZARD_NO_TRACES,
    BUG_WIZARD_NO_TRACES_FORMAT,
    BUG_WIZARD_NO_TRACES_HOW,
    BUG_WIZARD_NO_TRACES_WHERE,
    BUG_WIZARD_OBSERVED_NOTE,
    BUG_WIZARD_RUNTIME_ONLY_LABEL,
    BUG_WIZARD_RUNTIME_ONLY_NOTE,
    BUG_WIZARD_STATIC_EMPTY,
    BUG_WIZARD_STATIC_NOTE,
    BUG_WIZARD_STATIC_ONLY_LABEL,
    BUG_WIZARD_STATIC_ONLY_NOTE,
    BUG_WIZARD_STATIC_ORIGIN,
    BUG_WIZARD_STEP_DIVERGENCE,
    BUG_WIZARD_STEP_OBSERVED,
    BUG_WIZARD_STEP_STATIC,
    BUG_WIZARD_STEP_TARGET,
    BUG_WIZARD_SUBLINE,
    BUG_WIZARD_TITLE,
    bugWizardEdgeLabel,
    bugWizardFlowsCapped,
    bugWizardHopTooltip,
    bugWizardIngestCommand,
    bugWizardLastSeen,
    bugWizardObservedCount,
    bugWizardTruncated,
} from './bug-wizard-strings';

/** What the panel is doing. `idle` is before anything was asked. */
export type BugWizardStatus = 'idle' | 'loading' | 'ready' | 'failed';

export interface BugWizardProps {
    /** The project the reading is about, named so the command below is usable. */
    project: string;
    /** The symbol the wizard is about, adopted from the twin. */
    target?: BugPathNode;
    paths?: BugPathsDto;
    status: BugWizardStatus;
    /** An honest extra line: the failure, in the words the reading used. */
    message: string;
    onHop: (hop: BugPathNode) => void;
    onChangeTarget: () => void;
    onClose: () => void;
}

function Hop(props: { node: BugPathNode; onHop: (hop: BugPathNode) => void }): JSX.Element {
    const observed = props.node.observed;
    return (
        <li
            className="atlas-bugwizard-hop"
            data-testid={observed === undefined ? 'atlas-bugwizard-hop' : 'atlas-bugwizard-observed-hop'}
            data-name={props.node.name}
            {...(observed === undefined
                ? {}
                : {
                    'data-count': String(observed.count),
                    'data-label': observed.label,
                    'data-last-seen': observed.lastSeen,
                })}
        >
            <Hint name="bug-hop" text={bugWizardHopTooltip(props.node.name)}>
                <button
                    type="button"
                    className="atlas-bugwizard-link"
                    onClick={() => props.onHop(props.node)}
                >
                    {props.node.name}
                </button>
            </Hint>
            {props.node.entryPoint === true && (
                <Hint name="bug-entry" text={BUG_WIZARD_ENTRY_TOOLTIP}>
                    <span
                        className="atlas-bugwizard-badge"
                        data-testid="atlas-bugwizard-entry"
                    >
                        {BUG_WIZARD_ENTRY_BADGE}
                    </span>
                </Hint>
            )}
            {observed !== undefined && (
                <Hint name="bug-observed" text={bugWizardLastSeen(observed.lastSeen)}>
                    <span className="atlas-bugwizard-observed">
                        {bugWizardObservedCount(observed.count, observed.label)}
                    </span>
                </Hint>
            )}
        </li>
    );
}

function Chain(props: {
    kind: 'static' | 'observed';
    chain: readonly BugPathNode[];
    onHop: (hop: BugPathNode) => void;
}): JSX.Element {
    return (
        <ol
            className="atlas-bugwizard-chain"
            data-testid={`atlas-bugwizard-${props.kind}-chain`}
            data-length={props.chain.length}
        >
            {props.chain.map((node, position) => (
                <Hop key={`${position}:${node.filePath ?? ''}:${node.name}`} node={node} onHop={props.onHop} />
            ))}
        </ol>
    );
}

/** Which of the three things the index said about one observed call. */
function edgeVerdict(edge: BugPathEdge): string {
    if (edge.indexRecordsCall === undefined) {
        return BUG_WIZARD_EDGE_UNASKED;
    }
    return edge.indexRecordsCall ? BUG_WIZARD_EDGE_IN_INDEX : BUG_WIZARD_EDGE_NOT_IN_INDEX;
}

function Edges(props: {
    kind: 'static-only' | 'runtime-only';
    title: string;
    note: string;
    edges: readonly BugPathEdge[];
    onHop: (hop: BugPathNode) => void;
}): JSX.Element {
    return (
        <div
            className={`atlas-bugwizard-edges atlas-bugwizard-${props.kind}`}
            data-testid={`atlas-bugwizard-${props.kind}`}
            data-count={props.edges.length}
        >
            <span className="atlas-bugwizard-eyebrow">{props.title}</span>
            <p className="atlas-bugwizard-note">{props.note}</p>
            <ul>
                {props.edges.map((edge, index) => (
                    <li
                        key={`${index}:${edge.from.name}:${edge.to.name}`}
                        className="atlas-bugwizard-edge"
                        data-testid="atlas-bugwizard-edge"
                        data-kind={props.kind}
                        data-from={edge.from.name}
                        data-to={edge.to.name}
                        aria-label={bugWizardEdgeLabel(edge.from.name, edge.to.name)}
                    >
                        <Hint name="bug-edge-from" text={bugWizardHopTooltip(edge.from.name)}>
                            <button
                                type="button"
                                className="atlas-bugwizard-link"
                                data-testid="atlas-bugwizard-edge-from"
                                data-name={edge.from.name}
                                onClick={() => props.onHop(edge.from)}
                            >
                                {edge.from.name}
                            </button>
                        </Hint>
                        <span className="atlas-bugwizard-edge-verb">{BUG_WIZARD_EDGE_VERB}</span>
                        <Hint name="bug-edge-to" text={bugWizardHopTooltip(edge.to.name)}>
                            <button
                                type="button"
                                className="atlas-bugwizard-link"
                                data-testid="atlas-bugwizard-edge-to"
                                data-name={edge.to.name}
                                onClick={() => props.onHop(edge.to)}
                            >
                                {edge.to.name}
                            </button>
                        </Hint>
                        {edge.observed !== undefined && (
                            <span className="atlas-bugwizard-observed">
                                {bugWizardObservedCount(edge.observed.count, edge.observed.label)}
                            </span>
                        )}
                        {props.kind === 'runtime-only' && (
                            <span className="atlas-bugwizard-verdict" data-testid="atlas-bugwizard-edge-verdict">
                                {edgeVerdict(edge)}
                            </span>
                        )}
                    </li>
                ))}
            </ul>
        </div>
    );
}

function Step(props: { index: number; name: string; title: string; children: JSX.Element }): JSX.Element {
    return (
        <section
            className="atlas-bugwizard-step"
            data-testid={`atlas-bugwizard-${props.name}`}
            data-step={props.index}
        >
            <header className="atlas-bugwizard-step-head">
                <span className="atlas-bugwizard-step-number">{props.index}</span>
                <span className="atlas-bugwizard-eyebrow">{props.title}</span>
            </header>
            {props.children}
        </section>
    );
}

export default function BugWizard(props: BugWizardProps): JSX.Element {
    const paths = props.paths;
    const target = props.target;
    const staticPaths = paths?.staticPaths ?? [];
    const observedPaths = paths?.observedPaths ?? [];
    const staticOnly = paths?.staticOnly ?? [];
    const runtimeOnly = paths?.runtimeOnly ?? [];
    const events = paths?.observedEvents ?? 0;
    const noDivergence = paths !== undefined && staticOnly.length === 0 && runtimeOnly.length === 0;

    return (
        <div
            className="atlas-bugwizard"
            data-testid="atlas-bugwizard"
            role="dialog"
            aria-label={BUG_WIZARD_TITLE}
            data-target={target?.name ?? ''}
            data-status={props.status}
            data-events={events}
            data-static-paths={staticPaths.length}
            data-observed-paths={observedPaths.length}
            data-static-only={staticOnly.length}
            data-runtime-only={runtimeOnly.length}
        >
            <div className="atlas-bugwizard-inner">
                <header className="atlas-bugwizard-head">
                    <h2 className="atlas-bugwizard-title">{BUG_WIZARD_TITLE}</h2>
                    <button
                        type="button"
                        className="atlas-bugwizard-close"
                        data-testid="atlas-bugwizard-close"
                        aria-label={messages.wizard.closeLabel}
                        onClick={props.onClose}
                    >
                        {BUG_WIZARD_CLOSE}
                    </button>
                </header>
                <p className="atlas-bugwizard-subline">{BUG_WIZARD_SUBLINE}</p>

                <Step index={1} name="target" title={BUG_WIZARD_STEP_TARGET}>
                    <div className="atlas-bugwizard-target">
                        {props.project.length === 0 ? (
                            <p className="atlas-bugwizard-empty" data-testid="atlas-bugwizard-no-project">
                                {BUG_WIZARD_NO_PROJECT}
                            </p>
                        ) : target === undefined ? (
                            <p className="atlas-bugwizard-empty" data-testid="atlas-bugwizard-no-target">
                                {BUG_WIZARD_NO_TARGET}
                            </p>
                        ) : (
                            <p className="atlas-bugwizard-target-name" data-testid="atlas-bugwizard-target">
                                <span className="atlas-bugwizard-symbol">{target.name}</span>
                                {target.qualifiedName !== undefined && (
                                    <span className="atlas-bugwizard-qualified">{target.qualifiedName}</span>
                                )}
                            </p>
                        )}
                        <Hint name="bug-change-target" text={BUG_WIZARD_CHANGE_TOOLTIP}>
                            <button
                                type="button"
                                className="atlas-bugwizard-button"
                                data-testid="atlas-bugwizard-change"
                                onClick={props.onChangeTarget}
                            >
                                {BUG_WIZARD_CHANGE_TARGET}
                            </button>
                        </Hint>
                        {props.status === 'loading' && (
                            <p className="atlas-bugwizard-busy" data-testid="atlas-bugwizard-busy">
                                {BUG_WIZARD_BUSY}
                            </p>
                        )}
                        {props.status === 'failed' && (
                            <p className="atlas-bugwizard-alarm" data-testid="atlas-bugwizard-failed">
                                {BUG_WIZARD_FAILED}
                                {props.message.length > 0 ? `: ${props.message}` : ''}
                            </p>
                        )}
                        {/*
                          * Eine Meldung, die keine Absage der ganzen Lesung ist:
                          * ein Hop, den der Index nicht aufloest, oeffnet nichts,
                          * und dass nichts passiert ist, muss dastehen. Still zu
                          * bleiben waere ein Klick, der ins Leere geht, ohne dass
                          * jemand sagt warum.
                          */}
                        {props.status !== 'failed' && props.message.length > 0 && (
                            <p className="atlas-bugwizard-hint" data-testid="atlas-bugwizard-message">
                                {props.message}
                            </p>
                        )}
                    </div>
                </Step>

                <Step index={2} name="static" title={BUG_WIZARD_STEP_STATIC}>
                    <div className="atlas-bugwizard-paths" data-testid="atlas-bugwizard-static-body">
                        <p className="atlas-bugwizard-note">
                            <span className="atlas-bugwizard-origin" data-testid="atlas-bugwizard-static-origin">
                                {BUG_WIZARD_STATIC_ORIGIN}
                            </span>
                            {' '}
                            {BUG_WIZARD_STATIC_NOTE}
                        </p>
                        {/*
                          * Nothing at all before the first answer, and a sentence
                          * after it. An empty paragraph while the walk is running
                          * would read as "there is no way in", which is a finding
                          * this panel has not made yet.
                          */}
                        {staticPaths.length === 0 && paths !== undefined && (
                            <p className="atlas-bugwizard-empty" data-testid="atlas-bugwizard-static-empty">
                                {BUG_WIZARD_STATIC_EMPTY}
                            </p>
                        )}
                        {staticPaths.map((chain, index) => (
                            <Chain key={`static-${index}`} kind="static" chain={chain} onHop={props.onHop} />
                        ))}
                        {paths?.truncated === true && (
                            <p className="atlas-bugwizard-note" data-testid="atlas-bugwizard-truncated">
                                {bugWizardTruncated(paths.depth, paths.chains)}
                            </p>
                        )}
                    </div>
                </Step>

                <Step index={3} name="observed" title={BUG_WIZARD_STEP_OBSERVED}>
                    <div className="atlas-bugwizard-paths" data-testid="atlas-bugwizard-observed-body">
                        {events === 0 ? (
                            <div className="atlas-bugwizard-notraces" data-testid="atlas-bugwizard-no-traces">
                                <p className="atlas-bugwizard-empty" data-testid="atlas-bugwizard-no-traces-message">
                                    {BUG_WIZARD_NO_TRACES}
                                </p>
                                <p className="atlas-bugwizard-hint" data-testid="atlas-bugwizard-no-traces-how">
                                    {BUG_WIZARD_NO_TRACES_HOW}
                                </p>
                                <pre className="atlas-bugwizard-command" data-testid="atlas-bugwizard-no-traces-command">
                                    {bugWizardIngestCommand(props.project)}
                                </pre>
                                <p className="atlas-bugwizard-hint" data-testid="atlas-bugwizard-no-traces-format">
                                    {BUG_WIZARD_NO_TRACES_FORMAT}
                                </p>
                                <p className="atlas-bugwizard-hint" data-testid="atlas-bugwizard-no-traces-where">
                                    {BUG_WIZARD_NO_TRACES_WHERE}
                                </p>
                            </div>
                        ) : (
                            <>
                                <p className="atlas-bugwizard-note">{BUG_WIZARD_OBSERVED_NOTE}</p>
                                {observedPaths.map((chain, index) => (
                                    <Chain
                                        key={`observed-${index}`}
                                        kind="observed"
                                        chain={chain}
                                        onHop={props.onHop}
                                    />
                                ))}
                                {paths?.flowsTruncated === true && (
                                    <p className="atlas-bugwizard-note" data-testid="atlas-bugwizard-flows-capped">
                                        {bugWizardFlowsCapped(paths.flowsRead)}
                                    </p>
                                )}
                            </>
                        )}
                    </div>
                </Step>

                <Step index={4} name="divergence" title={BUG_WIZARD_STEP_DIVERGENCE}>
                    <div className="atlas-bugwizard-divergence" data-testid="atlas-bugwizard-divergence-body">
                        {staticOnly.length > 0 && (
                            <Edges
                                kind="static-only"
                                title={BUG_WIZARD_STATIC_ONLY_LABEL}
                                note={BUG_WIZARD_STATIC_ONLY_NOTE}
                                edges={staticOnly}
                                onHop={props.onHop}
                            />
                        )}
                        {runtimeOnly.length > 0 && (
                            <Edges
                                kind="runtime-only"
                                title={BUG_WIZARD_RUNTIME_ONLY_LABEL}
                                note={BUG_WIZARD_RUNTIME_ONLY_NOTE}
                                edges={runtimeOnly}
                                onHop={props.onHop}
                            />
                        )}
                        {noDivergence && (
                            <p className="atlas-bugwizard-empty" data-testid="atlas-bugwizard-no-divergence">
                                {BUG_WIZARD_NO_DIVERGENCE}
                            </p>
                        )}
                    </div>
                </Step>
            </div>
        </div>
    );
}
